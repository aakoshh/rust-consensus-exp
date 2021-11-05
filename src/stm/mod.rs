use std::any::Any;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::thread::Thread;
use std::time::Duration;
use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc, RwLock},
};
use std::{mem, thread};

mod queues;

pub enum STMError {
    /// The transaction failed because a value changed.
    /// It can be retried straight away.
    Failure,
    /// Retry was called and now the transaction has
    /// to wait until at least one of the variables it
    /// read have changed, before being retried.
    Retry,
}

type STMResult<T> = Result<T, STMError>;

/// Unique ID for a `TVar`.
type ID = u64;

/// MVCC version.
type Version = u64;

/// Get and increment the vector clock.
fn next_version() -> Version {
    static VERSION: AtomicU64 = AtomicU64::new(0);
    VERSION.fetch_add(1, Ordering::SeqCst)
}

/// The value can be read by many threads, so it has to be tracked by an `Arc`.
/// Keeping it dynamic becuase trying to make `LVar` generic turned out to be
/// a bit of a nightmare.
type DynValue = Arc<dyn Any + Send + Sync>;

/// A versioned value. It will only be accessed through a transaction and a `TVar`.
#[derive(Clone)]
struct VVar {
    version: Version,
    value: DynValue,
}

impl VVar {
    /// Perform a downcast on a var. Returns an `Arc` that tracks when that variable
    /// will go out of scope. This avoids cloning on reads, if the value needs to be
    /// mutated then it can be cloned after being read.
    fn downcast<T: Any + Sync + Send>(&self) -> Arc<T> {
        match self.value.clone().downcast::<T>() {
            Ok(s) => s,
            Err(_) => unreachable!("TVar has wrong type"),
        }
    }
}

/// Sync variable, hold the committed value and the waiting threads.
struct SVar {
    vvar: RwLock<VVar>,
    /// Threads with their notifications flags that wait for the `TVar` to get an update.
    queue: Mutex<Vec<(Thread, Arc<AtomicBool>)>>,
}

/// A variable in the transaction log that remembers if it has been read and/or written to.
#[derive(Clone)]
struct LVar {
    // Hold on the original that we need to commit to.
    svar: Arc<SVar>,
    // Hold on to the value as it was read or written for MVCC comparison.
    vvar: VVar,
    /// Remember reads; these are the variables we need to watch if we retry.
    read: bool,
    /// Remember writes; these are the variables that need to be stored at the
    /// end of the transaction, but they don't need to be watched if we retry.
    write: bool,
}

/// `TVar` is our handle to a variable, but reading and writing go through a transaction.
/// It also tracks which threads are waiting on it.
#[derive(Clone)]
pub struct TVar<T> {
    id: ID,
    svar: Arc<SVar>,
    phantom: PhantomData<T>,
}

impl<T: Any + Sync + Send + Clone> TVar<T> {
    /// Create a new `TVar`. The initial version is 0, so that if a
    /// `TVar` is created in the middle of a transaction it will
    /// not cause any MVCC conflict during the commit.
    pub fn new(value: T) -> TVar<T> {
        // This is shared between all `TVar`s.
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        TVar {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            svar: Arc::new(SVar {
                vvar: RwLock::new(VVar {
                    version: Default::default(),
                    value: Arc::new(value),
                }),
                queue: Mutex::new(Vec::new()),
            }),
            phantom: PhantomData,
        }
    }

    pub fn read_clone(&self, tx: &mut Transaction) -> STMResult<T> {
        tx.read_tvar(self).map(|r| r.as_ref().clone())
    }

    pub fn read(&self, tx: &mut Transaction) -> STMResult<Arc<T>> {
        tx.read_tvar(self)
    }

    pub fn write(&self, tx: &mut Transaction, value: T) -> STMResult<()> {
        tx.write_tvar(self, value)
    }

    pub fn update<F>(&self, tx: &mut Transaction, f: F) -> STMResult<()>
    where
        F: FnOnce(&T) -> T,
    {
        let v = self.read(tx)?;
        self.write(tx, f(v.as_ref()))
    }
}

pub fn retry<T>() -> STMResult<T> {
    Err(STMError::Retry)
}

pub fn guard(cond: bool) -> STMResult<()> {
    if cond {
        Ok(())
    } else {
        retry()
    }
}

/// Create a new transaction and run `f` until it retunrs a successful result and
/// can be committed without running into version conflicts. Make sure `f` is free
/// of any side effects.
pub fn atomically<F, T>(f: F) -> T
where
    F: Fn(&mut Transaction) -> STMResult<T>,
{
    loop {
        let mut tx = Transaction::new();
        match f(&mut tx) {
            Ok(value) => {
                if tx.commit() {
                    tx.notify();
                    return value;
                }
            }
            Err(STMError::Failure) => {
                // We can retry straight awy.
            }
            Err(STMError::Retry) => {
                // Block this thread until there's a change.
                tx.wait()
            }
        }
    }
}

#[derive(Clone)]
pub struct Transaction {
    /// Version of the STM at the start of the transaction.
    /// When we commit, it's going to be done with the version
    /// at the end of the transaction, so that we can detect
    /// if another transaction committed a write-only value
    /// after we have started.
    version: Version,
    /// The local store of the transaction will only be accessed by a single thread,
    /// so it doesn't need to be wrapped in an `Arc`. We have exclusive access through
    /// the mutable reference to the transaction.
    log: HashMap<ID, LVar>,
    /// Time to wait during retries if no variables have been
    /// read by the transaction. This would be strange, but
    /// it's better than blocking a thread forever.
    pub empty_retry_wait_timeout: Duration,
}

impl Transaction {
    fn new() -> Transaction {
        Transaction {
            // Increment the version when we start a new transaction, so we can always
            // tell which one should get preference and nothing ends at the same time
            // another starts at.
            version: next_version(),
            log: HashMap::new(),
            empty_retry_wait_timeout: Duration::from_secs(60),
        }
    }

    /// Read a value from the local store, or the STM system.
    /// If it has changed since the beginning of the transaction,
    /// return a failure immediately, because we are not reading
    /// a consistent snapshot.
    pub fn read_tvar<T: Any + Sync + Send>(&mut self, tvar: &TVar<T>) -> STMResult<Arc<T>> {
        match self.log.get(&tvar.id) {
            Some(lvar) => Ok(lvar.vvar.downcast()),
            None => {
                let guard = tvar.svar.vvar.read().unwrap();
                if guard.version >= self.version {
                    // The TVar has been written to since we started this transaction.
                    // There is no point carrying on with the rest of it, but we can retry.
                    Err(STMError::Failure)
                } else {
                    self.log.insert(
                        tvar.id,
                        LVar {
                            svar: tvar.svar.clone(),
                            vvar: guard.clone(),
                            read: true,
                            write: false,
                        },
                    );
                    Ok(guard.downcast())
                }
            }
        }
    }

    /// Write a value into the local store. If it has not been read
    /// before, just insert it with the version at the start of the
    /// transaction.
    pub fn write_tvar<T: Any + Sync + Send>(&mut self, tvar: &TVar<T>, value: T) -> STMResult<()> {
        match self.log.get_mut(&tvar.id) {
            Some(lvar) => {
                lvar.write = true;
                lvar.vvar.value = Arc::new(value);
            }
            None => {
                self.log.insert(
                    tvar.id,
                    LVar {
                        svar: tvar.svar.clone(),
                        vvar: VVar {
                            version: self.version,
                            value: Arc::new(value),
                        },
                        read: false,
                        write: true,
                    },
                );
            }
        };
        Ok(())
    }

    /// Run the first function; if it returns a `Retry`,
    /// run the second function; if that too returns `Retry`
    /// then combine the values they have read, so that
    /// the overall retry will react to any change.
    ///
    /// If they return `Failure` then just return that result,
    /// since the transaction can be retried right now.
    pub fn or<F, G, T>(&mut self, f: F, g: G) -> STMResult<T>
    where
        F: Fn(&mut Transaction) -> STMResult<T>,
        G: Fn(&mut Transaction) -> STMResult<T>,
    {
        let mut snapshot = self.clone();
        match f(self) {
            Err(STMError::Retry) => {
                // Restore the original transaction state.
                mem::swap(self, &mut snapshot);

                match g(self) {
                    retry @ Err(STMError::Retry) =>
                    // Add any variable read in the first attempt.
                    {
                        for (id, lvar) in snapshot.log.into_iter() {
                            match self.log.get(&id) {
                                Some(lvar) if lvar.read => {}
                                _ => {
                                    self.log.insert(id, lvar);
                                }
                            }
                        }
                        retry
                    }
                    other => other,
                }
            }
            other => other,
        }
    }

    /// Sort the logs by ID so we can acquire locks in a deterministic order
    /// and avoid deadlocks.
    fn sorted_log(&self) -> Vec<(&ID, &LVar)> {
        let mut log = self.log.iter().collect::<Vec<_>>();
        log.sort_unstable_by_key(|(id, _)| *id);
        log
    }

    /// In a critical section, check that every variable we have read/written
    /// hasn't got a higher version number in the committed store.
    /// If so, add all written values to the store.
    fn commit(&self) -> bool {
        // Acquire write locks to all `SVar`s in the transaction.
        let log = self.sorted_log();

        let locks = log
            .iter()
            .map(|(_, lvar)| (lvar, lvar.svar.vvar.write().unwrap()))
            .collect::<Vec<_>>();

        let has_conflict = locks
            .iter()
            .any(|(lvar, lock)| lock.version > lvar.vvar.version);

        if has_conflict {
            false
        } else {
            let commit_version = next_version();
            for (lvar, mut lock) in locks {
                if lvar.write {
                    lock.version = commit_version;
                    lock.value = lvar.vvar.value.clone();
                }
            }
            true
        }
    }

    /// For each variable that the transaction has read, subscribe to future
    /// change notifications, then park this thread.
    fn wait(self) {
        let read_log = self
            .sorted_log()
            .into_iter()
            .filter(|(_, lvar)| lvar.read)
            .collect::<Vec<_>>();

        // If there are no variables subscribed to then just wait a bit.
        let notified = Arc::new(AtomicBool::new(read_log.is_empty()));

        // Register in the wait queues.
        if !read_log.is_empty() {
            let locks = read_log
                .iter()
                .map(|(_, lvar)| lvar.svar.queue.lock().unwrap())
                .collect::<Vec<_>>();

            for mut lock in locks {
                lock.push((thread::current(), notified.clone()));
            }
        }

        loop {
            thread::park_timeout(self.empty_retry_wait_timeout);
            if notified.load(Ordering::Acquire) {
                break;
            }
        }
    }

    /// Unpark any thread waiting on any of the modified `TVar`s.
    fn notify(self) {
        let write_log = self
            .sorted_log()
            .into_iter()
            .filter(|(_, lvar)| lvar.write)
            .collect::<Vec<_>>();

        if !write_log.is_empty() {
            let locks = write_log
                .iter()
                .map(|(_, lvar)| lvar.svar.queue.lock().unwrap());

            // Only unpark a thread once per transaction, even if it waited on multiple vars.
            let mut unparked = HashSet::new();

            for mut lock in locks {
                let queue = mem::replace(&mut *lock, Vec::new());
                for (thread, notified) in queue {
                    if unparked.contains(&thread.id()) {
                        continue;
                    }
                    notified.store(true, Ordering::SeqCst);
                    thread.unpark();
                    unparked.insert(thread.id());
                }
            }
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn next_version_increments() {
        let a = next_version();
        let b = next_version();
        assert!(b > a)
    }

    #[test]
    fn id_increments() {
        let a = TVar::new(42);
        let b = TVar::new(42);
        assert!(b.id > a.id)
    }

    #[test]
    fn basics() {
        let ta = TVar::new(1);
        let tb = TVar::new(vec![1, 2, 3]);

        let (a0, b0) = atomically(|tx| {
            let a = ta.read(tx)?;
            let b = tb.read(tx)?;
            let mut b1 = b.as_ref().clone();
            b1.push(4);
            tb.write(tx, b1)?;
            Ok((a, b))
        });

        assert_eq!(*a0, 1);
        assert_eq!(*b0, vec![1, 2, 3]);

        let b1 = atomically(|tx| tb.read(tx));
        assert_eq!(*b1, vec![1, 2, 3, 4]);
    }

    #[test]
    fn conflict() {
        let ta = Arc::new(TVar::new(1));

        // Need to clone for the other thread.
        let tac = ta.clone();

        let t = thread::spawn(move || {
            atomically(|tx| {
                let a = tac.read(tx)?;
                thread::sleep(Duration::from_millis(100));
                Ok(a)
            })
        });

        thread::sleep(Duration::from_millis(50));
        atomically(|tx| ta.update(tx, |x| x + 1));

        let a = t.join().unwrap();

        assert_eq!(*a, 2);
    }

    #[test]
    fn or() {
        let ta = TVar::new(1);
        let tb = TVar::new("Hello");

        let (a, b) = atomically(|tx| {
            tb.write(tx, "World")?;
            tx.or(
                |tx| {
                    ta.write(tx, 2)?;
                    retry()
                },
                |tx| Ok((ta.read(tx)?, tb.read(tx)?)),
            )
        });

        assert_eq!(*a, 1);
        assert_eq!(*b, "World");
    }

    #[test]
    fn retry_wait_notify() {
        let ta = Arc::new(TVar::new(1));
        let tac = ta.clone();

        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            let a = atomically(|tx| {
                let a = tac.read(tx)?;
                guard(*a > 1)?;
                Ok(a)
            });
            sender.send(*a).unwrap()
        });

        thread::sleep(Duration::from_millis(250));
        atomically(|tx| ta.write(tx, 2));

        let a = receiver.recv_timeout(Duration::from_millis(500)).unwrap();
        assert_eq!(a, 2);
    }

    #[test]
    fn new_tvar_in_transaction() {
        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            let a = atomically(|tx| {
                let t = TVar::new(1);
                t.write(tx, 2)?;
                t.read(tx)
            });
            sender.send(*a).unwrap();
        });

        let a = receiver.recv_timeout(Duration::from_millis(500)).unwrap();
        assert_eq!(a, 2);
    }
}
