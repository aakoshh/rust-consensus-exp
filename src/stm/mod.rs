use std::any::Any;
use std::cell::RefCell;
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
    VERSION.fetch_add(1, Ordering::Relaxed)
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

struct WaitQueue {
    /// Store the last version which was written to avoid race condition where the notification
    /// happens before the waiters would subscribe and then there's no further event.
    last_written_version: Version,
    /// Threads with their notifications flags that wait for the `TVar` to get an update.
    queue: Vec<(Thread, Arc<AtomicBool>)>,
}

/// Sync variable, hold the committed value and the waiting threads.
struct SVar {
    vvar: RwLock<VVar>,
    waiting: Mutex<WaitQueue>,
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
                waiting: Mutex::new(WaitQueue {
                    last_written_version: Default::default(),
                    queue: Vec::new(),
                }),
            }),
            phantom: PhantomData,
        }
    }

    /// Read the value of the `TVar` as a clone, for subsequent modification. Only call this inside `atomically`.
    pub fn read_clone(&self) -> STMResult<T> {
        with_tx(|tx| tx.read(self).map(|r| r.as_ref().clone()))
    }

    /// Read the value of the `TVar`. Only call this inside `atomically`.
    pub fn read(&self) -> STMResult<Arc<T>> {
        with_tx(|tx| tx.read(self))
    }

    /// Replace the value of the `TVar`. Oly call this inside `atomically`.
    pub fn write(&self, value: T) -> STMResult<()> {
        with_tx(move |tx| tx.write(self, value))
    }

    /// Apply an update on the value of the `TVar`. Only call this inside `atomically`.
    pub fn update<F>(&self, f: F) -> STMResult<()>
    where
        F: FnOnce(&T) -> T,
    {
        let v = self.read()?;
        self.write(f(v.as_ref()))
    }

    /// Apply an update on the value of the `TVar` and return a value. Only call this inside `atomically`.
    pub fn modify<F, R>(&self, f: F) -> STMResult<R>
    where
        F: FnOnce(&T) -> (T, R),
    {
        let v = self.read()?;
        let (w, r) = f(v.as_ref());
        self.write(w)?;
        Ok(r)
    }

    /// Replace the value of the `TVar` and return the previous value. Only call this inside `atomically`.
    pub fn replace(&self, value: T) -> STMResult<Arc<T>> {
        let v = self.read()?;
        self.write(value)?;
        Ok(v)
    }
}

/// Abandon the transaction and retry after some of the variables read have changed.
pub fn retry<T>() -> STMResult<T> {
    Err(STMError::Retry)
}

/// Retry unless a given condition has been met.
pub fn guard(cond: bool) -> STMResult<()> {
    if cond {
        Ok(())
    } else {
        retry()
    }
}

/// Run the first function; if it returns a `Retry`,
/// run the second function; if that too returns `Retry`
/// then combine the values they have read, so that
/// the overall retry will react to any change.
///
/// If they return `Failure` then just return that result,
/// since the transaction can be retried right now.
pub fn or<F, G, T>(f: F, g: G) -> STMResult<T>
where
    F: FnOnce() -> STMResult<T>,
    G: FnOnce() -> STMResult<T>,
{
    let mut snapshot = with_tx(|tx| tx.clone());

    match f() {
        Err(STMError::Retry) => {
            // Restore the original transaction state.
            with_tx(|tx| {
                mem::swap(tx, &mut snapshot);
            });

            match g() {
                retry @ Err(STMError::Retry) =>
                // Add any variable read in the first attempt.
                {
                    with_tx(|tx| {
                        for (id, lvar) in snapshot.log.into_iter() {
                            match tx.log.get(&id) {
                                Some(lvar) if lvar.read => {}
                                _ => {
                                    tx.log.insert(id, lvar);
                                }
                            }
                        }
                    });
                    retry
                }
                other => other,
            }
        }
        other => other,
    }
}

/// Create a new transaction and run `f` until it retunrs a successful result and
/// can be committed without running into version conflicts. Make sure `f` is free
/// of any side effects.
pub fn atomically<F, T>(f: F) -> T
where
    F: Fn() -> STMResult<T>,
{
    loop {
        TX.with(|tref| {
            let mut t = tref.borrow_mut();
            if t.is_some() {
                // Nesting is not supported. Use `or` instead.
                panic!("Already running in an atomic transaction!")
            }
            *t = Some(Transaction::new());
        });

        let result = f();

        if let Some(value) = TX.with(|tref| {
            let tx = tref.borrow_mut().take().unwrap();
            match result {
                Ok(value) => {
                    if let Some(version) = tx.commit() {
                        tx.notify(version);
                        Some(value)
                    } else {
                        None
                    }
                }
                Err(STMError::Failure) => {
                    // We can retry straight away.
                    None
                }
                Err(STMError::Retry) => {
                    // Block this thread until there's a change.
                    tx.wait();
                    None
                }
            }
        }) {
            return value;
        }
    }
}

/// Borrow the thread local transaction and pass it to a function.
pub fn with_tx<F, T>(f: F) -> T
where
    F: FnOnce(&mut Transaction) -> T,
{
    TX.with(|tref| {
        let mut tx = tref.borrow_mut();
        match tx.as_mut() {
            None => panic!("Not running in an atomic transaction!"),
            Some(tx) => f(tx),
        }
    })
}

thread_local! {
  /// Using a thread local transaction for easier syntax than
  /// if we had to pass around the transaction everywhere.
  /// There is a 2x performance penalty for this.
  static TX: RefCell<Option<Transaction>> = RefCell::new(None);
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
    pub fn read<T: Any + Sync + Send>(&mut self, tvar: &TVar<T>) -> STMResult<Arc<T>> {
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
    pub fn write<T: Any + Sync + Send>(&mut self, tvar: &TVar<T>, value: T) -> STMResult<()> {
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
    fn commit(&self) -> Option<Version> {
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
            None
        } else {
            let commit_version = next_version();
            for (lvar, mut lock) in locks {
                if lvar.write {
                    lock.version = commit_version;
                    lock.value = lvar.vvar.value.clone();
                }
            }
            Some(commit_version)
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
                .map(|(_, lvar)| lvar.svar.waiting.lock().unwrap())
                .collect::<Vec<_>>();

            // Don't register if a producer already committed changes by the time we got here.
            let has_updates = locks
                .iter()
                .any(|lock| lock.last_written_version > self.version);

            if has_updates {
                return;
            }

            for mut lock in locks {
                lock.queue.push((thread::current(), notified.clone()));
            }
        }

        loop {
            thread::park_timeout(Duration::from_secs(1));
            if notified.load(Ordering::Acquire) {
                break;
            }
        }
    }

    /// Unpark any thread waiting on any of the modified `TVar`s.
    fn notify(self, commit_version: Version) {
        let write_log = self
            .sorted_log()
            .into_iter()
            .filter(|(_, lvar)| lvar.write)
            .collect::<Vec<_>>();

        if !write_log.is_empty() {
            let locks = write_log
                .iter()
                .map(|(_, lvar)| lvar.svar.waiting.lock().unwrap());

            // Only unpark a thread once per transaction, even if it waited on multiple vars.
            let mut unparked = HashSet::new();

            for mut lock in locks {
                lock.last_written_version = commit_version;

                let queue = mem::replace(&mut lock.queue, Vec::new());
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

        let (a0, b0) = atomically(|| {
            let a = ta.read()?;
            let b = tb.read()?;
            let mut b1 = b.as_ref().clone();
            b1.push(4);
            tb.write(b1)?;
            Ok((a, b))
        });

        assert_eq!(*a0, 1);
        assert_eq!(*b0, vec![1, 2, 3]);

        let b1 = atomically(|| tb.read());
        assert_eq!(*b1, vec![1, 2, 3, 4]);
    }

    #[test]
    fn conflict() {
        let ta = Arc::new(TVar::new(1));

        // Need to clone for the other thread.
        let tac = ta.clone();

        let t = thread::spawn(move || {
            atomically(|| {
                let a = tac.read()?;
                thread::sleep(Duration::from_millis(100));
                Ok(a)
            })
        });

        thread::sleep(Duration::from_millis(50));
        atomically(|| ta.update(|x| x + 1));

        let a = t.join().unwrap();

        assert_eq!(*a, 2);
    }

    #[test]
    fn or_retry_first_return_second() {
        let ta = TVar::new(1);
        let tb = TVar::new("Hello");

        let (a, b) = atomically(|| {
            tb.write("World")?;
            or(
                || {
                    ta.write(2)?;
                    retry()
                },
                || Ok((ta.read()?, tb.read()?)),
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
            let a = atomically(|| {
                let a = tac.read()?;
                guard(*a > 1)?;
                Ok(a)
            });
            sender.send(*a).unwrap()
        });

        thread::sleep(Duration::from_millis(250));
        atomically(|| ta.write(2));

        let a = receiver.recv_timeout(Duration::from_millis(500)).unwrap();
        assert_eq!(a, 2);
    }

    #[test]
    fn new_tvar_in_transaction() {
        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            let a = atomically(|| {
                let t = TVar::new(1);
                t.write(2)?;
                t.read()
            });
            sender.send(*a).unwrap();
        });

        let a = receiver.recv_timeout(Duration::from_millis(500)).unwrap();
        assert_eq!(a, 2);
    }

    #[test]
    #[should_panic]
    fn nested_atomically() {
        atomically(|| Ok(atomically(|| Ok(()))))
    }

    #[test]
    #[should_panic]
    fn read_outside_atomically() {
        let _ = TVar::new("Don't read it!").read();
    }

    #[test]
    #[should_panic]
    fn write_outside_atomically() {
        let _ = TVar::new(0).write(1);
    }
}
