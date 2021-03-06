use parking_lot::{Mutex, RwLock};
use std::any::Any;
use std::cell::RefCell;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{Thread, ThreadId};
use std::time::Duration;
use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc},
};
use std::{mem, thread};

pub mod queues;

pub enum StmError {
    /// The transaction failed because a value changed.
    /// It can be retried straight away.
    Failure,
    /// Retry was called and now the transaction has
    /// to wait until at least one of the variables it
    /// read have changed, before being retried.
    Retry,
}

/// STM error extended with the ability to abort the transaction
/// with a dynamic error. It is separate so that we rest assured
/// that `atomically` will not throw an error, that only
/// `atomically_or_err` allows abortions.
pub enum StmDynError {
    /// Regular error.
    Control(StmError),
    /// Abort the tranasction and return an error.
    Abort(Box<dyn Error + Send + Sync>),
}

impl From<StmError> for StmDynError {
    fn from(e: StmError) -> Self {
        StmDynError::Control(e)
    }
}

pub type StmResult<T> = Result<T, StmError>;
pub type StmDynResult<T> = Result<T, StmDynError>;

/// Unique ID for a `TVar`.
type ID = u64;

/// MVCC version.
type Version = u64;

/// Global vector clock.
static VERSION: AtomicU64 = AtomicU64::new(0);

/// Increment the global vector clock and return the new value.
fn next_version() -> Version {
    let prev = VERSION.fetch_add(1, Ordering::Relaxed);
    prev + 1
}

/// Get the current version of the vector clock.
fn current_version() -> Version {
    VERSION.load(Ordering::Relaxed)
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
    /// happens before the waiters would subscribe and then there's no further event that would
    /// unpark them, causing them to wait forever or until they time out.
    ///
    /// This can happen if the order of events on thread A and B are:
    /// 1. `atomically` on A returns `Retry`
    /// 2. `commit` on B updates the versions
    /// 3. `notify` on B finds nobody in the wait queues
    /// 4. `wait` on A adds itself to the wait queues
    ///
    /// By having `notify` update the `last_written_version` we make sure that `wait` sees it.
    last_written_version: Version,
    /// Threads with their notifications flags that wait for the `TVar` to get an update.
    waiting: HashMap<ThreadId, (Thread, Arc<AtomicBool>)>,
}

/// Sync variable, hold the committed value and the waiting threads.
struct SVar {
    vvar: RwLock<VVar>,
    queue: Mutex<WaitQueue>,
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
                queue: Mutex::new(WaitQueue {
                    last_written_version: Default::default(),
                    waiting: HashMap::new(),
                }),
            }),
            phantom: PhantomData,
        }
    }

    /// Read the value of the `TVar` as a clone, for subsequent modification. Only call this inside `atomically`.
    pub fn read_clone(&self) -> StmResult<T> {
        with_tx(|tx| tx.read(self).map(|r| r.as_ref().clone()))
    }

    /// Read the value of the `TVar`. Only call this inside `atomically`.
    pub fn read(&self) -> StmResult<Arc<T>> {
        with_tx(|tx| tx.read(self))
    }

    /// Replace the value of the `TVar`. Oly call this inside `atomically`.
    pub fn write(&self, value: T) -> StmResult<()> {
        with_tx(move |tx| tx.write(self, value))
    }

    /// Apply an update on the value of the `TVar`. Only call this inside `atomically`.
    pub fn update<F>(&self, f: F) -> StmResult<()>
    where
        F: FnOnce(&T) -> T,
    {
        let v = self.read()?;
        self.write(f(v.as_ref()))
    }

    /// Apply an update on the value of the `TVar` and return a value. Only call this inside `atomically`.
    pub fn modify<F, R>(&self, f: F) -> StmResult<R>
    where
        F: FnOnce(&T) -> (T, R),
    {
        let v = self.read()?;
        let (w, r) = f(v.as_ref());
        self.write(w)?;
        Ok(r)
    }

    /// Replace the value of the `TVar` and return the previous value. Only call this inside `atomically`.
    pub fn replace(&self, value: T) -> StmResult<Arc<T>> {
        let v = self.read()?;
        self.write(value)?;
        Ok(v)
    }
}

/// Abandon the transaction and retry after some of the variables read have changed.
pub fn retry<T>() -> StmResult<T> {
    Err(StmError::Retry)
}

/// Retry unless a given condition has been met.
pub fn guard(cond: bool) -> StmResult<()> {
    if cond {
        Ok(())
    } else {
        retry()
    }
}

pub fn abort<T, E: Error + Send + Sync + 'static>(e: E) -> StmDynResult<T> {
    Err(StmDynError::Abort(Box::new(e)))
}

/// Run the first function; if it returns a `Retry`,
/// run the second function; if that too returns `Retry`
/// then combine the values they have read, so that
/// the overall retry will react to any change.
///
/// If they return `Failure` then just return that result,
/// since the transaction can be retried right now.
pub fn or<F, G, T>(f: F, g: G) -> StmResult<T>
where
    F: FnOnce() -> StmResult<T>,
    G: FnOnce() -> StmResult<T>,
{
    let mut snapshot = with_tx(|tx| tx.clone());

    match f() {
        Err(StmError::Retry) => {
            // Restore the original transaction state.
            with_tx(|tx| {
                mem::swap(tx, &mut snapshot);
            });

            match g() {
                retry @ Err(StmError::Retry) =>
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

/// Create a new transaction and run `f` until it returns a successful result and
/// can be committed without running into version conflicts.
/// Make sure `f` is free of any side effects.
pub fn atomically<F, T>(f: F) -> T
where
    F: Fn() -> StmResult<T>,
{
    atomically_or_err(|| f().map_err(|e| StmDynError::Control(e)))
        .expect("Didn't expect `abort`. Use `atomically_or_err` instead.")
}

/// Create a new transaction and run `f` until it returns a successful result and
/// can be committed without running into version conflicts, or until it returns
/// an `Abort` in which case the contained error is returned.
/// Make sure `f` is free of any side effects.
pub fn atomically_or_err<F, T>(f: F) -> Result<T, Box<dyn Error + Send + Sync>>
where
    F: Fn() -> StmDynResult<T>,
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
                        Some(Ok(value))
                    } else {
                        None
                    }
                }
                Err(StmDynError::Control(StmError::Failure)) => {
                    // We can retry straight away.
                    None
                }
                Err(StmDynError::Control(StmError::Retry)) => {
                    // Block this thread until there's a change.
                    tx.wait();
                    None
                }
                Err(StmDynError::Abort(e)) => Some(Err(e)),
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
    /// A read-only transaction can be committed without taking out locks a second time.
    has_writes: bool,
    /// Time to wait during retries if no variables have been
    /// read by the transaction. This would be strange, but
    /// it's better than blocking a thread forever.
    pub empty_retry_wait_timeout: Duration,
}

impl Transaction {
    fn new() -> Transaction {
        Transaction {
            version: current_version(),
            log: HashMap::new(),
            has_writes: false,
            empty_retry_wait_timeout: Duration::from_secs(60),
        }
    }

    /// Read a value from the local store, or the STM system.
    /// If it has changed since the beginning of the transaction,
    /// return a failure immediately, because we are not reading
    /// a consistent snapshot.
    pub fn read<T: Any + Sync + Send>(&mut self, tvar: &TVar<T>) -> StmResult<Arc<T>> {
        match self.log.get(&tvar.id) {
            Some(lvar) => Ok(lvar.vvar.downcast()),
            None => {
                let guard = tvar.svar.vvar.read();
                if guard.version > self.version {
                    // The TVar has been written to since we started this transaction.
                    // There is no point carrying on with the rest of it, but we can retry.
                    Err(StmError::Failure)
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
    pub fn write<T: Any + Sync + Send>(&mut self, tvar: &TVar<T>, value: T) -> StmResult<()> {
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
                            // So we didn't bother reading the value before attempting to overwrite,
                            // and therefore we don't know what version it had. Let's use the maximum
                            // it could have had at the time of the transaction.
                            version: self.version,
                            value: Arc::new(value),
                        },
                        read: false,
                        write: true,
                    },
                );
            }
        };
        self.has_writes = true;
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
        // If there were no writes, then the read would have already detected conflicts when their
        // values were retrieved. We can go ahead and just return without locking again.
        if !self.has_writes {
            return Some(self.version);
        }

        // Acquire write locks to all values written in the transaction, read locks for the rest,
        // but do this in the deterministic order of IDs to avoid deadlocks.
        let mut write_locks = Vec::new();
        let mut read_locks = Vec::new();
        let log = self.sorted_log();

        for (_, lvar) in log {
            if lvar.write {
                let lock = lvar.svar.vvar.write();
                if lock.version > lvar.vvar.version {
                    return None;
                }
                write_locks.push((lvar, lock));
            } else {
                let lock = lvar.svar.vvar.read();
                if lock.version > lvar.vvar.version {
                    return None;
                }
                read_locks.push(lock);
            }
        }

        // Incrementing after locks are taken; if it only differs by one, no other transaction took place;
        // but we already checked for conflicts, it looks like at this point there's no way to use this info.
        let commit_version = next_version();

        for (lvar, mut lock) in write_locks {
            lock.version = commit_version;
            lock.value = lvar.vvar.value.clone();
        }

        Some(commit_version)
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
        let thread_id = thread::current().id();

        // Register in the wait queues.
        if !read_log.is_empty() {
            let locks = read_log
                .iter()
                .map(|(_, lvar)| lvar.svar.queue.lock())
                .collect::<Vec<_>>();

            // Don't register if a producer already committed changes by the time we got here.
            let has_updates = locks
                .iter()
                .any(|lock| lock.last_written_version > self.version);

            if has_updates {
                return;
            }

            for mut lock in locks {
                lock.waiting
                    .insert(thread_id, (thread::current(), notified.clone()));
            }
        }

        loop {
            thread::park_timeout(self.empty_retry_wait_timeout);
            if notified.load(Ordering::Acquire) {
                break;
            }
        }

        // NOTE: Here we could deregister from the wait queues, but that would require
        // taking out the locks again. Since the notifiers take locks too to increment
        // the version, let them do the clean up. One side effect is that a thread
        // may be unparked some variable that changes less frequently, which still
        // remembers it with an obsolete notification flag. In this case the thread
        // will just park itself again.
    }

    /// Unpark any thread waiting on any of the modified `TVar`s.
    fn notify(self, commit_version: Version) {
        if !self.has_writes {
            return;
        }

        let write_log = self
            .sorted_log()
            .into_iter()
            .filter(|(_, lvar)| lvar.write)
            .collect::<Vec<_>>();

        // Only unpark threads at the end, to make sure they the most recent flag.
        let mut unpark = HashMap::new();

        let locks = write_log
            .iter()
            .map(|(_, lvar)| lvar.svar.queue.lock())
            .collect::<Vec<_>>();

        for mut lock in locks {
            lock.last_written_version = commit_version;
            if lock.waiting.is_empty() {
                continue;
            }

            let waiting = mem::replace(&mut lock.waiting, HashMap::new());

            for (thread_id, (thread, notified)) in waiting {
                notified.store(true, Ordering::Release);
                unpark.insert(thread_id, thread);
            }
        }

        for (_, thread) in unpark {
            thread.unpark();
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
    fn conflict_if_written_after_start() {
        let ta = Arc::new(TVar::new(1));
        let tac = ta.clone();

        let t = thread::spawn(move || {
            atomically(|| {
                thread::sleep(Duration::from_millis(100));
                tac.read()
            })
        });

        thread::sleep(Duration::from_millis(50));
        atomically(|| ta.update(|x| x + 1));

        let a = t.join().unwrap();
        // We have written a between the start of the transaction
        // and the time it read the value, so it should have restarted.
        assert_eq!(*a, 2);
    }

    #[test]
    fn no_confict_if_read_before_write() {
        let ta = Arc::new(TVar::new(1));
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
        // Even though we spent a lot of time after reading the value
        // we didn't read anything else that changed, so it's a consistent
        // state for the lengthy calculation that followed.
        assert_eq!(*a, 1);
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

    #[test]
    fn nested_abort() {
        let r = TVar::new(0);
        let show = |label| Ok(println!("{}: r = {}", label, r.read()?));
        let add1 = |x: &i32| x + 1;
        let abort = || retry();

        fn nested<F>(f: F) -> StmResult<()>
        where
            F: FnOnce() -> StmResult<()>,
        {
            or(f, || Ok(()))
        }

        let v = atomically(|| {
            show('a')?; // 0
            r.update(add1)?;
            show('b')?; // 1
            nested(|| {
                show('c')?; // still 1
                r.update(add1)?;
                show('d')?; // 2
                abort()
            })?;
            show('e')?; // back to 1 because abort
            nested(|| {
                show('f')?; // still 1
                r.update(add1)?;
                show('g') // 2
            })?;
            show('h')?; // 2
            r.update(add1)?;
            show('i')?; // 3
            r.read()
        });

        assert_eq!(*v, 3);
    }

    #[test]
    fn abort_with_error() {
        let a = TVar::new(0);

        #[derive(Debug, Clone)]
        struct TestError;

        impl std::fmt::Display for TestError {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "test error instance")
            }
        }

        impl Error for TestError {}

        let r = atomically_or_err(|| {
            a.write(1)?;
            abort(TestError)?;
            Ok(())
        });

        assert_eq!(
            r.err().map(|e| e.to_string()),
            Some("test error instance".to_owned())
        );
    }
}
