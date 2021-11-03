use std::any::Any;
use std::collections::hash_map::Entry;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{Thread, ThreadId};
use std::time::Duration;
use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc, RwLock},
};
use std::{mem, thread};

enum STMError {
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

/// The value can be read by many threads, so it has to be tracked by an `Arc`.
type DynValue = Arc<dyn Any + Send + Sync>;

fn retry<T>() -> STMResult<T> {
    Err(STMError::Retry)
}

fn guard(cond: bool) -> STMResult<()> {
    if cond {
        Ok(())
    } else {
        retry()
    }
}

/// A versioned value. It will only be access through a transaction and
/// a `TVar` that knows what the value can be downcast to.
#[derive(Clone)]
struct VVar {
    version: Version,
    value: DynValue,
}
/// A variable in the transaction log that remembers if it has been read and/or written to.
#[derive(Clone)]
struct LVar {
    var: VVar,
    /// Remember reads; these are the variables we need to watch if we retry.
    read: bool,
    /// Remember writes; these are the variables that need to be stored at the
    /// end of the transaction, but they don't need to be watched if we retry.
    write: bool,
}

/// `TVar` is our handle to a variable, but reading and writing always goes through a transaction.
#[derive(Clone)]
struct TVar<T> {
    id: ID,
    phantom: PhantomData<T>,
}

impl<T: Any + Sync + Send> TVar<T> {
    /// Create a new `TVar` with a fresh ID and insert
    /// its value into the transaction log.
    pub fn new(tx: &mut Transaction, value: T) -> TVar<T> {
        tx.new_tvar(value)
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

struct WaitQueue {
    /// Wait queue for the `TVar` IDs.
    waiting: HashMap<ID, HashSet<ThreadId>>,
    /// Flip a flag before waking a thread to indicate that it hasn't been a spurious event.
    notified: HashMap<ThreadId, (Thread, Arc<AtomicBool>)>,
}

/// STM holds the committed values and has a vector clock (ie. the version)
/// that is incremented every time we start or commit a transaction.
struct STM {
    id: AtomicU64,
    version: AtomicU64,
    // Transactions form multiple threads can try to access the storage,
    // so it needs to be protected by a lock.
    store: RwLock<HashMap<ID, VVar>>,
    queue: RwLock<WaitQueue>,
}

impl STM {
    pub fn new() -> STM {
        STM {
            id: AtomicU64::new(0),
            version: AtomicU64::new(0),
            store: RwLock::new(HashMap::new()),
            queue: RwLock::new(WaitQueue {
                waiting: HashMap::new(),
                notified: HashMap::new(),
            }),
        }
    }

    /// Atomically create a new `TVar`.
    pub fn new_tvar<T: Any + Send + Sync + Clone>(&self, value: &T) -> TVar<T> {
        // Need to clone because the `f` might be invoked multiple times,
        // so it cannot move a value.
        self.atomically(|tx| Ok(tx.new_tvar(value.clone())))
    }

    /// Atomically read a `TVar`.
    pub fn read_tvar<T: Any + Send + Sync + Clone>(&self, tvar: &TVar<T>) -> Arc<T> {
        self.atomically(|tx| tvar.read(tx))
    }

    /// Create a new transaction and run `f` until it
    pub fn atomically<F, T>(&self, f: F) -> T
    where
        F: Fn(&mut Transaction) -> STMResult<T>,
    {
        loop {
            let mut tx = Transaction::new(self);
            match f(&mut tx) {
                Ok(value) => {
                    if self.commit(&tx) {
                        self.notify(tx);
                        return value;
                    }
                }
                Err(STMError::Failure) => {
                    // We can retry straight awy.
                }
                Err(STMError::Retry) => {
                    // Block this thread until there's a change.
                    self.wait(tx)
                }
            }
        }
    }

    fn next_version(&self) -> Version {
        self.version.fetch_add(1, Ordering::SeqCst)
    }

    fn next_id(&self) -> ID {
        self.id.fetch_add(1, Ordering::SeqCst)
    }

    /// In a critical section, check that every variable we have read/written
    /// hasn't got a higher version number in the committed store. If so, add
    /// all written values to the store.
    fn commit(&self, tx: &Transaction) -> bool {
        let mut guard = self.store.write().unwrap();
        let conflict = tx.store.iter().any(|(id, lvar)| match guard.get(id) {
            None => false,
            Some(vvar) => vvar.version > lvar.var.version,
        });
        if conflict {
            false
        } else {
            let commit_version = self.next_version();
            for (id, lvar) in &tx.store {
                if lvar.write {
                    guard.insert(
                        *id,
                        VVar {
                            version: commit_version,
                            value: lvar.var.value.clone(),
                        },
                    );
                }
            }
            true
        }
    }

    /// For each variable that the transaction has read, subscribe to future
    /// change notifications, then park this thread.
    fn wait(&self, tx: Transaction) {
        let read_ids = tx
            .store
            .into_iter()
            .filter_map(|(id, lvar)| if lvar.read { Some(id) } else { None })
            .collect::<Vec<_>>();

        let notified = Arc::new(AtomicBool::new(false));
        let thread_id = thread::current().id();

        // Register in wait queue.
        if !read_ids.is_empty() {
            let mut guard = self.queue.write().unwrap();
            for id in &read_ids {
                let threads = guard.waiting.entry(*id).or_insert(HashSet::new());
                threads.insert(thread_id);
            }
            guard
                .notified
                .insert(thread_id, (thread::current(), notified.clone()));
        }

        while !notified.load(Ordering::Acquire) {
            // In case we didn't actually subscribe to anything, don't block forever.
            thread::park_timeout(Duration::from_secs(60));
        }

        // Unregister from wait queue.
        if !read_ids.is_empty() {
            let mut guard = self.queue.write().unwrap();
            for id in read_ids {
                if let Entry::Occupied(mut o) = guard.waiting.entry(id) {
                    o.get_mut().remove(&thread_id);
                    if o.get().is_empty() {
                        o.remove();
                    }
                }
            }
            guard.notified.remove(&thread_id);
        }
    }

    /// Unpark any thread waiting on any of the modified `TVar`s.
    fn notify(&self, tx: Transaction) {
        let write_ids = tx
            .store
            .into_iter()
            .filter_map(|(id, lvar)| if lvar.write { Some(id) } else { None })
            .collect::<Vec<_>>();

        if !write_ids.is_empty() {
            let guard = self.queue.read().unwrap();
            let thread_ids = write_ids
                .iter()
                .filter_map(|id| guard.waiting.get(id))
                .flatten()
                .collect::<HashSet<_>>();
            for thread_id in thread_ids {
                if let Some((thread, notified)) = guard.notified.get(thread_id) {
                    notified.store(true, Ordering::SeqCst);
                    thread.unpark();
                }
            }
        }
    }
}

#[derive(Clone)]
struct Transaction<'a> {
    stm: &'a STM,
    /// Version of the STM at the start of the transaction.
    /// When we commit, it's going to be done with the version
    /// at the end of the transaction, so that we can detect
    /// if another transaction committed a write-only value
    /// after we have started.
    version: Version,
    /// The local store of the transaction will only be accessed by a single thread,
    /// so it doesn't need to be
    store: HashMap<ID, LVar>,
}

impl<'a> Transaction<'a> {
    fn new(stm: &STM) -> Transaction {
        Transaction {
            stm,
            // Increment the version when we start a new transaction, so we can always
            // tell which one should get preference and nothing ends at the same time
            // another starts at.
            version: stm.next_version(),
            store: HashMap::new(),
        }
    }

    /// Create a new `TVar` in this transaction.
    /// It will only be added to the STM store when the transaction is committed.
    pub fn new_tvar<T: Any + Send + Sync>(&mut self, value: T) -> TVar<T> {
        let tvar = TVar {
            id: self.stm.next_id(),
            phantom: PhantomData,
        };

        self.store.insert(
            tvar.id,
            LVar {
                var: VVar {
                    version: self.version,
                    value: Arc::new(value),
                },
                read: false,
                write: true,
            },
        );

        tvar
    }

    /// Read a value from the local store, or the STM system.
    /// If it has changed since the beginning of the transaction,
    /// return a failure immediately, because we are not reading
    /// a consistent snapshot.
    pub fn read_tvar<T: Any + Sync + Send>(&mut self, tvar: &TVar<T>) -> STMResult<Arc<T>> {
        match self.store.get_mut(&tvar.id) {
            Some(lvar) => {
                // NOTE: Not changing `lvar.read` since it's not coming from the STM store now.
                Ok(Transaction::downcast(&lvar.var.value))
            }
            None => {
                let guard = self.stm.store.read().unwrap();

                match guard.get(&tvar.id) {
                    Some(vvar) if vvar.version >= self.version => {
                        // There's no point carrying on with the transaction.
                        Err(STMError::Failure)
                    }

                    Some(vvar) => {
                        self.store.insert(
                            tvar.id,
                            LVar {
                                var: vvar.clone(),
                                read: true,
                                write: false,
                            },
                        );
                        Ok(Transaction::downcast(&vvar.value))
                    }
                    None => {
                        panic!("Cannot read the TVar from STM!")
                    }
                }
            }
        }
    }

    /// Write a value into the local store. If it has not been read
    /// before, just insert it with the version at the start of the
    /// transaction.
    pub fn write_tvar<T: Any + Send + Sync>(&mut self, tvar: &TVar<T>, value: T) -> STMResult<()> {
        match self.store.get_mut(&tvar.id) {
            Some(lvar) => {
                lvar.write = true;
                lvar.var.value = Arc::new(value);
            }
            None => {
                self.store.insert(
                    tvar.id,
                    LVar {
                        var: VVar {
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
                        for (id, lvar) in snapshot.store.into_iter() {
                            match self.store.get(&id) {
                                Some(lvar) if lvar.read => {}
                                _ => {
                                    self.store.insert(id, lvar);
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

    /// Perform a downcast on a var. Returns an `Arc` that tracks when that variable
    /// will go out of scope. This avoids cloning on reads, if the value needs to be
    /// mutated then it can be cloned after being read.
    fn downcast<T: Any + Sync + Send>(value: &DynValue) -> Arc<T> {
        match value.clone().downcast::<T>() {
            Ok(s) => s,
            Err(_) => unreachable!("TVar has wrong type"),
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
    fn basics() {
        let stm = STM::new();
        let ta = stm.new_tvar(&1);
        let tb = stm.new_tvar(&vec![1, 2, 3]);

        let (a0, b0) = stm.atomically(|tx| {
            let a = ta.read(tx)?;
            let b = tb.read(tx)?;
            let mut b1 = b.as_ref().clone();
            b1.push(4);
            tb.write(tx, b1)?;
            Ok((a, b))
        });

        assert_eq!(*a0, 1);
        assert_eq!(*b0, vec![1, 2, 3]);

        let b1 = stm.atomically(|tx| tb.read(tx));
        assert_eq!(*b1, vec![1, 2, 3, 4]);
    }

    #[test]
    fn conflict() {
        let stm = Arc::new(STM::new());
        let ta = stm.new_tvar(&1);

        // Need to clone for the other thread.
        let stmc = stm.clone(); // For the other thread.
        let tac = ta.clone();

        let t = thread::spawn(move || {
            stmc.atomically(|tx| {
                let a = tac.read(tx)?;
                thread::sleep(Duration::from_millis(100));
                Ok(a)
            })
        });

        thread::sleep(Duration::from_millis(50));
        stm.atomically(|tx| ta.update(tx, |x| x + 1));

        let a = t.join().unwrap();

        assert_eq!(*a, 2);
    }

    #[test]
    fn or() {
        let stm = STM::new();
        let ta = stm.new_tvar(&1);
        let tb = stm.new_tvar(&"Hello");

        let (a, b) = stm.atomically(|tx| {
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
        let stm = Arc::new(STM::new());
        let stmc = stm.clone();
        let ta = stm.new_tvar(&1);
        let tac = ta.clone();

        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            let a = stmc.atomically(|tx| {
                let a = tac.read(tx)?;
                guard(*a > 1)?;
                Ok(a)
            });
            sender.send(*a).unwrap()
        });

        thread::sleep(Duration::from_millis(100));
        stm.atomically(|tx| ta.write(tx, 2));

        let a = receiver.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(a, 2);
    }
}
