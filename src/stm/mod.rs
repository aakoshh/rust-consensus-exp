use std::any::Any;
use std::sync::atomic::Ordering;
use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc, RwLock},
};

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

/// A versioned value. It will only be access through a transaction and
/// a `TVar` that knows what the value can be downcast to.
#[derive(Clone)]
struct VVar {
    version: Version,
    value: DynValue,
}
/// A variable in the transaction log that remembers if it has been read and/or written to.
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

/// STM holds the committed values and has a vector clock (ie. the version)
/// that is incremented every time we start or commit a transaction.
#[derive(Clone)]
struct STM {
    id: Arc<AtomicU64>,
    version: Arc<AtomicU64>,
    // Transactions form multiple threads can try to access the storage,
    // so it needs to be protected by a lock.
    store: Arc<RwLock<HashMap<ID, VVar>>>,
}

impl STM {
    pub fn new() -> STM {
        STM {
            id: Arc::new(AtomicU64::new(0)),
            version: Arc::new(AtomicU64::new(0)),
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Atomically create a new `TVar`.
    pub fn new_tvar<T: Any + Send + Sync + Clone>(&self, value: &T) -> TVar<T> {
        // Need to clone because the `f` might be invoked multiple times,
        // so it cannot move a value.
        self.atomically(|tx| Ok(tx.new_tvar(value.clone())))
    }

    /// Create a new transaction and run `f` until it
    pub fn atomically<F, T>(&self, f: F) -> T
    where
        F: Fn(&mut Transaction) -> STMResult<T>,
    {
        let mut tx = Transaction::new(self);
        loop {
            match f(&mut tx) {
                Ok(value) => {
                    if self.commit(&tx) {
                        return value;
                    } else {
                        tx.reset()
                    }
                }
                Err(STMError::Failure) => tx.reset(),
                Err(STMError::Retry) => todo!(),
            }
        }
    }

    fn next_version(&self) -> Version {
        self.version.fetch_add(1, Ordering::SeqCst)
    }

    fn next_id(&self) -> ID {
        self.id.fetch_add(1, Ordering::SeqCst)
    }

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
}

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

    fn reset(&mut self) {
        self.store.clear();
        self.version = self.stm.next_version();
    }

    /// Create a new `TVar` in this transaction.
    /// It will only be added to the STM store when the transaction is committed.
    fn new_tvar<T: Any + Send + Sync>(&mut self, value: T) -> TVar<T> {
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
    fn read_tvar<T: Any + Sync + Send>(&mut self, tvar: &TVar<T>) -> STMResult<Arc<T>> {
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
    fn write_tvar<T: Any + Send + Sync>(&mut self, tvar: &TVar<T>, value: T) -> STMResult<()> {
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
        let stm = STM::new();
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
}
