use super::TQueueLike;
use crate::stm::{guard, retry, STMResult, TVar};
use crate::test_queue_mod;
use std::any::Any;

/// Bounded queue using two vectors.
///
/// Similar to `TQueue` but every read and write touches a common `TVar`
/// to track the current capacity, retrying if the queue is full.
#[derive(Clone)]
pub struct TBQueue<T> {
    capacity: TVar<u32>,
    read: TVar<Vec<T>>,
    write: TVar<Vec<T>>,
}

impl<T> TBQueue<T>
where
    T: Any + Sync + Send + Clone,
{
    /// Create an empty `TBQueue`.
    pub fn new(capacity: u32) -> TBQueue<T> {
        TBQueue {
            capacity: TVar::new(capacity),
            read: TVar::new(Vec::new()),
            write: TVar::new(Vec::new()),
        }
    }
}

impl<T> TQueueLike<T> for TBQueue<T>
where
    T: Any + Sync + Send + Clone,
{
    fn write(&self, value: T) -> STMResult<()> {
        let capacity = self.capacity.read()?;
        guard(*capacity > 0)?;
        self.capacity.write(*capacity - 1)?;

        // Same as TQueue.
        let mut v = self.write.read_clone()?;
        v.push(value);
        self.write.write(v)
    }

    fn read(&self) -> STMResult<T> {
        let capacity = self.capacity.read()?;
        self.capacity.write(*capacity + 1)?;

        // Same as TQueue.
        let mut rv = self.read.read_clone()?;
        // Elements are stored in reverse order.
        match rv.pop() {
            Some(value) => {
                self.read.write(rv)?; // XXX
                Ok(value)
            }
            None => {
                let mut wv = self.write.read_clone()?;
                if wv.is_empty() {
                    retry()
                } else {
                    wv.reverse();
                    let value = wv.pop().unwrap();
                    self.read.write(wv)?;
                    self.write.write(Vec::new())?;
                    Ok(value)
                }
            }
        }
    }

    fn is_empty(&self) -> STMResult<bool> {
        if self.read.read()?.is_empty() {
            Ok(self.write.read()?.is_empty())
        } else {
            Ok(false)
        }
    }
}

test_queue_mod!(|| { crate::stm::queues::tbqueue::TBQueue::<i32>::new(1_000_000) });

#[cfg(test)]
mod test {
    use super::{TBQueue, TQueueLike};
    use crate::stm::atomically;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn threaded_bounded_blocks() {
        let queue = TBQueue::<i32>::new(1);

        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            atomically(|| {
                queue.write(1)?;
                queue.write(2)
            });
            sender.send(()).unwrap();
        });

        let terminated = receiver.recv_timeout(Duration::from_millis(100)).is_ok();
        assert!(!terminated);
    }

    #[test]
    fn threaded_bounded_unblocks() {
        let queue1 = TBQueue::<i32>::new(1);
        let queue2 = queue1.clone();

        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            // Don't try to write 2 items at the same time or both will be retried,
            // and the reader will retry because of an empty queue.
            atomically(|| queue2.write(1));
            atomically(|| queue2.write(2));
            sender.send(()).unwrap();
        });

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            atomically(|| queue1.read());
        });

        let terminated = receiver.recv_timeout(Duration::from_millis(500)).is_ok();

        assert!(terminated);
    }
}
