use super::TQueueLike;
use crate::stm::{retry, STMResult, TVar, Transaction};
use crate::test_queue_mod;
use std::{any::Any, collections::VecDeque};

#[derive(Clone)]
/// Unbounded queue backed by a single `VecDequeue`.
///
/// The drawback is that reads and writes both touch the same `TVar`.
pub struct TVecDequeue<T> {
    queue: TVar<VecDeque<T>>,
}

impl<T> TVecDequeue<T>
where
    T: Any + Sync + Send + Clone,
{
    /// Create an empty `TVecDequeue`.
    #[allow(dead_code)]
    pub fn new() -> TVecDequeue<T> {
        TVecDequeue {
            queue: TVar::new(VecDeque::new()),
        }
    }
}

impl<T> TQueueLike<T> for TVecDequeue<T>
where
    T: Any + Sync + Send + Clone,
{
    fn write(&self, transaction: &mut Transaction, value: T) -> STMResult<()> {
        let mut queue = self.queue.read_clone(transaction)?;
        queue.push_back(value);
        self.queue.write(transaction, queue)
    }

    fn read(&self, transaction: &mut Transaction) -> STMResult<T> {
        let mut queue = self.queue.read_clone(transaction)?;
        match queue.pop_front() {
            None => retry(),
            Some(value) => {
                self.queue.write(transaction, queue)?;
                Ok(value)
            }
        }
    }

    fn is_empty(&self, transaction: &mut Transaction) -> STMResult<bool> {
        self.queue.read(transaction).map(|v| v.is_empty())
    }
}

test_queue_mod!(|| { crate::stm::queues::tvecdequeue::TVecDequeue::<i32>::new() });
