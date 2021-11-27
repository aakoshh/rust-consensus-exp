pub mod sync;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub struct Cancelable {
    is_canceled: AtomicBool,
}

impl Cancelable {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            is_canceled: AtomicBool::new(false),
        })
    }

    pub fn is_canceled(&self) -> bool {
        self.is_canceled.load(Ordering::Relaxed)
    }

    pub fn cancel(&self) {
        self.is_canceled.store(true, Ordering::Relaxed)
    }
}
