use std::{error::Error, fmt::Display};

pub mod block;
pub mod chain;

pub use block::BlockStore;
pub use chain::ChainStore;

#[derive(Debug)]
pub enum StoreError {
    DoesNotExtendChain,
}

impl Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for StoreError {}
