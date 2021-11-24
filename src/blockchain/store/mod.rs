use std::{error::Error, fmt::Display};

pub mod block;
pub mod chain;

pub use block::BlockStore;
pub use chain::ChainStore;

#[derive(Debug)]
pub enum StoreError {
    /// The ranking block does not build on the tip of the chain.
    DoesNotExtendChain,
    /// The ranking block refers to input blocks we do not have.
    MissingInputs,
}

impl Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for StoreError {}
