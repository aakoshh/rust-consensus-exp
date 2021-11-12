use crate::stm::Transaction;

use super::CryptoHash;

pub trait HasHash {
    type Hash: Into<CryptoHash>;
    fn hash(&self) -> Self::Hash;
}

pub trait HasHeader {
    type Header;
    fn header(&self) -> &Self::Header;
}

pub trait HasTransactions {
    type Transaction;
    fn transactions(&self) -> &Vec<Self::Transaction>;
}

impl<B> HasHash for B
where
    B: HasHeader,
    B::Header: HasHash,
{
    type Hash = <<B as HasHeader>::Header as HasHash>::Hash;

    fn hash(&self) -> Self::Hash {
        self.header().hash()
    }
}

pub trait RankingBlock: HasHash {
    type InputBlockHash;
    fn parent_hash(&self) -> Self::Hash;
    fn height(&self) -> u64;
    fn input_block_hashes(&self) -> Vec<Self::InputBlockHash>;
}

pub trait Ledger
where
    Self: Sized,
{
    type Transaction;
    type Error;
    fn apply_transaction(&self, tx: &Self::Transaction) -> Result<Self, Self::Error>;
}

pub trait Era {
    type Transaction;
    type RankingBlock: RankingBlock;
    type InputBlock: HasTransactions<Transaction = Self::Transaction>;
    type Ledger: Ledger<Transaction = Self::Transaction>;
}
