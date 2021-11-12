use super::{eras::Crossing, CryptoHash};

pub trait HasHash {
    type Hash: Into<CryptoHash>;

    fn hash(&self) -> Self::Hash;
}

pub trait HasHeader {
    type Header;

    fn header(&self) -> &Self::Header;
}

pub trait HasTransactions {
    type Transaction: 'static;
    type Transactions<'a>: Iterator<Item = &'a Self::Transaction>;

    fn transactions<'a>(&'a self) -> Self::Transactions<'a>;
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

// NOTE: Ranking blocks are supposed to be small, so maybe instead of the traditional
// header/body split, we can communicate in terms of ranking blocks and then input
// headers and input body. That is so that we can validate the input block header
// before downloading the transactions, so we can decide whether to switch to forks
// just based on the light chain information.
//
// In the case when we have a single chain, we can set the ranking block to be the
// block header, and the input block to be the full block. We can somehow detect
// that we already have the input header if it's the same as the ranking block
// itself. The storage for ranking blocks and input headers can then be the same.

pub trait RankingBlock: HasHash {
    type PrevEraHash;
    type InputBlockHash;

    fn parent_hash(&self) -> Crossing<Self::PrevEraHash, Self::Hash>;
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
    type InputBlock: HasHash + HasHeader + HasTransactions<Transaction = Self::Transaction>;
    type RankingBlock: RankingBlock<InputBlockHash = <Self::InputBlock as HasHash>::Hash>;
    type Ledger: Ledger<Transaction = Self::Transaction>;
}
