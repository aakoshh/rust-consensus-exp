use super::{eras::Crossing, CryptoHash};

pub trait HasHash<'a> {
    type Hash: Into<CryptoHash>;

    fn hash(&self) -> Self::Hash;
}

pub trait HasHeader {
    type Header;

    fn header(&self) -> Self::Header;
}

pub trait HasTransactions<'a> {
    type Transaction;

    // Using fold because otherwise there's no way to avoid cloning
    // when trying to combine multiple eras into a coproduct.
    fn fold_transactions<F, R>(&'a self, init: R, f: F) -> R
    where
        F: Fn(R, &Self::Transaction) -> R;
}

impl<'a, B> HasHash<'a> for B
where
    B: HasHeader,
    B::Header: HasHash<'a>,
{
    type Hash = <<B as HasHeader>::Header as HasHash<'a>>::Hash;

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

pub trait RankingBlock<'a>: HasHash<'a> {
    type PrevEraHash;
    type InputBlockHash;

    fn parent_hash(&self) -> Crossing<Self::PrevEraHash, Self::Hash>;
    fn height(&self) -> u64;
    fn input_block_hashes(&self) -> Vec<Self::InputBlockHash>;
}

pub trait Ledger<'a>
where
    Self: Sized,
{
    type Transaction;
    type Error;

    fn apply_transaction(&'a self, tx: &Self::Transaction) -> Result<Self, Self::Error>;
}

pub trait Era {
    type Transaction<'a>;

    type InputBlock<'a>: HasHash<'a>
        + HasHeader
        + HasTransactions<'a, Transaction = Self::Transaction<'a>>;

    type RankingBlock<'a>: RankingBlock<
        'a,
        InputBlockHash = <Self::InputBlock<'a> as HasHash<'a>>::Hash,
    >;

    type Ledger<'a>: Ledger<'a, Transaction = Self::Transaction<'a>>;
}
