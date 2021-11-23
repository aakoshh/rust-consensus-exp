use std::fmt::Debug;

use super::CryptoHash;

/// The rank, or distance from the genesis block.
/// The genesis block has height 0.
pub type Height = u64;

/// An "either" type for things that can cross two eras,
/// like the parent hash of a block, it may be pointing
/// at a parent in the previous era.
#[derive(Clone, PartialEq, Debug)]
pub enum Crossing<P, C> {
    Prev(P),
    Curr(C),
}

/// Unwrap the current value when there is no previous possibility.
pub fn uncross<C>(c: Crossing<!, C>) -> C {
    match c {
        Crossing::Curr(c) => c,
        Crossing::Prev(_) => unreachable!(),
    }
}

pub trait HasHash<'a> {
    type Hash: Into<CryptoHash> + Send + PartialEq + Debug;

    fn hash(&self) -> Self::Hash;
}

pub trait HasHeader {
    type Header;

    fn header(&self) -> Self::Header;
}

/// Derive `HasHash` for things that have a header which has a hash.
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

pub trait HasTransactions<'a> {
    type Transaction;

    // Using fold because otherwise there's no way to avoid cloning
    // when trying to combine multiple eras into a coproduct.
    fn fold_transactions<F, R>(&'a self, init: R, f: F) -> R
    where
        F: Fn(R, &Self::Transaction) -> R;
}

/// Ranking blocks are what determine the ordering of blocks of transactions,
/// but they don't carry data, although they can, in which case they act as
/// both ranking blocks and input blocks.
///
/// Ranking blocks are supposed to be small, so maybe instead of the traditional
/// header/body split, we can communicate in terms of ranking blocks and then input
/// headers and input body. That is so that we can validate the input block header
/// before downloading the transactions, so we can decide whether to switch to forks
/// just based on the light chain information.
///
/// In the case when we have a single chain, we can set the ranking block to be the
/// block header, and the input block to be the full block. We can somehow detect
/// that we already have the input header if it's the same as the ranking block
/// itself. The storage for ranking blocks and input headers can then be the same.
pub trait RankingBlock<'a>: HasHash<'a> {
    type PrevEraHash: PartialEq + Debug;
    type InputBlockHash;

    fn parent_hash(&self) -> Crossing<Self::PrevEraHash, Self::Hash>;
    fn height(&self) -> Height;
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

/// An era combines all the block types into a type family.
/// We will have an overarching era that combines all the eras using a coproduct construct.
pub trait Era {
    /// The transaction type which we can apply on the ledger.
    type Transaction<'a>;

    /// Input block carry the transactions.
    type InputBlock<'a>: HasHash<'a>
        + HasHeader
        + HasTransactions<'a, Transaction = Self::Transaction<'a>>;

    /// The ranking blocks refer to input blocks by their hashes.
    /// This could be a self-reference.
    type RankingBlock<'a>: RankingBlock<'a, InputBlockHash = <Self::InputBlock<'a> as HasHash<'a>>::Hash>
        + Clone
        + Send
        + Sync;

    /// The ledger accepts transactions, but it can also contain data
    /// to validate ranking blocks, e.g. PoS stake distribution.
    type Ledger<'a>: Ledger<'a, Transaction = Self::Transaction<'a>>;
}

pub type EraRankingBlock<E: Era> = E::RankingBlock<'static>;
pub type EraRankingBlockHash<E: Era> = <E::RankingBlock<'static> as HasHash<'static>>::Hash;
pub type EraInputBlockHash<E: Era> = <E::InputBlock<'static> as HasHash<'static>>::Hash;
pub type EraInputBlockHeader<E: Era> = <E::InputBlock<'static> as HasHeader>::Header;
