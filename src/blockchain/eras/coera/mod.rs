use crate::define_coprod_impl;
use paste::paste;

use crate::blockchain::property::Era;

pub mod store;
use super::era1::{self, Era1};
use super::era2::{self, Era2};
use super::era3::{self, Era3};

use crate::blockchain::{
    property::{Crossing, HasHash, HasHeader, HasTransactions, Ledger, RankingBlock},
    CryptoHash,
};

#[derive(Clone, PartialEq, Debug, Eq, Hash)]
pub enum Eras<E1, E2, E3> {
    Era1(E1),
    Era2(E2),
    Era3(E3),
}

// For some reason Rust Analyzer says "{unknown}" in pattern matches
// if I use the macro the generate the enum :(
//define_coprod! {Eras { Era1, Era2, Era3 }}
// Now just used to generate the companion.
// Even this way the lambda parameters are "{unknown}" but they are quite mechanical.
define_coprod_impl! {Eras { Era1, Era2, Era3 }}

type CoTransaction<'a> = Eras<
    &'a <Era1 as Era>::Transaction<'a>,
    &'a <Era2 as Era>::Transaction<'a>,
    &'a <Era3 as Era>::Transaction<'a>,
>;

type CoInputBlock<'a> = Eras<
    <Era1 as Era>::InputBlock<'a>,
    <Era2 as Era>::InputBlock<'a>,
    <Era3 as Era>::InputBlock<'a>,
>;

type CoRankingBlock = Eras<era1::BlockHeader, era2::RankingBlock, era3::RankingBlock>;

type CoRankingBlockHash = Eras<era1::BlockHash, era2::RankingBlockHash, era3::RankingBlockHash>;

type CoInputBlockHash = Eras<era1::BlockHash, era2::InputBlockHash, era3::InputBlockHash>;

type CoInputBlockHeader = Eras<era1::BlockHeader, era2::InputBlockHeader, era3::InputBlockHeader>;

type CoLedger<'a> =
    Eras<<Era1 as Era>::Ledger<'a>, <Era2 as Era>::Ledger<'a>, <Era3 as Era>::Ledger<'a>>;

type CoLedgerError<'a> = Eras<
    <<Era1 as Era>::Ledger<'a> as Ledger<'a>>::Error,
    <<Era2 as Era>::Ledger<'a> as Ledger<'a>>::Error,
    <<Era3 as Era>::Ledger<'a> as Ledger<'a>>::Error,
>;

impl From<CoInputBlockHash> for CryptoHash {
    fn from(hash: CoInputBlockHash) -> Self {
        hash.fold_into(|h| h.into(), |h| h.into(), |h| h.into())
    }
}

impl From<CoRankingBlockHash> for CryptoHash {
    fn from(hash: CoRankingBlockHash) -> Self {
        hash.fold_into(|h| h.into(), |h| h.into(), |h| h.into())
    }
}

impl HasHash for CoInputBlockHeader {
    type Hash = CoInputBlockHash;

    fn hash(&self) -> Self::Hash {
        self.map(|h| h.hash(), |h| h.hash(), |h| h.hash())
    }
}

impl HasHeader for CoInputBlock<'_> {
    type Header = CoInputBlockHeader;

    fn header(&self) -> Self::Header {
        self.map(|b| b.header(), |b| b.header(), |b| b.header())
    }
}

impl<'a> HasTransactions<'a> for CoInputBlock<'a> {
    type Transaction = CoTransaction<'a>;

    fn fold_transactions<F, R>(&'a self, init: R, f: F) -> R
    where
        F: Fn(R, &Self::Transaction) -> R,
    {
        match self {
            Eras::Era1(b) => b
                .transactions
                .iter()
                .fold(init, |acc, tx| f(acc, &Eras::Era1(tx))),

            Eras::Era2(b) => b
                .transactions
                .iter()
                .fold(init, |acc, tx| f(acc, &Eras::Era2(tx))),

            Eras::Era3(b) => b
                .transactions
                .iter()
                .fold(init, |acc, tx| f(acc, &Eras::Era3(tx))),
        }
    }
}

impl HasHash for CoRankingBlock {
    type Hash = CoRankingBlockHash;

    fn hash(&self) -> Self::Hash {
        self.map(|h| h.hash(), |h| h.hash(), |h| h.hash())
    }
}

impl RankingBlock for CoRankingBlock {
    type PrevEraHash = !;
    type InputBlockHash = CoInputBlockHash;

    fn parent_hash(&self) -> Crossing<Self::PrevEraHash, Self::Hash> {
        let parent_hash = match self {
            Eras::Era1(h) => Eras::Era1(h.parent_hash.clone()),
            Eras::Era2(h) => match h.parent_hash.clone() {
                Crossing::Prev(p) => Eras::Era1(p),
                Crossing::Curr(p) => Eras::Era2(p),
            },
            Eras::Era3(h) => match h.parent_hash.clone() {
                Crossing::Prev(p) => Eras::Era2(p),
                Crossing::Curr(p) => Eras::Era3(p),
            },
        };
        Crossing::Curr(parent_hash)
    }

    fn height(&self) -> u64 {
        self.fold(|h| h.height, |h| h.height, |h| h.height)
    }

    fn input_block_hashes(&self) -> Vec<Self::InputBlockHash> {
        match self {
            Eras::Era1(h) => h
                .input_block_hashes()
                .iter()
                .map(|h| Eras::Era1(h.clone()))
                .collect(),

            Eras::Era2(h) => h
                .input_block_hashes()
                .iter()
                .map(|h| Eras::Era2(h.clone()))
                .collect(),

            Eras::Era3(h) => h
                .input_block_hashes()
                .iter()
                .map(|h| Eras::Era3(h.clone()))
                .collect(),
        }
    }
}

impl<'a> Ledger<'a> for CoLedger<'a> {
    type Transaction = CoTransaction<'a>;
    type Error = Crossing<String, CoLedgerError<'a>>;

    fn apply_transaction(&'a self, tx: &Self::Transaction) -> Result<Self, Self::Error> {
        match (self, tx) {
            (Eras::Era1(l), Eras::Era1(tx)) => l
                .apply_transaction(tx)
                .map(Eras::Era1)
                .map_err(Eras::Era1)
                .map_err(Crossing::Curr),

            (Eras::Era2(l), Eras::Era2(tx)) => l
                .apply_transaction(tx)
                .map(Eras::Era2)
                .map_err(Eras::Era2)
                .map_err(Crossing::Curr),

            (Eras::Era3(l), Eras::Era3(tx)) => l
                .apply_transaction(tx)
                .map(Eras::Era3)
                .map_err(Eras::Era3)
                .map_err(Crossing::Curr),

            (_, _) => Err(Crossing::Prev(
                "Mismatch between ledger and transaction.".into(),
            )),
        }
    }
}

/// The era that encompasses all eras, over which will
/// define our consensus and related instances.
pub struct CoEra;

impl Era for CoEra {
    type Transaction<'a> = CoTransaction<'a>;
    type InputBlock<'a> = CoInputBlock<'a>;
    type RankingBlock = CoRankingBlock;
    type Ledger<'a> = CoLedger<'a>;
}
