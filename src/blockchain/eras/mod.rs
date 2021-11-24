use crate::define_coprod_impl;
use paste::paste;

use crate::blockchain::property::Era;

use era1::Era1;
use era2::Era2;
use era3::Era3;

use super::{
    property::{Crossing, HasHash, HasHeader, HasTransactions, Ledger, RankingBlock},
    CryptoHash,
};

#[macro_use]
mod coprod;

pub mod era1;
pub mod era2;
pub mod era3;

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

type CoRankingBlock<'a> = Eras<
    <Era1 as Era>::RankingBlock<'a>,
    <Era2 as Era>::RankingBlock<'a>,
    <Era3 as Era>::RankingBlock<'a>,
>;

type CoRankingBlockHash<'a> = Eras<
    <<Era1 as Era>::RankingBlock<'a> as HasHash<'a>>::Hash,
    <<Era2 as Era>::RankingBlock<'a> as HasHash<'a>>::Hash,
    <<Era3 as Era>::RankingBlock<'a> as HasHash<'a>>::Hash,
>;

type CoInputBlockHash<'a> = Eras<
    <<Era1 as Era>::InputBlock<'a> as HasHash<'a>>::Hash,
    <<Era2 as Era>::InputBlock<'a> as HasHash<'a>>::Hash,
    <<Era3 as Era>::InputBlock<'a> as HasHash<'a>>::Hash,
>;

type CoInputBlockHeader<'a> = Eras<
    <<Era1 as Era>::InputBlock<'a> as HasHeader<'a>>::Header,
    <<Era2 as Era>::InputBlock<'a> as HasHeader<'a>>::Header,
    <<Era3 as Era>::InputBlock<'a> as HasHeader<'a>>::Header,
>;

type CoLedger<'a> =
    Eras<<Era1 as Era>::Ledger<'a>, <Era2 as Era>::Ledger<'a>, <Era3 as Era>::Ledger<'a>>;

type CoLedgerError<'a> = Eras<
    <<Era1 as Era>::Ledger<'a> as Ledger<'a>>::Error,
    <<Era2 as Era>::Ledger<'a> as Ledger<'a>>::Error,
    <<Era3 as Era>::Ledger<'a> as Ledger<'a>>::Error,
>;

impl<'a> From<CoInputBlockHash<'a>> for CryptoHash {
    fn from(hash: CoInputBlockHash<'a>) -> Self {
        hash.fold_into(|h| h.into(), |h| h.into(), |h| h.into())
    }
}

impl<'a> From<CoRankingBlockHash<'a>> for CryptoHash {
    fn from(hash: CoRankingBlockHash<'a>) -> Self {
        hash.fold_into(|h| h.into(), |h| h.into(), |h| h.into())
    }
}

impl<'a> HasHash<'a> for CoInputBlockHeader<'a> {
    type Hash = CoInputBlockHash<'a>;

    fn hash(&self) -> Self::Hash {
        self.map(|h| h.hash(), |h| h.hash(), |h| h.hash())
    }
}

impl<'a> HasHeader<'a> for CoInputBlock<'_> {
    type Header = CoInputBlockHeader<'a>;

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

impl<'a> HasHash<'a> for CoRankingBlock<'a> {
    type Hash = CoRankingBlockHash<'a>;

    fn hash(&self) -> Self::Hash {
        self.map(|h| h.hash(), |h| h.hash(), |h| h.hash())
    }
}

impl<'a> RankingBlock<'a> for CoRankingBlock<'a> {
    type PrevEraHash = !;
    type InputBlockHash = CoInputBlockHash<'a>;

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
    type RankingBlock<'a> = CoRankingBlock<'a>;
    type Ledger<'a> = CoLedger<'a>;
}
