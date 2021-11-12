use crate::blockchain::property::Era;

use era1::Era1;
use era2::Era2;
use era3::Era3;

use super::{
    property::{HasHash, HasHeader, HasTransactions, Ledger, RankingBlock},
    CryptoHash,
};

mod era1;
mod era2;
mod era3;

/// An "either" type for things that can cross two eras,
/// like the parent hash of a block, it may be pointing
/// at a parent in the previous era.
#[derive(Clone)]
pub enum Crossing<P, C> {
    Prev(P),
    Curr(C),
}

pub enum Coprod<E1, E2, E3> {
    Era1(E1),
    Era2(E2),
    Era3(E3),
}

type CoTransaction<'a> = Coprod<
    &'a <Era1 as Era>::Transaction<'a>,
    &'a <Era2 as Era>::Transaction<'a>,
    &'a <Era3 as Era>::Transaction<'a>,
>;

type CoInputBlock<'a> = Coprod<
    <Era1 as Era>::InputBlock<'a>,
    <Era2 as Era>::InputBlock<'a>,
    <Era3 as Era>::InputBlock<'a>,
>;

type CoRankingBlock<'a> = Coprod<
    <Era1 as Era>::RankingBlock<'a>,
    <Era2 as Era>::RankingBlock<'a>,
    <Era3 as Era>::RankingBlock<'a>,
>;

type CoRankingBlockHash<'a> = Coprod<
    <<Era1 as Era>::RankingBlock<'a> as HasHash<'a>>::Hash,
    <<Era2 as Era>::RankingBlock<'a> as HasHash<'a>>::Hash,
    <<Era3 as Era>::RankingBlock<'a> as HasHash<'a>>::Hash,
>;

type CoInputBlockHash<'a> = Coprod<
    <<Era1 as Era>::InputBlock<'a> as HasHash<'a>>::Hash,
    <<Era2 as Era>::InputBlock<'a> as HasHash<'a>>::Hash,
    <<Era3 as Era>::InputBlock<'a> as HasHash<'a>>::Hash,
>;

type CoInputBlockHeader<'a> = Coprod<
    <<Era1 as Era>::InputBlock<'a> as HasHeader>::Header,
    <<Era2 as Era>::InputBlock<'a> as HasHeader>::Header,
    <<Era3 as Era>::InputBlock<'a> as HasHeader>::Header,
>;

type CoLedger<'a> =
    Coprod<<Era1 as Era>::Ledger<'a>, <Era2 as Era>::Ledger<'a>, <Era3 as Era>::Ledger<'a>>;

type CoLedgerError<'a> = Coprod<
    <<Era1 as Era>::Ledger<'a> as Ledger<'a>>::Error,
    <<Era2 as Era>::Ledger<'a> as Ledger<'a>>::Error,
    <<Era3 as Era>::Ledger<'a> as Ledger<'a>>::Error,
>;

impl<'a> From<CoInputBlockHash<'a>> for CryptoHash {
    fn from(hash: CoInputBlockHash<'a>) -> Self {
        match hash {
            Coprod::Era1(h) => h.into(),
            Coprod::Era2(h) => h.into(),
            Coprod::Era3(h) => h.into(),
        }
    }
}

impl<'a> From<CoRankingBlockHash<'a>> for CryptoHash {
    fn from(hash: CoRankingBlockHash<'a>) -> Self {
        match hash {
            Coprod::Era1(h) => h.into(),
            Coprod::Era2(h) => h.into(),
            Coprod::Era3(h) => h.into(),
        }
    }
}

impl<'a> HasHash<'a> for CoInputBlockHeader<'a> {
    type Hash = CoInputBlockHash<'a>;

    fn hash(&self) -> Self::Hash {
        match self {
            Coprod::Era1(h) => Coprod::Era1(h.hash()),
            Coprod::Era2(h) => Coprod::Era2(h.hash()),
            Coprod::Era3(h) => Coprod::Era3(h.hash()),
        }
    }
}

impl HasHeader for CoInputBlock<'_> {
    type Header = CoInputBlockHeader<'static>;

    fn header(&self) -> Self::Header {
        match self {
            Coprod::Era1(b) => Coprod::Era1(b.header()),
            Coprod::Era2(b) => Coprod::Era2(b.header()),
            Coprod::Era3(b) => Coprod::Era3(b.header()),
        }
    }
}

impl<'a> HasTransactions<'a> for CoInputBlock<'a> {
    type Transaction = CoTransaction<'a>;

    fn fold_transactions<F, R>(&'a self, init: R, f: F) -> R
    where
        F: Fn(R, &Self::Transaction) -> R,
    {
        match self {
            Coprod::Era1(b) => b
                .transactions
                .iter()
                .fold(init, |acc, tx| f(acc, &Coprod::Era1(tx))),

            Coprod::Era2(b) => b
                .transactions
                .iter()
                .fold(init, |acc, tx| f(acc, &Coprod::Era2(tx))),

            Coprod::Era3(b) => b
                .transactions
                .iter()
                .fold(init, |acc, tx| f(acc, &Coprod::Era3(tx))),
        }
    }
}

impl<'a> HasHash<'a> for CoRankingBlock<'a> {
    type Hash = CoRankingBlockHash<'a>;

    fn hash(&self) -> Self::Hash {
        match self {
            Coprod::Era1(h) => Coprod::Era1(h.hash()),
            Coprod::Era2(h) => Coprod::Era2(h.hash()),
            Coprod::Era3(h) => Coprod::Era3(h.hash()),
        }
    }
}

impl<'a> RankingBlock<'a> for CoRankingBlock<'a> {
    type PrevEraHash = !;
    type InputBlockHash = CoInputBlockHash<'a>;

    fn parent_hash(&self) -> Crossing<Self::PrevEraHash, Self::Hash> {
        let parent_hash = match self {
            Coprod::Era1(h) => Coprod::Era1(h.parent_hash.clone()),
            Coprod::Era2(h) => match h.parent_hash.clone() {
                Crossing::Prev(p) => Coprod::Era1(p),
                Crossing::Curr(p) => Coprod::Era2(p),
            },
            Coprod::Era3(h) => match h.parent_hash.clone() {
                Crossing::Prev(p) => Coprod::Era2(p),
                Crossing::Curr(p) => Coprod::Era3(p),
            },
        };
        Crossing::Curr(parent_hash)
    }

    fn height(&self) -> u64 {
        match self {
            Coprod::Era1(h) => h.height,
            Coprod::Era2(h) => h.height,
            Coprod::Era3(h) => h.height,
        }
    }

    fn input_block_hashes(&self) -> Vec<Self::InputBlockHash> {
        match self {
            Coprod::Era1(h) => h
                .input_block_hashes()
                .iter()
                .map(|h| Coprod::Era1(h.clone()))
                .collect(),

            Coprod::Era2(h) => h
                .input_block_hashes()
                .iter()
                .map(|h| Coprod::Era2(h.clone()))
                .collect(),

            Coprod::Era3(h) => h
                .input_block_hashes()
                .iter()
                .map(|h| Coprod::Era3(h.clone()))
                .collect(),
        }
    }
}

impl<'a> Ledger<'a> for CoLedger<'a> {
    type Transaction = CoTransaction<'a>;
    type Error = Crossing<String, CoLedgerError<'a>>;

    fn apply_transaction(&'a self, tx: &Self::Transaction) -> Result<Self, Self::Error> {
        match (self, tx) {
            (Coprod::Era1(l), Coprod::Era1(tx)) => l
                .apply_transaction(tx)
                .map(Coprod::Era1)
                .map_err(Coprod::Era1)
                .map_err(Crossing::Curr),

            (Coprod::Era2(l), Coprod::Era2(tx)) => l
                .apply_transaction(tx)
                .map(Coprod::Era2)
                .map_err(Coprod::Era2)
                .map_err(Crossing::Curr),

            (Coprod::Era3(l), Coprod::Era3(tx)) => l
                .apply_transaction(tx)
                .map(Coprod::Era3)
                .map_err(Coprod::Era3)
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
