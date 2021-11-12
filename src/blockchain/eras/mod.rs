use crate::blockchain::property::Era;

use era1::Era1;
use era2::Era2;
use era3::Era3;

use super::{
    property::{HasHash, HasTransactions},
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

type CoTransaction =
    Coprod<<Era1 as Era>::Transaction, <Era2 as Era>::Transaction, <Era3 as Era>::Transaction>;

type CoInputBlock =
    Coprod<<Era1 as Era>::InputBlock, <Era2 as Era>::InputBlock, <Era3 as Era>::InputBlock>;

type CoRankingBlock =
    Coprod<<Era1 as Era>::RankingBlock, <Era2 as Era>::RankingBlock, <Era3 as Era>::RankingBlock>;

type CoLedger = Coprod<<Era1 as Era>::Ledger, <Era2 as Era>::Ledger, <Era3 as Era>::Ledger>;

type CoInputBlockHash = Coprod<
    <<Era1 as Era>::InputBlock as HasHash>::Hash,
    <<Era2 as Era>::InputBlock as HasHash>::Hash,
    <<Era3 as Era>::InputBlock as HasHash>::Hash,
>;

impl Into<CryptoHash> for CoInputBlockHash {
    fn into(self) -> CryptoHash {
        match self {
            Coprod::Era1(hash) => hash.into(),
            Coprod::Era2(hash) => hash.into(),
            Coprod::Era3(hash) => hash.into(),
        }
    }
}

impl HasHash for CoInputBlock {
    type Hash = CoInputBlockHash;

    fn hash(&self) -> Self::Hash {
        todo!()
    }
}

// impl HasTransactions for CoInputBlock {
//     type Transaction = CoTransaction;
//     type Transactions<'a> = impl Iterator<Item = CoTransaction>;

//     fn transactions<'a>(&'a self) -> Self::Transactions<'a> {
//         match self {
//             Coprod::Era1(block) => {
//                 let it = block.transactions().map(|tx| &Coprod::Era1(tx));
//             }
//             Coprod::Era2(block) => todo!(),
//             Coprod::Era3(block) => todo!(),
//         }
//     }
// }

/// The era that encompasses all eras, over which will
/// define our consensus and related instances.
pub struct CoEra;

// impl Era for CoEra {
//     type Transaction = CoTransaction;
//     type InputBlock = CoInputBlock;
//     type RankingBlock = CoRankingBlock;
//     type Ledger = CoLedger;
// }
