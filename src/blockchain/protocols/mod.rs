pub mod sync;

use crate::blockchain::property::*;

type EraRankingBlock<E: Era> = E::RankingBlock<'static>;
type EraRankingBlockHash<E: Era> = <E::RankingBlock<'static> as HasHash<'static>>::Hash;
type EraInputBlockHash<E: Era> = <E::InputBlock<'static> as HasHash<'static>>::Hash;
type EraInputBlockHeader<E: Era> = <E::InputBlock<'static> as HasHeader>::Header;
