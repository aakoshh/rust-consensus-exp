pub mod sync;

use crate::blockchain::property::*;

type RankingBlock<E: Era> = E::RankingBlock<'static>;
type RankingBlockHash<E: Era> = <E::RankingBlock<'static> as HasHash<'static>>::Hash;
type InputBlockHash<E: Era> = <E::InputBlock<'static> as HasHash<'static>>::Hash;
type InputBlockHeader<E: Era> = <E::InputBlock<'static> as HasHeader>::Header;
