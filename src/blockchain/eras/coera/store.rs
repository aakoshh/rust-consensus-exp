use crate::{
    blockchain::{
        property::*,
        store::{BlockStore, StoreError},
    },
    stm::{abort, atomically_or_err, StmResult},
};

use crate::blockchain::eras::{
    era1::store::BlockStore1, era2::store::BlockStore2, era3::store::BlockStore3,
};

use super::{CoEra, Eras};

/// Block store imlementation for the coproduct of eras.
pub struct CoBlockStore {
    store1: BlockStore1,
    store2: BlockStore2,
    store3: BlockStore3,
}

impl CoBlockStore {
    pub fn new(store1: BlockStore1, store2: BlockStore2, store3: BlockStore3) -> Self {
        // Sanity check that the stores form a single chain.
        CoBlockStore::assert_continuation(&store1, &store2);
        CoBlockStore::assert_continuation(&store2, &store3);

        Self {
            store1,
            store2,
            store3,
        }
    }

    fn assert_continuation<R1, R2, E1, E2, S1, S2>(s1: &S1, s2: &S2)
    where
        R1: RankingBlock,
        R2: RankingBlock<PrevEraHash = <R1 as HasHash>::Hash>,
        E1: Era<RankingBlock = R1>,
        E2: Era<RankingBlock = R2>,
        S1: BlockStore<E1>,
        S2: BlockStore<E2>,
    {
        atomically_or_err(|| {
            let last1 = s1.last_ranking_block()?;
            let first2 = s2.first_ranking_block()?;

            if let (Some(parent), Some(child)) = (&last1, &first2) {
                if child.parent_hash() != Crossing::Prev(parent.hash()) {
                    return abort(StoreError::DoesNotExtendChain);
                }
            } else if last1.is_none() && first2.is_some() {
                return abort(StoreError::DoesNotExtendChain);
            }

            Ok(())
        })
        .unwrap()
    }

    fn earliest<F1, F2, F3, T>(&self, f1: F1, f2: F2, f3: F3) -> StmResult<Option<T>>
    where
        F1: Fn(&BlockStore1) -> StmResult<Option<T>>,
        F2: Fn(&BlockStore2) -> StmResult<Option<T>>,
        F3: Fn(&BlockStore3) -> StmResult<Option<T>>,
    {
        if let Some(v) = f1(&self.store1)? {
            Ok(Some(v))
        } else if let Some(v) = f2(&self.store2)? {
            Ok(Some(v))
        } else if let Some(v) = f3(&self.store3)? {
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    fn latest<F1, F2, F3, T>(&self, f1: F1, f2: F2, f3: F3) -> StmResult<Option<T>>
    where
        F1: Fn(&BlockStore1) -> StmResult<Option<T>>,
        F2: Fn(&BlockStore2) -> StmResult<Option<T>>,
        F3: Fn(&BlockStore3) -> StmResult<Option<T>>,
    {
        if let Some(v) = f3(&self.store3)? {
            Ok(Some(v))
        } else if let Some(v) = f2(&self.store2)? {
            Ok(Some(v))
        } else if let Some(v) = f1(&self.store1)? {
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }
}

impl BlockStore<CoEra> for CoBlockStore {
    fn first_ranking_block(&self) -> StmResult<Option<EraRankingBlock<CoEra>>> {
        self.earliest(
            |s| s.first_ranking_block().map(|b| b.map(Eras::Era1)),
            |s| s.first_ranking_block().map(|b| b.map(Eras::Era2)),
            |s| s.first_ranking_block().map(|b| b.map(Eras::Era3)),
        )
    }

    fn last_ranking_block(&self) -> StmResult<Option<EraRankingBlock<CoEra>>> {
        self.latest(
            |s| s.last_ranking_block().map(|b| b.map(Eras::Era1)),
            |s| s.last_ranking_block().map(|b| b.map(Eras::Era2)),
            |s| s.last_ranking_block().map(|b| b.map(Eras::Era3)),
        )
    }

    fn add_ranking_block(&self, b: EraRankingBlock<CoEra>) -> StmResult<()> {
        match b {
            Eras::Era1(b) => self.store1.add_ranking_block(b),
            Eras::Era2(b) => self.store2.add_ranking_block(b),
            Eras::Era3(b) => self.store3.add_ranking_block(b),
        }
    }

    fn get_ranking_block_by_height(&self, h: Height) -> StmResult<Option<EraRankingBlock<CoEra>>> {
        self.latest(
            |s| s.get_ranking_block_by_height(h).map(|b| b.map(Eras::Era1)),
            |s| s.get_ranking_block_by_height(h).map(|b| b.map(Eras::Era2)),
            |s| s.get_ranking_block_by_height(h).map(|b| b.map(Eras::Era3)),
        )
    }

    fn get_ranking_block_by_hash(
        &self,
        h: &EraRankingBlockHash<CoEra>,
    ) -> StmResult<Option<EraRankingBlock<CoEra>>> {
        match h {
            Eras::Era1(h) => self
                .store1
                .get_ranking_block_by_hash(h)
                .map(|b| b.map(Eras::Era1)),
            Eras::Era2(h) => self
                .store2
                .get_ranking_block_by_hash(h)
                .map(|b| b.map(Eras::Era2)),
            Eras::Era3(h) => self
                .store3
                .get_ranking_block_by_hash(h)
                .map(|b| b.map(Eras::Era3)),
        }
    }

    fn has_ranking_block(&self, h: &EraRankingBlockHash<CoEra>) -> StmResult<bool> {
        match h {
            Eras::Era1(h) => self.store1.has_ranking_block(h),
            Eras::Era2(h) => self.store2.has_ranking_block(h),
            Eras::Era3(h) => self.store3.has_ranking_block(h),
        }
    }

    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()> {
        self.store1.remove_ranking_blocks_above_height(h)?;
        self.store2.remove_ranking_blocks_above_height(h)?;
        self.store3.remove_ranking_blocks_above_height(h)?;
        Ok(())
    }

    fn add_input_block_header(&self, h: EraInputBlockHeader<CoEra>) -> StmResult<()> {
        match h {
            Eras::Era1(h) => self.store1.add_input_block_header(h),
            Eras::Era2(h) => self.store2.add_input_block_header(h),
            Eras::Era3(h) => self.store3.add_input_block_header(h),
        }
    }

    fn has_input_block_header(&self, h: &EraInputBlockHash<CoEra>) -> StmResult<bool> {
        match h {
            Eras::Era1(h) => self.store1.has_input_block_header(h),
            Eras::Era2(h) => self.store2.has_input_block_header(h),
            Eras::Era3(h) => self.store3.has_input_block_header(h),
        }
    }

    fn get_input_block_header_by_hash(
        &self,
        h: &EraInputBlockHash<CoEra>,
    ) -> StmResult<Option<EraInputBlockHeader<CoEra>>> {
        match h {
            Eras::Era1(h) => self
                .store1
                .get_input_block_header_by_hash(h)
                .map(|b| b.map(Eras::Era1)),
            Eras::Era2(h) => self
                .store2
                .get_input_block_header_by_hash(h)
                .map(|b| b.map(Eras::Era2)),
            Eras::Era3(h) => self
                .store3
                .get_input_block_header_by_hash(h)
                .map(|b| b.map(Eras::Era3)),
        }
    }
}
