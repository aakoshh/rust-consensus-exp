use std::sync::Arc;

use crate::{
    blockchain::property::*,
    stm::{abort, atomically, StmResult, TVar},
};

use super::{block::BlockStore, StoreError};

/// Overall chain level storage, spanning all the eras.
pub struct ChainStore<E: Era, S> {
    block_store: S,
    // Not in the `BlockStore` because only the first era has "the" genesis block.
    genesis: EraRankingBlock<E>,
    // Not in the `BlockStore` because the tip only makes sense for the last era.
    tip: TVar<EraRankingBlock<E>>,
}

impl<E: Era + 'static, S: BlockStore<E>> ChainStore<E, S> {
    pub fn new(block_store: S) -> ChainStore<E, S> {
        let (genesis, tip) = atomically(|| {
            let h = block_store.max_height()?;
            let t = block_store.get_ranking_block_by_height(h)?;
            let g = block_store.get_ranking_block_by_height(0)?;
            Ok((g.unwrap(), t.unwrap()))
        });

        ChainStore {
            block_store,
            genesis,
            tip: TVar::new(tip),
        }
    }

    pub fn tip(&self) -> StmResult<Arc<EraRankingBlock<E>>> {
        self.tip.read()
    }

    /// Roll back to a given block hash.
    ///
    /// Return `false` if the hash is unknown, `true` if we successfully rolled back.
    pub fn rollback_to_hash(&self, h: &EraRankingBlockHash<E>) -> StmResult<bool> {
        if let Some(b) = self.block_store.get_ranking_block_by_hash(h)? {
            self.remove_ranking_blocks_above_height(b.height())?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// A `ChainStore` is a facade for the `BlockStore`, while also maintaining the tip.
impl<E: Era + 'static, S: BlockStore<E>> BlockStore<E> for ChainStore<E, S> {
    fn max_height(&self) -> StmResult<Height> {
        self.tip.read().map(|b| b.height())
    }

    fn add_ranking_block(&self, b: EraRankingBlock<E>) -> StmResult<()> {
        let tip = self.tip.read()?;
        // We're in the context of a single era, so the parent will always be `Curr`,
        // by the virtue of the overall era repackaging the wrappers.
        if b.parent_hash() != Crossing::Curr(tip.hash()) {
            abort(StoreError::DoesNotExtendChain)
        } else {
            self.tip.write(b.clone())?;
            self.block_store.add_ranking_block(b)
        }
    }

    fn get_ranking_block_by_height(&self, h: Height) -> StmResult<Option<EraRankingBlock<E>>> {
        self.block_store.get_ranking_block_by_height(h)
    }

    fn get_ranking_block_by_hash(
        &self,
        h: &EraRankingBlockHash<E>,
    ) -> StmResult<Option<EraRankingBlock<E>>> {
        self.block_store.get_ranking_block_by_hash(h)
    }

    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()> {
        self.block_store
            .get_ranking_block_by_height(h)?
            .map_or(Ok(()), |b| {
                self.block_store.remove_ranking_blocks_above_height(h)?;
                self.tip.write(b)
            })
    }

    fn add_input_block_header(&self, h: EraInputBlockHeader<E>) -> StmResult<()> {
        self.block_store.add_input_block_header(h)
    }

    fn has_input_block_header(&self, h: &EraInputBlockHash<E>) -> StmResult<bool> {
        self.block_store.has_input_block_header(h)
    }

    fn get_input_block_header_by_hash(
        &self,
        h: &EraInputBlockHash<E>,
    ) -> StmResult<Option<EraInputBlockHeader<E>>> {
        self.block_store.get_input_block_header_by_hash(h)
    }
}
