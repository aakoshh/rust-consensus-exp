use std::sync::Arc;

use crate::{
    blockchain::property::*,
    stm::{abort, atomically, StmDynResult, StmResult, TVar},
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
    pub fn new(block_store: S) -> Arc<Self> {
        // `ChainStore` represents a full chain, so it is expected to be called with a non-empty block store.
        let (genesis, tip) = atomically(|| {
            let g = block_store.first_ranking_block()?;
            let t = block_store.last_ranking_block()?;
            Ok((g.unwrap(), t.unwrap()))
        });

        Arc::new(Self {
            block_store,
            genesis,
            tip: TVar::new(tip),
        })
    }

    pub fn tip(&self) -> StmResult<Arc<EraRankingBlock<E>>> {
        self.tip.read()
    }

    pub fn genesis(&self) -> &EraRankingBlock<E> {
        &self.genesis
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
    fn first_ranking_block(&self) -> StmResult<Option<EraRankingBlock<E>>> {
        Ok(Some(self.genesis.clone()))
    }

    fn last_ranking_block(&self) -> StmResult<Option<EraRankingBlock<E>>> {
        self.tip.read().map(|t| Some(t.as_ref().clone()))
    }

    fn add_ranking_block(&self, b: EraRankingBlock<E>) -> StmDynResult<()> {
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

    fn has_ranking_block(&self, h: &EraRankingBlockHash<E>) -> StmResult<bool> {
        self.block_store.has_ranking_block(h)
    }

    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()> {
        self.block_store
            .get_ranking_block_by_height(h)?
            .map_or(Ok(()), |b| {
                self.block_store.remove_ranking_blocks_above_height(h)?;
                self.tip.write(b)
            })
    }

    fn add_input_block_header(&self, h: EraInputBlockHeader<E>) -> StmDynResult<()> {
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
