use crate::{
    blockchain::{
        eras::era2::Era2,
        property::*,
        store::{block::BlockStoreRnI, BlockStore},
    },
    stm::{StmDynResult, StmResult},
};

/// Block store for era 2.
pub struct BlockStore2(BlockStoreRnI<Era2>);

impl BlockStore2 {
    pub fn new(
        rankings: Vec<EraRankingBlock<Era2>>,
        inputs: Vec<EraInputBlockHeader<Era2>>,
    ) -> Self {
        Self(BlockStoreRnI::new(rankings, inputs))
    }
}

impl BlockStore<Era2> for BlockStore2 {
    fn first_ranking_block(&self) -> StmResult<Option<EraRankingBlock<Era2>>> {
        self.0.first_ranking_block()
    }

    fn last_ranking_block(&self) -> StmResult<Option<EraRankingBlock<Era2>>> {
        self.0.last_ranking_block()
    }

    fn add_ranking_block(&self, b: EraRankingBlock<Era2>) -> StmDynResult<()> {
        self.0.add_ranking_block(b)
    }

    fn get_ranking_block_by_height(&self, h: Height) -> StmResult<Option<EraRankingBlock<Era2>>> {
        self.0.get_ranking_block_by_height(h)
    }

    fn get_ranking_block_by_hash(
        &self,
        h: &EraRankingBlockHash<Era2>,
    ) -> StmResult<Option<EraRankingBlock<Era2>>> {
        self.0.get_ranking_block_by_hash(h)
    }

    fn has_ranking_block(&self, h: &EraRankingBlockHash<Era2>) -> StmResult<bool> {
        self.0.has_ranking_block(h)
    }

    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()> {
        self.0.remove_ranking_blocks_above_height(h)
    }

    fn add_input_block_header(&self, h: EraInputBlockHeader<Era2>) -> StmDynResult<()> {
        self.0.add_input_block_header(h)
    }

    fn has_input_block_header(&self, h: &EraInputBlockHash<Era2>) -> StmResult<bool> {
        self.0.has_input_block_header(h)
    }

    fn get_input_block_header_by_hash(
        &self,
        h: &EraInputBlockHash<Era2>,
    ) -> StmResult<Option<EraInputBlockHeader<Era2>>> {
        self.0.get_input_block_header_by_hash(h)
    }
}
