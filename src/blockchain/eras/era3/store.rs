use crate::{
    blockchain::{
        eras::era3::Era3,
        property::*,
        store::{block::BlockStoreRnI, BlockStore, StoreError},
    },
    stm::{abort, atomically, StmDynResult, StmResult},
};

/// Block store for era 3.
pub struct BlockStore3(BlockStoreRnI<Era3>);

impl BlockStore3 {
    pub fn new(
        rankings: Vec<EraRankingBlock<Era3>>,
        inputs: Vec<EraInputBlockHeader<Era3>>,
    ) -> Self {
        let store = Self(BlockStoreRnI::new(rankings, inputs));

        // Sanity check that all input blocks reference existing inputs.
        let inputs = atomically(|| store.0.inputs.read());

        for h in inputs.values() {
            for i in &h.parent_hashes {
                assert!(inputs.contains_key(&i))
            }
        }

        store
    }
}

impl BlockStore<Era3> for BlockStore3 {
    fn first_ranking_block(&self) -> StmResult<Option<EraRankingBlock<Era3>>> {
        self.0.first_ranking_block()
    }

    fn last_ranking_block(&self) -> StmResult<Option<EraRankingBlock<Era3>>> {
        self.0.last_ranking_block()
    }

    fn add_ranking_block(&self, b: EraRankingBlock<Era3>) -> StmDynResult<()> {
        self.0.add_ranking_block(b)
    }

    fn get_ranking_block_by_height(&self, h: Height) -> StmResult<Option<EraRankingBlock<Era3>>> {
        self.0.get_ranking_block_by_height(h)
    }

    fn get_ranking_block_by_hash(
        &self,
        h: &EraRankingBlockHash<Era3>,
    ) -> StmResult<Option<EraRankingBlock<Era3>>> {
        self.0.get_ranking_block_by_hash(h)
    }

    fn has_ranking_block(&self, h: &EraRankingBlockHash<Era3>) -> StmResult<bool> {
        self.0.has_ranking_block(h)
    }

    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()> {
        self.0.remove_ranking_blocks_above_height(h)
    }

    fn add_input_block_header(&self, h: EraInputBlockHeader<Era3>) -> StmDynResult<()> {
        // Sanity check that all referenced input blocks have already been added.
        for i in &h.parent_hashes {
            if !self.has_input_block_header(&i)? {
                return abort(StoreError::MissingInputs);
            }
        }
        self.0.add_input_block_header(h)
    }

    fn has_input_block_header(&self, h: &EraInputBlockHash<Era3>) -> StmResult<bool> {
        self.0.has_input_block_header(h)
    }

    fn get_input_block_header_by_hash(
        &self,
        h: &EraInputBlockHash<Era3>,
    ) -> StmResult<Option<EraInputBlockHeader<Era3>>> {
        self.0.get_input_block_header_by_hash(h)
    }
}
