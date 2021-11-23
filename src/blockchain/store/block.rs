use crate::{
    blockchain::{eras::era1, property::*},
    stm::{abort, StmResult, TVar},
};

use super::StoreError;

/// Block level storage operations. There will be a hierarchy of these,
/// with the era specific implementations at the bottom.
///
/// In this example we pretend that we only care about the main chain.
pub trait BlockStore<E: Era> {
    fn max_height(&self) -> StmResult<Height>;
    /// Extend the chain with a ranking block.
    fn add_ranking_block(&self, b: EraRankingBlock<E>) -> StmResult<()>;
    fn get_ranking_block_by_height(&self, h: Height) -> StmResult<Option<EraRankingBlock<E>>>;
    fn get_ranking_block_by_hash(
        &self,
        h: &EraRankingBlockHash<E>,
    ) -> StmResult<Option<EraRankingBlock<E>>>;

    /// Roll back all ranking blocks from the top, all the way back to a certain height.
    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()>;

    /// Add an input block referenced by a ranking block.
    fn add_input_block_header(&self, h: EraInputBlockHeader<E>) -> StmResult<()>;
    fn has_input_block_header(&self, h: &EraInputBlockHash<E>) -> StmResult<bool>;
    fn get_input_block_header_by_hash(
        &self,
        h: &EraInputBlockHash<E>,
    ) -> StmResult<Option<EraInputBlockHeader<E>>>;
}

struct BlockStore1 {
    headers: TVar<im::HashMap<era1::BlockHash, era1::BlockHeader>>,
    height_to_hash: TVar<im::Vector<era1::BlockHash>>,
}

impl BlockStore1 {
    fn new(genesis: era1::BlockHeader, blocks: Vec<era1::BlockHeader>) -> BlockStore1 {
        let mut all = vec![(genesis.hash(), genesis)];
        all.extend(blocks.into_iter().map(|b| (b.hash(), b)));

        // Sanity check that this is a chain.
        for (p, c) in all.iter().zip(all.iter().skip(1)) {
            assert_eq!(p.0, c.1.parent_hash);
        }

        let mut height_to_hash = im::Vector::new();
        height_to_hash.extend(all.iter().map(|(h, _)| h.clone()));

        let mut headers = im::HashMap::new();
        for (h, b) in all {
            headers.insert(h, b);
        }

        BlockStore1 {
            headers: TVar::new(headers),
            height_to_hash: TVar::new(height_to_hash),
        }
    }
}

impl BlockStore<era1::Era1> for BlockStore1 {
    fn max_height(&self) -> StmResult<Height> {
        self.height_to_hash.read().map(|v| v.len() as Height - 1)
    }

    fn add_ranking_block(&self, b: EraRankingBlock<era1::Era1>) -> StmResult<()> {
        let height_to_hash = self.height_to_hash.read()?;
        let last_hash = height_to_hash.last().unwrap();
        let parent_hash = uncross(b.parent_hash());

        if &parent_hash != last_hash {
            abort(StoreError::DoesNotExtendChain)
        } else {
            let hash = b.hash();

            self.headers.update(|h| {
                let mut c = h.clone();
                c.insert(hash.clone(), b);
                c
            })?;

            self.height_to_hash.update(|h| {
                let mut c = h.clone();
                c.push_back(hash);
                c
            })
        }
    }

    fn get_ranking_block_by_height(
        &self,
        h: Height,
    ) -> StmResult<Option<EraRankingBlock<era1::Era1>>> {
        match self.height_to_hash.read()?.get(h as usize) {
            None => Ok(None),
            Some(h) => self.get_ranking_block_by_hash(h),
        }
    }

    fn get_ranking_block_by_hash(
        &self,
        h: &EraRankingBlockHash<era1::Era1>,
    ) -> StmResult<Option<EraRankingBlock<era1::Era1>>> {
        self.headers.read().map(|m| m.get(h).map(|h| h.clone()))
    }

    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()> {
        let height_to_hash = self.height_to_hash.read()?;

        if height_to_hash.len() as Height <= h + 1 {
            return Ok(());
        }

        let mut height_to_hash = self.height_to_hash.read_clone()?;
        let removed = height_to_hash.split_off((h + 1) as usize);
        self.height_to_hash.write(height_to_hash)?;

        let mut headers = self.headers.read_clone()?;
        for hash in removed {
            headers.remove(&hash);
        }
        self.headers.write(headers)
    }

    fn add_input_block_header(&self, h: EraInputBlockHeader<era1::Era1>) -> StmResult<()> {
        // In this era the two are the same, so in fact this method should never be called.
        self.add_ranking_block(h)
    }

    fn has_input_block_header(&self, h: &EraInputBlockHash<era1::Era1>) -> StmResult<bool> {
        self.headers.read().map(|m| m.contains_key(h))
    }

    fn get_input_block_header_by_hash(
        &self,
        h: &EraInputBlockHash<era1::Era1>,
    ) -> StmResult<Option<EraInputBlockHeader<era1::Era1>>> {
        // In this era the two are the same.
        self.get_ranking_block_by_hash(h)
    }
}
