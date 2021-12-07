use crate::{
    blockchain::{
        eras::era1::{self, Era1},
        property::*,
        store::{block::Hashed, BlockStore, StoreError},
    },
    stm::{abort, StmResult, TVar},
};

/// Block store for era 1.
pub struct BlockStore1 {
    headers: TVar<im::Vector<Hashed<era1::BlockHeader>>>,
    hash_to_height: TVar<im::HashMap<era1::BlockHash, usize>>,
}

impl BlockStore1 {
    pub fn new(genesis: era1::BlockHeader, blocks: Vec<era1::BlockHeader>) -> Self {
        let mut headers = im::Vector::new();
        headers.push_back(Hashed::new(genesis));
        headers.extend(blocks.into_iter().map(Hashed::new));

        // Sanity check that this is a chain.
        for (p, c) in headers.iter().zip(headers.iter().skip(1)) {
            assert_eq!(p.0, c.1.parent_hash);
        }

        let mut hash_to_height = im::HashMap::new();
        for (idx, h) in headers.iter().enumerate() {
            hash_to_height.insert(h.0.clone(), idx);
        }

        Self {
            headers: TVar::new(headers),
            hash_to_height: TVar::new(hash_to_height),
        }
    }
}

impl BlockStore<Era1> for BlockStore1 {
    fn first_ranking_block(&self) -> StmResult<Option<EraRankingBlock<Era1>>> {
        self.headers.read().map(|v| v.front().map(|h| h.1.clone()))
    }

    fn last_ranking_block(&self) -> StmResult<Option<EraRankingBlock<Era1>>> {
        self.headers.read().map(|v| v.back().map(|h| h.1.clone()))
    }

    fn add_ranking_block(&self, b: EraRankingBlock<Era1>) -> StmResult<()> {
        let headers = self.headers.read()?;
        let last_hash = headers.last().unwrap().0.clone();
        let parent_hash = uncross(b.parent_hash());

        if parent_hash != last_hash {
            abort(StoreError::DoesNotExtendChain)
        } else {
            let hashed = Hashed::new(b);

            self.hash_to_height.update(|h| {
                let mut c = h.clone();
                c.insert(hashed.0.clone(), headers.len());
                c
            })?;

            self.headers.update(|h| {
                let mut c = h.clone();
                c.push_back(hashed);
                c
            })?;

            Ok(())
        }
    }

    fn get_ranking_block_by_height(&self, h: Height) -> StmResult<Option<EraRankingBlock<Era1>>> {
        match self.headers.read()?.get(h as usize) {
            None => Ok(None),
            Some(h) => Ok(Some(h.1.clone())),
        }
    }

    fn get_ranking_block_by_hash(
        &self,
        h: &EraRankingBlockHash<Era1>,
    ) -> StmResult<Option<EraRankingBlock<Era1>>> {
        match self.hash_to_height.read()?.get(h) {
            None => Ok(None),
            Some(h) => self.get_ranking_block_by_height(*h as Height),
        }
    }

    fn has_ranking_block(&self, h: &EraRankingBlockHash<Era1>) -> StmResult<bool> {
        self.hash_to_height.read().map(|m| m.contains_key(h))
    }

    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()> {
        let headers = self.headers.read()?;

        // Index of the first element to remove.
        let idx = (h + 1) as usize;
        if headers.len() <= idx {
            return Ok(());
        }

        let mut headers = headers.as_ref().clone();
        let removed = headers.split_off(idx);
        self.headers.write(headers)?;

        self.hash_to_height.update(|m| {
            let mut c = m.clone();
            for h in removed {
                c.remove(&h.0);
            }
            c
        })
    }

    fn add_input_block_header(&self, h: EraInputBlockHeader<Era1>) -> StmResult<()> {
        // In this era the two are the same, so in fact this method should never be called.
        self.add_ranking_block(h)
    }

    fn has_input_block_header(&self, h: &EraInputBlockHash<Era1>) -> StmResult<bool> {
        // In this era the two are the same.
        self.has_ranking_block(h)
    }

    fn get_input_block_header_by_hash(
        &self,
        h: &EraInputBlockHash<Era1>,
    ) -> StmResult<Option<EraInputBlockHeader<Era1>>> {
        // In this era the two are the same.
        self.get_ranking_block_by_hash(h)
    }
}
