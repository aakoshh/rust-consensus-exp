use crate::{
    blockchain::{
        eras::{era1, era2},
        property::*,
    },
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

/// Calculate the hash of an item before it's stored.
#[derive(Clone)]
struct Hashed<T: HasHash<'static>>(T::Hash, T);

impl<T: HasHash<'static>> Hashed<T> {
    fn new(value: T) -> Hashed<T> {
        Hashed(value.hash(), value)
    }
}

/// Block store for era 1.
pub struct BlockStore1 {
    headers: TVar<im::Vector<Hashed<era1::BlockHeader>>>,
    hash_to_height: TVar<im::HashMap<era1::BlockHash, usize>>,
}

impl BlockStore1 {
    pub fn new(genesis: era1::BlockHeader, blocks: Vec<era1::BlockHeader>) -> BlockStore1 {
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

        BlockStore1 {
            headers: TVar::new(headers),
            hash_to_height: TVar::new(hash_to_height),
        }
    }
}

impl BlockStore<era1::Era1> for BlockStore1 {
    fn max_height(&self) -> StmResult<Height> {
        self.hash_to_height.read().map(|v| v.len() as Height - 1)
    }

    fn add_ranking_block(&self, b: EraRankingBlock<era1::Era1>) -> StmResult<()> {
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

    fn get_ranking_block_by_height(
        &self,
        h: Height,
    ) -> StmResult<Option<EraRankingBlock<era1::Era1>>> {
        match self.headers.read()?.get(h as usize) {
            None => Ok(None),
            Some(h) => Ok(Some(h.1.clone())),
        }
    }

    fn get_ranking_block_by_hash(
        &self,
        h: &EraRankingBlockHash<era1::Era1>,
    ) -> StmResult<Option<EraRankingBlock<era1::Era1>>> {
        match self.hash_to_height.read()?.get(h) {
            None => Ok(None),
            Some(h) => self.get_ranking_block_by_height(*h as Height),
        }
    }

    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()> {
        let headers = self.headers.read()?;

        if headers.len() as Height <= h + 1 {
            return Ok(());
        }

        let mut headers = headers.as_ref().clone();
        let removed = headers.split_off((h + 1) as usize);
        self.headers.write(headers)?;

        self.hash_to_height.update(|m| {
            let mut c = m.clone();
            for h in removed {
                c.remove(&h.0);
            }
            c
        })
    }

    fn add_input_block_header(&self, h: EraInputBlockHeader<era1::Era1>) -> StmResult<()> {
        // In this era the two are the same, so in fact this method should never be called.
        self.add_ranking_block(h)
    }

    fn has_input_block_header(&self, h: &EraInputBlockHash<era1::Era1>) -> StmResult<bool> {
        self.hash_to_height.read().map(|m| m.contains_key(h))
    }

    fn get_input_block_header_by_hash(
        &self,
        h: &EraInputBlockHash<era1::Era1>,
    ) -> StmResult<Option<EraInputBlockHeader<era1::Era1>>> {
        // In this era the two are the same.
        self.get_ranking_block_by_hash(h)
    }
}

/// Block store for era 2.
pub struct BlockStore2 {
    rankings: TVar<im::HashMap<era2::RankingBlockHash, era2::RankingBlock>>,
    inputs: TVar<im::HashMap<era2::InputBlockHash, era2::InputBlockHeader>>,
    height_to_hash: TVar<im::Vector<era2::RankingBlockHash>>,
    min_height: TVar<u64>,
}

// impl BlockStore2 {
//     pub fn new(
//         rankings: Vec<era2::RankingBlock>,
//         inputs: Vec<era2::InputBlockHeader>,
//     ) -> BlockStore2 {

//     }
// }
