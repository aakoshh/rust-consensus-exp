use crate::{
    blockchain::{
        eras::{
            era1::{self, Era1},
            era2::Era2,
            era3::Era3,
        },
        property::*,
    },
    stm::{abort, atomically, atomically_or_err, StmResult, TVar},
};

use super::StoreError;

/// Block level storage operations. There will be a hierarchy of these,
/// with the era specific implementations at the bottom.
///
/// In this example we pretend that we only care about the main chain.
pub trait BlockStore<E: Era> {
    fn first_ranking_block(&self) -> StmResult<Option<EraRankingBlock<E>>>;
    fn last_ranking_block(&self) -> StmResult<Option<EraRankingBlock<E>>>;

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
struct Hashed<T: HasHash>(T::Hash, T);

impl<T: HasHash> Hashed<T> {
    fn new(value: T) -> Self {
        Self(value.hash(), value)
    }
}

impl<B: RankingBlock> Hashed<B> {
    /// Are two block within the same era in a parent-child relationship?
    fn is_parent_of(&self, other: &Self) -> bool {
        match other.1.parent_hash() {
            Crossing::Curr(p) => self.0 == p,
            Crossing::Prev(_) => false,
        }
    }
}

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

    fn add_input_block_header(&self, h: EraInputBlockHeader<Era1>) -> StmResult<()> {
        // In this era the two are the same, so in fact this method should never be called.
        self.add_ranking_block(h)
    }

    fn has_input_block_header(&self, h: &EraInputBlockHash<Era1>) -> StmResult<bool> {
        self.hash_to_height.read().map(|m| m.contains_key(h))
    }

    fn get_input_block_header_by_hash(
        &self,
        h: &EraInputBlockHash<Era1>,
    ) -> StmResult<Option<EraInputBlockHeader<Era1>>> {
        // In this era the two are the same.
        self.get_ranking_block_by_hash(h)
    }
}

/// Generic block store for ranking and input blocks.
struct BlockStoreRnI<E: Era> {
    rankings: TVar<im::Vector<Hashed<EraRankingBlock<E>>>>,
    inputs: TVar<im::HashMap<EraInputBlockHash<E>, EraInputBlockHeader<E>>>,
    hash_to_height: TVar<im::HashMap<EraRankingBlockHash<E>, usize>>,
}

impl<E: Era + 'static> BlockStoreRnI<E> {
    fn new(rankings: Vec<EraRankingBlock<E>>, inputs: Vec<EraInputBlockHeader<E>>) -> Self {
        let mut iheaders = im::HashMap::new();
        for h in inputs {
            iheaders.insert(h.hash(), h);
        }

        let mut rheaders = im::Vector::new();
        rheaders.extend(rankings.into_iter().map(Hashed::new));

        let mut hash_to_height = im::HashMap::new();
        for (idx, h) in rheaders.iter().enumerate() {
            hash_to_height.insert(h.0.clone(), idx);
        }

        // Sanity check that this is a chain.
        for (p, c) in rheaders.iter().zip(rheaders.iter().skip(1)) {
            assert!(p.is_parent_of(c));
        }

        // Sanity check that all inputs are present.
        for r in &rheaders {
            for i in r.1.input_block_hashes() {
                assert!(iheaders.contains_key(&i));
            }
        }

        BlockStoreRnI {
            rankings: TVar::new(rheaders),
            inputs: TVar::new(iheaders),
            hash_to_height: TVar::new(hash_to_height),
        }
    }
}

impl<E: Era + 'static> BlockStore<E> for BlockStoreRnI<E> {
    fn first_ranking_block(&self) -> StmResult<Option<EraRankingBlock<E>>> {
        self.rankings.read().map(|v| v.front().map(|h| h.1.clone()))
    }

    fn last_ranking_block(&self) -> StmResult<Option<EraRankingBlock<E>>> {
        self.rankings.read().map(|v| v.back().map(|h| h.1.clone()))
    }

    fn add_ranking_block(&self, b: EraRankingBlock<E>) -> StmResult<()> {
        let is = self.inputs.read()?;
        for h in b.input_block_hashes() {
            if !is.contains_key(&h) {
                return abort(StoreError::MissingInputs);
            }
        }

        let rs = self.rankings.read()?;
        let hashed = Hashed::new(b);

        if let Some(tip) = rs.last() {
            if !tip.is_parent_of(&hashed) {
                return abort(StoreError::DoesNotExtendChain);
            }
        }

        self.hash_to_height.update(|m| {
            let mut c = m.clone();
            c.insert(hashed.0.clone(), rs.len());
            c
        })?;

        let mut rs = rs.as_ref().clone();
        rs.push_back(hashed);
        self.rankings.write(rs)
    }

    fn get_ranking_block_by_height(&self, h: Height) -> StmResult<Option<EraRankingBlock<E>>> {
        let v = self.rankings.read()?;
        let b = v.head().and_then(|b| {
            let min_height = b.1.height();
            v.get((h - min_height) as usize).map(|h| h.1.clone())
        });
        Ok(b)
    }

    fn get_ranking_block_by_hash(
        &self,
        h: &EraRankingBlockHash<E>,
    ) -> StmResult<Option<EraRankingBlock<E>>> {
        self.hash_to_height
            .read()?
            .get(h)
            .map_or(Ok(None), |h| self.get_ranking_block_by_height(*h as Height))
    }

    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()> {
        todo!()
    }

    fn add_input_block_header(&self, h: EraInputBlockHeader<E>) -> StmResult<()> {
        self.inputs.update(|m| {
            let mut c = m.clone();
            c.insert(h.hash(), h);
            c
        })
    }

    fn has_input_block_header(&self, h: &EraInputBlockHash<E>) -> StmResult<bool> {
        self.inputs.read().map(|m| m.contains_key(h))
    }

    fn get_input_block_header_by_hash(
        &self,
        h: &EraInputBlockHash<E>,
    ) -> StmResult<Option<EraInputBlockHeader<E>>> {
        self.inputs.read().map(|m| m.get(h).map(|h| h.clone()))
    }
}

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

    fn add_ranking_block(&self, b: EraRankingBlock<Era2>) -> StmResult<()> {
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

    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()> {
        self.0.remove_ranking_blocks_above_height(h)
    }

    fn add_input_block_header(&self, h: EraInputBlockHeader<Era2>) -> StmResult<()> {
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

/// Block store for era 2.
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

    fn add_ranking_block(&self, b: EraRankingBlock<Era3>) -> StmResult<()> {
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

    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()> {
        self.0.remove_ranking_blocks_above_height(h)
    }

    fn add_input_block_header(&self, h: EraInputBlockHeader<Era3>) -> StmResult<()> {
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

/// Block store imlementation for the coproduct of eras.
pub struct CoBlockStore {
    block_store_1: BlockStore1,
    block_store_2: BlockStore2,
    block_store_3: BlockStore3,
}

impl CoBlockStore {
    pub fn new(
        block_store_1: BlockStore1,
        block_store_2: BlockStore2,
        block_store_3: BlockStore3,
    ) -> Self {
        // Sanity check that the stores form a single chain.
        CoBlockStore::assert_continuation(&block_store_1, &block_store_2);
        CoBlockStore::assert_continuation(&block_store_2, &block_store_3);

        Self {
            block_store_1,
            block_store_2,
            block_store_3,
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
}
