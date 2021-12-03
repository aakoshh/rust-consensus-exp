use crate::{
    blockchain::{
        eras::{
            era1::{self, Era1},
            era2::Era2,
            era3::Era3,
            CoEra, Eras,
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
    fn has_ranking_block(&self, h: &EraRankingBlockHash<E>) -> StmResult<bool>;

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
    hash_to_height: TVar<im::HashMap<EraRankingBlockHash<E>, Height>>,
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
        for h in rheaders.iter() {
            hash_to_height.insert(h.0.clone(), h.1.height());
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
            c.insert(hashed.0.clone(), hashed.1.height());
            c
        })?;

        let mut rs = rs.as_ref().clone();
        rs.push_back(hashed);
        self.rankings.write(rs)
    }

    fn get_ranking_block_by_height(&self, h: Height) -> StmResult<Option<EraRankingBlock<E>>> {
        let v = self.rankings.read()?;
        let b = v.head().filter(|b| h >= b.1.height()).and_then(|b| {
            let idx = (h - b.1.height()) as usize;
            v.get(idx).map(|h| h.1.clone())
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
            .map_or(Ok(None), |h| self.get_ranking_block_by_height(*h))
    }

    fn has_ranking_block(&self, h: &EraRankingBlockHash<E>) -> StmResult<bool> {
        self.hash_to_height.read().map(|m| m.contains_key(h))
    }

    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()> {
        let v = self.rankings.read()?;
        if let Some(b) = v.head().filter(|b| h >= b.1.height()) {
            // Index of the first item to remove.
            // To remove the 0th item, the height has to be less than what it has.
            let idx = (h + 1 - b.1.height()) as usize;
            if v.len() <= idx {
                return Ok(());
            }

            let mut v = v.as_ref().clone();
            let removed = v.split_off(idx);
            if !removed.is_empty() {
                self.rankings.write(v)?;
                self.hash_to_height.update(|m| {
                    let mut c = m.clone();
                    for b in removed {
                        c.remove(&b.0);
                    }
                    c
                })?;
            }
        }
        Ok(())
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

    fn has_ranking_block(&self, h: &EraRankingBlockHash<Era2>) -> StmResult<bool> {
        self.0.has_ranking_block(h)
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

    fn has_ranking_block(&self, h: &EraRankingBlockHash<Era3>) -> StmResult<bool> {
        self.0.has_ranking_block(h)
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
