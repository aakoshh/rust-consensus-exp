use crate::{
    blockchain::property::*,
    stm::{abort, StmDynResult, StmResult, TVar},
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
    fn add_ranking_block(&self, b: EraRankingBlock<E>) -> StmDynResult<()>;
    fn get_ranking_block_by_height(&self, h: Height) -> StmResult<Option<EraRankingBlock<E>>>;
    fn get_ranking_block_by_hash(
        &self,
        h: &EraRankingBlockHash<E>,
    ) -> StmResult<Option<EraRankingBlock<E>>>;
    fn has_ranking_block(&self, h: &EraRankingBlockHash<E>) -> StmResult<bool>;

    /// Roll back all ranking blocks from the top, all the way back to a certain height.
    fn remove_ranking_blocks_above_height(&self, h: Height) -> StmResult<()>;

    /// Add an input block referenced by a ranking block.
    fn add_input_block_header(&self, h: EraInputBlockHeader<E>) -> StmDynResult<()>;
    fn has_input_block_header(&self, h: &EraInputBlockHash<E>) -> StmResult<bool>;
    fn get_input_block_header_by_hash(
        &self,
        h: &EraInputBlockHash<E>,
    ) -> StmResult<Option<EraInputBlockHeader<E>>>;
}

/// Calculate the hash of an item before it's stored.
#[derive(Clone)]
pub(crate) struct Hashed<T: HasHash>(pub T::Hash, pub T);

impl<T: HasHash> Hashed<T> {
    pub fn new(value: T) -> Self {
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

/// Generic block store for ranking and input blocks.
pub(crate) struct BlockStoreRnI<E: Era> {
    pub(crate) rankings: TVar<im::Vector<Hashed<EraRankingBlock<E>>>>,
    pub(crate) inputs: TVar<im::HashMap<EraInputBlockHash<E>, EraInputBlockHeader<E>>>,
    hash_to_height: TVar<im::HashMap<EraRankingBlockHash<E>, Height>>,
}

impl<E: Era + 'static> BlockStoreRnI<E> {
    pub fn new(rankings: Vec<EraRankingBlock<E>>, inputs: Vec<EraInputBlockHeader<E>>) -> Self {
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

    fn add_ranking_block(&self, b: EraRankingBlock<E>) -> StmDynResult<()> {
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
        self.rankings.write(rs)?;
        Ok(())
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

    fn add_input_block_header(&self, h: EraInputBlockHeader<E>) -> StmDynResult<()> {
        self.inputs
            .update(|m| {
                let mut c = m.clone();
                c.insert(h.hash(), h);
                c
            })
            .map_err(|e| e.into())
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
