use crate::{
    blockchain::{
        property::*,
        protocols::{EraRankingBlock, EraRankingBlockHash},
    },
    stm::{StmResult, TVar},
};

pub struct ChainState<E: Era> {
    chain: TVar<Vec<EraRankingBlock<E>>>,
}

impl<E: Era + 'static> ChainState<E> {
    /// In this example pretend that we only keep track of the main chain.
    pub fn new(genesis: EraRankingBlock<E>, mut blocks: Vec<EraRankingBlock<E>>) -> ChainState<E> {
        let mut chain = vec![genesis];
        chain.append(&mut blocks);

        // Sanity check that we are creating a chain.
        for (p, b) in chain.iter().zip(chain.iter().skip(1)) {
            assert_eq!(Crossing::Curr(p.hash()), b.parent_hash())
        }

        ChainState {
            chain: TVar::new(chain),
        }
    }

    pub fn tip(&self) -> StmResult<EraRankingBlockHash<E>> {
        let b = self.chain.read()?;
        let l = b.last().unwrap();
        Ok(l.hash())
    }
}
