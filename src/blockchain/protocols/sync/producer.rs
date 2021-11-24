use std::{sync::Arc, time::Duration};

use crate::{
    blockchain::{
        property::{Era, EraRankingBlock},
        protocols::sync::messages,
        store::{BlockStore, ChainStore},
    },
    offer,
    session_types::{Chan, Rec, SessionResult},
    stm::TVar,
};

use super::protocol;

/// Server top-channel, after calling `.enter()` or `.zero()`.
type SChan0<E: Era> = SChan1<E, protocol::Server<E>>;

/// Server sub-protocol channel.
type SChan1<E: Era, P> = Chan<P, (protocol::Server<E>, ())>;

/// The read pointer is shared between the producer and the chain selector thread.
///
/// If the chains selector thread decided to switch forks, it will go through all
/// the read pointers of the connected consumers and update them accordingly,
/// leaving an instruction to roll back if necessary. The Producer then sees the
/// updated read pointer and issues the rollback to the Consumer.
pub struct ReadPointer<E: Era> {
    /// Remember the last header we have relayed to the consumer.
    /// Give them the next block when they ask, unless we have to
    /// roll back first.
    last_ranking_block: TVar<EraRankingBlock<E>>,
    /// Indicate that the next message needs to be a rollback
    /// to the `last_header`, and then we go on from there.
    needs_rollback: TVar<bool>,
}

impl<E: Era + 'static> ReadPointer<E> {
    /// Initialise the read pointer to the genesis of the chain.
    /// The consumer can use intersect to refine it.
    pub fn new<S: BlockStore<E>>(chain_state: Arc<ChainStore<E, S>>) -> Self {
        let genesis = chain_state.genesis().to_owned();
        Self {
            last_ranking_block: TVar::new(genesis),
            needs_rollback: TVar::new(false),
        }
    }
}

/// Implementation of the Server protocol, a.k.a. the Producer.
pub struct Producer<E: Era, S> {
    /// The state of the producer, which it feeds to the consumer.
    ///
    /// It is wrapped in an `Arc` because this is the main chain
    /// instance for this node, shared between all producers.
    /// They can await changes via STM and send them to their
    /// respective consumers; but the chainge is initiated by
    /// the sync control thread that switches between forks.
    chain_state: Arc<ChainStore<E, S>>,
    /// Shared read pointer.
    read_pointer: Arc<ReadPointer<E>>,
}

impl<E: Era + 'static, S: BlockStore<E>> Producer<E, S> {
    pub fn new(chain_state: Arc<ChainStore<E, S>>, read_pointer: Arc<ReadPointer<E>>) -> Self {
        Self {
            chain_state,
            read_pointer,
        }
    }

    /// Protocol implementation for the producer, feeding a consumer its longest chain.
    pub fn sync_chain(&self, c: Chan<Rec<protocol::Server<E>>, ()>) -> SessionResult<()> {
        let mut c = c.enter();
        let t = Duration::from_secs(60);
        loop {
            c = offer! { c, t,
                Intersect => {
                    self.intersect(c)?
                },
                Next => {
                  self.next(c)?
                },
                Missing => {
                  self.missing(c)?
                },
                Quit => {
                  return self.quit(c)
                }
            }
        }
    }

    fn intersect(&self, c: SChan1<E, protocol::Intersect<E>>) -> SessionResult<SChan0<E>> {
        let (c, messages::FindIntersect(hashes)) = c.recv(Duration::ZERO)?;
        todo!()
    }

    fn next(&self, c: SChan1<E, protocol::Next<E>>) -> SessionResult<SChan0<E>> {
        let (c, messages::RequestNext) = c.recv(Duration::ZERO)?;
        todo!()
    }

    fn missing(&self, c: SChan1<E, protocol::Missing<E>>) -> SessionResult<SChan0<E>> {
        let (c, messages::RequestInputs(hashes)) = c.recv(Duration::ZERO)?;
        todo!()
    }

    fn quit(&self, c: SChan1<E, protocol::Quit>) -> SessionResult<()> {
        let (c, messages::Done) = c.recv(Duration::ZERO)?;
        c.close()
    }
}
