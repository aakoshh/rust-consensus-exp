use std::{sync::Arc, time::Duration};

use crate::{
    blockchain::{
        property::*,
        protocols::sync::messages::{self, AwaitReply, FindIntersect, RollBackward, RollForward},
        store::{BlockStore, ChainStore},
    },
    offer,
    session_types::{Chan, Rec, SessionResult},
    stm::{atomically, retry, TVar},
};

use super::{
    messages::{IntersectFound, IntersectNotFound},
    protocol,
};

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

    /// Find the first known hash in the request.
    fn intersect(&self, c: SChan1<E, protocol::Intersect<E>>) -> SessionResult<SChan0<E>> {
        let (c, FindIntersect(hashes)) = c.recv(Duration::ZERO)?;

        let first_known = atomically(|| {
            for h in &hashes {
                if self.chain_state.has_ranking_block(h)? {
                    return Ok(Some(h.clone()));
                }
            }
            Ok(None)
        });

        let c = match first_known {
            Some(h) => c.sel1().send(IntersectFound(h))?,
            None => c.sel2().send(IntersectNotFound)?,
        };

        c.zero()
    }

    /// Check the read pointer. If we have to roll back, let the consumer know where to.
    /// If we have the next block available, update the read pointer and tell the client to roll forward.
    /// Otherwise retry until we have new blocks available.
    fn next(&self, c: SChan1<E, protocol::Next<E>>) -> SessionResult<SChan0<E>> {
        let (c, messages::RequestNext) = c.recv(Duration::ZERO)?;
        match self.get_next(false) {
            Some((b, false)) => c.sel1().sel1().send(RollForward(b))?.zero(),
            Some((b, true)) => c.sel1().sel2().send(RollBackward(b.hash()))?.zero(),
            None => {
                let c = c.sel2().send(AwaitReply)?;
                match self.get_next(true) {
                    Some((b, false)) => c.sel1().send(RollForward(b))?.zero(),
                    Some((b, true)) => c.sel2().send(RollBackward(b.hash()))?.zero(),
                    None => unreachable!(),
                }
            }
        }
    }

    /// Fetch the next thing to feed to the consumer.
    fn get_next(&self, wait_for_change: bool) -> Option<(EraRankingBlock<E>, bool)> {
        atomically(|| {
            let last_ranking = self.read_pointer.last_ranking_block.read()?;
            let needs_rollback = self.read_pointer.needs_rollback.read()?;

            if *needs_rollback {
                self.read_pointer.needs_rollback.write(false)?;
                return Ok(Some((last_ranking.as_ref().clone(), true)));
            }

            let next_ranking = self
                .chain_state
                .get_ranking_block_by_height(last_ranking.height() + 1)?;

            match next_ranking {
                None if wait_for_change => retry(),
                None => Ok(None),
                Some(b) => {
                    self.read_pointer.last_ranking_block.write(b.clone())?;
                    Ok(Some((b, false)))
                }
            }
        })
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
