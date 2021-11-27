use std::{sync::Arc, time::Duration};

use crate::{
    blockchain::{
        property::*,
        protocols::sync::messages::{AwaitReply, RequestNext, RollBackward, RollForward},
        store::{BlockStore, ChainStore, StoreError},
    },
    offer,
    session_types::{Chan, HasDual, Rec, SessionError, SessionResult},
    stm::{atomically, atomically_or_err},
};

use super::{messages::*, protocol};

/// Client sub-protocol channel.
type CChan1<E: Era, P: HasDual> = Chan<P::Dual, (protocol::Client<E>, ())>;
/// Client top-channel, after calling `.enter()` or `.zero()`.
type CChan0<E: Era> = CChan1<E, protocol::Server<E>>;

/// Implementation of the Client protocol, a.k.a. the Consumer.
pub struct Consumer<E: Era, S> {
    /// Track the state of the producer.
    ///
    /// In the beginning we can initialise this to our own, then trim it back
    /// to where we find the intersect.
    ///
    /// It's wrapped in `Arc` because the chain store is also part of an STM map
    /// that contains all peer's chain stores, so that a sync control thread can
    /// see all state at once and can make overall decisions on which blocks
    /// to download.
    chain_store: Arc<ChainStore<E, S>>,
}

impl<E: Era + 'static, S: BlockStore<E>> Consumer<E, S> {
    pub fn new(chain_store: Arc<ChainStore<E, S>>) -> Consumer<E, S> {
        Consumer { chain_store }
    }

    /// Protocol implementation for a consumer following a producer.
    pub fn sync_chain(&self, c: Chan<Rec<protocol::Client<E>>, ()>) -> SessionResult<()> {
        let mut c: CChan0<E> = c.enter();

        // First find where our chain intersects with that of the producer.
        c = self.intersect(c.sel1())?;

        // From now, just keep requesting new updates and ask for missing inputs.
        loop {
            c = self.next(c.sel2().sel1())?;
        }
    }

    /// Find the last intersecting block we have with the producer and trim the chain store accordingly.
    fn intersect(&self, c: CChan1<E, protocol::Intersect<E>>) -> SessionResult<CChan0<E>> {
        // Select blocks at exponentially larger gaps in our chain.
        // Ask the producer what the best intersect is.
        // Repeat until we cannot improve the intersection by doing bisecting between the point
        // that we found and the one before it that we didn't.
        // Drop anything from our chain that sits above the intersect.
        // Return the zeroed client.
        let anchor_hashes = atomically(|| {
            let tip = self.chain_store.tip()?;
            let mut hashes = vec![tip.hash()];
            let mut delta = 1;
            let mut height = tip.height();
            // In this example just go back as far as genesis. In a real blockchain there would probably
            // be a cap to how far we have to go, beyond which the chain would be considered stable.
            while height > 0 {
                height = if height < delta { 0 } else { height - delta };
                delta = delta * 2;
                if let Some(header) = self.chain_store.get_ranking_block_by_height(height)? {
                    hashes.push(header.hash());
                } else {
                    break;
                }
            }
            Ok(hashes)
        });

        let c = c.send(FindIntersect(anchor_hashes.clone()))?;

        offer! { c, Duration::from_secs(60),
            Found => {
                let (c, IntersectFound(hash)) = c.recv(Duration::ZERO)?;

                // Check that the producer is not sending rubbish.
                if !anchor_hashes.contains(&hash) {
                    return Err(SessionError::Abort(Box::new(StoreError::RankingBlockDoesNotExist)))
                }

                // TODO: Here we could do bisection to find the best possible intersect,
                // but for this example I'll just use whatever we found in the first round.

                let ok = atomically(|| {
                    if let Some(block) = self.chain_store.get_ranking_block_by_hash(&hash)? {
                        self.chain_store.remove_ranking_blocks_above_height(block.height())?;
                        Ok(true)
                    } else {
                        // The block could have been removed if we switched to a different fork.
                        Ok(false)
                    }
                });

                if ok {
                    c.zero()
                } else {
                    // Try syncing again.
                    self.intersect(c.zero()?.sel1())
                }
            },
            NotFound => {
                let (_c, IntersectNotFound) = c.recv(Duration::ZERO)?;
                // We went back to genesis and still could not connect!
                Err(SessionError::Abort(Box::new(StoreError::DoesNotExtendChain)))
            }
        }
    }

    /// Ask the next update; wait if we have to. If we need to roll back, then drop
    /// any blocks from the chain above the indicated hash. Otherwise append the
    /// new header and ask for any missing dependencies.
    fn next(&self, c: CChan1<E, protocol::Next<E>>) -> SessionResult<CChan0<E>> {
        let c = c.send(RequestNext)?;
        offer! {c, Duration::from_secs(60),
            Roll => {
                self.roll(c, Duration::ZERO)
            },
            Await => {
                let (c, AwaitReply) = c.recv(Duration::ZERO)?;
                self.roll(c, Duration::from_secs(60*60))
            }
        }
    }

    /// Wait for the instruction roll forward or backwards.
    /// If we went forward, request any missing dependencies.
    fn roll(&self, c: CChan1<E, protocol::Roll<E>>, timeout: Duration) -> SessionResult<CChan0<E>> {
        offer! {c, timeout,
            Forward => {
                let (c, RollForward(ranking)) = c.recv(Duration::ZERO)?;
                let c = c.zero()?;

                // Try to store the new ranking block. If there are missing inputs,
                // the transaction will be aborted and we have to fetch those first.
                // But we have to attempt to store, because it may be a self reference,
                // which is handled by the storages that understand the actual types.
                let result = atomically_or_err(|| {
                    self.chain_store.add_ranking_block(ranking.clone())
                });

                match result {
                    Ok(()) => Ok(c),
                    Err(e) => {
                        match e.downcast_ref::<StoreError>() {
                            Some(StoreError::MissingInputs) => {
                                self.missing(c.sel2().sel2().sel1(), ranking)
                            },
                            _ => Err(SessionError::Abort(e))
                        }
                    }
                }
            },
            Backward => {
                let (c, RollBackward(hash)) = c.recv(Duration::ZERO)?;

                atomically(|| {
                    if let Some(b) = self.chain_store.get_ranking_block_by_hash(&hash)? {
                        self.chain_store.remove_ranking_blocks_above_height(b.height())
                    } else {
                        Ok(())
                    }
                });

                c.zero()
            }
        }
    }

    /// Ask for all the missing dependencies of a ranking block. Since it must refer to all the input blocks,
    /// including all the transitively included ones, we don't have to do any recursion. After the missing
    /// blocks arrive, we insert them and the ranking block as well into the store.
    fn missing(
        &self,
        c: CChan1<E, protocol::Missing<E>>,
        ranking: EraRankingBlock<E>,
    ) -> SessionResult<CChan0<E>> {
        let missing = atomically(|| {
            let mut missing = Vec::new();
            for h in ranking.input_block_hashes() {
                if !self.chain_store.has_input_block_header(&h)? {
                    missing.push(h);
                }
            }
            Ok(missing)
        });
        let c = c.send(RequestInputs(missing))?;
        let (c, ReplyInputs(headers)) = c.recv(Duration::from_secs(60))?;
        let result = atomically_or_err(|| {
            for h in headers.clone() {
                self.chain_store.add_input_block_header(h)?;
            }
            self.chain_store.add_ranking_block(ranking.clone())
        });
        // If adding still fails then the peer didn't send us the right headers, or they were in a wrong order.
        // In any case it would be a protocol violation, so we can abort the connection.
        match result {
            Ok(()) => c.zero(),
            Err(e) => Err(SessionError::Abort(e)),
        }
    }

    fn quit(&self, c: CChan1<E, protocol::Quit>) -> SessionResult<()> {
        c.send(Done)?.close()
    }
}
