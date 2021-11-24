use std::{sync::Arc, time::Duration};

use crate::{
    blockchain::{
        property::Era,
        store::{BlockStore, ChainStore},
    },
    session_types::{Chan, HasDual, Rec, SessionResult},
};

use super::{messages, protocol};

/// Client top-channel, after calling `.enter()` or `.zero()`.
type CChan0<E: Era> = CChan1<E, protocol::Server<E>>;

/// Client sub-protocol channel.
type CChan1<E: Era, P: HasDual> = Chan<P::Dual, (protocol::Client<E>, ())>;

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

impl<E: Era, S: BlockStore<E>> Consumer<E, S> {
    pub fn new(chain_store: Arc<ChainStore<E, S>>) -> Consumer<E, S> {
        Consumer { chain_store }
    }
    /// Protocol implementation for a consumer following a producer.
    pub fn sync_chain(&self, c: Chan<Rec<protocol::Client<E>>, ()>) -> SessionResult<()> {
        let t = Duration::from_secs(60);
        let mut c = c.enter();

        // First find where our chain intersects with that of the producer.
        c = self.intersect(c.skip0())?;

        // From now, just keep requesting new updates and ask for missing inputs.
        loop {
            // Make it compile by quitting.
            return self.quit(c.skip3());
        }
    }

    /// Find the last intersecting block we have with the producer and trim the chain store accordingly.
    fn intersect(&self, c: CChan1<E, protocol::Intersect<E>>) -> SessionResult<CChan0<E>> {
        // Select blocks at exponentially larger gaps in our chain.
        // Ask the producer what the best intersect is.
        // Repeat until we cannot improve the intersection.
        // Drop anything from our chain that sits above the intersect.
        // Return the zeroed client.
        todo!()
    }

    /// Ask the next update; wait if we have to. If we need to roll back, then drop
    /// any blocks from the chain above the indicated hash. Otherwise append the
    /// new header and ask for any missing dependencies.
    fn next(&self, c: CChan1<E, protocol::Next<E>>) -> SessionResult<CChan0<E>> {
        todo!()
    }

    /// Recursively ask for all the missing dependencies along the parents.
    fn missing(&self, c: CChan1<E, protocol::Missing<E>>) -> SessionResult<CChan0<E>> {
        todo!()
    }

    fn quit(&self, c: CChan1<E, protocol::Quit>) -> SessionResult<()> {
        c.send(messages::Done)?.close()
    }
}
