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
        loop {
            c = self.intersect(c.skip0())?;

            // Make it compile by quitting.
            return self.quit(c.skip3());
        }
    }

    fn intersect(&self, c: CChan1<E, protocol::Intersect<E>>) -> SessionResult<CChan0<E>> {
        todo!()
    }

    fn next(&self, c: CChan1<E, protocol::Next<E>>) -> SessionResult<CChan0<E>> {
        todo!()
    }

    fn missing(&self, c: CChan1<E, protocol::Missing<E>>) -> SessionResult<CChan0<E>> {
        todo!()
    }

    fn quit(&self, c: CChan1<E, protocol::Quit>) -> SessionResult<()> {
        c.send(messages::Done)?.close()
    }
}
