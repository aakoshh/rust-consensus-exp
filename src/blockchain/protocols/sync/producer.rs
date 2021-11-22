use std::{marker::PhantomData, time::Duration};

use crate::{
    blockchain::{property::Era, protocols::sync::messages},
    offer,
    session_types::{Chan, Rec, SessionResult},
};

use super::protocol;

/// Server top-channel, after calling `.enter()` or `.zero()`.
type SChan0<E: Era> = SChan1<E, protocol::Server<E>>;

/// Server sub-protocol channel.
type SChan1<E: Era, P> = Chan<P, (protocol::Server<E>, ())>;

/// Implementation of the Server protocol, a.k.a. the Producer.
pub struct Producer<E: Era> {
    _phantom: PhantomData<E>,
}

impl<E: Era + 'static> Producer<E> {
    pub fn new() -> Producer<E> {
        Producer {
            _phantom: PhantomData,
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
