use std::{marker::PhantomData, time::Duration};

use crate::{
    blockchain::property::Era,
    session_types::{Chan, HasDual, Rec, SessionResult},
};

use super::{messages, protocol};

/// Client top-channel, after calling `.enter()` or `.zero()`.
type CChan0<E: Era> = CChan1<E, protocol::Server<E>>;

/// Client sub-protocol channel.
type CChan1<E: Era, P: HasDual> = Chan<P::Dual, (protocol::Client<E>, ())>;

pub struct Consumer<E: Era> {
    _phantom: PhantomData<E>,
}

impl<E: Era> Consumer<E> {
    pub fn new() -> Consumer<E> {
        Consumer {
            _phantom: PhantomData,
        }
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
        //let mut c = c;

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
