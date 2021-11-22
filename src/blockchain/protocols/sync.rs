use std::{marker::PhantomData, time::Duration};

use crate::{blockchain::property::Era, offer, session_types::*};

mod messages {

    use crate::blockchain::{
        property::*,
        protocols::{InputBlockHash, InputBlockHeader, RankingBlockHash},
    };

    /// Ask the producer to find the newest point that exists on its blockchain.
    pub struct FindIntersect<E: Era>(pub Vec<RankingBlockHash<E>>);

    /// Tell the consumer about the first point that can be found on the producer's chain.
    /// They can start consuming from here, or try to find further points.
    /// The intersect found will become the read pointer for the consumer, so following
    /// up with a `RequestNext` message will go from here. But the consumer can also
    /// send further `FindIntersect` messages, bisecting until the best possible match
    /// is found; the better results will adjust the read pointer.
    pub struct IntersectFound<E: Era>(pub RankingBlockHash<E>);

    /// Tell the consumer that none of the identifiers in `FindIntersect` are known.
    pub struct IntersectNotFound;

    /// Ask the producer to send the next ranking block.
    pub struct RequestNext;

    /// Tell the consumer that they are caught up with the chain, and the next even is going
    /// to arrive when the producer's chain changes.
    pub struct AwaitReply;

    /// Tell the consumer to extend its chain with the next connecting ranking block.
    /// The ranking block is small, it could be the header of a traditional block,
    /// or a dedicated block type.
    pub struct RollForward<E: Era>(pub E::RankingBlock<'static>);

    /// Tell the consumer to an earlier block hash.
    pub struct RollBackward<E: Era>(pub RankingBlockHash<E>);

    /// Ask the producer to send a list of missing input block headers.
    /// The consumer should only ask for inputs that they don't have after
    /// being given a new ranking block with `RollForward`. The producer
    /// can work out which input blocks fall in the range between the latest
    /// ranking block it sent the consumer and the one before that, so it
    /// can detect if the consumer is asking for things that are unreasonable.
    pub struct RequestInputs<E: Era>(pub Vec<InputBlockHash<E>>);

    /// The producer responds to the consumer with the missing inputs block headers.
    /// The response contains the headers for the hashes the consumer asked for in
    /// the preceding `RequestInputs`. Since the producer earlier sent a ranking block
    /// that referenced these, it must have all the inputs as well, so the number of
    /// items must be exactly the same.
    pub struct ReplyInputs<E: Era>(pub Vec<<E::InputBlock<'static> as HasHeader>::Header>);

    /// Tell the consumer about a new input block header. They can decide if they want to
    /// get the body for it or not. The input blocks are supposed to be generated by PoW,
    /// so accepting random input blocks is not risky, as checking their consistency is
    /// cheap but generating them is expensive, it's not a good mechanism to attack someone.
    ///
    /// At this point it is expected that the producer and consumer already found an intersect
    /// in their chains. The consumer can detect malicious intent like sending old blocks
    /// by checking that the input block sent is not in the past-cone of the shared read pointer,
    /// or for example that the height of the input follows on previous heights.
    pub struct AddInput<E: Era>(pub InputBlockHeader<E>);

    /// Terminate the protocol.
    pub struct Done;
}

/// Sync protocols described from the server's perspective.
mod protocol {
    use super::messages::*;
    use crate::blockchain::property::Era;
    use crate::session_types::*;

    /// Protocol to find the latest block that intersects in the chains of the client and the server.
    pub type Intersect<E: Era> = Recv<
        FindIntersect<E>,
        Choose<Send<IntersectFound<E>, Var<Z>>, Send<IntersectNotFound, Var<Z>>>,
    >;

    /// Respond to the consumer with the next available ranking block, or a rollback to an earlier one;
    /// alternatively inform the consumer about an available input block header.
    pub type Roll<E: Era> = Choose<
        Send<AddInput<E>, Var<Z>>,
        Choose<Send<RollForward<E>, Var<Z>>, Send<RollBackward<E>, Var<Z>>>,
    >;

    /// Protocol to request the next available ranking block or input block header
    pub type Next<E: Era> = Recv<RequestNext, Choose<Roll<E>, Send<AwaitReply, Roll<E>>>>;

    /// Protocol to request missing input block headers.
    pub type Missing<E: Era> = Recv<RequestInputs<E>, Send<ReplyInputs<E>, Var<Z>>>;

    /// Receive a quit request to quit from the client.
    pub type Quit = Recv<Done, Eps>;

    /// Protocols offered by the server.
    pub type Offers<E: Era> = Offer<Intersect<E>, Offer<Next<E>, Offer<Missing<E>, Quit>>>;

    pub type Server<E: Era> = Rec<Offers<E>>;

    pub type Client<E: Era> = <Server<E> as HasDual>::Dual;
}

struct Consumer<E: Era> {
    _phantom: PhantomData<E>,
}

// Unfortunately the protocol is too complex and the Rust Analyzer just says `{unknown}`
// for the variables with type `Client`. One workaround is to pass it to subroutines which
// have simpler protocols, and return the channel at the spot where it has consumed all the steps,
// and all we have to do is call `.zero()` on it.
type SubChan<P: HasDual, R> = Chan<P::Dual, R>;

/// Channel that we can call `.zero()` on to return to the top level state.
type ZeroChan<R> = Chan<Var<Z>, R>;

impl<E: Era> Consumer<E> {
    pub fn new() -> Consumer<E> {
        Consumer {
            _phantom: PhantomData,
        }
    }
    /// Protocol implementation for a consumer following a producer.
    pub fn sync_chain(&self, c: Chan<protocol::Client<E>, ()>) -> SessionResult<()> {
        let t = Duration::from_secs(60);
        let mut c = c.enter();
        loop {
            c = self.intersect(c.sel1())?.zero();

            // Make it compile by quitting.
            return self.quit(c.sel2().sel2().sel2());
        }
    }

    fn intersect<R>(&self, c: SubChan<protocol::Intersect<E>, R>) -> SessionResult<ZeroChan<R>> {
        todo!()
    }

    fn next<R>(&self, c: SubChan<protocol::Next<E>, R>) -> SessionResult<ZeroChan<R>> {
        todo!()
    }

    fn missing<R>(&self, c: SubChan<protocol::Missing<E>, R>) -> SessionResult<ZeroChan<R>> {
        todo!()
    }

    fn quit<R>(&self, c: SubChan<protocol::Quit, R>) -> SessionResult<()> {
        c.send(messages::Done)?.close()
    }
}

/// Implementation of the Server protocol, a.k.a. the Producer.
struct Producer<E: Era> {
    _phantom: PhantomData<E>,
}

impl<E: Era + 'static> Producer<E> {
    pub fn new() -> Producer<E> {
        Producer {
            _phantom: PhantomData,
        }
    }

    /// Protocol implementation for the producer, feeding a consumer its longest chain.
    pub fn sync_chain(&self, c: Chan<protocol::Server<E>, ()>) -> SessionResult<()> {
        let mut c = c.enter();
        let t = Duration::from_secs(60);
        loop {
            c = offer! { c, t,
                Intersect => {
                    self.intersect(c)?.zero()
                },
                Next => {
                  self.next(c)?.zero()
                },
                Missing => {
                  self.missing(c)?.zero()
                },
                Quit => {
                  return self.quit(c)
                }
            }
        }
    }

    fn intersect<R>(&self, c: Chan<protocol::Intersect<E>, R>) -> SessionResult<ZeroChan<R>> {
        let (c, messages::FindIntersect(hashes)) = c.recv(Duration::ZERO)?;
        todo!()
    }

    fn next<R>(&self, c: Chan<protocol::Next<E>, R>) -> SessionResult<ZeroChan<R>> {
        let (c, messages::RequestNext) = c.recv(Duration::ZERO)?;
        todo!()
    }

    fn missing<R>(&self, c: Chan<protocol::Missing<E>, R>) -> SessionResult<ZeroChan<R>> {
        let (c, messages::RequestInputs(hashes)) = c.recv(Duration::ZERO)?;
        todo!()
    }

    fn quit<R>(&self, c: Chan<protocol::Quit, R>) -> SessionResult<()> {
        let (c, messages::Done) = c.recv(Duration::ZERO)?;
        c.close()
    }
}

#[cfg(test)]
mod test {
    use std::thread;

    use crate::{blockchain::eras::CoEra, session_types::session_channel};

    use super::{messages::Done, protocol, Consumer, Producer};

    #[test]
    fn chain_sync() {
        let (server_chan, client_chan) = session_channel::<protocol::Server<CoEra>>();

        let producer = Producer::<CoEra>::new();
        let consumer = Consumer::<CoEra>::new();

        let srv_t = thread::spawn(move || producer.sync_chain(server_chan));

        client_chan
            .enter()
            .sel2()
            .sel2()
            .sel2()
            .send(Done)
            .unwrap()
            .close()
            .unwrap();

        srv_t.join().unwrap().unwrap();
    }
}
