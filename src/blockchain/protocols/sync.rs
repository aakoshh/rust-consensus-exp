mod messages {
    use crate::blockchain::property::*;

    type RankingBlockHash<E: Era> = <E::RankingBlock<'static> as HasHash<'static>>::Hash;

    /// Ask the producer to send the next header.
    pub struct RequestNext;

    /// Tell the consumer that they are caught up with the chain, and the next even is going
    /// to arrive when the producer's chain changes.
    pub struct AwaitReply;

    /// Tell the consumer to extend its chain with the next connecting ranking block.
    /// The ranking block is small, it could be the header of a traditional block,
    /// or a dedicated block type.
    pub struct RollForward<E: Era>(E::RankingBlock<'static>);

    /// Tell the consumer to an earlier block hash.
    pub struct RollBackward<E: Era>(RankingBlockHash<E>);

    /// Ask the producer to find the newest point that exists on its blockchain.
    pub struct FindIntersect<E: Era>(Vec<RankingBlockHash<E>>);

    /// Tell the consumer about the first point that can be found on the producer's chain.
    /// They can start consuming from here, or try to find further points.
    pub struct IntersectFound<E: Era>(RankingBlockHash<E>);

    /// Tell the consumer that none of the identifiers in `FindIntersect` are known.
    pub struct IntersectNotFound;

    /// Terminate the protocol.
    pub struct Done;
}

// Protocols described from the server's perspective.
mod protocol {
    use super::messages::*;
    use crate::blockchain::property::Era;
    use crate::session_types::*;

    /// Protocol to find the latest block that intersects in the chains of the client and the server.
    type Intersect<E: Era> = Recv<
        FindIntersect<E>,
        Choose<Send<IntersectFound<E>, Var<Z>>, Send<IntersectNotFound, Var<Z>>>,
    >;

    /// Respond to the client with the next available block, or a rollback to an earlier one.
    type Roll<E: Era> = Choose<Send<RollForward<E>, Var<Z>>, Send<RollBackward<E>, Var<Z>>>;

    /// Protocol to request the next available block.
    type Next<E: Era> = Recv<RequestNext, Choose<Roll<E>, Send<AwaitReply, Roll<E>>>>;

    /// Receive a quit request to quit from the client.
    type Quit = Recv<Done, Eps>;

    type Server<E: Era> = Offer<Intersect<E>, Offer<Next<E>, Quit>>;

    type Client<E: Era> = <Server<E> as HasDual>::Dual;
}
