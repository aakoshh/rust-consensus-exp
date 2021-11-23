use crate::blockchain::{
    ecdsa::{PublicKey, Signature},
    eras::{CoEra, Eras},
    property::EraRankingBlock,
    CryptoHash,
};

fn make_genesis() -> EraRankingBlock<CoEra> {
    use crate::blockchain::eras::era1::*;
    let h = BlockHeader {
        parent_hash: BlockHash(CryptoHash([0; 32])),
        epoch_id: 0,
        slot_id: 0,
        height: 0,
        content_hash: CryptoHash([0; 32]),
        validator_id: ValidatorId(PublicKey([0; 64])),
        signature: Signature::new([0; 65]),
    };
    Eras::Era1(h)
}

#[test]
fn chain_sync() {
    let genesis = make_genesis();

    // // TODO: Add some blocks to the consumer state, and some others to the consumer,
    // // so they are on different forks initially.
    // let producer_state = ChainState::new(genesis.clone(), vec![]);
    // let consumer_state = ChainState::new(genesis.clone(), vec![]);

    // let producer = Producer::<CoEra>::new(producer_state);
    // let consumer = Consumer::<CoEra>::new(consumer_state);

    // let (server_chan, client_chan) = session_channel();

    // let srv_t = thread::spawn(move || producer.sync_chain(server_chan));

    // // TODO: Add blocks to the producer state. Check that they make their way into the consumer state.

    // client_chan
    //     .enter()
    //     .sel2()
    //     .sel2()
    //     .sel2()
    //     .send(Done)
    //     .unwrap()
    //     .close()
    //     .unwrap();

    // srv_t.join().unwrap().unwrap();
}
