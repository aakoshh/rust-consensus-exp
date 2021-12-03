use std::{
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::Duration,
};

use crate::{
    blockchain::{
        ecdsa::{PublicKey, Signature},
        eras::{
            era1::{self, ValidatorId},
            era2, era3, CoEra, Eras,
        },
        property::*,
        protocols::{
            sync::{
                consumer::Consumer,
                producer::{Producer, ReadPointer},
            },
            Cancelable,
        },
        store::{
            block::{BlockStore1, BlockStore2, BlockStore3, CoBlockStore},
            BlockStore, ChainStore,
        },
        CryptoHash,
    },
    session_types::session_channel,
    stm::atomically,
};

static SLOT: AtomicU64 = AtomicU64::new(0);

// TODO: Use arbitrary test data generators like in QuickCheck.

fn make_genesis() -> EraRankingBlock<CoEra> {
    use crate::blockchain::eras::era1::*;
    let b = BlockHeader {
        parent_hash: BlockHash(CryptoHash([0; 32])),
        epoch_id: 0,
        slot_id: 0,
        height: 0,
        content_hash: CryptoHash([0; 32]),
        validator_id: ValidatorId(PublicKey([0; 64])),
        signature: Signature::new([0; 65]),
    };
    Eras::Era1(b)
}

fn make_block_era1(parent: &EraRankingBlock<CoEra>) -> EraRankingBlock<CoEra> {
    use crate::blockchain::eras::era1::*;
    let p = match parent.hash() {
        Eras::Era1(h) => h,
        _ => panic!("Expected era1 block"),
    };
    let b = BlockHeader {
        parent_hash: p,
        epoch_id: 0,
        slot_id: SLOT.fetch_add(1, Ordering::Relaxed),
        height: parent.height() + 1,
        content_hash: CryptoHash([0; 32]),
        validator_id: ValidatorId(PublicKey([0; 64])),
        signature: Signature::new([0; 65]),
    };
    Eras::Era1(b)
}

fn make_block_era2(parent: &EraRankingBlock<CoEra>) -> EraRankingBlock<CoEra> {
    use crate::blockchain::eras::era2::*;
    let p = match parent.hash() {
        Eras::Era1(h) => Crossing::Prev(h),
        Eras::Era2(h) => Crossing::Curr(h),
        _ => panic!("Expected era1 or era2 block"),
    };
    let b = RankingBlock {
        parent_hash: p,
        epoch_id: 0,
        slot_id: SLOT.fetch_add(1, Ordering::Relaxed),
        height: parent.height() + 1,
        input_block_hashes: vec![],
        validator_id: ValidatorId(PublicKey([0; 64])),
        signature: Signature::new([0; 65]),
    };
    Eras::Era2(b)
}

fn make_block_era3(parent: &EraRankingBlock<CoEra>) -> EraRankingBlock<CoEra> {
    use crate::blockchain::eras::era3::*;
    let p = match parent.hash() {
        Eras::Era2(h) => Crossing::Prev(h),
        Eras::Era3(h) => Crossing::Curr(h),
        _ => panic!("Expected era2 or era3 block"),
    };
    let b = RankingBlock {
        parent_hash: p,
        epoch_id: 0,
        slot_id: SLOT.fetch_add(1, Ordering::Relaxed),
        height: parent.height() + 1,
        input_block_hashes: vec![],
        validator_id: ValidatorId(PublicKey([0; 64])),
        signature: Signature::new([0; 65]),
    };
    Eras::Era3(b)
}

fn unera1(b: &EraRankingBlock<CoEra>) -> era1::BlockHeader {
    match b {
        Eras::Era1(b) => b.clone(),
        _ => panic!("Not an era1 block."),
    }
}

fn unera2(b: &EraRankingBlock<CoEra>) -> era2::RankingBlock {
    match b {
        Eras::Era2(b) => b.clone(),
        _ => panic!("Not an era2 block."),
    }
}

fn unera3(b: &EraRankingBlock<CoEra>) -> era3::RankingBlock {
    match b {
        Eras::Era3(b) => b.clone(),
        _ => panic!("Not an era3 block."),
    }
}

#[test]
fn chain_sync() {
    let eg = make_genesis();
    let eb1 = make_block_era1(&eg);
    let eb2 = make_block_era1(&eb1);
    let eb3 = make_block_era2(&eb2);
    let eb4a = make_block_era2(&eb3);
    let eb5a = make_block_era3(&eb4a);
    let eb6a = make_block_era3(&eb5a);
    let eb4b = make_block_era2(&eb3);

    let g = unera1(&eg);
    let b1 = unera1(&eb1);
    let b2 = unera1(&eb2);
    let b3 = unera2(&eb3);
    let b4a = unera2(&eb4a);
    let b5a = unera3(&eb5a);
    let b6a = unera3(&eb6a);
    let b4b = unera2(&eb4b);

    println!("block g = {:?}", eg.hash());
    println!("block b1 = {:?}", eb1.hash());
    println!("block b2 = {:?}", eb2.hash());
    println!("block b3 = {:?}", eb3.hash());
    println!("block b4a = {:?}", eb4a.hash());
    println!("block b5a = {:?}", eb5a.hash());
    println!("block b6a = {:?}", eb6a.hash());
    println!("block b4b = {:?}", eb4b.hash());

    // Producer block storage.
    let producer_store = ChainStore::new(CoBlockStore::new(
        BlockStore1::new(g.clone(), vec![b1.clone(), b2.clone()]),
        BlockStore2::new(vec![b3.clone(), b4a], vec![]),
        BlockStore3::new(vec![b5a, b6a], vec![]),
    ));

    // Initialise the read pointer for the producer from its own chain state, since it hasn't talked to the consumer yet.
    let read_pointer = ReadPointer::new(producer_store.clone());

    // Consumer block storage, on a different fork than the producer.
    let consumer_store = ChainStore::new(CoBlockStore::new(
        BlockStore1::new(g, vec![b1, b2]),
        BlockStore2::new(vec![b3.clone(), b4b], vec![]),
        BlockStore3::new(vec![], vec![]),
    ));

    let cancel_token = Cancelable::new();
    let consumer =
        Consumer::<CoEra, CoBlockStore>::new(consumer_store.clone(), cancel_token.clone());
    let producer =
        Producer::<CoEra, CoBlockStore>::new(producer_store.clone(), read_pointer.clone());

    let (server_chan, client_chan) = session_channel();

    let prod_t = thread::spawn(move || producer.sync_chain(server_chan));
    let cons_t = thread::spawn(move || consumer.sync_chain(client_chan));

    thread::sleep(Duration::from_millis(250));

    let new_read_pointer = atomically(|| read_pointer.last_ranking_block());
    assert_eq!(new_read_pointer.hash(), eb6a.hash());
    assert!(atomically(|| {
        consumer_store.has_ranking_block(&eb6a.hash())
    }));

    // Add blocks to the producer state. Check that they make their way into the consumer state.
    let eb7a = make_block_era3(&eb6a);
    atomically(|| producer_store.add_ranking_block(eb7a.clone()));

    thread::sleep(Duration::from_millis(250));

    assert!(atomically(|| {
        consumer_store.has_ranking_block(&eb7a.hash())
    }));

    // Cancel the consumer. This should close the channel as it goes out of scope,
    // which should cause the producer to exit as well.
    cancel_token.cancel();

    // Currently we only check cancel conditions between the handling of next calls,
    // so first the client needs to have an event.
    let eb8a = make_block_era3(&eb7a);
    atomically(|| producer_store.add_ranking_block(eb8a.clone()));

    cons_t.join().unwrap().unwrap();
    prod_t.join().unwrap().unwrap();
}
