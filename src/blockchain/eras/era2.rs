use crate::blockchain::eras::era1::{EpochId, Ledger, SlotId, Transaction, ValidatorId};
use crate::blockchain::property;
use crate::blockchain::{
    ecdsa::{PublicKey, Signature},
    CryptoHash,
};

use super::{era1, Crossing};

#[derive(Clone)]
pub struct InputBlockHash(CryptoHash);

#[derive(Clone)]
pub struct RankingBlockHash(CryptoHash);

impl From<RankingBlockHash> for CryptoHash {
    fn from(h: RankingBlockHash) -> Self {
        h.0
    }
}

impl From<InputBlockHash> for CryptoHash {
    fn from(h: InputBlockHash) -> Self {
        h.0
    }
}

pub struct RankingBlock {
    pub parent_hash: Crossing<era1::BlockHash, RankingBlockHash>,
    pub epoch_id: EpochId,
    pub slot_id: SlotId,
    pub height: u64,
    pub input_block_hashes: Vec<InputBlockHash>,
    pub validator_id: ValidatorId,
    pub signature: Signature<ValidatorId, RankingBlock>,
}

impl property::HasHash for RankingBlock {
    type Hash = RankingBlockHash;

    fn hash(&self) -> Self::Hash {
        todo!()
    }
}

impl property::RankingBlock for RankingBlock {
    type PrevEraHash = era1::BlockHash;
    type InputBlockHash = InputBlockHash;
    fn parent_hash(&self) -> Crossing<Self::PrevEraHash, Self::Hash> {
        self.parent_hash.clone()
    }

    fn height(&self) -> u64 {
        self.height
    }

    fn input_block_hashes(&self) -> Vec<Self::InputBlockHash> {
        self.input_block_hashes.clone()
    }
}

pub struct MinerId(PublicKey);

pub struct InputBlockHeader {
    pub content_hash: CryptoHash,
    pub nonce: [u8; 32],
    pub miner_id: MinerId,
    pub signature: Signature<MinerId, InputBlockHeader>,
}

impl InputBlockHeader {
    /// Verify that `hash(hash_without_nonce ++ nonce)` starts with `target_difficulty` number of zeroes.
    pub fn verify_pow(&self, target_difficulty: u8) -> bool {
        todo!()
    }

    fn hash_without_nonce(&self) -> CryptoHash {
        todo!()
    }
}

impl property::HasHash for InputBlockHeader {
    type Hash = InputBlockHash;

    fn hash(&self) -> Self::Hash {
        todo!()
    }
}

pub struct InputBlock {
    pub header: InputBlockHeader,
    pub transactions: Vec<Transaction>,
}

impl property::HasTransactions for InputBlock {
    type Transaction = Transaction;
    type Transactions<'a> = std::slice::Iter<'a, Transaction>;

    fn transactions<'a>(&'a self) -> Self::Transactions<'a> {
        self.transactions.iter()
    }
}

impl property::HasHeader for InputBlock {
    type Header = InputBlockHeader;

    fn header(&self) -> &Self::Header {
        &self.header
    }
}

pub struct Era2;

impl property::Era for Era2 {
    type RankingBlock = RankingBlock;
    type InputBlock = InputBlock;
    type Transaction = Transaction;
    type Ledger = Ledger;
}
