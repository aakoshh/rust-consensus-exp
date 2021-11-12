use super::era1::{EpochId, Ledger, SlotId, Transaction, ValidatorId};
use super::property;
use super::property::HasHash;
use super::{
    ecdsa::{PublicKey, Signature},
    CryptoHash,
};

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
    pub parent_hash: RankingBlockHash,
    pub epoch_id: EpochId,
    pub slot_id: SlotId,
    pub height: u64,
    pub input_block_hashes: Vec<InputBlockHash>,
    pub validator_id: ValidatorId,
    pub signature: Signature<ValidatorId, RankingBlock>,
}

impl HasHash for RankingBlock {
    type Hash = RankingBlockHash;

    fn hash(&self) -> Self::Hash {
        todo!()
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

pub struct InputBock {
    pub header: InputBlockHeader,
    pub transactions: Vec<Transaction>,
}

impl HasHash for InputBlockHeader {
    type Hash = InputBlockHash;

    fn hash(&self) -> Self::Hash {
        todo!()
    }
}

impl property::Block for RankingBlock {
    fn parent_hash(&self) -> Self::Hash {
        self.parent_hash.clone()
    }

    fn height(&self) -> u64 {
        self.height
    }
}
pub struct Era2;

// TODO: How do you encode the relationship between the input block, transaction and the ledger.
impl property::Era for Era2 {
    type Block = RankingBlock;
    type Transaction = Transaction;
    type Ledger = Ledger;
}
