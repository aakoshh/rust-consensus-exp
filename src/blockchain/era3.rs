use im::HashMap;

use super::era1::{
    Account, AccountId, Amount, EpochId, SlotId, Transaction as Transfer, ValidatorId,
};
use super::era2::MinerId;
use super::property::{HasHash, HasHeader};
use super::{ecdsa::Signature, CryptoHash};

#[derive(Clone)]
pub struct UsefulWorkHash(CryptoHash);

#[derive(Clone)]
pub struct UsefulWorkHeader {
    account_id: AccountId,
    problem: Vec<u8>,
    total_reward: Amount,
    signature: Signature<AccountId, UsefulWorkHeader>,
}

#[derive(Clone)]
pub struct UsefulWorkStatus {
    remaining_reward: Amount,
}

pub struct UsefulWorkSubmission {
    useful_work_hash: UsefulWorkHash,
    solution: Vec<u8>,
}

pub enum Transaction {
    AddUsefulWork(UsefulWorkHeader),
    Transfer(Transfer),
}

#[derive(Clone)]
pub struct Ledger {
    pub accounts: HashMap<AccountId, Account>,
    pub useful_work_classifieds: HashMap<UsefulWorkHash, (UsefulWorkHeader, UsefulWorkStatus)>,
}

pub struct InputBlockHash(CryptoHash);
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

pub struct InputBlockHeader {
    pub parent_hashes: Vec<InputBlockHash>,
    pub content_hash: CryptoHash,
    pub useful_work: UsefulWorkSubmission,
    pub nonce: [u8; 32],
    pub miner_id: MinerId,
    pub signature: Signature<MinerId, InputBlockHeader>,
}

impl InputBlockHeader {
    /// Verify that `hash(hash_without_nonce ++ nonce)` starts with `target_difficulty` number of zeroes.
    pub fn verify_pow(&self, target_difficulty: u8) -> bool {
        todo!()
    }

    pub fn verify_pouw(&self) -> bool {
        todo!()
    }

    fn hash_without_nonce(&self) -> CryptoHash {
        todo!()
    }
}

pub struct InputBlock {
    pub header: InputBlockHeader,
    pub transactions: Vec<Transaction>,
}

impl HasHash for InputBlockHeader {
    type Hash = InputBlockHash;

    fn hash(&self) -> Self::Hash {
        todo!()
    }
}

impl HasHeader for InputBlock {
    type Header = InputBlockHeader;

    fn header(&self) -> &Self::Header {
        &self.header
    }
}
