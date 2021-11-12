use im::HashMap;

use crate::blockchain::eras::era1::{
    Account, AccountId, Amount, EpochId, SlotId, Transaction as Transfer, TransactionError,
    ValidatorId,
};
use crate::blockchain::eras::era2::MinerId;
use crate::blockchain::property;
use crate::blockchain::{ecdsa::Signature, CryptoHash};

use super::{era2, Crossing};

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

impl property::Ledger for Ledger {
    type Transaction = Transaction;
    type Error = TransactionError;

    fn apply_transaction(&self, tx: &Self::Transaction) -> Result<Self, Self::Error> {
        todo!()
    }
}

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
    pub parent_hash: Crossing<era2::RankingBlockHash, RankingBlockHash>,
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
    type PrevEraHash = era2::RankingBlockHash;
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

impl property::HasHeader for InputBlock {
    type Header = InputBlockHeader;

    fn header(&self) -> &Self::Header {
        &self.header
    }
}

impl property::HasTransactions for InputBlock {
    type Transaction = Transaction;
    type Transactions<'a> = std::slice::Iter<'a, Transaction>;

    fn transactions<'a>(&'a self) -> Self::Transactions<'a> {
        self.transactions.iter()
    }
}

pub struct Era3;

impl property::Era for Era3 {
    type RankingBlock = RankingBlock;
    type InputBlock = InputBlock;
    type Transaction = Transaction;
    type Ledger = Ledger;
}
