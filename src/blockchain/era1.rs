use super::property;
use super::property::{HasHash, HasHeader};
use super::{
    ecdsa::{PublicKey, Signature},
    CryptoHash,
};
use im::HashMap;

pub type Amount = u64;
pub type Nonce = u64;
pub type EpochId = u64;
pub type SlotId = u64;

pub enum TransactionError {
    InsufficientFunds,
    IncorrectNonce,
}

#[derive(Clone)]
pub struct Account {
    pub nonce: Nonce,
    pub balance: Amount,
}

#[derive(Clone)]
pub struct AccountId(PublicKey);

#[derive(Clone)]
pub struct Ledger {
    pub accounts: HashMap<AccountId, Account>,
}

impl property::Ledger for Ledger {
    type Transaction = Transaction;
    type Error = TransactionError;

    fn apply_transaction(&self, tx: Self::Transaction) -> Result<Self, Self::Error> {
        todo!()
    }
}

pub struct TransactionHash(CryptoHash);

impl From<TransactionHash> for CryptoHash {
    fn from(h: TransactionHash) -> Self {
        h.0
    }
}

pub struct Transaction {
    pub from: PublicKey,
    pub to: PublicKey,
    pub amount: Amount,
    pub nonce: Nonce,
    pub signature: Signature<AccountId, Transaction>,
}

impl HasHash for Transaction {
    type Hash = TransactionHash;

    fn hash(&self) -> Self::Hash {
        // TODO: Find a way to make this lazy and computed only once.
        todo!()
    }
}

#[derive(Clone)]
pub struct BlockHash(CryptoHash);

impl From<BlockHash> for CryptoHash {
    fn from(h: BlockHash) -> Self {
        h.0
    }
}

pub struct ValidatorId(PublicKey);

pub struct BlockHeader {
    pub parent_hash: BlockHash,
    pub epoch_id: EpochId,
    pub slot_id: SlotId,
    pub height: u64,
    pub content_hash: CryptoHash,
    pub validator_id: ValidatorId,
    pub signature: Signature<ValidatorId, BlockHeader>,
}

impl HasHash for BlockHeader {
    type Hash = BlockHash;

    fn hash(&self) -> Self::Hash {
        todo!()
    }
}

pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
}

impl HasHeader for Block {
    type Header = BlockHeader;

    fn header(&self) -> &Self::Header {
        &self.header
    }
}

impl property::Block for Block {
    fn parent_hash(&self) -> Self::Hash {
        self.header.parent_hash.clone()
    }

    fn height(&self) -> u64 {
        self.header.height
    }
}
pub struct Era1;

impl property::Era for Era1 {
    type Block = Block;
    type Transaction = Transaction;
    type Ledger = Ledger;
}
