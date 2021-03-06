use crate::blockchain::property::{self, Crossing, HasHash};
use crate::blockchain::{
    ecdsa::{PublicKey, Signature},
    CryptoHash,
};
use im::HashMap;

pub mod store;

pub type Amount = u64;
pub type Nonce = u64;
pub type EpochId = u64;
pub type SlotId = u64;

pub enum TransactionError {
    NonExistentAccount,
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

impl<'a> property::Ledger<'a> for Ledger {
    type Transaction = Transaction;
    type Error = TransactionError;

    fn apply_transaction(&'a self, tx: &Self::Transaction) -> Result<Self, Self::Error> {
        todo!()
    }
}

#[derive(PartialEq, Clone, Debug, Eq, Hash)]
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

impl property::HasHash for Transaction {
    type Hash = TransactionHash;

    fn hash(&self) -> Self::Hash {
        // TODO: Find a way to make this lazy and computed only once.
        todo!()
    }
}

#[derive(Clone, PartialEq, Debug, Hash, Eq)]
pub struct BlockHash(pub CryptoHash);

impl From<BlockHash> for CryptoHash {
    fn from(h: BlockHash) -> Self {
        h.0
    }
}

#[derive(Clone, Hash)]
pub struct ValidatorId(pub PublicKey);

impl From<ValidatorId> for PublicKey {
    fn from(v: ValidatorId) -> Self {
        v.0
    }
}

#[derive(Clone, Hash)]
pub struct BlockHeader {
    pub parent_hash: BlockHash,
    pub epoch_id: EpochId,
    pub slot_id: SlotId,
    pub height: u64,
    pub content_hash: CryptoHash,
    pub validator_id: ValidatorId,
    pub signature: Signature<ValidatorId, BlockHeader>,
}

impl property::HasHash for BlockHeader {
    type Hash = BlockHash;

    fn hash(&self) -> Self::Hash {
        BlockHash(CryptoHash::mock(&self))
    }
}

/// By implementing `RankingBlock` for the `BlockHeader` instead of the `Block`,
/// we can treat ranking blocks as small blocks, as if they were the traditional
/// headers, and treat input blocks as full.
///
impl property::RankingBlock for BlockHeader {
    type PrevEraHash = !;
    type InputBlockHash = BlockHash;

    fn parent_hash(&self) -> Crossing<Self::PrevEraHash, Self::Hash> {
        Crossing::Curr(self.parent_hash.clone())
    }

    fn height(&self) -> u64 {
        self.height
    }

    fn input_block_hashes(&self) -> Vec<Self::InputBlockHash> {
        vec![self.hash()]
    }
}

pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
}

impl property::HasHeader for Block {
    type Header = BlockHeader;

    fn header(&self) -> Self::Header {
        self.header.clone()
    }
}

impl<'a> property::HasTransactions<'a> for Block {
    type Transaction = Transaction;

    fn fold_transactions<F, R>(&'a self, init: R, f: F) -> R
    where
        F: Fn(R, &Self::Transaction) -> R,
    {
        self.transactions.iter().fold(init, f)
    }
}

pub struct Era1;

impl property::Era for Era1 {
    type Transaction<'a> = Transaction;
    type RankingBlock = BlockHeader;
    type InputBlock<'a> = Block;
    type Ledger<'a> = Ledger;
}
