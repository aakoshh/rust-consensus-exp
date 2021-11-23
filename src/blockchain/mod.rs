#![allow(unused_variables)]

mod ecdsa;
mod eras;
mod property;
mod protocols;
mod store;

#[derive(Clone, PartialEq, Debug, Hash, Eq)]
pub struct CryptoHash([u8; 32]);
