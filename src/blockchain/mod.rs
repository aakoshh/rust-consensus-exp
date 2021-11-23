#![allow(unused_variables)]

mod ecdsa;
mod eras;
mod property;
mod protocols;

#[derive(Clone, PartialEq, Debug)]
pub struct CryptoHash([u8; 32]);
