#![allow(unused_variables)]

mod ecdsa;
mod eras;
mod property;

#[derive(Clone)]
pub struct CryptoHash([u8; 32]);
