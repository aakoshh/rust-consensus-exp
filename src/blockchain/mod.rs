#![allow(unused_variables)]

use std::collections::hash_map::DefaultHasher;

mod ecdsa;
mod eras;
mod property;
mod protocols;
mod store;

#[derive(Clone, PartialEq, Debug, Hash, Eq)]
pub struct CryptoHash([u8; 32]);

impl CryptoHash {
    pub fn mock<T: std::hash::Hash>(value: &T) -> CryptoHash {
        use std::hash::Hasher;
        let mut s = DefaultHasher::new();
        std::hash::Hash::hash(value, &mut s);
        let h = s.finish();
        let b = h.to_be_bytes();
        let mut a = [0u8; 32];
        a[24..32].copy_from_slice(&b);
        CryptoHash(a)
    }
}
