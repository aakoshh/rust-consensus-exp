use super::property::HasHash;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct PrivateKey(pub [u8; 32]);

impl PrivateKey {
    pub fn sign<'a, K: Into<PublicKey>, T: HasHash>(&self, data: &'a T) -> Signature<K, T> {
        todo!()
    }
}

#[derive(Clone, Hash)]
pub struct PublicKey(pub [u8; 64]);

#[derive(Clone, Hash)]
pub struct Signature<K, T>(pub [u8; 65], PhantomData<K>, PhantomData<T>);

impl<'a, K: Into<PublicKey>, T: HasHash> Signature<K, T> {
    pub fn new(sig: [u8; 65]) -> Signature<K, T> {
        Signature(sig, PhantomData, PhantomData)
    }
    pub fn verify(&self, public_key: &K, data: &'a T) -> bool {
        todo!()
    }
}
