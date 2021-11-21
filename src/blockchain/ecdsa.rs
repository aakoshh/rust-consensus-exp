use super::property::HasHash;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct PrivateKey([u8; 32]);

impl PrivateKey {
    pub fn sign<'a, K: Into<PublicKey>, T: HasHash<'a>>(&self, data: &'a T) -> Signature<K, T> {
        todo!()
    }
}

#[derive(Clone)]
pub struct PublicKey([u8; 64]);

#[derive(Clone)]
pub struct Signature<K, T>([u8; 65], PhantomData<K>, PhantomData<T>);

impl<'a, K: Into<PublicKey>, T: HasHash<'a>> Signature<K, T> {
    pub fn new(sig: [u8; 65]) -> Signature<K, T> {
        Signature(sig, PhantomData, PhantomData)
    }
    pub fn verify(&self, public_key: &K, data: &'a T) -> bool {
        todo!()
    }
}
