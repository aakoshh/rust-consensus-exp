#![allow(dead_code)]
#![allow(type_alias_bounds)]
#![feature(associated_type_defaults)]
#![feature(option_zip)]
#![feature(never_type)]
#![feature(generic_associated_types)]
// For benchmarks.
#![feature(test)]
extern crate test as etest;

mod blockchain;
mod paxos;
pub mod session_types;
pub mod stm;
