#![allow(dead_code)]
#![allow(type_alias_bounds)]
#![feature(associated_type_defaults)]
#![feature(option_zip)]
// For benchmarks.
#![feature(test)]
extern crate test as etest;

pub mod paxos;
pub mod session_types;
pub mod stm;
