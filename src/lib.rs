#![allow(dead_code)]
#![allow(type_alias_bounds)]
#![feature(associated_type_defaults)]
#![feature(option_zip)]
// For benchmarks.
#![feature(test)]
extern crate test as etest;

mod paxos;
mod stm;
