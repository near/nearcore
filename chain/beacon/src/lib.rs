#![feature(box_syntax)]

#[macro_use]
extern crate log;
extern crate rand;
extern crate serde_derive;
extern crate parking_lot;

extern crate chain;
extern crate primitives;
extern crate storage;

pub mod authority;
pub mod beacon_chain;
