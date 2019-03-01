//! This library contains tools for consensus that are not dependent on specific implementation of
//! TxFlow or other gossip-based consensus protocol. It also provides simple pass-through consensus
//! that can be used for DevNet.
extern crate beacon;
extern crate chrono;
extern crate client;
extern crate coroutines;
extern crate futures;
#[macro_use]
extern crate log;
extern crate network;
extern crate primitives;
extern crate rand;
extern crate tokio;
extern crate typed_arena;

pub mod adapters;
pub mod passthrough;
