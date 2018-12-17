//! This library contains tools for consensus that are not dependent on specific implementation of
//! TxFlow or other gossip-based consensus protocol. It also provides simple pass-through consensus
//! that can be used for DevNet.
#[macro_use]
extern crate log;
extern crate rand;
extern crate chrono;
extern crate tokio;
extern crate futures;
extern crate typed_arena;
extern crate primitives;
extern crate network;
extern crate beacon;
extern crate beacon_chain_handler;
extern crate substrate_network_libp2p;
pub mod adapters;
pub mod passthrough;
