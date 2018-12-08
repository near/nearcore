extern crate beacon;
extern crate beacon_chain_handler;
extern crate chain;
extern crate futures;
#[macro_use]
extern crate log;
extern crate node_rpc;
extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate storage;
extern crate tokio;

mod runner;
mod pass_through_consensus;

fn main() {
    runner::start_service();
}
