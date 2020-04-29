///! Runs given number of nodes from scratch for testing / integration / load testing purposes.
use std::thread;
use std::time::Duration;

use clap::{App, Arg};

use near_logger_utils::init_integration_logger;
use testlib::node::{create_nodes, Node, NodeConfig};

fn main() {
    init_integration_logger();

    let matches = App::new("run-nodes")
        .arg(
            Arg::with_name("num_nodes")
                .short("n")
                .long("num-nodes")
                .value_name("NUM_NODES")
                .required(true)
                .default_value("7")
                .takes_value(true),
        )
        .get_matches();

    let num_nodes = matches.value_of("num_nodes").map(|x| x.parse::<usize>().unwrap()).unwrap();

    let nodes = create_nodes(num_nodes, "test");

    print!("Connect via RPC to: ");
    for i in 0..num_nodes {
        match &nodes[i] {
            NodeConfig::Thread(cfg) => print!("{}, ", cfg.rpc_config.addr),
            _ => (),
        }
    }
    println!();

    let nodes: Vec<_> = nodes.into_iter().map(|cfg| Node::new_sharable(cfg)).collect();

    // Start nodes.
    for i in 0..num_nodes {
        nodes[i].write().unwrap().start();
    }

    // Loop infinitely.
    loop {
        thread::sleep(Duration::from_secs(1))
    }
}
