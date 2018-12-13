extern crate node_cli;

use std::path::PathBuf;

pub fn start_devnet() {
    let config = node_cli::ServiceConfig {
        base_path: PathBuf::from("."),
        chain_spec_path: None,
        p2p_port: None,
        rpc_port: None,
        test_node_index: None,
    };
    node_cli::start_service(
        &config,
        &node_cli::test_utils::create_passthrough_beacon_block_consensus_task,
    );
}
