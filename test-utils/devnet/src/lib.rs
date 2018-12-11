extern crate node_cli;

use std::path::Path;

pub fn start_devnet() {
    let base_path = Path::new(".");
    let chain_spec_path = None;
    let p2p_port = None;
    let rpc_port = None;
    node_cli::start_service(
        base_path,
        chain_spec_path,
        p2p_port,
        rpc_port,
        &node_cli::test_utils::create_passthrough_beacon_block_consensus_task,
    );
}
