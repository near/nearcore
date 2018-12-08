extern crate node_cli;

use std::path::Path;

fn main() {
    let base_path = Path::new(".");
    let chain_spec_path = None;
    node_cli::start_service(
        base_path,
        chain_spec_path,
        &node_cli::test_utils::create_passthrough_beacon_block_consensus_task,
    );
}
