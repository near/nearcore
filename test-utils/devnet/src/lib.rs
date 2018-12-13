extern crate node_cli;

use std::path::Path;

pub fn start_devnet(base_path: Option<&Path>) {
    let base_path = base_path
        .unwrap_or_else(|| Path::new("."))
        .to_owned();
    let config = node_cli::service::ServiceConfig {
        base_path,
        chain_spec_path: None,
        p2p_port: None,
        rpc_port: None,
        test_node_index: None,
    };
    node_cli::service::start_service(
        config,
        node_cli::test_utils::spawn_pasthrough_consensus,
    );
}
