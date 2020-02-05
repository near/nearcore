use std::sync::Arc;

use actix::Addr;

use near_chain_configs::GenesisConfig;
use near_client::test_utils::setup_no_network_with_validity_period;
use near_client::ViewClientActor;
use near_jsonrpc::{start_http, RpcConfig};
use near_network::test_utils::open_port;
use near_primitives::types::NumBlocks;

lazy_static::lazy_static! {
    pub static ref TEST_GENESIS_CONFIG: Arc<GenesisConfig> = Arc::new(GenesisConfig::from(include_str!("../../../near/res/testnet.json")));
}

pub fn start_all(validator: bool) -> (Addr<ViewClientActor>, String) {
    start_all_with_validity_period(validator, 100, false)
}

pub fn start_all_with_validity_period(
    validator: bool,
    transaction_validity_period: NumBlocks,
    enable_doomslug: bool,
) -> (Addr<ViewClientActor>, String) {
    let (client_addr, view_client_addr) = setup_no_network_with_validity_period(
        vec!["test1", "test2"],
        if validator { "test1" } else { "other" },
        true,
        transaction_validity_period,
        enable_doomslug,
    );

    let addr = format!("127.0.0.1:{}", open_port());

    start_http(
        RpcConfig::new(&addr),
        Arc::clone(&TEST_GENESIS_CONFIG),
        client_addr.clone(),
        view_client_addr.clone(),
    );
    (view_client_addr, addr)
}
