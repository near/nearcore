use actix::Addr;

use near_client::test_utils::setup_no_network_with_validity_period;
use near_client::ViewClientActor;
use near_network::test_utils::open_port;

use crate::{start_http, RpcConfig};
use near_primitives::types::NumBlocks;

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
    start_http(RpcConfig::new(&addr), client_addr.clone(), view_client_addr.clone());
    (view_client_addr, addr)
}
