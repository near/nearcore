use actix::Addr;

use near_client::test_utils::setup_no_network;
use near_client::ViewClientActor;
use near_network::test_utils::open_port;

use crate::{start_http, RpcConfig};

pub fn start_all(validator: bool) -> (Addr<ViewClientActor>, String) {
    let (client_addr, view_client_addr) =
        setup_no_network(vec!["test1", "test2"], if validator { "test1" } else { "other" }, true);

    let addr = format!("127.0.0.1:{}", open_port());
    start_http(RpcConfig::new(&addr), client_addr.clone(), view_client_addr.clone());
    (view_client_addr, addr)
}
