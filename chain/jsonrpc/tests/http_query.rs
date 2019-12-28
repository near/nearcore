use actix::System;
use futures::future;
use futures::future::Future;

use near_jsonrpc::client::new_http_client;
use near_jsonrpc::test_utils::start_all;
use near_primitives::test_utils::init_test_logger;

/// Retrieve client status via HTTP GET.
#[test]
fn test_status() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(false);

        let mut client = new_http_client(&format!("http://{}", addr));
        actix::spawn(client.status().then(|res| {
            let res = res.unwrap();
            assert_eq!(res.chain_id, "unittest");
            assert_eq!(res.sync_info.latest_block_height, 0);
            assert_eq!(res.sync_info.syncing, false);
            System::current().stop();
            future::result(Ok(()))
        }));
    })
    .unwrap();
}
