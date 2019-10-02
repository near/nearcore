use actix::System;
use futures::future;
use futures::future::Future;

use near_crypto::BlsSignature;
use near_jsonrpc::client::new_client;
use near_jsonrpc::test_utils::start_all;
use near_primitives::test_utils::init_test_logger;

/// Retrieve blocks via json rpc
#[test]
fn test_block() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(false);

        let mut client = new_client(&format!("http://{}", addr));
        actix::spawn(client.block(0).then(|res| {
            let res = res.unwrap();
            assert_eq!(res.header.height, 0);
            assert_eq!(res.header.epoch_id.0, &[0; 32]);
            assert_eq!(res.header.hash.0.len(), 32);
            assert_eq!(res.header.prev_hash.0, &[0; 32]);
            assert_eq!(
                res.header.prev_state_root.0,
                &[
                    102, 104, 122, 173, 248, 98, 189, 119, 108, 143, 193, 139, 142, 159, 142, 32,
                    8, 151, 20, 133, 110, 226, 51, 179, 144, 42, 89, 29, 13, 95, 41, 37
                ]
            );
            assert!(res.header.timestamp > 0);
            assert_eq!(res.header.approval_mask.len(), 0);
            assert_eq!(res.header.approval_sigs, BlsSignature::empty());
            assert_eq!(res.header.total_weight, 0);
            assert_eq!(res.header.validator_proposals.len(), 0);
            System::current().stop();
            future::result(Ok(()))
        }));
    })
    .unwrap();
}

/// Connect to json rpc and query the client.
#[test]
fn test_query() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(false);

        let mut client = new_client(&format!("http://{}", addr));
        actix::spawn(client.query("account/test".to_string(), "".to_string()).then(|res| {
            assert!(res.is_ok());
            System::current().stop();
            future::result(Ok(()))
        }));
    })
    .unwrap();
}

/// Retrieve client status via JSON RPC.
#[test]
fn test_status() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(false);

        let mut client = new_client(&format!("http://{}", addr));
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

/// Check health fails when node is absent.
#[test]
fn test_health_fail() {
    init_test_logger();

    System::run(|| {
        let mut client = new_client(&"http://127.0.0.1:12322/health");
        actix::spawn(client.health().then(|res| {
            assert!(res.is_err());
            System::current().stop();
            future::result(Ok(()))
        }));
    })
    .unwrap();
}

/// Retrieve client health.
#[test]
fn test_health_ok() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(false);

        let mut client = new_client(&format!("http://{}", addr));
        actix::spawn(client.health().then(|res| {
            assert!(res.is_ok());
            System::current().stop();
            future::result(Ok(()))
        }));
    })
    .unwrap();
}
