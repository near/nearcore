use std::convert::TryFrom;

use actix::{Actor, System};
use futures::future;
use futures::future::Future;

use near_crypto::Signature;
use near_jsonrpc::client::new_client;
use near_jsonrpc::test_utils::start_all;
use near_jsonrpc_client::{BlockId, ChunkId};
use near_network::test_utils::WaitOrTimeout;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::init_test_logger;
use near_primitives::types::ShardId;

/// Retrieve blocks via json rpc
#[test]
fn test_block() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(false);

        let mut client = new_client(&format!("http://{}", addr));

        actix::spawn(client.block(BlockId::Height(0)).then(|res| {
            let res = res.unwrap();
            assert_eq!(res.header.height, 0);
            assert_eq!(res.header.epoch_id.0.as_ref(), &[0; 32]);
            assert_eq!(res.header.hash.0.as_ref().len(), 32);
            assert_eq!(res.header.prev_hash.0.as_ref(), &[0; 32]);
            assert_eq!(
                res.header.prev_state_root,
                CryptoHash::try_from("7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t").unwrap()
            );
            assert!(res.header.timestamp > 0);
            assert_eq!(res.header.total_weight, 0);
            assert_eq!(res.header.validator_proposals.len(), 0);
            System::current().stop();
            future::ok(())
        }));
    })
    .unwrap();
}

/// Retrieve blocks via json rpc
#[test]
fn test_block_by_hash() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(false);

        let mut client = new_client(&format!("http://{}", addr.clone()));
        actix::spawn(client.block(BlockId::Height(0)).then(move |res| {
            let res = res.unwrap();
            let mut client = new_client(&format!("http://{}", addr));
            client.block(BlockId::Hash(res.header.hash)).then(move |res| {
                let res = res.unwrap();
                assert_eq!(res.header.height, 0);
                System::current().stop();
                future::ok(())
            })
        }));
    })
    .unwrap();
}

/// Retrieve blocks via json rpc
#[test]
fn test_chunk_by_hash() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(true);

        let mut client = new_client(&format!("http://{}", addr.clone()));
        actix::spawn(
            client.chunk(ChunkId::BlockShardId(BlockId::Height(0), ShardId::from(0u64))).then(
                move |chunk| {
                    let chunk = chunk.unwrap();
                    assert_eq!(chunk.header.balance_burnt, 0);
                    assert_eq!(chunk.header.chunk_hash.as_ref().len(), 32);
                    assert_eq!(chunk.header.encoded_length, 8);
                    assert_eq!(chunk.header.encoded_merkle_root.as_ref().len(), 32);
                    assert_eq!(chunk.header.gas_limit, 1000000);
                    assert_eq!(chunk.header.gas_used, 0);
                    assert_eq!(chunk.header.height_created, 0);
                    assert_eq!(chunk.header.height_included, 0);
                    assert_eq!(chunk.header.outgoing_receipts_root.as_ref().len(), 32);
                    assert_eq!(chunk.header.prev_block_hash.as_ref().len(), 32);
                    assert_eq!(chunk.header.prev_state_root.as_ref().len(), 32);
                    assert_eq!(chunk.header.rent_paid, 0);
                    assert_eq!(chunk.header.shard_id, 0);
                    assert!(if let Signature::ED25519(_) = chunk.header.signature {
                        true
                    } else {
                        false
                    });
                    assert_eq!(chunk.header.tx_root.as_ref(), &[0; 32]);
                    assert_eq!(chunk.header.validator_proposals, vec![]);
                    assert_eq!(chunk.header.validator_reward, 0);
                    let mut client = new_client(&format!("http://{}", addr));
                    client.chunk(ChunkId::Hash(chunk.header.chunk_hash)).then(move |same_chunk| {
                        let same_chunk = same_chunk.unwrap();
                        assert_eq!(chunk.header.chunk_hash, same_chunk.header.chunk_hash);
                        System::current().stop();
                        future::ok(())
                    })
                },
            ),
        );
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

/// Retrieve client status failed.
#[test]
fn test_status_fail() {
    init_test_logger();

    System::run(|| {
        let (_, addr) = start_all(false);

        let mut client = new_client(&format!("http://{}", addr));
        WaitOrTimeout::new(
            Box::new(move |_| {
                actix::spawn(client.health().then(|res| {
                    if res.is_err() {
                        System::current().stop();
                    }
                    future::result(Ok(()))
                }));
            }),
            100,
            10000,
        )
        .start();
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

/// Health fails when node doesn't produce block for period of time.
#[test]
fn test_health_fail_no_blocks() {
    init_test_logger();

    System::run(|| {
        let (_, addr) = start_all(false);

        let mut client = new_client(&format!("http://{}", addr));
        WaitOrTimeout::new(
            Box::new(move |_| {
                actix::spawn(client.health().then(|res| {
                    if res.is_err() {
                        System::current().stop();
                    }
                    future::result(Ok(()))
                }));
            }),
            300,
            10000,
        )
        .start();
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
