use actix::{Actor, System};
use borsh::BorshSerialize;
use futures::future::Future;
use tempdir::TempDir;

use near_client::{GetBlock, TxStatus};
use near_crypto::{InMemorySigner, KeyType};
use near_jsonrpc::client::new_client;
use near_network::test_utils::WaitOrTimeout;
use near_primitives::serialize::to_base64;
use near_primitives::test_utils::{heavy_test, init_integration_logger};
use near_primitives::transaction::SignedTransaction;
use near_primitives::views::FinalExecutionStatus;
use testlib::{genesis_block, start_nodes};

/// Starts 2 validators and 2 light clients (not tracking anything).
/// Sends tx to first light client and checks that a node can return tx status.
/// TODO: here the other light client should be able to return result via RPC forwarding.
#[test]
fn tx_propagation() {
    init_integration_logger();
    heavy_test(|| {
        let system = System::new("NEAR");
        let num_nodes = 4;
        let dirs = (0..num_nodes)
            .map(|i| TempDir::new(&format!("tx_propagation{}", i)).unwrap())
            .collect::<Vec<_>>();
        let (genesis_config, rpc_addrs, clients) = start_nodes(4, &dirs, 2, 2, 10);
        let view_client = clients[0].1.clone();

        let genesis_hash = genesis_block(genesis_config).hash();
        let signer = InMemorySigner::from_seed("near.1", KeyType::ED25519, "near.1");
        let transaction = SignedTransaction::send_money(
            1,
            "near.1".to_string(),
            "near.2".to_string(),
            &signer,
            10000,
            genesis_hash,
        );
        let tx_hash = transaction.get_hash();

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();
                let transaction_copy = transaction.clone();
                let tx_hash_clone = tx_hash.clone();
                // We are sending this tx unstop, just to get over the warm up period.
                // Probably make sense to stop after 1 time though.
                actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
                    if res.unwrap().unwrap().header.height > 1 {
                        let mut client = new_client(&format!("http://{}", rpc_addrs_copy[2]));
                        let bytes = transaction_copy.try_to_vec().unwrap();
                        actix::spawn(
                            client
                                .broadcast_tx_async(to_base64(&bytes))
                                .map_err(|err| panic!(err))
                                .map(move |result| {
                                    assert_eq!(String::from(&tx_hash_clone), result)
                                }),
                        );
                    }
                    futures::future::ok(())
                }));
                actix::spawn(view_client.send(TxStatus { tx_hash }).then(move |res| {
                    match &res {
                        Ok(Ok(feo))
                            if feo.status == FinalExecutionStatus::SuccessValue("".to_string()) =>
                        {
                            System::current().stop();
                        }
                        _ => return futures::future::err(()),
                    };
                    futures::future::ok(())
                }));
            }),
            100,
            20000,
        )
        .start();

        system.run().unwrap();
    });
}
