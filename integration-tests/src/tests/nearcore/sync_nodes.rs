use crate::tests::genesis_helpers::genesis_block;
use crate::tests::test_helpers::heavy_test;
use actix::{Actor, System};
use futures::{future, FutureExt};
use near_actix_test_utils::run_actix;
use near_async::time::Duration;
use near_chain_configs::test_utils::TESTING_INIT_STAKE;
use near_chain_configs::Genesis;
use near_client::{GetBlock, ProcessTxRequest};
use near_crypto::{InMemorySigner, KeyType};
use near_network::tcp;
use near_network::test_utils::{convert_boot_nodes, WaitOrTimeoutActor};
use near_o11y::testonly::init_integration_logger;
use near_o11y::WithSpanContextExt;
use near_primitives::transaction::SignedTransaction;
use nearcore::{load_test_config, start_with_config};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

/// Starts one validation node, it reduces it's stake to 1/2 of the stake.
/// Second node starts after 1s, needs to catchup & state sync and then make sure it's
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn sync_state_stake_change() {
    heavy_test(|| {
        init_integration_logger();

        let mut genesis = Genesis::test(vec!["test1".parse().unwrap()], 1);
        let epoch_length = 20;
        genesis.config.epoch_length = epoch_length;
        genesis.config.block_producer_kickout_threshold = 80;

        let (port1, port2) =
            (tcp::ListenerAddr::reserve_for_test(), tcp::ListenerAddr::reserve_for_test());
        let mut near1 = load_test_config("test1", port1, genesis.clone());
        near1.network_config.peer_store.boot_nodes = convert_boot_nodes(vec![("test2", *port2)]);
        near1.client_config.min_num_peers = 0;
        near1.client_config.min_block_production_delay = Duration::milliseconds(200);
        let mut near2 = load_test_config("test2", port2, genesis.clone());
        near2.network_config.peer_store.boot_nodes = convert_boot_nodes(vec![("test1", *port1)]);
        near2.client_config.min_block_production_delay = Duration::milliseconds(200);
        near2.client_config.min_num_peers = 1;
        near2.client_config.skip_sync_wait = false;

        let dir1 = tempfile::Builder::new().prefix("sync_state_stake_change_1").tempdir().unwrap();
        let dir2 = tempfile::Builder::new().prefix("sync_state_stake_change_2").tempdir().unwrap();
        run_actix(async {
            let nearcore::NearNode { client: client1, view_client: view_client1, .. } =
                start_with_config(dir1.path(), near1.clone()).expect("start_with_config");

            let genesis_hash = *genesis_block(&genesis).hash();
            let signer = Arc::new(
                InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1")
                    .into(),
            );
            let unstake_transaction = SignedTransaction::stake(
                1,
                "test1".parse().unwrap(),
                &*signer,
                TESTING_INIT_STAKE / 2,
                near1.validator_signer.get().unwrap().public_key(),
                genesis_hash,
            );
            actix::spawn(
                client1
                    .send(
                        ProcessTxRequest {
                            transaction: unstake_transaction,
                            is_forwarded: false,
                            check_only: false,
                        }
                        .with_span_context(),
                    )
                    .map(drop),
            );

            let started = Arc::new(AtomicBool::new(false));
            let dir2_path = dir2.path().to_path_buf();
            let arbiters_holder = Arc::new(RwLock::new(vec![]));
            let arbiters_holder2 = arbiters_holder;
            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    let started_copy = started.clone();
                    let near2_copy = near2.clone();
                    let dir2_path_copy = dir2_path.clone();
                    let arbiters_holder2 = arbiters_holder2.clone();
                    let actor = view_client1.send(GetBlock::latest().with_span_context());
                    let actor = actor.then(move |res| {
                        let latest_height =
                            if let Ok(Ok(block)) = res { block.header.height } else { 0 };
                        if !started_copy.load(Ordering::SeqCst) && latest_height > 2 * epoch_length
                        {
                            started_copy.store(true, Ordering::SeqCst);
                            let nearcore::NearNode { view_client: view_client2, arbiters, .. } =
                                start_with_config(&dir2_path_copy, near2_copy)
                                    .expect("start_with_config");
                            *arbiters_holder2.write().unwrap() = arbiters;

                            WaitOrTimeoutActor::new(
                                Box::new(move |_ctx| {
                                    actix::spawn(
                                        view_client2
                                            .send(GetBlock::latest().with_span_context())
                                            .then(move |res| {
                                                if let Ok(Ok(block)) = res {
                                                    if block.header.height > latest_height + 1 {
                                                        System::current().stop()
                                                    }
                                                }
                                                future::ready(())
                                            }),
                                    );
                                }),
                                100,
                                30000,
                            )
                            .start();
                        }
                        future::ready(())
                    });
                    actix::spawn(actor);
                }),
                100,
                35000,
            )
            .start();
        });
    });
}
