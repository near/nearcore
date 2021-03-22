use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use actix::{Actor, Addr, System};
use futures::{future, FutureExt};
use num_rational::Rational;

use near_actix_test_utils::run_actix_until_stop;
use near_chain::{Block, Chain};
use near_chain_configs::Genesis;
use near_client::{ClientActor, GetBlock};
use near_crypto::{InMemorySigner, KeyType};
use near_logger_utils::init_integration_logger;
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_network::{NetworkClientMessages, PeerInfo};
use near_primitives::block::Approval;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{BlockHeightDelta, EpochId};
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::version::PROTOCOL_VERSION;
use neard::config::{GenesisExt, TESTING_INIT_STAKE};
use neard::{load_test_config, start_with_config, NearConfig};
use testlib::{genesis_block, test_helpers::heavy_test};

// This assumes that there is no height skipped. Otherwise epoch hash calculation will be wrong.
fn add_blocks(
    mut blocks: Vec<Block>,
    client: Addr<ClientActor>,
    num: usize,
    epoch_length: BlockHeightDelta,
    signer: &dyn ValidatorSigner,
) -> Vec<Block> {
    let mut prev = &blocks[blocks.len() - 1];
    let mut block_merkle_tree = PartialMerkleTree::default();
    for block in blocks.iter() {
        block_merkle_tree.insert(*block.hash());
    }
    for _ in 0..num {
        let epoch_id = match prev.header().height() + 1 {
            height if height <= epoch_length => EpochId::default(),
            height => {
                EpochId(*blocks[(((height - 1) / epoch_length - 1) * epoch_length) as usize].hash())
            }
        };
        let next_epoch_id = EpochId(
            *blocks[(((prev.header().height()) / epoch_length) * epoch_length) as usize].hash(),
        );
        let block = Block::produce(
            PROTOCOL_VERSION,
            &prev.header(),
            prev.header().height() + 1,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            (prev.header().block_ordinal() + 1),
            blocks[0].chunks().iter().cloned().collect(),
            epoch_id,
            next_epoch_id,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            None,
            vec![Some(
                Approval::new(
                    *prev.hash(),
                    prev.header().height(),
                    prev.header().height() + 1,
                    signer,
                )
                .signature,
            )],
            Rational::from_integer(0),
            0,
            1000,
            Some(0),
            vec![],
            vec![],
            signer,
            Chain::compute_collection_hash(vec![ValidatorStake::new(
                "other".to_string(),
                signer.public_key(),
                TESTING_INIT_STAKE,
            )])
            .unwrap(),
            block_merkle_tree.root(),
        );
        block_merkle_tree.insert(*block.hash());
        let _ = client.do_send(NetworkClientMessages::Block(
            block.clone(),
            PeerInfo::random().id,
            false,
        ));
        blocks.push(block);
        prev = &blocks[blocks.len() - 1];
    }
    blocks
}

fn setup_configs() -> (Genesis, Block, NearConfig, NearConfig) {
    let mut genesis = Genesis::test(vec!["other"], 1);
    genesis.config.epoch_length = 5;
    let genesis_block = genesis_block(&genesis);

    let (port1, port2) = (open_port(), open_port());
    let mut near1 = load_test_config("test1", port1, genesis.clone());
    near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", port2)]);
    near1.client_config.min_num_peers = 1;
    near1.client_config.epoch_sync_enabled = false;
    let mut near2 = load_test_config("test2", port2, genesis.clone());
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", port1)]);
    near2.client_config.min_num_peers = 1;
    near2.client_config.epoch_sync_enabled = false;
    (genesis, genesis_block, near1, near2)
}

/// One client is in front, another must sync to it before they can produce blocks.
#[test]
fn sync_nodes() {
    heavy_test(|| {
        init_integration_logger();

        let (genesis, genesis_block, near1, near2) = setup_configs();

        run_actix_until_stop(async move {
            let dir1 = tempfile::Builder::new().prefix("sync_nodes_1").tempdir().unwrap();
            let (client1, _, _) = start_with_config(dir1.path(), near1);

            let signer = InMemoryValidatorSigner::from_seed("other", KeyType::ED25519, "other");
            let _ =
                add_blocks(vec![genesis_block], client1, 13, genesis.config.epoch_length, &signer);

            let dir2 = tempfile::Builder::new().prefix("sync_nodes_2").tempdir().unwrap();
            let (_, view_client2, _) = start_with_config(dir2.path(), near2);

            WaitOrTimeout::new(
                Box::new(move |_ctx| {
                    actix::spawn(view_client2.send(GetBlock::latest()).then(|res| {
                        match &res {
                            Ok(Ok(b)) if b.header.height == 13 => System::current().stop(),
                            Err(_) => return future::ready(()),
                            _ => {}
                        };
                        future::ready(())
                    }));
                }),
                100,
                60000,
            )
            .start();
        });
    });
}

/// Clients connect and then one of them becomes in front. The other one must then sync to it.
#[test]
fn sync_after_sync_nodes() {
    heavy_test(|| {
        init_integration_logger();

        let (genesis, genesis_block, near1, near2) = setup_configs();

        run_actix_until_stop(async move {
            let dir1 = tempfile::Builder::new().prefix("sync_nodes_1").tempdir().unwrap();
            let (client1, _, _) = start_with_config(dir1.path(), near1);

            let dir2 = tempfile::Builder::new().prefix("sync_nodes_2").tempdir().unwrap();
            let (_, view_client2, _) = start_with_config(dir2.path(), near2);

            let signer = InMemoryValidatorSigner::from_seed("other", KeyType::ED25519, "other");
            let blocks = add_blocks(
                vec![genesis_block],
                client1.clone(),
                13,
                genesis.config.epoch_length,
                &signer,
            );

            let next_step = Arc::new(AtomicBool::new(false));
            let epoch_length = genesis.config.epoch_length;
            WaitOrTimeout::new(
                Box::new(move |_ctx| {
                    let blocks1 = blocks.clone();
                    let client11 = client1.clone();
                    let signer1 = signer.clone();
                    let next_step1 = next_step.clone();
                    actix::spawn(view_client2.send(GetBlock::latest()).then(move |res| {
                        match &res {
                            Ok(Ok(b)) if b.header.height == 13 => {
                                if !next_step1.load(Ordering::Relaxed) {
                                    let _ =
                                        add_blocks(blocks1, client11, 10, epoch_length, &signer1);
                                    next_step1.store(true, Ordering::Relaxed);
                                }
                            }
                            Ok(Ok(b)) if b.header.height > 20 => System::current().stop(),
                            Err(_) => return future::ready(()),
                            _ => {}
                        };
                        future::ready(())
                    }));
                }),
                100,
                60000,
            )
            .start();
        });
    });
}

/// Starts one validation node, it reduces it's stake to 1/2 of the stake.
/// Second node starts after 1s, needs to catchup & state sync and then make sure it's
#[test]
fn sync_state_stake_change() {
    heavy_test(|| {
        init_integration_logger();

        let mut genesis = Genesis::test(vec!["test1"], 1);
        genesis.config.epoch_length = 5;
        genesis.config.block_producer_kickout_threshold = 80;

        let (port1, port2) = (open_port(), open_port());
        let mut near1 = load_test_config("test1", port1, genesis.clone());
        near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", port2)]);
        near1.client_config.min_num_peers = 0;
        near1.client_config.min_block_production_delay = Duration::from_millis(200);
        near1.client_config.epoch_sync_enabled = false;
        let mut near2 = load_test_config("test2", port2, genesis.clone());
        near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", port1)]);
        near2.client_config.min_block_production_delay = Duration::from_millis(200);
        near2.client_config.min_num_peers = 1;
        near2.client_config.skip_sync_wait = false;
        near2.client_config.epoch_sync_enabled = false;

        run_actix_until_stop(async move {
            let dir1 =
                tempfile::Builder::new().prefix("sync_state_stake_change_1").tempdir().unwrap();
            let dir2 =
                tempfile::Builder::new().prefix("sync_state_stake_change_2").tempdir().unwrap();
            let (client1, view_client1, _) = start_with_config(dir1.path(), near1.clone());

            let genesis_hash = *genesis_block(&genesis).hash();
            let signer = Arc::new(InMemorySigner::from_seed("test1", KeyType::ED25519, "test1"));
            let unstake_transaction = SignedTransaction::stake(
                1,
                "test1".to_string(),
                &*signer,
                TESTING_INIT_STAKE / 2,
                near1.validator_signer.as_ref().unwrap().public_key(),
                genesis_hash,
            );
            actix::spawn(
                client1
                    .send(NetworkClientMessages::Transaction {
                        transaction: unstake_transaction,
                        is_forwarded: false,
                        check_only: false,
                    })
                    .map(drop),
            );

            let started = Arc::new(AtomicBool::new(false));
            let dir2_path = dir2.path().to_path_buf();
            let arbiters_holder = Arc::new(RwLock::new(vec![]));
            let arbiters_holder2 = arbiters_holder.clone();
            WaitOrTimeout::new(
                Box::new(move |_ctx| {
                    let started_copy = started.clone();
                    let near2_copy = near2.clone();
                    let dir2_path_copy = dir2_path.clone();
                    let arbiters_holder2 = arbiters_holder2.clone();
                    actix::spawn(view_client1.send(GetBlock::latest()).then(move |res| {
                        let latest_height =
                            if let Ok(Ok(block)) = res { block.header.height } else { 0 };
                        if !started_copy.load(Ordering::SeqCst) && latest_height > 10 {
                            started_copy.store(true, Ordering::SeqCst);
                            let (_, view_client2, arbiters) =
                                start_with_config(&dir2_path_copy, near2_copy);
                            *arbiters_holder2.write().unwrap() = arbiters;

                            WaitOrTimeout::new(
                                Box::new(move |_ctx| {
                                    actix::spawn(view_client2.send(GetBlock::latest()).then(
                                        move |res| {
                                            if let Ok(Ok(block)) = res {
                                                if block.header.height > latest_height + 1 {
                                                    System::current().stop()
                                                }
                                            }
                                            future::ready(())
                                        },
                                    ));
                                }),
                                100,
                                30000,
                            )
                            .start();
                        }
                        future::ready(())
                    }));
                }),
                100,
                35000,
            )
            .start();
        });
    });
}
