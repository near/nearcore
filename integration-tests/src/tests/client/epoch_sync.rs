use crate::nearcore_utils::{add_blocks, setup_configs_with_epoch_length};
use crate::test_helpers::heavy_test;
use actix::Actor;
use actix_rt::System;
use futures::{future, FutureExt};
use near_actix_test_utils::run_actix;
use near_async::time::Clock;
use near_chain::Provenance;
use near_chain::{BlockProcessingArtifact, ChainStoreAccess};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_client_primitives::types::GetBlock;
use near_crypto::{InMemorySigner, KeyType};
use near_network::test_utils::WaitOrTimeoutActor;
use near_o11y::testonly::{init_integration_logger, init_test_logger};
use near_o11y::WithSpanContextExt;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_sync::EpochSyncInfo;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::get_num_state_parts;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives::types::EpochId;
use near_primitives::utils::MaybeValidated;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::BlockHeight;
use near_store::Mode::ReadOnly;
use near_store::{DBCol, NodeStorage};
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use nearcore::{start_with_config, NearConfig};
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock};

fn generate_transactions(last_hash: &CryptoHash, h: BlockHeight) -> Vec<SignedTransaction> {
    let mut txs = vec![];
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    if h == 1 {
        txs.push(SignedTransaction::from_actions(
            h,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::DeployContract(DeployContractAction {
                code: near_test_contracts::rs_contract().to_vec(),
            })],
            *last_hash,
            0,
        ));
    }

    for i in 0..5 {
        txs.push(SignedTransaction::from_actions(
            h * 10 + i,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "write_random_value".to_string(),
                args: vec![],
                gas: 100_000_000_000_000,
                deposit: 0,
            }))],
            *last_hash,
            0,
        ));
    }

    for i in 0..5 {
        txs.push(SignedTransaction::send_money(
            h * 10 + i,
            "test0".parse().unwrap(),
            "test1".parse().unwrap(),
            &signer,
            1,
            *last_hash,
        ));
    }
    txs
}

/// Produce 4 epochs with some transactions.
/// When the first block of the next epoch is finalised check that `EpochSyncInfo` has been recorded.
#[test]
fn test_continuous_epoch_sync_info_population() {
    init_test_logger();

    let epoch_length = 5;
    let max_height = epoch_length * 4 + 3;

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();

    let mut last_hash = *env.clients[0].chain.genesis().hash();
    let mut last_epoch_id = EpochId::default();

    for h in 1..max_height {
        for tx in generate_transactions(&last_hash, h) {
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        last_hash = *block.hash();

        let last_final_hash = block.header().last_final_block();
        if *last_final_hash == CryptoHash::default() {
            continue;
        }
        let last_final_header =
            env.clients[0].chain.chain_store().get_block_header(last_final_hash).unwrap();

        if *last_final_header.epoch_id() != last_epoch_id {
            let epoch_id = last_epoch_id.clone();

            tracing::debug!("Checking epoch: {:?}", &epoch_id);
            assert!(env.clients[0].chain.chain_store().get_epoch_sync_info(&epoch_id).is_ok());
            tracing::debug!("OK");
        }

        last_epoch_id = last_final_header.epoch_id().clone();
    }
}

/// Produce 4 epochs + 10 blocks.
/// Start second node without epoch sync, but with state sync.
/// Sync second node to first node (at least headers).
/// Check that it has all EpochSyncInfo records and all of them are correct.
///
/// The header sync part is based on `integration-tests::nearcore::sync_nodes::sync_nodes`.
#[test]
fn test_continuous_epoch_sync_info_population_on_header_sync() {
    heavy_test(|| {
        init_integration_logger();

        let (genesis, genesis_block, mut near1_base, mut near2_base) =
            setup_configs_with_epoch_length(50);

        let dir1_base =
            tempfile::Builder::new().prefix("epoch_sync_info_in_header_sync_1").tempdir().unwrap();
        let dir2_base =
            tempfile::Builder::new().prefix("epoch_sync_info_in_header_sync_2").tempdir().unwrap();
        let epoch_ids_base = Arc::new(RwLock::new(HashSet::new()));

        let near1 = near1_base.clone();
        let near2 = near2_base.clone();
        let dir1_path = dir1_base.path();
        let dir2_path = dir2_base.path();
        let epoch_ids = epoch_ids_base.clone();

        run_actix(async move {
            // Start first node
            let nearcore::NearNode { client: client1, .. } =
                start_with_config(dir1_path, near1).expect("start_with_config");

            // Generate 4 epochs + 10 blocks
            let signer = create_test_signer("other");
            let blocks = add_blocks(
                Clock::real(),
                vec![genesis_block],
                client1,
                210,
                genesis.config.epoch_length,
                &signer,
            );

            // Save all finished epoch_ids
            let mut epoch_ids = epoch_ids.write().unwrap();
            for block in blocks[0..200].iter() {
                epoch_ids.insert(block.header().epoch_id().clone());
            }

            // Start second node
            let nearcore::NearNode { view_client: view_client2, .. } =
                start_with_config(dir2_path, near2).expect("start_with_config");

            // Wait for second node's headers to sync.
            // Timeout here means that header sync is not working.
            // Header sync is better debugged through other tests.
            // For example, run `integration-tests::nearcore::sync_nodes::sync_nodes` test,
            // on which this test's setup is based.
            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    let actor = view_client2.send(GetBlock::latest().with_span_context());
                    let actor = actor.then(|res| {
                        match &res {
                            Ok(Ok(b)) if b.header.height == 210 => System::current().stop(),
                            Err(_) => return future::ready(()),
                            _ => {}
                        };
                        future::ready(())
                    });
                    actix::spawn(actor);
                }),
                100,
                120000,
            )
            .start();
        });

        // Open storages of both nodes
        let open_read_only_storage = |home_dir: &Path, near_config: &NearConfig| -> NodeStorage {
            let opener = NodeStorage::opener(home_dir, false, &near_config.config.store, None);
            opener.open_in_mode(ReadOnly).unwrap()
        };

        let store1 = open_read_only_storage(dir1_base.path(), &mut near1_base).get_hot_store();
        let store2 = open_read_only_storage(dir2_base.path(), &mut near2_base).get_hot_store();

        // Check that for every epoch second store has EpochSyncInfo.
        // And that values in both stores are the same.
        let epoch_ids = epoch_ids_base.read().unwrap();
        for epoch_id in epoch_ids.iter() {
            // Check that we have a value for EpochSyncInfo in the synced node
            assert!(
                store2
                    .get_ser::<EpochSyncInfo>(DBCol::EpochSyncInfo, epoch_id.as_ref())
                    .unwrap()
                    .is_some(),
                "{:?}",
                epoch_id
            );
            // Check that it matches value in full node exactly
            assert_eq!(
                store1
                    .get_ser::<EpochSyncInfo>(DBCol::EpochSyncInfo, epoch_id.as_ref())
                    .unwrap()
                    .unwrap(),
                store2
                    .get_ser::<EpochSyncInfo>(DBCol::EpochSyncInfo, epoch_id.as_ref())
                    .unwrap()
                    .unwrap(),
                "{:?}",
                epoch_id
            );
        }
    });
}

/// Check that we can reconstruct `BlockInfo` and `epoch_sync_data_hash` from `EpochSyncInfo`.
#[test]
fn test_epoch_sync_data_hash_from_epoch_sync_info() {
    init_test_logger();

    let epoch_length = 5;
    let max_height = epoch_length * 4 + 3;

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();

    let mut last_hash = *env.clients[0].chain.genesis().hash();
    let mut last_epoch_id = EpochId::default();

    for h in 1..max_height {
        for tx in generate_transactions(&last_hash, h) {
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        last_hash = *block.hash();

        let last_final_hash = block.header().last_final_block();
        if *last_final_hash == CryptoHash::default() {
            continue;
        }
        let last_final_header =
            env.clients[0].chain.chain_store().get_block_header(last_final_hash).unwrap();

        if *last_final_header.epoch_id() != last_epoch_id {
            let epoch_id = last_epoch_id.clone();

            let epoch_sync_info =
                env.clients[0].chain.chain_store().get_epoch_sync_info(&epoch_id).unwrap();

            tracing::debug!("Checking epoch sync info: {:?}", &epoch_sync_info);

            // Check that all BlockInfos needed for new epoch sync can be reconstructed.
            // This also helps with debugging if `epoch_sync_data_hash` doesn't match.
            for hash in &epoch_sync_info.headers_to_save {
                let block_info = env.clients[0]
                    .chain
                    .chain_store()
                    .store()
                    .get_ser::<BlockInfo>(DBCol::BlockInfo, hash.as_ref())
                    .unwrap()
                    .unwrap();
                let reconstructed_block_info = epoch_sync_info.get_block_info(hash).unwrap();
                assert_eq!(block_info, reconstructed_block_info);
            }

            assert_eq!(
                epoch_sync_info.calculate_epoch_sync_data_hash().unwrap(),
                epoch_sync_info.get_epoch_sync_data_hash().unwrap().unwrap(),
            );

            tracing::debug!("OK");
        }

        last_epoch_id = last_final_header.epoch_id().clone();
    }
}

/// This is an unreliable test that mocks/reimplements sync logic.
/// After epoch sync is integrated into sync process we can write a better test.
///
/// The test simulates two clients, one of which is
/// - stopped after one epoch
/// - synced through epoch sync, header sync, state sync, and block sync
/// - in sync with other client for two more epochs
#[test]
#[ignore]
fn test_node_after_simulated_sync() {
    init_test_logger();
    let num_clients = 2;
    let epoch_length = 20;
    let num_epochs = 5;
    // Max height for clients[0] before sync.
    let max_height_0 = epoch_length * num_epochs - 1;
    // Max height for clients[1] before sync.
    let max_height_1 = epoch_length;

    // TestEnv setup
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;

    let mut env = TestEnv::builder(&genesis.config)
        .clients_count(num_clients)
        .real_stores()
        .use_state_snapshots()
        .nightshade_runtimes(&genesis)
        .build();

    // Produce blocks
    let mut last_hash = *env.clients[0].chain.genesis().hash();
    let mut blocks = vec![];

    for h in 1..max_height_0 {
        for tx in generate_transactions(&last_hash, h) {
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        last_hash = *block.hash();
        blocks.push(block.clone());

        if h < max_height_1 {
            env.process_block(1, block.clone(), Provenance::NONE);
        }
    }

    // Do "epoch sync" up to last epoch

    // Current epoch for clients[0].
    let epoch_id0 = env.clients[0].chain.header_head().unwrap().epoch_id;
    // Next epoch for clients[1].
    let mut epoch_id1 = env.clients[1].chain.header_head().unwrap().epoch_id;

    // We rely on the fact that epoch_id0 is not finished for clients[0].
    // So we need to "sync" all epochs in [epoch_id1, epoch_id0).
    while epoch_id1 != epoch_id0 {
        tracing::debug!("Syncing epoch {:?}", epoch_id1);

        let epoch_sync_data =
            env.clients[0].chain.chain_store().get_epoch_sync_info(&epoch_id1).unwrap();
        env.clients[1].chain.validate_and_record_epoch_sync_info(&epoch_sync_data).unwrap();

        epoch_id1 = env.clients[1]
            .epoch_manager
            .get_next_epoch_id(&env.clients[1].chain.header_head().unwrap().last_block_hash)
            .unwrap();
    }

    // Do "header sync" for the current epoch for clients[0].
    tracing::debug!("Client 0 Header Head: {:?}", env.clients[0].chain.header_head());
    tracing::debug!("Client 1 Header Head Before: {:?}", env.clients[1].chain.header_head());

    let mut last_epoch_headers = vec![];
    for block in &blocks {
        if *block.header().epoch_id() == epoch_id0 {
            last_epoch_headers.push(block.header().clone());
        }
    }
    env.clients[1].chain.sync_block_headers(last_epoch_headers, &mut vec![]).unwrap();

    tracing::debug!("Client 0 Header Head: {:?}", env.clients[0].chain.header_head());
    tracing::debug!("Client 1 Header Head After: {:?}", env.clients[1].chain.header_head());

    // Do "state sync" for the last epoch
    // write last block of prev epoch
    {
        let mut store_update = env.clients[1].chain.chain_store().store().store_update();

        let mut last_block = &blocks[0];
        for block in &blocks {
            if *block.header().epoch_id() == epoch_id0 {
                break;
            }
            last_block = block;
        }

        tracing::debug!("Write block {:?}", last_block.header());

        store_update.insert_ser(DBCol::Block, last_block.hash().as_ref(), last_block).unwrap();
        store_update.commit().unwrap();
    }

    let sync_hash = *env.clients[0]
        .epoch_manager
        .get_block_info(&env.clients[0].chain.header_head().unwrap().last_block_hash)
        .unwrap()
        .epoch_first_block();
    tracing::debug!("SYNC HASH: {:?}", sync_hash);
    for shard_id in env.clients[0].epoch_manager.shard_ids(&epoch_id0).unwrap() {
        tracing::debug!("Start syncing shard {:?}", shard_id);
        let sync_block_header = env.clients[0].chain.get_block_header(&sync_hash).unwrap();
        let sync_prev_header =
            env.clients[0].chain.get_previous_header(&sync_block_header).unwrap();
        let sync_prev_prev_hash = sync_prev_header.prev_hash();

        let state_header =
            env.clients[0].chain.compute_state_response_header(shard_id, sync_hash).unwrap();
        let state_root = state_header.chunk_prev_state_root();
        let num_parts = get_num_state_parts(state_header.state_root_node().memory_usage);

        for part_id in 0..num_parts {
            tracing::debug!("Syncing part {:?} of {:?}", part_id, num_parts);
            let state_part = env.clients[0]
                .chain
                .runtime_adapter
                .obtain_state_part(
                    shard_id,
                    sync_prev_prev_hash,
                    &state_root,
                    PartId::new(part_id, num_parts),
                )
                .unwrap();

            env.clients[1]
                .runtime_adapter
                .apply_state_part(
                    shard_id,
                    &state_root,
                    PartId::new(part_id, num_parts),
                    state_part.as_ref(),
                    &epoch_id0,
                )
                .unwrap();
        }
    }

    env.clients[1]
        .chain
        .reset_heads_post_state_sync(
            &None,
            sync_hash,
            &mut BlockProcessingArtifact::default(),
            None,
        )
        .unwrap();

    tracing::debug!("Client 0 Head: {:?}", env.clients[0].chain.head());
    tracing::debug!("Client 1 Head: {:?}", env.clients[1].chain.head());

    // Do "block sync" for the last epoch

    for block in &blocks {
        if *block.header().epoch_id() == epoch_id0 {
            tracing::debug!("Receive block {:?}", block.header());
            env.clients[1]
                .process_block_test(MaybeValidated::from(block.clone()), Provenance::NONE)
                .unwrap();
        }
    }

    // Produce blocks on clients[0] and process them on clients[1]
    for h in max_height_0..(max_height_0 + 2 * epoch_length) {
        tracing::debug!("Produce and process block {}", h);
        for tx in generate_transactions(&last_hash, h) {
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        env.process_block(1, block.clone(), Provenance::NONE);
        last_hash = *block.hash();
    }
}
