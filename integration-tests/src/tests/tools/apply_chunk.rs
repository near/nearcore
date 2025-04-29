use crate::env::test_env::TestEnv;
use itertools::Itertools;
use near_async::time::Clock;
use near_chain::{ChainStore, ChainStoreAccess, Provenance};
use near_chain_configs::Genesis;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, Signer};
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::utils::get_num_seats_per_shard;
use near_state_viewer::cli::StorageSource;
use near_state_viewer::{apply_chunk_fn, apply_receipt, apply_tx};
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use nearcore::NightshadeRuntime;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::path::Path;

fn send_txs(env: &TestEnv, signers: &[Signer], height: u64, hash: CryptoHash) {
    for (i, signer) in signers.iter().enumerate() {
        let from = format!("test{}", i);
        let to = format!("test{}", (i + 1) % signers.len());
        let tx = SignedTransaction::send_money(
            height,
            from.parse().unwrap(),
            to.parse().unwrap(),
            &signer,
            100,
            hash,
        );
        assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    }
}

#[test]
fn test_apply_chunk() {
    let genesis = Genesis::test_sharded(
        Clock::real(),
        vec![
            "test0".parse().unwrap(),
            "test1".parse().unwrap(),
            "test2".parse().unwrap(),
            "test3".parse().unwrap(),
        ],
        1,
        get_num_seats_per_shard(4, 1),
    );

    let store = create_test_store();
    initialize_genesis_state(store.clone(), &genesis, None);
    let mut chain_store =
        ChainStore::new(store.clone(), false, genesis.config.transaction_validity_period);

    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime = NightshadeRuntime::test(
        Path::new("."),
        store.clone(),
        &genesis.config,
        epoch_manager.clone(),
    );

    let signers = (0..4)
        .map(|i| {
            let acc = format!("test{}", i);
            InMemorySigner::test_signer(&acc.parse().unwrap())
        })
        .collect::<Vec<_>>();

    let mut env = TestEnv::builder(&genesis.config)
        .stores(vec![store])
        .epoch_managers(vec![epoch_manager.clone()])
        .track_all_shards()
        .runtimes(vec![runtime.clone()])
        .build();
    let genesis_hash = *env.clients[0].chain.genesis().hash();

    for height in 1..10 {
        send_txs(&mut env, &signers, height, genesis_hash);

        let block = env.clients[0].produce_block(height).unwrap().unwrap();

        let hash = *block.hash();
        let chunk_hashes =
            block.chunks().iter_deprecated().map(|c| c.chunk_hash()).collect::<Vec<_>>();
        let epoch_id = *block.header().epoch_id();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

        env.process_block(0, block, Provenance::PRODUCED);

        let new_roots = shard_layout
            .shard_ids()
            .map(|shard_id| {
                let shard_uid =
                    shard_id_to_uid(epoch_manager.as_ref(), shard_id, &epoch_id).unwrap();
                *chain_store.get_chunk_extra(&hash, &shard_uid).unwrap().state_root()
            })
            .collect::<Vec<_>>();

        if height >= 2 {
            for shard in 0..4 {
                // we will shuffle receipts the same as in production, otherwise the state roots don't match
                let mut slice = [0u8; 32];
                slice.copy_from_slice(hash.as_ref());
                let rng: StdRng = SeedableRng::from_seed(slice);

                let chunk_hash = &chunk_hashes[shard];
                let new_root = new_roots[shard];

                let (apply_result, _) = apply_chunk_fn(
                    epoch_manager.as_ref(),
                    runtime.as_ref(),
                    &mut chain_store,
                    chunk_hash.clone(),
                    None,
                    Some(rng),
                    StorageSource::Trie,
                )
                .unwrap();
                assert_eq!(apply_result.new_root, new_root);
            }
        }
    }
}

#[test]
fn test_apply_tx_apply_receipt() {
    let genesis = Genesis::test_sharded(
        Clock::real(),
        vec![
            "test0".parse().unwrap(),
            "test1".parse().unwrap(),
            "test2".parse().unwrap(),
            "test3".parse().unwrap(),
        ],
        1,
        get_num_seats_per_shard(4, 1),
    );

    let store = create_test_store();
    initialize_genesis_state(store.clone(), &genesis, None);
    let chain_store =
        ChainStore::new(store.clone(), false, genesis.config.transaction_validity_period);

    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime = NightshadeRuntime::test(
        Path::new("."),
        store.clone(),
        &genesis.config,
        epoch_manager.clone(),
    );

    let signers = (0..4)
        .map(|i| {
            let acc = format!("test{}", i);
            InMemorySigner::test_signer(&acc.parse().unwrap())
        })
        .collect::<Vec<_>>();

    let mut env = TestEnv::builder(&genesis.config)
        .stores(vec![store.clone()])
        .epoch_managers(vec![epoch_manager.clone()])
        .track_all_shards()
        .runtimes(vec![runtime.clone()])
        .build();
    let genesis_hash = *env.clients[0].chain.genesis().hash();

    // first check that applying txs and receipts works when the block exists

    for height in 1..5 {
        send_txs(&mut env, &signers, height, genesis_hash);

        let block = env.clients[0].produce_block(height).unwrap().unwrap();

        let hash = *block.hash();
        let chunk_hashes =
            block.chunks().iter_deprecated().map(|c| c.chunk_hash()).collect::<Vec<_>>();
        let epoch_id = *block.header().epoch_id();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

        env.process_block(0, block, Provenance::PRODUCED);

        let shard_ids = shard_layout.shard_ids().collect_vec();
        let new_roots = shard_ids
            .iter()
            .map(|&shard_id| {
                let shard_uid =
                    shard_id_to_uid(epoch_manager.as_ref(), shard_id, &epoch_id).unwrap();
                *chain_store.get_chunk_extra(&hash, &shard_uid).unwrap().state_root()
            })
            .collect::<Vec<_>>();

        if height >= 2 {
            for &shard_id in &shard_ids {
                let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
                let chunk = chain_store.get_chunk(&chunk_hashes[shard_index]).unwrap();

                for tx in chunk.to_transactions() {
                    let results = apply_tx(
                        &genesis.config,
                        &epoch_manager,
                        runtime.as_ref(),
                        store.clone(),
                        tx.get_hash(),
                        StorageSource::Trie,
                    )
                    .unwrap();
                    assert_eq!(results.len(), 1);
                    assert_eq!(results[0].new_root, new_roots[shard_index]);
                }

                for receipt in chunk.prev_outgoing_receipts() {
                    let to_shard_id = shard_layout.account_id_to_shard_id(receipt.receiver_id());
                    let to_shard_index = shard_layout.get_shard_index(to_shard_id).unwrap();

                    let results = apply_receipt(
                        &genesis.config,
                        &epoch_manager,
                        runtime.as_ref(),
                        store.clone(),
                        receipt.get_hash(),
                        StorageSource::Trie,
                    )
                    .unwrap();
                    assert_eq!(results.len(), 1);
                    assert_eq!(results[0].new_root, new_roots[to_shard_index]);
                }
            }
        }
    }

    // then check what happens when the block doesn't exist
    // it won't exist because the chunks for the last height
    // in the loop above are produced by env.process_block() but
    // there was no corresponding env.clients[0].produce_block() after

    let chunks = chain_store.get_all_chunk_hashes_by_height(5).unwrap();
    let blocks = chain_store.get_all_header_hashes_by_height(5).unwrap();
    assert_ne!(chunks.len(), 0);
    assert_eq!(blocks.len(), 0);

    for chunk_hash in chunks {
        let chunk = chain_store.get_chunk(&chunk_hash).unwrap();

        for tx in chunk.to_transactions() {
            let results = apply_tx(
                &genesis.config,
                &epoch_manager,
                runtime.as_ref(),
                store.clone(),
                tx.get_hash(),
                StorageSource::Trie,
            )
            .unwrap();
            for result in results {
                let mut applied = false;
                for outcome in result.outcomes {
                    if outcome.id == tx.get_hash() {
                        applied = true;
                        break;
                    }
                }
                assert!(applied);
            }
        }
        for receipt in chunk.prev_outgoing_receipts() {
            let results = apply_receipt(
                &genesis.config,
                &epoch_manager,
                runtime.as_ref(),
                store.clone(),
                receipt.get_hash(),
                StorageSource::Trie,
            )
            .unwrap();
            for result in results {
                let mut applied = false;
                for outcome in result.outcomes {
                    if outcome.id == receipt.get_hash() {
                        applied = true;
                        break;
                    }
                }
                assert!(applied);
            }
        }
    }
}
