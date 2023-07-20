use anyhow::{anyhow, Context};
use borsh::BorshDeserialize;
use near_chain::chain::collect_receipts_from_response;
use near_chain::migrations::check_if_block_is_first_with_chunk_of_version;
use near_chain::types::{ApplyTransactionResult, RuntimeAdapter};
use near_chain::{ChainStore, ChainStoreAccess};
use near_epoch_manager::{EpochManagerAdapter, EpochManagerHandle};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::combine_hash;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout;
use near_primitives::sharding::{ChunkHash, ReceiptProof};
use near_primitives::syncing::ReceiptProofResponse;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives_core::hash::hash;
use near_primitives_core::types::Gas;
use near_store::DBCol;
use near_store::Store;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::warn;

// like ChainStoreUpdate::get_incoming_receipts_for_shard(), but for the case when we don't
// know of a block containing the target chunk
fn get_incoming_receipts(
    chain_store: &mut ChainStore,
    chunk_hash: &ChunkHash,
    shard_id: u64,
    target_height: u64,
    prev_hash: &CryptoHash,
    prev_height_included: u64,
    rng: Option<StdRng>,
) -> anyhow::Result<Vec<Receipt>> {
    let mut receipt_proofs = vec![];

    let chunk_hashes = chain_store.get_all_chunk_hashes_by_height(target_height)?;
    if !chunk_hashes.contains(chunk_hash) {
        return Err(anyhow!(
            "given chunk hash is not listed in DBCol::ChunkHashesByHeight[{}]",
            target_height
        ));
    }

    let mut chunks =
        chunk_hashes.iter().map(|h| chain_store.get_chunk(h).unwrap()).collect::<Vec<_>>();
    chunks.sort_by_key(|chunk| chunk.shard_id());

    for chunk in chunks {
        let partial_encoded_chunk = chain_store.get_partial_chunk(&chunk.chunk_hash()).unwrap();
        for receipt in partial_encoded_chunk.receipts().iter() {
            let ReceiptProof(_, shard_proof) = receipt;
            if shard_proof.to_shard_id == shard_id {
                receipt_proofs.push(receipt.clone());
            }
        }
    }

    if let Some(mut rng) = rng {
        // for testing purposes, shuffle the receipts the same way it's done normally so we can compare the state roots
        receipt_proofs.shuffle(&mut rng);
    }
    let mut responses = vec![ReceiptProofResponse(CryptoHash::default(), Arc::new(receipt_proofs))];
    responses.extend_from_slice(&chain_store.store_update().get_incoming_receipts_for_shard(
        shard_id,
        *prev_hash,
        prev_height_included,
    )?);
    Ok(collect_receipts_from_response(&responses))
}

// returns (apply_result, gas limit)
pub(crate) fn apply_chunk(
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    chain_store: &mut ChainStore,
    chunk_hash: ChunkHash,
    target_height: Option<u64>,
    rng: Option<StdRng>,
    use_flat_storage: bool,
) -> anyhow::Result<(ApplyTransactionResult, Gas)> {
    let chunk = chain_store.get_chunk(&chunk_hash)?;
    let chunk_header = chunk.cloned_header();

    let prev_block_hash = chunk_header.prev_block_hash();
    let shard_id = chunk.shard_id();
    let prev_state_root = chunk.prev_state_root();

    let transactions = chunk.transactions();
    let prev_block =
        chain_store.get_block(prev_block_hash).context("Failed getting chunk's prev block")?;
    let prev_height_included = prev_block.chunks()[shard_id as usize].height_included();
    let prev_height = prev_block.header().height();
    let target_height = match target_height {
        Some(h) => h,
        None => prev_height + 1,
    };
    let prev_timestamp = prev_block.header().raw_timestamp();
    let gas_price = prev_block.header().gas_price();
    let receipts = get_incoming_receipts(
        chain_store,
        &chunk_hash,
        shard_id,
        target_height,
        prev_block_hash,
        prev_height_included,
        rng,
    )
    .context("Failed collecting incoming receipts")?;

    if use_flat_storage {
        let shard_uid =
            epoch_manager.shard_id_to_uid(shard_id as u64, prev_block.header().epoch_id()).unwrap();
        runtime
            .get_flat_storage_manager()
            .unwrap()
            .create_flat_storage_for_shard(shard_uid)
            .unwrap();
    }

    let is_first_block_with_chunk_of_version = check_if_block_is_first_with_chunk_of_version(
        chain_store,
        epoch_manager,
        prev_block_hash,
        shard_id,
    )?;

    Ok((
        runtime.apply_transactions(
            shard_id,
            &prev_state_root,
            target_height,
            prev_timestamp + 1_000_000_000,
            prev_block_hash,
            &combine_hash(
                prev_block_hash,
                &hash("nonsense block hash for testing purposes".as_ref()),
            ),
            &receipts,
            transactions,
            chunk_header.validator_proposals(),
            gas_price,
            chunk_header.gas_limit(),
            &vec![],
            hash("random seed".as_ref()),
            true,
            is_first_block_with_chunk_of_version,
            Default::default(),
            use_flat_storage,
        )?,
        chunk_header.gas_limit(),
    ))
}

enum HashType {
    Tx,
    Receipt,
}

fn find_tx_or_receipt(
    hash: &CryptoHash,
    block_hash: &CryptoHash,
    epoch_manager: &EpochManagerHandle,
    chain_store: &mut ChainStore,
) -> anyhow::Result<Option<(HashType, ShardId)>> {
    let block = chain_store.get_block(block_hash)?;
    let chunk_hashes = block.chunks().iter().map(|c| c.chunk_hash()).collect::<Vec<_>>();

    for (shard_id, chunk_hash) in chunk_hashes.iter().enumerate() {
        let chunk =
            chain_store.get_chunk(chunk_hash).context("Failed looking up canditate chunk")?;
        for tx in chunk.transactions() {
            if &tx.get_hash() == hash {
                return Ok(Some((HashType::Tx, shard_id as ShardId)));
            }
        }
        for receipt in chunk.receipts() {
            if &receipt.get_hash() == hash {
                let shard_layout =
                    epoch_manager.get_shard_layout_from_prev_block(chunk.prev_block())?;
                let to_shard =
                    shard_layout::account_id_to_shard_id(&receipt.receiver_id, &shard_layout);
                return Ok(Some((HashType::Receipt, to_shard)));
            }
        }
    }
    Ok(None)
}

fn apply_tx_in_block(
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    chain_store: &mut ChainStore,
    tx_hash: &CryptoHash,
    block_hash: CryptoHash,
    use_flat_storage: bool,
) -> anyhow::Result<ApplyTransactionResult> {
    match find_tx_or_receipt(tx_hash, &block_hash, epoch_manager, chain_store)? {
        Some((hash_type, shard_id)) => {
            match hash_type {
                HashType::Tx => {
                    println!("Found tx in block {} shard {}. equivalent command:\nview_state apply --height {} --shard-id {}\n",
                             &block_hash, shard_id, chain_store.get_block_header(&block_hash)?.height(), shard_id);
                    let (block, apply_result) = crate::commands::apply_block(block_hash, shard_id, epoch_manager, runtime, chain_store, use_flat_storage);
                    crate::commands::check_apply_block_result(&block, &apply_result, epoch_manager, chain_store, shard_id)?;
                    Ok(apply_result)
                },
                HashType::Receipt => {
                    Err(anyhow!("{} appears to be a Receipt ID, not a tx hash. Try running:\nview_state apply_receipt --hash {}", tx_hash, tx_hash))
                },
            }
        },
        None => {
            Err(anyhow!("Could not find tx with hash {} in block {}, even though `DBCol::TransactionResultForBlock` says it should be there", tx_hash, block_hash))
        }
    }
}

fn apply_tx_in_chunk(
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    store: Store,
    chain_store: &mut ChainStore,
    tx_hash: &CryptoHash,
    use_flat_storage: bool,
) -> anyhow::Result<Vec<ApplyTransactionResult>> {
    if chain_store.get_transaction(tx_hash)?.is_none() {
        return Err(anyhow!("tx with hash {} not known", tx_hash));
    }

    println!("Transaction is known but doesn't seem to have been applied. Searching in chunks that haven't been applied...");

    let head = chain_store.head()?.height;
    let mut chunk_hashes = vec![];

    for item in store.iter(DBCol::ChunkHashesByHeight) {
        let (k, v) = item.context("scanning ChunkHashesByHeight column")?;
        let height = BlockHeight::from_le_bytes(k[..].try_into().unwrap());
        if height > head {
            let hashes = HashSet::<ChunkHash>::try_from_slice(&v).unwrap();
            for chunk_hash in hashes {
                let chunk = match chain_store.get_chunk(&chunk_hash) {
                    Ok(c) => c,
                    Err(_) => {
                        warn!(target: "state-viewer", "chunk hash {:?} appears in DBCol::ChunkHashesByHeight but the chunk is not saved", &chunk_hash);
                        continue;
                    }
                };
                for hash in chunk.transactions().iter().map(|tx| tx.get_hash()) {
                    if hash == *tx_hash {
                        chunk_hashes.push(chunk_hash);
                        break;
                    }
                }
            }
        }
    }

    if chunk_hashes.is_empty() {
        return Err(anyhow!(
            "Could not find tx with hash {} in any chunk that hasn't been applied yet",
            tx_hash
        ));
    }

    let mut results = Vec::new();
    for chunk_hash in chunk_hashes {
        println!("found tx in chunk {}. Equivalent command (which will run faster than apply_tx):\nview_state apply_chunk --chunk_hash {}\n", &chunk_hash.0, &chunk_hash.0);
        let (apply_result, gas_limit) = apply_chunk(
            epoch_manager,
            runtime,
            chain_store,
            chunk_hash,
            None,
            None,
            use_flat_storage,
        )?;
        println!(
            "resulting chunk extra:\n{:?}",
            crate::commands::resulting_chunk_extra(&apply_result, gas_limit)
        );
        results.push(apply_result);
    }
    Ok(results)
}

pub(crate) fn apply_tx(
    genesis_height: BlockHeight,
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    store: Store,
    tx_hash: CryptoHash,
    use_flat_storage: bool,
) -> anyhow::Result<Vec<ApplyTransactionResult>> {
    let mut chain_store = ChainStore::new(store.clone(), genesis_height, false);
    let outcomes = chain_store.get_outcomes_by_id(&tx_hash)?;

    if let Some(outcome) = outcomes.first() {
        Ok(vec![apply_tx_in_block(
            epoch_manager,
            runtime,
            &mut chain_store,
            &tx_hash,
            outcome.block_hash,
            use_flat_storage,
        )?])
    } else {
        apply_tx_in_chunk(
            epoch_manager,
            runtime,
            store,
            &mut chain_store,
            &tx_hash,
            use_flat_storage,
        )
    }
}

fn apply_receipt_in_block(
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    chain_store: &mut ChainStore,
    id: &CryptoHash,
    block_hash: CryptoHash,
    use_flat_storage: bool,
) -> anyhow::Result<ApplyTransactionResult> {
    match find_tx_or_receipt(id, &block_hash, epoch_manager, chain_store)? {
        Some((hash_type, shard_id)) => {
            match hash_type {
                HashType::Tx => {
                    Err(anyhow!("{} appears to be a tx hash, not a Receipt ID. Try running:\nview_state apply_tx --hash {}", id, id))
                },
                HashType::Receipt => {
                    println!("Found receipt in block {}. Receiver is in shard {}. equivalent command:\nview_state apply --height {} --shard-id {}\n",
                             &block_hash, shard_id, chain_store.get_block_header(&block_hash)?.height(), shard_id);
                    let (block, apply_result) = crate::commands::apply_block(block_hash, shard_id, epoch_manager, runtime, chain_store, use_flat_storage);
                    crate::commands::check_apply_block_result(&block, &apply_result, epoch_manager, chain_store, shard_id)?;
                    Ok(apply_result)
                },
            }
        },
        None => {
            // TODO: handle local/delayed receipts
            Err(anyhow!("Could not find receipt with ID {} in block {}. Is it a local or delayed receipt?", id, block_hash))
        }
    }
}

fn apply_receipt_in_chunk(
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    store: Store,
    chain_store: &mut ChainStore,
    id: &CryptoHash,
    use_flat_storage: bool,
) -> anyhow::Result<Vec<ApplyTransactionResult>> {
    if chain_store.get_receipt(id)?.is_none() {
        // TODO: handle local/delayed receipts
        return Err(anyhow!("receipt with ID {} not known. Is it a local or delayed receipt?", id));
    }

    println!(
        "Receipt is known but doesn't seem to have been applied. Searching in chunks that haven't been applied..."
    );

    let head = chain_store.head()?.height;
    let mut to_apply = HashSet::new();
    let mut non_applied_chunks = HashMap::new();

    for item in store.iter(DBCol::ChunkHashesByHeight) {
        let (k, v) = item.context("scanning ChunkHashesByHeight column")?;
        let height = BlockHeight::from_le_bytes(k[..].try_into().unwrap());
        if height > head {
            let hashes = HashSet::<ChunkHash>::try_from_slice(&v).unwrap();
            for chunk_hash in hashes {
                let chunk = match chain_store.get_chunk(&chunk_hash) {
                    Ok(c) => c,
                    Err(_) => {
                        warn!(target: "state-viewer", "chunk hash {:?} appears in DBCol::ChunkHashesByHeight but the chunk is not saved", &chunk_hash);
                        continue;
                    }
                };
                non_applied_chunks.insert((height, chunk.shard_id()), chunk_hash.clone());

                for receipt in chunk.receipts().iter() {
                    if receipt.get_hash() == *id {
                        let shard_layout =
                            epoch_manager.get_shard_layout_from_prev_block(chunk.prev_block())?;
                        let to_shard = shard_layout::account_id_to_shard_id(
                            &receipt.receiver_id,
                            &shard_layout,
                        );
                        to_apply.insert((height, to_shard));
                        println!(
                            "found receipt in chunk {}. Receiver is in shard {}",
                            &chunk_hash.0, to_shard
                        );
                        break;
                    }
                }
            }
        }
    }

    if to_apply.is_empty() {
        return Err(anyhow!(
            "Could not find receipt with hash {} in any chunk that hasn't been applied yet",
            id
        ));
    }

    let mut results = Vec::new();
    for (height, shard_id) in to_apply {
        let chunk_hash = match non_applied_chunks.get(&(height, shard_id)) {
            Some(h) => h,
            None => {
                eprintln!(
                    "Wanted to apply chunk in shard {} at height {}, but no such chunk was found.",
                    shard_id, height,
                );
                continue;
            }
        };
        println!("Applying chunk at height {} in shard {}. Equivalent command (which will run faster than apply_receipt):\nview_state apply_chunk --chunk_hash {}\n",
                 height, shard_id, chunk_hash.0);
        let (apply_result, gas_limit) = apply_chunk(
            epoch_manager,
            runtime,
            chain_store,
            chunk_hash.clone(),
            None,
            None,
            use_flat_storage,
        )?;
        let chunk_extra = crate::commands::resulting_chunk_extra(&apply_result, gas_limit);
        println!("resulting chunk extra:\n{:?}", chunk_extra);
        results.push(apply_result);
    }
    Ok(results)
}

pub(crate) fn apply_receipt(
    genesis_height: BlockHeight,
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    store: Store,
    id: CryptoHash,
    use_flat_storage: bool,
) -> anyhow::Result<Vec<ApplyTransactionResult>> {
    let mut chain_store = ChainStore::new(store.clone(), genesis_height, false);
    let outcomes = chain_store.get_outcomes_by_id(&id)?;
    if let Some(outcome) = outcomes.first() {
        Ok(vec![apply_receipt_in_block(
            epoch_manager,
            runtime,
            &mut chain_store,
            &id,
            outcome.block_hash,
            use_flat_storage,
        )?])
    } else {
        apply_receipt_in_chunk(
            epoch_manager,
            runtime,
            store,
            &mut chain_store,
            &id,
            use_flat_storage,
        )
    }
}

#[cfg(test)]
mod test {
    use near_chain::{ChainGenesis, ChainStore, ChainStoreAccess, Provenance};
    use near_chain_configs::Genesis;
    use near_client::test_utils::TestEnv;
    use near_client::ProcessTxResponse;
    use near_crypto::{InMemorySigner, KeyType};
    use near_epoch_manager::{EpochManager, EpochManagerAdapter};
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout;
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::utils::get_num_seats_per_shard;
    use near_store::genesis::initialize_genesis_state;
    use near_store::test_utils::create_test_store;
    use nearcore::config::GenesisExt;
    use nearcore::NightshadeRuntime;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::path::Path;

    fn send_txs(env: &mut TestEnv, signers: &[InMemorySigner], height: u64, hash: CryptoHash) {
        for (i, signer) in signers.iter().enumerate() {
            let from = format!("test{}", i);
            let to = format!("test{}", (i + 1) % signers.len());
            let tx = SignedTransaction::send_money(
                height,
                from.parse().unwrap(),
                to.parse().unwrap(),
                signer,
                100,
                hash,
            );
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }
    }

    #[test]
    fn test_apply_chunk() {
        let genesis = Genesis::test_sharded(
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
        let mut chain_store = ChainStore::new(store.clone(), genesis.config.genesis_height, false);

        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime = NightshadeRuntime::test(
            Path::new("."),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
        );
        let chain_genesis = ChainGenesis::test();

        let signers = (0..4)
            .map(|i| {
                let acc = format!("test{}", i);
                InMemorySigner::from_seed(acc.parse().unwrap(), KeyType::ED25519, &acc)
            })
            .collect::<Vec<_>>();

        let mut env = TestEnv::builder(chain_genesis)
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
            let chunk_hashes = block.chunks().iter().map(|c| c.chunk_hash()).collect::<Vec<_>>();
            let epoch_id = block.header().epoch_id().clone();

            env.process_block(0, block, Provenance::PRODUCED);

            let new_roots = (0..4)
                .map(|i| {
                    let shard_uid = epoch_manager.shard_id_to_uid(i, &epoch_id).unwrap();
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

                    let (apply_result, _) = crate::apply_chunk::apply_chunk(
                        epoch_manager.as_ref(),
                        runtime.as_ref(),
                        &mut chain_store,
                        chunk_hash.clone(),
                        None,
                        Some(rng),
                        false,
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
        let chain_store = ChainStore::new(store.clone(), genesis.config.genesis_height, false);

        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime = NightshadeRuntime::test(
            Path::new("."),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
        );
        let mut chain_genesis = ChainGenesis::test();
        // receipts get delayed with the small ChainGenesis::test() limit
        chain_genesis.gas_limit = genesis.config.gas_limit;

        let signers = (0..4)
            .map(|i| {
                let acc = format!("test{}", i);
                InMemorySigner::from_seed(acc.parse().unwrap(), KeyType::ED25519, &acc)
            })
            .collect::<Vec<_>>();

        let mut env = TestEnv::builder(chain_genesis)
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
            let prev_hash = *block.header().prev_hash();
            let chunk_hashes = block.chunks().iter().map(|c| c.chunk_hash()).collect::<Vec<_>>();
            let epoch_id = block.header().epoch_id().clone();

            env.process_block(0, block, Provenance::PRODUCED);

            let new_roots = (0..4)
                .map(|i| {
                    let shard_uid = epoch_manager.shard_id_to_uid(i, &epoch_id).unwrap();
                    *chain_store.get_chunk_extra(&hash, &shard_uid).unwrap().state_root()
                })
                .collect::<Vec<_>>();
            let shard_layout = epoch_manager.get_shard_layout_from_prev_block(&prev_hash).unwrap();

            if height >= 2 {
                for shard_id in 0..4 {
                    let chunk = chain_store.get_chunk(&chunk_hashes[shard_id]).unwrap();

                    for tx in chunk.transactions() {
                        let results = crate::apply_chunk::apply_tx(
                            genesis.config.genesis_height,
                            &epoch_manager,
                            runtime.as_ref(),
                            store.clone(),
                            tx.get_hash(),
                            false,
                        )
                        .unwrap();
                        assert_eq!(results.len(), 1);
                        assert_eq!(results[0].new_root, new_roots[shard_id as usize]);
                    }

                    for receipt in chunk.receipts() {
                        let to_shard = shard_layout::account_id_to_shard_id(
                            &receipt.receiver_id,
                            &shard_layout,
                        );

                        let results = crate::apply_chunk::apply_receipt(
                            genesis.config.genesis_height,
                            &epoch_manager,
                            runtime.as_ref(),
                            store.clone(),
                            receipt.get_hash(),
                            false,
                        )
                        .unwrap();
                        assert_eq!(results.len(), 1);
                        assert_eq!(results[0].new_root, new_roots[to_shard as usize]);
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

            for tx in chunk.transactions() {
                let results = crate::apply_chunk::apply_tx(
                    genesis.config.genesis_height,
                    &epoch_manager,
                    runtime.as_ref(),
                    store.clone(),
                    tx.get_hash(),
                    false,
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
            for receipt in chunk.receipts() {
                let results = crate::apply_chunk::apply_receipt(
                    genesis.config.genesis_height,
                    &epoch_manager,
                    runtime.as_ref(),
                    store.clone(),
                    receipt.get_hash(),
                    false,
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
}
