use crate::apply_chain_range::apply_chain_range;
use crate::cli::{ApplyRangeMode, EpochAnalysisMode, StorageSource};
use crate::contract_accounts::ContractAccount;
use crate::contract_accounts::ContractAccountFilter;
use crate::contract_accounts::Summary;
use crate::epoch_info::iterate_and_filter;
use crate::state_dump::state_dump;
use crate::state_dump::state_dump_redis;
use crate::tx_dump::dump_tx_from_block;
use crate::util::{
    check_apply_block_result, load_trie, load_trie_stop_at_height, resulting_chunk_extra,
    LoadTrieMode,
};
use crate::{apply_chunk, epoch_info};
use anyhow::Context;
use bytesize::ByteSize;
use itertools::GroupBy;
use itertools::Itertools;
use near_chain::chain::collect_receipts_from_response;
use near_chain::migrations::check_if_block_is_first_with_chunk_of_version;
use near_chain::types::{
    ApplyChunkBlockContext, ApplyChunkResult, ApplyChunkShardContext, RuntimeAdapter,
};
use near_chain::{Chain, ChainGenesis, ChainStore, ChainStoreAccess, ChainStoreUpdate, Error};
use near_chain_configs::GenesisChangeConfig;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::account::id::AccountId;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::block::Block;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::{ChunkHash, ShardChunk};
use near_primitives::state::FlatStateValue;
use near_primitives::state_record::state_record_to_account_id;
use near_primitives::state_record::StateRecord;
use near_primitives::trie_key::col::COLUMNS_WITH_ACCOUNT_ID_IN_KEY;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{BlockHeight, EpochId, ShardId};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives_core::types::{Balance, EpochHeight};
use near_store::adapter::trie_store::TrieStoreAdapter;
use near_store::adapter::StoreAdapter;
use near_store::flat::FlatStorageChunkView;
use near_store::flat::FlatStorageManager;
use near_store::TrieStorage;
use near_store::{DBCol, Store, Trie, TrieCache, TrieCachingStorage, TrieConfig, TrieDBStorage};
use nearcore::NightshadeRuntimeExt;
use nearcore::{NearConfig, NightshadeRuntime};
use node_runtime::adapter::ViewRuntimeAdapter;
use serde_json::json;
use std::collections::HashMap;
use std::collections::{BTreeMap, BinaryHeap};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use yansi::Color::Red;

pub(crate) fn apply_block(
    block_hash: CryptoHash,
    shard_id: ShardId,
    epoch_manager: &dyn EpochManagerAdapter,
    runtime: &dyn RuntimeAdapter,
    chain_store: &mut ChainStore,
    storage: StorageSource,
) -> (Block, ApplyChunkResult) {
    let block = chain_store.get_block(&block_hash).unwrap();
    let height = block.header().height();
    let shard_uid = epoch_manager.shard_id_to_uid(shard_id, block.header().epoch_id()).unwrap();
    if matches!(storage, StorageSource::FlatStorage) {
        runtime.get_flat_storage_manager().create_flat_storage_for_shard(shard_uid).unwrap();
    }
    let apply_result = if block.chunks()[shard_id as usize].height_included() == height {
        let chunk = chain_store.get_chunk(&block.chunks()[shard_id as usize].chunk_hash()).unwrap();
        let prev_block = chain_store.get_block(block.header().prev_hash()).unwrap();
        let chain_store_update = ChainStoreUpdate::new(chain_store);
        let receipt_proof_response = chain_store_update
            .get_incoming_receipts_for_shard(
                epoch_manager,
                shard_id,
                block_hash,
                prev_block.chunks()[shard_id as usize].height_included(),
            )
            .unwrap();
        let receipts = collect_receipts_from_response(&receipt_proof_response);

        let chunk_inner = chunk.cloned_header().take_inner();
        let is_first_block_with_chunk_of_version = check_if_block_is_first_with_chunk_of_version(
            chain_store,
            epoch_manager,
            block.header().prev_hash(),
            shard_id,
        )
        .unwrap();

        runtime
            .apply_chunk(
                storage.create_runtime_storage(*chunk_inner.prev_state_root()),
                ApplyChunkReason::UpdateTrackedShard,
                ApplyChunkShardContext {
                    shard_id,
                    last_validator_proposals: chunk_inner.prev_validator_proposals(),
                    gas_limit: chunk_inner.gas_limit(),
                    is_new_chunk: true,
                    is_first_block_with_chunk_of_version,
                },
                ApplyChunkBlockContext::from_header(
                    block.header(),
                    prev_block.header().next_gas_price(),
                    prev_block.block_congestion_info(),
                ),
                &receipts,
                chunk.transactions(),
            )
            .unwrap()
    } else {
        let chunk_extra =
            chain_store.get_chunk_extra(block.header().prev_hash(), &shard_uid).unwrap();
        let prev_block = chain_store.get_block(block.header().prev_hash()).unwrap();

        runtime
            .apply_chunk(
                storage.create_runtime_storage(*chunk_extra.state_root()),
                ApplyChunkReason::UpdateTrackedShard,
                ApplyChunkShardContext {
                    shard_id,
                    last_validator_proposals: chunk_extra.validator_proposals(),
                    gas_limit: chunk_extra.gas_limit(),
                    is_new_chunk: false,
                    is_first_block_with_chunk_of_version: false,
                },
                ApplyChunkBlockContext::from_header(
                    block.header(),
                    block.header().next_gas_price(),
                    prev_block.block_congestion_info(),
                ),
                &[],
                &[],
            )
            .unwrap()
    };
    (block, apply_result)
}

pub(crate) fn apply_block_at_height(
    height: BlockHeight,
    shard_id: ShardId,
    storage: StorageSource,
    home_dir: &Path,
    near_config: NearConfig,
    read_store: Store,
    write_store: Option<Store>,
) -> anyhow::Result<()> {
    let mut read_chain_store = ChainStore::new(
        read_store.clone(),
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );
    let epoch_manager =
        EpochManager::new_arc_handle(read_store.clone(), &near_config.genesis.config);
    let runtime =
        NightshadeRuntime::from_config(home_dir, read_store, &near_config, epoch_manager.clone())
            .context("could not create the transaction runtime")?;
    let block_hash = read_chain_store.get_block_hash_by_height(height).unwrap();
    let (block, apply_result) = apply_block(
        block_hash,
        shard_id,
        epoch_manager.as_ref(),
        runtime.as_ref(),
        &mut read_chain_store,
        storage,
    );
    check_apply_block_result(
        &block,
        &apply_result,
        epoch_manager.as_ref(),
        &mut read_chain_store,
        shard_id,
    )?;
    let result = maybe_save_trie_changes(
        write_store.clone(),
        near_config.genesis.config.genesis_height,
        apply_result,
        height,
        shard_id,
    );
    maybe_print_db_stats(write_store);
    result
}

pub(crate) fn apply_chunk(
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    chunk_hash: ChunkHash,
    target_height: Option<u64>,
    storage: StorageSource,
) -> anyhow::Result<()> {
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);
    let runtime = NightshadeRuntime::from_config(
        home_dir,
        store.clone(),
        &near_config,
        epoch_manager.clone(),
    )
    .context("could not create the transaction runtime")?;
    let mut chain_store = ChainStore::new(
        store,
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );
    let (apply_result, gas_limit) = apply_chunk::apply_chunk(
        epoch_manager.as_ref(),
        runtime.as_ref(),
        &mut chain_store,
        chunk_hash,
        target_height,
        None,
        storage,
    )?;
    let protocol_version = if let Some(height) = target_height {
        // Retrieve the protocol version at the given height.
        let block_hash = chain_store.get_block_hash_by_height(height)?;
        chain_store.get_block(&block_hash)?.header().latest_protocol_version()
    } else {
        // No block height specified, fallback to current protocol version.
        PROTOCOL_VERSION
    };
    // Most probably `PROTOCOL_VERSION` won't work if the target_height points to a time
    // before congestion control has been introduced.
    println!(
        "resulting chunk extra:\n{:?}",
        resulting_chunk_extra(&apply_result, gas_limit, protocol_version)
    );
    Ok(())
}

pub(crate) fn apply_range(
    mode: ApplyRangeMode,
    start_index: Option<BlockHeight>,
    end_index: Option<BlockHeight>,
    shard_id: ShardId,
    verbose_output: bool,
    csv_file: Option<PathBuf>,
    home_dir: &Path,
    near_config: NearConfig,
    read_store: Store,
    write_store: Option<Store>,
    only_contracts: bool,
    storage: StorageSource,
) {
    let mut csv_file = csv_file.map(|filename| std::fs::File::create(filename).unwrap());

    let epoch_manager =
        EpochManager::new_arc_handle(read_store.clone(), &near_config.genesis.config);
    let runtime = NightshadeRuntime::from_config(
        home_dir,
        read_store.clone(),
        &near_config,
        epoch_manager.clone(),
    )
    .expect("could not create the transaction runtime");
    apply_chain_range(
        mode,
        read_store,
        write_store.clone(),
        &near_config.genesis,
        start_index,
        end_index,
        shard_id,
        epoch_manager.as_ref(),
        runtime,
        verbose_output,
        csv_file.as_mut(),
        only_contracts,
        storage,
    );
    maybe_print_db_stats(write_store);
}

pub(crate) fn apply_receipt(
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    hash: CryptoHash,
    storage: StorageSource,
) -> anyhow::Result<()> {
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);
    let runtime = NightshadeRuntime::from_config(
        home_dir,
        store.clone(),
        &near_config,
        epoch_manager.clone(),
    )
    .context("could not create the transaction runtime")?;
    apply_chunk::apply_receipt(
        near_config.genesis.config.genesis_height,
        epoch_manager.as_ref(),
        runtime.as_ref(),
        store,
        hash,
        storage,
    )
    .map(|_| ())
}

pub(crate) fn apply_tx(
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    hash: CryptoHash,
    storage: StorageSource,
) -> anyhow::Result<()> {
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);
    let runtime = NightshadeRuntime::from_config(
        home_dir,
        store.clone(),
        &near_config,
        epoch_manager.clone(),
    )
    .context("could not create the transaction runtime")?;
    apply_chunk::apply_tx(
        near_config.genesis.config.genesis_height,
        epoch_manager.as_ref(),
        runtime.as_ref(),
        store,
        hash,
        storage,
    )
    .map(|_| ())
}

pub(crate) fn dump_account_storage(
    account_id: String,
    storage_key: String,
    output: &Path,
    block_height: String,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
) {
    let block_height = if block_height == "latest" {
        LoadTrieMode::Latest
    } else if let Ok(height) = block_height.parse::<u64>() {
        LoadTrieMode::Height(height)
    } else {
        panic!("block_height should be either number or \"latest\"")
    };
    let (_, runtime, state_roots, header) =
        load_trie_stop_at_height(store, home_dir, &near_config, block_height);
    for (shard_id, state_root) in state_roots.iter().enumerate() {
        let trie = runtime
            .get_trie_for_shard(shard_id as u64, header.prev_hash(), *state_root, false)
            .unwrap();
        let key = TrieKey::ContractData {
            account_id: account_id.parse().unwrap(),
            key: storage_key.as_bytes().to_vec(),
        };
        let item = trie.get(&key.to_vec());
        let value = item.unwrap();
        if let Some(value) = value {
            let record = StateRecord::from_raw_key_value(key.to_vec(), value).unwrap();
            match record {
                StateRecord::Data { account_id: _, data_key: _, value } => {
                    fs::write(output, value.as_slice()).unwrap();
                    println!(
                        "Dump contract storage under key {} of account {} into file {}",
                        storage_key,
                        account_id,
                        output.display()
                    );
                    std::process::exit(0);
                }
                _ => unreachable!(),
            }
        }
    }
    println!("Storage under key {} of account {} not found", storage_key, account_id);
    std::process::exit(1);
}

pub(crate) fn dump_code(
    account_id: String,
    output: &Path,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
) {
    let (epoch_manager, runtime, state_roots, header) = load_trie(store, home_dir, &near_config);
    let epoch_id = &epoch_manager.get_epoch_id(header.hash()).unwrap();

    for (shard_id, state_root) in state_roots.iter().enumerate() {
        let shard_uid = epoch_manager.shard_id_to_uid(shard_id as u64, epoch_id).unwrap();
        if let Ok(contract_code) =
            runtime.view_contract_code(&shard_uid, *state_root, &account_id.parse().unwrap())
        {
            let mut file = File::create(output).unwrap();
            file.write_all(contract_code.code()).unwrap();
            println!("Dump contract of account {} into file {}", account_id, output.display());

            std::process::exit(0);
        }
    }
    println!(
        "Account {} does not exist or do not have contract deployed in all shards",
        account_id
    );
}

pub(crate) fn dump_state(
    height: Option<BlockHeight>,
    stream: bool,
    file: Option<PathBuf>,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    change_config: &GenesisChangeConfig,
) {
    let mode = match height {
        Some(h) => LoadTrieMode::LastFinalFromHeight(h),
        None => LoadTrieMode::Latest,
    };
    let (epoch_manager, runtime, state_roots, header) =
        load_trie_stop_at_height(store, home_dir, &near_config, mode);
    let height = header.height();
    let home_dir = PathBuf::from(&home_dir);

    if stream {
        let output_dir = file.unwrap_or_else(|| home_dir.join("output"));
        let records_path = output_dir.join("records.json");
        let new_near_config = state_dump(
            epoch_manager.as_ref(),
            runtime,
            &state_roots,
            header,
            &near_config,
            Some(&records_path),
            change_config,
        );
        println!("Saving state at {:?} @ {} into {}", state_roots, height, output_dir.display(),);
        new_near_config.save_to_dir(&output_dir);
    } else {
        let new_near_config = state_dump(
            epoch_manager.as_ref(),
            runtime,
            &state_roots,
            header,
            &near_config,
            None,
            change_config,
        );
        let output_file = file.unwrap_or_else(|| home_dir.join("output.json"));
        println!("Saving state at {:?} @ {} into {}", state_roots, height, output_file.display(),);
        new_near_config.genesis.to_file(&output_file);
    }
}

pub(crate) fn dump_state_redis(
    height: Option<BlockHeight>,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
) {
    let mode = match height {
        Some(h) => LoadTrieMode::LastFinalFromHeight(h),
        None => LoadTrieMode::Latest,
    };
    let (_, runtime, state_roots, header) =
        load_trie_stop_at_height(store, home_dir, &near_config, mode);

    let res = state_dump_redis(runtime, &state_roots, header);
    assert_eq!(res, Ok(()));
}

pub(crate) fn dump_tx(
    start_height: BlockHeight,
    end_height: BlockHeight,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    select_account_ids: Option<&[AccountId]>,
    output_path: Option<String>,
) -> Result<(), Error> {
    let chain_store = ChainStore::new(
        store,
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );
    let mut txs = vec![];
    for height in start_height..=end_height {
        let hash_result = chain_store.get_block_hash_by_height(height);
        if let Ok(hash) = hash_result {
            let block = chain_store.get_block(&hash)?;
            txs.extend(dump_tx_from_block(&chain_store, &block, select_account_ids));
        }
    }
    let json_path = match output_path {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(&home_dir).join("tx.json"),
    };
    println!("Saving tx (height {} to {}) into {}", start_height, end_height, json_path.display(),);
    fs::write(json_path, json!(txs).to_string())
        .expect("Error writing the results to a json file.");
    Ok(())
}

pub(crate) fn get_chunk(chunk_hash: ChunkHash, near_config: NearConfig, store: Store) {
    let chain_store = ChainStore::new(
        store,
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );
    let chunk = chain_store.get_chunk(&chunk_hash);
    println!("Chunk: {:#?}", chunk);
}

pub(crate) fn get_partial_chunk(
    partial_chunk_hash: ChunkHash,
    near_config: NearConfig,
    store: Store,
) {
    let chain_store = ChainStore::new(
        store,
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );
    let partial_chunk = chain_store.get_partial_chunk(&partial_chunk_hash);
    println!("Partial chunk: {:#?}", partial_chunk);
}

pub(crate) fn get_receipt(receipt_id: CryptoHash, near_config: NearConfig, store: Store) {
    let chain_store = ChainStore::new(
        store,
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );
    let receipt = chain_store.get_receipt(&receipt_id);
    println!("Receipt: {:#?}", receipt);
}

pub(crate) fn print_chain(
    start_height: BlockHeight,
    end_height: BlockHeight,
    near_config: NearConfig,
    store: Store,
    show_full_hashes: bool,
) {
    let chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );
    let epoch_manager = EpochManager::new_arc_handle(store, &near_config.genesis.config);
    let mut account_id_to_blocks = HashMap::new();
    let mut cur_epoch_id = None;
    // TODO: Split into smaller functions.
    for height in start_height..=end_height {
        if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
            let header = chain_store.get_block_header(&block_hash).unwrap().clone();
            if height == 0 {
                println!(
                    "{: >3} {}",
                    header.height(),
                    format_hash(*header.hash(), show_full_hashes)
                );
            } else {
                let parent_header =
                    chain_store.get_block_header(header.prev_hash()).unwrap().clone();
                if let Ok(epoch_id) = epoch_manager.get_epoch_id_from_prev_block(header.prev_hash())
                {
                    cur_epoch_id = Some(epoch_id);
                    match epoch_manager.is_next_block_epoch_start(header.prev_hash()) {
                        Ok(true) => {
                            println!("{:?}", account_id_to_blocks);
                            account_id_to_blocks = HashMap::new();
                            println!(
                                "Epoch {} Validators {:?}",
                                format_hash(epoch_id.0, show_full_hashes),
                                epoch_manager
                                    .get_epoch_block_producers_ordered(&epoch_id, header.hash())
                            );
                        }
                        Err(err) => {
                            println!("Don't know if next block is epoch start: {err:?}");
                        }
                        _ => {}
                    };
                    let block_producer = epoch_manager
                        .get_block_producer(&epoch_id, header.height())
                        .map(|account_id| account_id.to_string())
                        .ok()
                        .unwrap_or_else(|| "error".to_owned());
                    account_id_to_blocks
                        .entry(block_producer.clone())
                        .and_modify(|e| *e += 1)
                        .or_insert(1);

                    let block = if let Ok(block) = chain_store.get_block(&block_hash) {
                        block.clone()
                    } else {
                        continue;
                    };

                    let mut chunk_debug_str: Vec<String> = Vec::new();

                    for shard_id in 0..header.chunk_mask().len() {
                        let chunk_producer = epoch_manager
                            .get_chunk_producer(&epoch_id, header.height(), shard_id as u64)
                            .map(|account_id| account_id.to_string())
                            .unwrap_or_else(|_| "CP Unknown".to_owned());
                        if header.chunk_mask()[shard_id] {
                            let chunk_hash = &block.chunks()[shard_id].chunk_hash();
                            if let Ok(chunk) = chain_store.get_chunk(chunk_hash) {
                                chunk_debug_str.push(format!(
                                    "{}: {} {: >3} Tgas {: >10}",
                                    shard_id,
                                    format_hash(chunk_hash.0, show_full_hashes),
                                    chunk.cloned_header().prev_gas_used() / (1_000_000_000_000),
                                    chunk_producer
                                ));
                            } else {
                                chunk_debug_str.push(format!(
                                    "{}: {} ChunkMissing",
                                    shard_id,
                                    format_hash(chunk_hash.0, show_full_hashes),
                                ));
                            }
                        } else {
                            chunk_debug_str
                                .push(format!("{}: MISSING {: >10}", shard_id, chunk_producer));
                        }
                    }

                    println!(
                        "{: >3} {} {} | {: >10} | parent: {: >3} {} | {} {}",
                        header.height(),
                        header.raw_timestamp(),
                        format_hash(*header.hash(), show_full_hashes),
                        block_producer,
                        parent_header.height(),
                        format_hash(*parent_header.hash(), show_full_hashes),
                        chunk_mask_to_str(header.chunk_mask()),
                        chunk_debug_str.join("|")
                    );
                } else {
                    println!("{height} MISSING {:?}", header.prev_hash());
                }
            }
        } else if let Some(epoch_id) = &cur_epoch_id {
            let block_producer = epoch_manager
                .get_block_producer(epoch_id, height)
                .map(|account_id| account_id.to_string())
                .unwrap_or_else(|_| "error".to_owned());
            println!(
                "{: >3} {} | {: >10}",
                height,
                Red.style().bold().paint("MISSING"),
                block_producer
            );
        } else {
            println!("{: >3} {}", height, Red.style().bold().paint("MISSING"));
        }
    }
}

pub(crate) fn state(home_dir: &Path, near_config: NearConfig, store: Store) {
    let (_, runtime, state_roots, header) = load_trie(store, home_dir, &near_config);
    println!("Storage roots are {:?}, block height is {}", state_roots, header.height());
    for (shard_id, state_root) in state_roots.iter().enumerate() {
        let trie = runtime
            .get_trie_for_shard(shard_id as u64, header.prev_hash(), *state_root, false)
            .unwrap();
        for item in trie.disk_iter().unwrap() {
            let (key, value) = item.unwrap();
            if let Some(state_record) = StateRecord::from_raw_key_value(key, value) {
                println!("{}", state_record);
            }
        }
    }
}

pub(crate) fn view_chain(
    height: Option<BlockHeight>,
    view_block: bool,
    view_chunks: bool,
    near_config: NearConfig,
    store: Store,
) {
    let chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );
    let block = {
        match height {
            Some(h) => {
                let block_hash =
                    chain_store.get_block_hash_by_height(h).expect("Block does not exist");
                chain_store.get_block(&block_hash).unwrap()
            }
            None => {
                let head = chain_store.head().unwrap();
                chain_store.get_block(&head.last_block_hash).unwrap()
            }
        }
    };
    let epoch_manager = EpochManager::new_from_genesis_config(store, &near_config.genesis.config)
        .expect("Failed to start Epoch Manager");
    let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();

    let mut chunk_extras = vec![];
    let mut chunks = vec![];
    for (i, chunk_header) in block.chunks().iter().enumerate() {
        if chunk_header.height_included() == block.header().height() {
            let shard_uid = ShardUId::from_shard_id_and_layout(i as ShardId, &shard_layout);
            chunk_extras
                .push((i, chain_store.get_chunk_extra(block.hash(), &shard_uid).ok().clone()));
            chunks.push((i, chain_store.get_chunk(&chunk_header.chunk_hash()).ok().clone()));
        }
    }
    let chunk_extras = block
        .chunks()
        .iter()
        .enumerate()
        .filter_map(|(i, chunk_header)| {
            if chunk_header.height_included() == block.header().height() {
                let shard_uid = ShardUId::from_shard_id_and_layout(i as ShardId, &shard_layout);
                Some((i, chain_store.get_chunk_extra(block.hash(), &shard_uid).ok()))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if height.is_none() {
        let head = chain_store.head().unwrap();
        println!("head: {:#?}", head);
    } else {
        println!("block height {}, hash {}", block.header().height(), block.hash());
    }

    println!("shard layout {:#?}", shard_layout);

    for (shard_id, chunk_extra) in chunk_extras {
        println!("shard {}, chunk extra: {:#?}", shard_id, chunk_extra);
    }
    if view_block {
        println!("last block: {:#?}", block);
    }
    if view_chunks {
        for (shard_id, chunk) in chunks {
            println!("shard {}, chunk: {:#?}", shard_id, chunk);
        }
    }
}

pub(crate) fn view_genesis(
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    view_config: bool,
    view_store: bool,
    compare: bool,
) {
    let chain_genesis = ChainGenesis::new(&near_config.genesis.config);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);
    let runtime_adapter = NightshadeRuntime::from_config(
        home_dir,
        store.clone(),
        &near_config,
        epoch_manager.clone(),
    )
    .unwrap();
    let genesis_height = near_config.genesis.config.genesis_height;
    let chain_store =
        ChainStore::new(store, genesis_height, near_config.client_config.save_trie_changes);

    if view_config || compare {
        tracing::info!(target: "state_viewer", "Computing genesis from config...");
        let state_roots =
            near_store::get_genesis_state_roots(&chain_store.store()).unwrap().unwrap();
        let (genesis_block, genesis_chunks) = Chain::make_genesis_block(
            epoch_manager.as_ref(),
            runtime_adapter.as_ref(),
            &chain_genesis,
            state_roots,
        )
        .unwrap();

        if view_config {
            println!("Genesis block from config: {:#?}", genesis_block);
            for chunk in genesis_chunks {
                println!("Genesis chunk from config at shard {}: {:#?}", chunk.shard_id(), chunk);
            }
        }

        // Check that genesis in the store is the same as genesis given in the config.
        if compare {
            let genesis_hash_in_storage =
                chain_store.get_block_hash_by_height(chain_genesis.height).unwrap();
            let genesis_hash_in_config = genesis_block.hash();
            if &genesis_hash_in_storage == genesis_hash_in_config {
                println!("Genesis in storage and config match.");
            } else {
                println!(
                    "Genesis mismatch between storage and config: {:?} vs {:?}",
                    genesis_hash_in_storage, genesis_hash_in_config
                );
            }
        }
    }

    if view_store {
        tracing::info!(target: "state_viewer", genesis_height, "Reading genesis from store...");
        match read_genesis_from_store(&chain_store, genesis_height) {
            Ok((genesis_block, genesis_chunks)) => {
                println!("Genesis block from store: {:#?}", genesis_block);
                for chunk in genesis_chunks {
                    println!(
                        "Genesis chunk from store at shard {}: {:#?}",
                        chunk.shard_id(),
                        chunk
                    );
                }
            }
            Err(error) => {
                println!("Failed to read genesis block from store. Error: {}", error);
                if !near_config.config.archive {
                    println!("Hint: This is not an archival node. Try running this command from an archival node since genesis block may be garbage collected.");
                }
            }
        }
    }
}

fn read_genesis_from_store(
    chain_store: &ChainStore,
    genesis_height: u64,
) -> Result<(Block, Vec<Arc<ShardChunk>>), Error> {
    let genesis_hash = chain_store.get_block_hash_by_height(genesis_height)?;
    let genesis_block = chain_store.get_block(&genesis_hash)?;
    let mut genesis_chunks = vec![];
    for chunk_header in genesis_block.chunks().iter() {
        if chunk_header.height_included() == genesis_height {
            genesis_chunks.push(chain_store.get_chunk(&chunk_header.chunk_hash())?);
        }
    }
    Ok((genesis_block, genesis_chunks))
}

pub(crate) fn check_block_chunk_existence(near_config: NearConfig, store: Store) {
    let genesis_height = near_config.genesis.config.genesis_height;
    let chain_store =
        ChainStore::new(store, genesis_height, near_config.client_config.save_trie_changes);
    let head = chain_store.head().unwrap();
    let mut cur_block = chain_store.get_block(&head.last_block_hash).unwrap();
    while cur_block.header().height() > genesis_height {
        for chunk_header in cur_block.chunks().iter() {
            if chunk_header.height_included() == cur_block.header().height()
                && chain_store.get_chunk(&chunk_header.chunk_hash()).is_err()
            {
                panic!(
                    "chunk {:?} cannot be found in storage, last block {:?}",
                    chunk_header, cur_block
                );
            }
        }
        cur_block = match chain_store.get_block(cur_block.header().prev_hash()) {
            Ok(b) => b.clone(),
            Err(_) => {
                panic!("last block is {:?}", cur_block);
            }
        }
    }
    println!("Block check succeed");
}

pub(crate) fn print_epoch_info(
    epoch_selection: epoch_info::EpochSelection,
    validator_account_id: Option<AccountId>,
    kickouts_summary: bool,
    near_config: NearConfig,
    store: Store,
) {
    let genesis_height = near_config.genesis.config.genesis_height;
    let mut chain_store =
        ChainStore::new(store.clone(), genesis_height, near_config.client_config.save_trie_changes);
    let epoch_manager =
        EpochManager::new_from_genesis_config(store.clone(), &near_config.genesis.config)
            .expect("Failed to start Epoch Manager")
            .into_handle();

    epoch_info::print_epoch_info(
        epoch_selection,
        validator_account_id,
        kickouts_summary,
        store,
        &mut chain_store,
        &epoch_manager,
    );
}

pub(crate) fn print_epoch_analysis(
    epoch_height: EpochHeight,
    mode: EpochAnalysisMode,
    near_config: NearConfig,
    store: Store,
) {
    let epoch_manager =
        EpochManager::new_from_genesis_config(store.clone(), &near_config.genesis.config)
            .expect("Failed to start Epoch Manager");

    let epoch_ids = iterate_and_filter(store, |_| true);
    let epoch_infos: HashMap<EpochId, Arc<EpochInfo>> = HashMap::from_iter(
        epoch_ids
            .into_iter()
            .map(|epoch_id| (epoch_id, epoch_manager.get_epoch_info(&epoch_id).unwrap())),
    );
    let epoch_heights_to_ids = BTreeMap::from_iter(
        epoch_infos.iter().map(|(epoch_id, epoch_info)| (epoch_info.epoch_height(), *epoch_id)),
    );
    let min_epoch_height = epoch_height;
    let max_stored_epoch_height = *epoch_heights_to_ids.keys().max().unwrap();
    // We can analyze only epochs without last two because these are not
    // finalized yet, so we don't have next next epoch info stored for them.
    let max_epoch_height = max_stored_epoch_height.saturating_sub(2);

    let epoch_heights_to_infos =
        BTreeMap::from_iter(epoch_infos.values().map(|e| (e.epoch_height(), e)));
    let epoch_heights_to_validator_infos =
        BTreeMap::from_iter(epoch_heights_to_ids.iter().filter_map(|(&epoch_height, epoch_id)| {
            // Filter out too small epoch heights due to #11477.
            if epoch_height < min_epoch_height {
                return None;
            }
            // Filter out too big epoch heights because they may not be
            // finalized yet.
            if epoch_height > max_epoch_height {
                return None;
            }
            Some((epoch_height, epoch_manager.get_epoch_validator_info(epoch_id).unwrap()))
        }));

    // The parameters below are required for the next next epoch generation.
    // For `CheckConsistency` mode, they will be overridden in the loop.
    // For `Backtest` mode, they will stay the same and override information
    // stored on disk.
    let mut next_epoch_info =
        epoch_heights_to_infos.get(&min_epoch_height.saturating_add(1)).unwrap().as_ref().clone();
    let mut next_next_epoch_config =
        epoch_manager.get_config_for_protocol_version(PROTOCOL_VERSION).unwrap();
    let mut has_same_shard_layout;
    let mut epoch_protocol_version;
    let mut next_next_protocol_version;

    // Print data header.
    match mode {
        EpochAnalysisMode::CheckConsistency => {
            println!("HEIGHT | VERSION | STATE SYNCS");
        }
        EpochAnalysisMode::Backtest => {
            println!("epoch_height,original_protocol_version,state_syncs,min_validator_num,diff_validator_num,min_stake,diff_stake,rel_diff_stake");
            // Start from empty assignment for correct number of shards.
            *next_epoch_info.chunk_producers_settlement_mut() =
                vec![vec![]; next_next_epoch_config.shard_layout.shard_ids().collect_vec().len()];
            // This in fact sets maximal number of all validators to 100 for
            // `StatelessValidationV0`.
            // Needed because otherwise generation fails at epoch 1327 with
            // assertion `stake < threshold` in
            // chain/epoch-manager/src/validator_selection.rs:227:13.
            // Probably has something to do with extreme case where all
            // proposals are selected.
            next_next_epoch_config.validator_selection_config.num_chunk_validator_seats = 100;
        }
    }

    // Each iteration will generate and print *next next* epoch info based on
    // *next* epoch info for `epoch_height`. This follows epoch generation
    // logic in the protocol.
    for (epoch_height, epoch_info) in
        epoch_heights_to_infos.range(min_epoch_height..=max_epoch_height)
    {
        let next_epoch_height = epoch_height.saturating_add(1);
        let next_next_epoch_height = epoch_height.saturating_add(2);
        let next_epoch_id = epoch_heights_to_ids.get(&next_epoch_height).unwrap();
        let next_next_epoch_id = epoch_heights_to_ids.get(&next_next_epoch_height).unwrap();
        let epoch_summary = epoch_heights_to_validator_infos.get(epoch_height).unwrap();
        let next_epoch_config = epoch_manager.get_epoch_config(next_epoch_id).unwrap();
        let original_next_next_protocol_version = epoch_summary.next_next_epoch_version;

        match mode {
            EpochAnalysisMode::CheckConsistency => {
                // Retrieve remaining parameters from the stored information
                // about epochs.
                next_epoch_info =
                    epoch_heights_to_infos.get(&next_epoch_height).unwrap().as_ref().clone();
                next_next_epoch_config =
                    epoch_manager.get_epoch_config(next_next_epoch_id).unwrap();
                has_same_shard_layout =
                    next_epoch_config.shard_layout == next_next_epoch_config.shard_layout;
                epoch_protocol_version = epoch_info.protocol_version();
                next_next_protocol_version = original_next_next_protocol_version;
            }
            EpochAnalysisMode::Backtest => {
                has_same_shard_layout = true;
                epoch_protocol_version = PROTOCOL_VERSION;
                next_next_protocol_version = PROTOCOL_VERSION;
            }
        };

        // Use "future" information to generate next next epoch which is stored
        // in DB already. Epoch info generation doesn't modify it.
        let stored_next_next_epoch_info =
            epoch_heights_to_infos.get(&next_next_epoch_height).unwrap();
        let rng_seed = stored_next_next_epoch_info.rng_seed();

        let next_next_epoch_info = near_epoch_manager::proposals_to_epoch_info(
            &next_next_epoch_config,
            rng_seed,
            &next_epoch_info,
            epoch_summary.all_proposals.clone(),
            epoch_summary.validator_kickout.clone(),
            stored_next_next_epoch_info.validator_reward().clone(),
            stored_next_next_epoch_info.minted_amount(),
            epoch_protocol_version,
            next_next_protocol_version,
            has_same_shard_layout,
        )
        .unwrap();

        // Compute difference between chunk producer assignments.
        let next_assignment = next_epoch_info.chunk_producers_settlement();
        let next_next_assignment = next_next_epoch_info.chunk_producers_settlement();

        let mut next_validator_to_shard = HashMap::<AccountId, Vec<usize>>::default();
        for (i, validator_ids) in next_assignment.iter().enumerate() {
            for validator_id in validator_ids {
                let validator = next_epoch_info.get_validator(*validator_id).take_account_id();
                next_validator_to_shard.entry(validator).or_default().push(i);
            }
        }
        let mut state_syncs = 0;
        let mut next_next_validator_to_shard = HashMap::<AccountId, usize>::default();
        let mut stakes: HashMap<usize, Balance> = HashMap::default();
        let mut validator_num: HashMap<usize, usize> = HashMap::default();
        for (i, validator_ids) in next_next_assignment.iter().enumerate() {
            for validator_id in validator_ids {
                let validator = next_next_epoch_info.get_validator(*validator_id);
                let account_id = validator.account_id().clone();
                *stakes.entry(i).or_insert(0) += validator.stake();
                *validator_num.entry(i).or_insert(0) += 1;
                if !next_validator_to_shard
                    .get(&account_id)
                    .is_some_and(|shards| shards.contains(&i))
                {
                    state_syncs += 1;
                }
                next_next_validator_to_shard.insert(account_id, i);
            }
        }

        let min_stake = stakes.values().min().unwrap();
        let max_stake = stakes.values().max().unwrap();

        // Process generated epoch info.
        match mode {
            EpochAnalysisMode::CheckConsistency => {
                // Print stats on screen.
                println!(
                    "{next_next_epoch_height: >6} | {original_next_next_protocol_version: >7} | {state_syncs: >11}",
                );
                // Check that the generated epoch info is the same as the stored one.
                assert_eq!(
                    stored_next_next_epoch_info.as_ref(),
                    &next_next_epoch_info,
                    "Unequal epoch info at height {epoch_height}"
                );
            }
            EpochAnalysisMode::Backtest => {
                // Print csv-style stats on screen.
                println!(
                    "{next_next_epoch_height},{original_next_next_protocol_version},{state_syncs},{},{},{},{},{}",
                    validator_num.values().min().unwrap(),
                    validator_num.values().max().unwrap() - validator_num.values().min().unwrap(),
                    min_stake,
                    max_stake - min_stake,
                    ((max_stake - min_stake) as f64) / (*max_stake as f64)
                );
                // Use the generated epoch info for the next iteration.
                next_epoch_info = next_next_epoch_info;
            }
        }
    }
}

fn get_trie(store: TrieStoreAdapter, hash: CryptoHash, shard_id: u32, shard_version: u32) -> Trie {
    let shard_uid = ShardUId { version: shard_version, shard_id };
    let trie_config: TrieConfig = Default::default();
    let shard_cache = TrieCache::new(&trie_config, shard_uid, true);
    let trie_storage = TrieCachingStorage::new(store, shard_cache, shard_uid, true, None);
    Trie::new(Arc::new(trie_storage), hash, None)
}

pub(crate) fn view_trie(
    store: TrieStoreAdapter,
    hash: CryptoHash,
    shard_id: u32,
    shard_version: u32,
    max_depth: u32,
    limit: Option<u32>,
    record_type: Option<u8>,
    from: Option<AccountId>,
    to: Option<AccountId>,
) -> anyhow::Result<()> {
    let trie = get_trie(store, hash, shard_id, shard_version);
    trie.print_recursive(
        &mut std::io::stdout().lock(),
        &hash,
        max_depth,
        limit,
        record_type,
        &from.as_ref(),
        &to.as_ref(),
    );
    Ok(())
}

pub(crate) fn view_trie_leaves(
    store: TrieStoreAdapter,
    state_root_hash: CryptoHash,
    shard_id: u32,
    shard_version: u32,
    max_depth: u32,
    limit: Option<u32>,
    record_type: Option<u8>,
    from: Option<AccountId>,
    to: Option<AccountId>,
) -> anyhow::Result<()> {
    let trie = get_trie(store, state_root_hash, shard_id, shard_version);
    trie.print_recursive_leaves(
        &mut std::io::stdout().lock(),
        max_depth,
        limit,
        record_type,
        &from.as_ref(),
        &to.as_ref(),
    );
    Ok(())
}

fn format_hash(h: CryptoHash, show_full_hashes: bool) -> String {
    let mut hash = h.to_string();
    if !show_full_hashes {
        hash.truncate(7);
    }
    hash
}

pub fn chunk_mask_to_str(mask: &[bool]) -> String {
    mask.iter().map(|f| if *f { '.' } else { 'X' }).collect()
}

pub(crate) fn contract_accounts(
    home_dir: &Path,
    store: Store,
    near_config: NearConfig,
    filter: ContractAccountFilter,
) -> anyhow::Result<()> {
    let (_, _runtime, state_roots, _header) = load_trie(store.clone(), home_dir, &near_config);

    let tries = state_roots.iter().enumerate().map(|(shard_id, &state_root)| {
        // TODO: This assumes simple nightshade layout, it will need an update when we reshard.
        let shard_uid = ShardUId::from_shard_id_and_layout(
            shard_id as u64,
            &ShardLayout::get_simple_nightshade_layout(),
        );
        // Use simple non-caching storage, we don't expect many duplicate lookups while iterating.
        let storage = TrieDBStorage::new(store.trie_store(), shard_uid);
        // We don't need flat state to traverse all accounts.
        let flat_storage_chunk_view = None;
        Trie::new(Arc::new(storage), state_root, flat_storage_chunk_view)
    });

    filter.write_header(&mut std::io::stdout().lock())?;
    // Prefer streaming the results, to use less memory and provide
    // a feedback more quickly.
    if filter.can_stream() {
        // Process account after account and display results immediately.
        for (i, trie) in tries.enumerate() {
            eprintln!("Starting shard {i}");
            let trie_iter = ContractAccount::in_trie(trie, filter.clone())?;
            for contract in trie_iter {
                match contract {
                    Ok(contract) => println!("{contract}"),
                    Err(err) => eprintln!("{err}"),
                }
            }
        }
    } else {
        // Load all results into memory, which allows advanced lookups but also
        // means we have to wait for everything to complete before output can be
        // shown.
        let tries_iterator = ContractAccount::in_tries(tries.collect(), &filter)?;
        let result = tries_iterator.summary(&store, &filter);
        println!("{result}");
    }

    Ok(())
}

pub(crate) fn clear_cache(store: Store) {
    let mut store_update = store.store_update();
    store_update.delete_all(DBCol::CachedContractCode);
    store_update.commit().unwrap();
}

/// Prints the state statistics for all shards. Please note that it relies on
/// the live flat storage and may break if the node is not stopped.
pub(crate) fn print_state_stats(home_dir: &Path, store: Store, near_config: NearConfig) {
    let (epoch_manager, runtime, _, block_header) =
        load_trie(store.clone(), home_dir, &near_config);

    let block_hash = *block_header.hash();
    let shard_layout = epoch_manager.get_shard_layout_from_prev_block(&block_hash).unwrap();

    let flat_storage_manager = runtime.get_flat_storage_manager();
    for shard_uid in shard_layout.shard_uids() {
        print_state_stats_for_shard_uid(
            store.trie_store(),
            &flat_storage_manager,
            block_hash,
            shard_uid,
        );
    }
}

/// Persists the trie changes expressed by `apply_result` in the given storage.
pub(crate) fn maybe_save_trie_changes(
    store: Option<Store>,
    genesis_height: u64,
    apply_result: ApplyChunkResult,
    block_height: u64,
    shard_id: u64,
) -> anyhow::Result<()> {
    if let Some(store) = store {
        let mut chain_store = ChainStore::new(store, genesis_height, false);
        let mut chain_store_update = chain_store.store_update();
        chain_store_update.save_trie_changes(apply_result.trie_changes);
        chain_store_update.commit()?;
        tracing::debug!("Trie changes persisted for block {block_height}, shard {shard_id}");
    }
    Ok(())
}

pub(crate) fn maybe_print_db_stats(store: Option<Store>) {
    store.map(|store| {
        store.get_store_statistics().map(|stats| {
            stats.data.iter().for_each(|(metric, values)| {
                tracing::info!(%metric, ?values);
            })
        })
    });
}

/// Prints the state statistics for a single shard.
fn print_state_stats_for_shard_uid(
    store: TrieStoreAdapter,
    flat_storage_manager: &FlatStorageManager,
    block_hash: CryptoHash,
    shard_uid: ShardUId,
) {
    flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
    let trie_storage = TrieDBStorage::new(store, shard_uid);
    let chunk_view = flat_storage_manager.chunk_view(shard_uid, block_hash).unwrap();

    let mut state_stats = StateStats::default();

    // iteratate for the first time to get the size statistics
    let group_by = get_state_stats_group_by(&chunk_view, &trie_storage);
    let iter = get_state_stats_account_iter(&group_by);
    for state_stats_account in iter {
        state_stats.push(state_stats_account);
    }

    // iterate for the second time to find the middle account
    let group_by = get_state_stats_group_by(&chunk_view, &trie_storage);
    let iter = get_state_stats_account_iter(&group_by);
    let mut current_size = ByteSize::default();
    for state_stats_account in iter {
        let new_size = current_size + state_stats_account.size;
        if 2 * new_size.as_u64() > state_stats.total_size.as_u64() {
            state_stats.middle_account = Some(state_stats_account);
            state_stats.middle_account_leading_size = Some(current_size);
            break;
        }
        current_size = new_size;
    }

    tracing::info!(target: "state_viewer", "{shard_uid:?}");
    tracing::info!(target: "state_viewer", "{state_stats:#?}");
}

/// Gets the flat state iterator from the chunk view, rearranges it to be sorted
/// by the account id, rather than type, account id and finally groups the
/// records by account id while collecting aggregate statistics.
fn get_state_stats_group_by<'a>(
    chunk_view: &'a FlatStorageChunkView,
    trie_storage: &'a TrieDBStorage,
) -> GroupBy<
    AccountId,
    impl Iterator<Item = StateStatsStateRecord> + 'a,
    impl FnMut(&StateStatsStateRecord) -> AccountId,
> {
    // The flat state iterator is sorted by type, account id. In order to
    // rearrange it we get the iterators for each type and later merge them by
    // the account id.
    let type_iters = COLUMNS_WITH_ACCOUNT_ID_IN_KEY
        .iter()
        .map(|(type_byte, _)| chunk_view.iter_range(Some(&[*type_byte]), Some(&[*type_byte + 1])))
        .into_iter();

    // Filter out any errors.
    let type_iters = type_iters.map(|type_iter| type_iter.filter_map(|item| item.ok())).into_iter();

    // Read the values from and convert items to StateStatsStateRecord.
    let type_iters = type_iters
        .map(move |type_iter| {
            type_iter.filter_map(move |(key, value)| {
                let value = read_flat_state_value(&trie_storage, value);
                let key_size = key.len() as u64;
                let value_size = value.len() as u64;
                let size = ByteSize::b(key_size + value_size);
                let state_record = StateRecord::from_raw_key_value(key, value);
                state_record.map(|state_record| StateStatsStateRecord {
                    account_id: state_record_to_account_id(&state_record).clone(),
                    state_record,
                    size,
                })
            })
        })
        .into_iter();

    // Merge the iterators for different types. The StateStatsStateRecord
    // implements the Ord and PartialOrd traits that compare items by their
    // account ids.
    let iter = type_iters.kmerge().into_iter();

    // Finally, group by the account id.
    iter.group_by(|state_record| state_record.account_id.clone())
}

/// Given the StateStatsStateRecords grouped by the account id returns an
/// iterator of StateStatsAccount.
fn get_state_stats_account_iter<'a>(
    group_by: &'a GroupBy<
        AccountId,
        impl Iterator<Item = StateStatsStateRecord> + 'a,
        impl FnMut(&StateStatsStateRecord) -> AccountId,
    >,
) -> impl Iterator<Item = StateStatsAccount> + 'a {
    // aggregate size for each account id group
    group_by
        .into_iter()
        .map(|(account_id, group)| {
            let mut size = ByteSize::b(0);
            for state_stats_state_record in group {
                size += state_stats_state_record.size;
            }
            StateStatsAccount { account_id, size }
        })
        .into_iter()
}

/// Helper function to read the value from flat storage.
/// It either returns the inlined value or reads ref value from the storage.
fn read_flat_state_value(
    trie_storage: &TrieDBStorage,
    flat_state_value: FlatStateValue,
) -> Vec<u8> {
    match flat_state_value {
        FlatStateValue::Ref(val) => trie_storage.retrieve_raw_bytes(&val.hash).unwrap().to_vec(),
        FlatStateValue::Inlined(val) => val,
    }
}

/// StateStats is used for storing the state statistics of a single trie.
#[derive(Default)]
pub struct StateStats {
    pub total_size: ByteSize,
    pub total_count: usize,

    // The account that is in the middle of the state in respect to storage.
    pub middle_account: Option<StateStatsAccount>,
    // The total size of all accounts leading to the middle account.
    // Can be used to determin how does the middle account split the state.
    pub middle_account_leading_size: Option<ByteSize>,

    pub top_accounts: BinaryHeap<StateStatsAccount>,
}

impl core::fmt::Debug for StateStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let average_size = self
            .total_size
            .as_u64()
            .checked_div(self.total_count as u64)
            .map(ByteSize::b)
            .unwrap_or_default();

        let left_size = self.middle_account_leading_size.unwrap_or_default();
        let middle_size = self.middle_account.as_ref().map(|a| a.size).unwrap_or_default();
        let right_size = self.total_size.as_u64() - left_size.as_u64() - middle_size.as_u64();
        let right_size = ByteSize::b(right_size);

        let left_percent = 100 * left_size.as_u64() / self.total_size.as_u64();
        let middle_percent = 100 * middle_size.as_u64() / self.total_size.as_u64();
        let right_percent = 100 * right_size.as_u64() / self.total_size.as_u64();

        f.debug_struct("StateStats")
            .field("total_size", &self.total_size)
            .field("total_count", &self.total_count)
            .field("average_size", &average_size)
            .field("middle_account", &self.middle_account.as_ref().unwrap())
            .field("split_size", &format!("{left_size:?} : {middle_size:?} : {right_size:?}"))
            .field("split_percent", &format!("{left_percent}:{middle_percent}:{right_percent}"))
            .field("top_accounts", &self.top_accounts)
            .finish()
    }
}

impl StateStats {
    pub fn push(&mut self, state_stats_account: StateStatsAccount) {
        self.total_size += state_stats_account.size;
        self.total_count += 1;

        self.top_accounts.push(state_stats_account);
        if self.top_accounts.len() > 5 {
            self.top_accounts.pop();
        }
    }
}

/// StateStatsStateRecord stores the state record and associated information.
/// It's used as a helper struct for merging state records from different record types.
#[derive(Eq, PartialEq)]
struct StateStatsStateRecord {
    state_record: StateRecord,
    account_id: AccountId,
    size: ByteSize,
}

impl Ord for StateStatsStateRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.account_id.cmp(&other.account_id)
    }
}

impl PartialOrd for StateStatsStateRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// StateStatsAccount stores aggregated information about an account.
/// It is the result of the grouping of state records belonging to the same account.
#[derive(PartialEq, Eq)]
pub struct StateStatsAccount {
    pub account_id: AccountId,
    pub size: ByteSize,
}

impl Ord for StateStatsAccount {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.size.cmp(&other.size).reverse()
    }
}

impl PartialOrd for StateStatsAccount {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Debug for StateStatsAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateStatsAccount")
            .field("account_id", &self.account_id.as_str())
            .field("size", &self.size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use near_chain::types::RuntimeAdapter;
    use near_chain_configs::{Genesis, MutableConfigValue};
    use near_client::test_utils::TestEnv;
    use near_crypto::{InMemorySigner, KeyFile, KeyType};
    use near_epoch_manager::EpochManager;
    use near_primitives::shard_layout::ShardUId;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_primitives::types::AccountId;
    use near_store::genesis::initialize_genesis_state;
    use nearcore::config::Config;
    use nearcore::{NearConfig, NightshadeRuntime};
    use std::sync::Arc;

    #[test]
    /// Tests that getting the latest trie state actually gets the latest state.
    /// Adds a transaction and waits for it to be included in a block.
    /// Checks that the change of state caused by that transaction is visible to `load_trie()`.
    fn test_latest_trie_state() {
        near_o11y::testonly::init_test_logger();
        let validators = vec!["test0".parse::<AccountId>().unwrap()];
        let genesis = Genesis::test_sharded_new_version(validators, 1, vec![1]);

        let tmp_dir = tempfile::tempdir().unwrap();
        let home_dir = tmp_dir.path();

        let store = near_store::test_utils::create_test_store();
        initialize_genesis_state(store.clone(), &genesis, Some(home_dir));
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime = NightshadeRuntime::test(
            home_dir,
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
        ) as Arc<dyn RuntimeAdapter>;

        let stores = vec![store.clone()];
        let epoch_managers = vec![epoch_manager];
        let runtimes = vec![runtime];

        let mut env = TestEnv::builder(&genesis.config)
            .stores(stores)
            .epoch_managers(epoch_managers)
            .runtimes(runtimes)
            .build();

        let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        assert_eq!(env.send_money(0), near_client::ProcessTxResponse::ValidTx);

        // It takes 2 blocks to record a transaction on chain and apply the receipts.
        env.produce_block(0, 1);
        env.produce_block(0, 2);

        let chunk_extras: Vec<Arc<ChunkExtra>> = (1..=2)
            .map(|height| {
                let block = env.clients[0].chain.get_block_by_height(height).unwrap();
                let hash = *block.hash();
                let chunk_extra = env.clients[0]
                    .chain
                    .get_chunk_extra(&hash, &ShardUId { version: 1, shard_id: 0 })
                    .unwrap();
                chunk_extra
            })
            .collect();

        // Check that `send_money()` actually changed state.
        assert_ne!(chunk_extras[0].state_root(), chunk_extras[1].state_root());

        let near_config = NearConfig::new(
            Config::default(),
            genesis,
            KeyFile::from(&signer),
            MutableConfigValue::new(None, "validator_signer"),
        )
        .unwrap();
        let (_epoch_manager, _runtime, state_roots, block_header) =
            crate::commands::load_trie(store, home_dir, &near_config);
        assert_eq!(&state_roots[0], chunk_extras[1].state_root());
        assert_eq!(block_header.height(), 2);
    }
}
