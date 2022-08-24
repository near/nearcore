use crate::apply_chain_range::apply_chain_range;
use crate::state_dump::state_dump;
use crate::state_dump::state_dump_redis;
use crate::tx_dump::dump_tx_from_block;
use crate::{apply_chunk, epoch_info};
use ansi_term::Color::Red;
use near_chain::chain::collect_receipts_from_response;
use near_chain::migrations::check_if_block_is_first_with_chunk_of_version;
use near_chain::types::{ApplyTransactionResult, BlockHeaderInfo};
use near_chain::Error;
use near_chain::{ChainStore, ChainStoreAccess, ChainStoreUpdate, RuntimeAdapter};
use near_chain_configs::GenesisChangeConfig;
use near_epoch_manager::EpochManager;
use near_network::iter_peers_from_store;
use near_primitives::account::id::AccountId;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::ChunkHash;
use near_primitives::state_record::StateRecord;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ShardId, StateRoot};
use near_primitives_core::types::Gas;
use near_store::test_utils::create_test_store;
use near_store::Store;
use nearcore::{NearConfig, NightshadeRuntime};
use node_runtime::adapter::ViewRuntimeAdapter;
use serde_json::json;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) fn peers(store: Store) {
    iter_peers_from_store(store, |(peer_id, peer_info)| {
        println!("{} {:?}", peer_id, peer_info);
    })
}

pub(crate) fn state(home_dir: &Path, near_config: NearConfig, store: Store) {
    let (runtime, state_roots, header) = load_trie(store, home_dir, &near_config);
    println!("Storage roots are {:?}, block height is {}", state_roots, header.height());
    for (shard_id, state_root) in state_roots.iter().enumerate() {
        let trie = runtime
            .get_trie_for_shard(shard_id as u64, header.prev_hash(), state_root.clone())
            .unwrap();
        for item in trie.iter().unwrap() {
            let (key, value) = item.unwrap();
            if let Some(state_record) = StateRecord::from_raw_key_value(key, value) {
                println!("{}", state_record);
            }
        }
    }
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
    let (runtime, state_roots, header) =
        load_trie_stop_at_height(store, home_dir, &near_config, mode);
    let height = header.height();
    let home_dir = PathBuf::from(&home_dir);

    if stream {
        let output_dir = file.unwrap_or(home_dir.join("output"));
        let records_path = output_dir.join("records.json");
        let new_near_config = state_dump(
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
        let new_near_config =
            state_dump(runtime, &state_roots, header, &near_config, None, change_config);
        let output_file = file.unwrap_or(home_dir.join("output.json"));
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
    let (runtime, state_roots, header) =
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
    select_account_ids: Option<&Vec<AccountId>>,
    output_path: Option<String>,
) -> Result<(), Error> {
    let chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        !near_config.client_config.archive,
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
    return Ok(());
}

pub(crate) fn apply_range(
    start_index: Option<BlockHeight>,
    end_index: Option<BlockHeight>,
    shard_id: ShardId,
    verbose_output: bool,
    csv_file: Option<PathBuf>,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    only_contracts: bool,
    sequential: bool,
) {
    let mut csv_file = csv_file.map(|filename| std::fs::File::create(filename).unwrap());

    let runtime = NightshadeRuntime::from_config(home_dir, store.clone(), &near_config);
    apply_chain_range(
        store,
        &near_config.genesis,
        start_index,
        end_index,
        shard_id,
        runtime,
        verbose_output,
        csv_file.as_mut(),
        only_contracts,
        sequential,
    );
}

pub(crate) fn dump_code(
    account_id: String,
    output: &Path,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
) {
    let (runtime, state_roots, header) = load_trie(store, home_dir, &near_config);
    let epoch_id = &runtime.get_epoch_id(header.hash()).unwrap();

    for (shard_id, state_root) in state_roots.iter().enumerate() {
        let shard_uid = runtime.shard_id_to_uid(shard_id as u64, epoch_id).unwrap();
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
    let (runtime, state_roots, header) =
        load_trie_stop_at_height(store, home_dir, &near_config, block_height);
    for (shard_id, state_root) in state_roots.iter().enumerate() {
        let trie = runtime
            .get_trie_for_shard(shard_id as u64, header.prev_hash(), state_root.clone())
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
                    fs::write(output, &value).unwrap();
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

pub(crate) fn print_chain(
    start_height: BlockHeight,
    end_height: BlockHeight,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    show_full_hashes: bool,
) {
    let chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        !near_config.client_config.archive,
    );
    let runtime = NightshadeRuntime::from_config(home_dir, store, &near_config);
    let mut account_id_to_blocks = HashMap::new();
    let mut cur_epoch_id = None;
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
                let epoch_id = runtime.get_epoch_id_from_prev_block(header.prev_hash()).unwrap();
                cur_epoch_id = Some(epoch_id.clone());
                if runtime.is_next_block_epoch_start(header.prev_hash()).unwrap() {
                    println!("{:?}", account_id_to_blocks);
                    account_id_to_blocks = HashMap::new();
                    println!(
                        "Epoch {} Validators {:?}",
                        format_hash(epoch_id.0, show_full_hashes),
                        runtime
                            .get_epoch_block_producers_ordered(&epoch_id, header.hash())
                            .unwrap()
                    );
                }
                let block_producer =
                    runtime.get_block_producer(&epoch_id, header.height()).unwrap();
                account_id_to_blocks
                    .entry(block_producer.clone())
                    .and_modify(|e| *e += 1)
                    .or_insert(1);

                let block = chain_store.get_block(&block_hash).unwrap().clone();

                let mut chunk_debug_str: Vec<String> = Vec::new();

                for shard_id in 0..header.chunk_mask().len() {
                    if header.chunk_mask()[shard_id] {
                        let chunk = chain_store
                            .get_chunk(&block.chunks()[shard_id as usize].chunk_hash())
                            .unwrap()
                            .clone();
                        chunk_debug_str.push(format!(
                            "{}: {} {: >3} Tgas ",
                            shard_id,
                            format_hash(chunk.chunk_hash().0, show_full_hashes),
                            chunk.cloned_header().gas_used() / (1024 * 1024 * 1024 * 1024)
                        ));
                    }
                }

                println!(
                    "{: >3} {} | {: >10} | parent: {: >3} {} | {} {}",
                    header.height(),
                    format_hash(*header.hash(), show_full_hashes),
                    block_producer,
                    parent_header.height(),
                    format_hash(*parent_header.hash(), show_full_hashes),
                    chunk_mask_to_str(header.chunk_mask()),
                    chunk_debug_str.join("|")
                );
            }
        } else {
            if let Some(epoch_id) = &cur_epoch_id {
                let block_producer = runtime.get_block_producer(epoch_id, height).unwrap();
                println!(
                    "{: >3} {} | {: >10}",
                    height,
                    Red.bold().paint("MISSING"),
                    block_producer
                );
            } else {
                println!("{: >3} {}", height, Red.bold().paint("MISSING"));
            }
        }
    }
}

pub(crate) fn replay_chain(
    start_height: BlockHeight,
    end_height: BlockHeight,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
) {
    let chain_store = ChainStore::new(
        store,
        near_config.genesis.config.genesis_height,
        !near_config.client_config.archive,
    );
    let new_store = create_test_store();
    let runtime = NightshadeRuntime::from_config(home_dir, new_store, &near_config);
    for height in start_height..=end_height {
        if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
            let header = chain_store.get_block_header(&block_hash).unwrap().clone();
            println!("Height: {}, header: {:#?}", height, header);
            runtime
                .add_validator_proposals(BlockHeaderInfo::new(
                    &header,
                    chain_store.get_block_height(header.last_final_block()).unwrap(),
                ))
                .unwrap()
                .commit()
                .unwrap();
        }
    }
}

pub(crate) fn resulting_chunk_extra(result: &ApplyTransactionResult, gas_limit: Gas) -> ChunkExtra {
    let (outcome_root, _) = ApplyTransactionResult::compute_outcomes_proof(&result.outcomes);
    ChunkExtra::new(
        &result.new_root,
        outcome_root,
        result.validator_proposals.clone(),
        result.total_gas_burnt,
        gas_limit,
        result.total_balance_burnt,
    )
}

pub(crate) fn apply_block(
    block_hash: CryptoHash,
    shard_id: ShardId,
    runtime_adapter: &dyn RuntimeAdapter,
    chain_store: &mut ChainStore,
) -> (Block, ApplyTransactionResult) {
    let block = chain_store.get_block(&block_hash).unwrap();
    let height = block.header().height();
    let shard_uid = runtime_adapter.shard_id_to_uid(shard_id, block.header().epoch_id()).unwrap();
    let apply_result = if block.chunks()[shard_id as usize].height_included() == height {
        let chunk = chain_store.get_chunk(&block.chunks()[shard_id as usize].chunk_hash()).unwrap();
        let prev_block = chain_store.get_block(block.header().prev_hash()).unwrap();
        let chain_store_update = ChainStoreUpdate::new(chain_store);
        let receipt_proof_response = chain_store_update
            .get_incoming_receipts_for_shard(
                shard_id,
                block_hash,
                prev_block.chunks()[shard_id as usize].height_included(),
            )
            .unwrap();
        let receipts = collect_receipts_from_response(&receipt_proof_response);

        let chunk_inner = chunk.cloned_header().take_inner();
        let is_first_block_with_chunk_of_version = check_if_block_is_first_with_chunk_of_version(
            chain_store,
            runtime_adapter,
            block.header().prev_hash(),
            shard_id,
        )
        .unwrap();

        runtime_adapter
            .apply_transactions(
                shard_id,
                chunk_inner.prev_state_root(),
                height,
                block.header().raw_timestamp(),
                block.header().prev_hash(),
                block.hash(),
                &receipts,
                chunk.transactions(),
                chunk_inner.validator_proposals(),
                prev_block.header().gas_price(),
                chunk_inner.gas_limit(),
                block.header().challenges_result(),
                *block.header().random_value(),
                true,
                is_first_block_with_chunk_of_version,
                Default::default(),
            )
            .unwrap()
    } else {
        let chunk_extra =
            chain_store.get_chunk_extra(block.header().prev_hash(), &shard_uid).unwrap();

        runtime_adapter
            .apply_transactions(
                shard_id,
                chunk_extra.state_root(),
                block.header().height(),
                block.header().raw_timestamp(),
                block.header().prev_hash(),
                block.hash(),
                &[],
                &[],
                chunk_extra.validator_proposals(),
                block.header().gas_price(),
                chunk_extra.gas_limit(),
                block.header().challenges_result(),
                *block.header().random_value(),
                false,
                false,
                Default::default(),
            )
            .unwrap()
    };
    (block, apply_result)
}

pub(crate) fn print_apply_block_result(
    block: &Block,
    apply_result: &ApplyTransactionResult,
    runtime_adapter: &dyn RuntimeAdapter,
    chain_store: &mut ChainStore,
    shard_id: ShardId,
) {
    let height = block.header().height();
    let block_hash = block.header().hash();
    println!(
        "apply chunk for shard {} at height {}, resulting chunk extra {:?}",
        shard_id,
        height,
        resulting_chunk_extra(apply_result, block.chunks()[shard_id as usize].gas_limit())
    );
    let shard_uid = runtime_adapter.shard_id_to_uid(shard_id, block.header().epoch_id()).unwrap();
    if block.chunks()[shard_id as usize].height_included() == height {
        if let Ok(chunk_extra) = chain_store.get_chunk_extra(&block_hash, &shard_uid) {
            println!("Existing chunk extra: {:?}", chunk_extra);
        } else {
            println!("No existing chunk extra available");
        }
    } else {
        println!("No existing chunk extra available");
    }
}

pub(crate) fn apply_block_at_height(
    height: BlockHeight,
    shard_id: ShardId,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
) {
    let mut chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        !near_config.client_config.archive,
    );
    let runtime_adapter: Arc<dyn RuntimeAdapter> =
        Arc::new(NightshadeRuntime::from_config(home_dir, store, &near_config));
    let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
    let (block, apply_result) =
        apply_block(block_hash, shard_id, runtime_adapter.as_ref(), &mut chain_store);
    print_apply_block_result(
        &block,
        &apply_result,
        runtime_adapter.as_ref(),
        &mut chain_store,
        shard_id,
    );
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
        !near_config.client_config.archive,
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
                .push((i, chain_store.get_chunk_extra(block.hash(), &shard_uid).unwrap().clone()));
            chunks.push((i, chain_store.get_chunk(&chunk_header.chunk_hash()).unwrap().clone()));
        }
    }
    let chunk_extras = block
        .chunks()
        .iter()
        .enumerate()
        .filter_map(|(i, chunk_header)| {
            if chunk_header.height_included() == block.header().height() {
                let shard_uid = ShardUId::from_shard_id_and_layout(i as ShardId, &shard_layout);
                Some((i, chain_store.get_chunk_extra(block.hash(), &shard_uid).unwrap()))
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

pub(crate) fn check_block_chunk_existence(store: Store, near_config: NearConfig) {
    let genesis_height = near_config.genesis.config.genesis_height;
    let chain_store = ChainStore::new(store, genesis_height, !near_config.client_config.archive);
    let head = chain_store.head().unwrap();
    let mut cur_block = chain_store.get_block(&head.last_block_hash).unwrap();
    while cur_block.header().height() > genesis_height {
        for chunk_header in cur_block.chunks().iter() {
            if chunk_header.height_included() == cur_block.header().height() {
                if let Err(_) = chain_store.get_chunk(&chunk_header.chunk_hash()) {
                    panic!(
                        "chunk {:?} cannot be found in storage, last block {:?}",
                        chunk_header, cur_block
                    );
                }
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
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
) {
    let genesis_height = near_config.genesis.config.genesis_height;
    let mut chain_store =
        ChainStore::new(store.clone(), genesis_height, !near_config.client_config.archive);
    let mut epoch_manager =
        EpochManager::new_from_genesis_config(store.clone(), &near_config.genesis.config)
            .expect("Failed to start Epoch Manager");
    let runtime_adapter: Arc<dyn RuntimeAdapter> =
        Arc::new(NightshadeRuntime::from_config(&home_dir, store.clone(), &near_config));

    epoch_info::print_epoch_info(
        epoch_selection,
        validator_account_id,
        store,
        &mut chain_store,
        &mut epoch_manager,
        runtime_adapter,
    );
}

pub(crate) fn get_receipt(receipt_id: CryptoHash, near_config: NearConfig, store: Store) {
    let chain_store = ChainStore::new(
        store,
        near_config.genesis.config.genesis_height,
        !near_config.client_config.archive,
    );
    let receipt = chain_store.get_receipt(&receipt_id);
    println!("Receipt: {:#?}", receipt);
}

pub(crate) fn get_chunk(chunk_hash: ChunkHash, near_config: NearConfig, store: Store) {
    let chain_store = ChainStore::new(
        store,
        near_config.genesis.config.genesis_height,
        !near_config.client_config.archive,
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
        !near_config.client_config.archive,
    );
    let partial_chunk = chain_store.get_partial_chunk(&partial_chunk_hash);
    println!("Partial chunk: {:#?}", partial_chunk);
}

#[allow(unused)]
enum LoadTrieMode {
    /// Load latest state
    Latest,
    /// Load prev state at some height
    Height(BlockHeight),
    /// Load the prev state of the last final block from some height
    LastFinalFromHeight(BlockHeight),
}

fn load_trie(
    store: Store,
    home_dir: &Path,
    near_config: &NearConfig,
) -> (NightshadeRuntime, Vec<StateRoot>, BlockHeader) {
    load_trie_stop_at_height(store, home_dir, near_config, LoadTrieMode::Latest)
}

fn load_trie_stop_at_height(
    store: Store,
    home_dir: &Path,
    near_config: &NearConfig,
    mode: LoadTrieMode,
) -> (NightshadeRuntime, Vec<StateRoot>, BlockHeader) {
    let chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        !near_config.client_config.archive,
    );

    let runtime = NightshadeRuntime::from_config(home_dir, store, near_config);
    let head = chain_store.head().unwrap();
    let last_block = match mode {
        LoadTrieMode::LastFinalFromHeight(height) => {
            // find the first final block whose height is at least `height`.
            let mut cur_height = height + 1;
            loop {
                if cur_height >= head.height {
                    panic!("No final block with height >= {} exists", height);
                }
                let cur_block_hash = match chain_store.get_block_hash_by_height(cur_height) {
                    Ok(hash) => hash,
                    Err(_) => {
                        cur_height += 1;
                        continue;
                    }
                };
                let last_final_block_hash =
                    *chain_store.get_block_header(&cur_block_hash).unwrap().last_final_block();
                let last_final_block = chain_store.get_block(&last_final_block_hash).unwrap();
                if last_final_block.header().height() >= height {
                    break last_final_block;
                } else {
                    cur_height += 1;
                    continue;
                }
            }
        }
        LoadTrieMode::Height(height) => {
            let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
            chain_store.get_block(&block_hash).unwrap()
        }
        LoadTrieMode::Latest => chain_store.get_block(&head.last_block_hash).unwrap(),
    };
    let state_roots = last_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect();
    (runtime, state_roots, last_block.header().clone())
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

pub(crate) fn apply_chunk(
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    chunk_hash: ChunkHash,
    target_height: Option<u64>,
) -> anyhow::Result<()> {
    let runtime = NightshadeRuntime::from_config(home_dir, store.clone(), &near_config);
    let mut chain_store = ChainStore::new(
        store,
        near_config.genesis.config.genesis_height,
        !near_config.client_config.archive,
    );
    let (apply_result, gas_limit) =
        apply_chunk::apply_chunk(&runtime, &mut chain_store, chunk_hash, target_height, None)?;
    println!("resulting chunk extra:\n{:?}", resulting_chunk_extra(&apply_result, gas_limit));
    Ok(())
}

pub(crate) fn apply_tx(
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    hash: CryptoHash,
) -> anyhow::Result<()> {
    let runtime = NightshadeRuntime::from_config(home_dir, store.clone(), &near_config);
    apply_chunk::apply_tx(near_config.genesis.config.genesis_height, &runtime, store, hash)
        .map(|_| ())
}

pub(crate) fn apply_receipt(
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    hash: CryptoHash,
) -> anyhow::Result<()> {
    let runtime = NightshadeRuntime::from_config(home_dir, store.clone(), &near_config);
    apply_chunk::apply_receipt(near_config.genesis.config.genesis_height, &runtime, store, hash)
        .map(|_| ())
}
