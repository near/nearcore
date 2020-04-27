use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ansi_term::Color::Red;
use clap::{App, Arg, SubCommand};

use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_network::peer_store::PeerStore;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::to_base;
use near_primitives::state_record::StateRecord;
use near_primitives::test_utils::init_integration_logger;
use near_primitives::types::{BlockHeight, StateRoot};
use near_store::test_utils::create_test_store;
use near_store::{create_store, Store, TrieIterator};
use neard::{get_default_home, get_store_path, load_config, NearConfig, NightshadeRuntime};
use state_dump::state_dump;

mod state_dump;

fn load_trie(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
) -> (NightshadeRuntime, Vec<StateRoot>, BlockHeader) {
    load_trie_stop_at_height(store, home_dir, near_config, None)
}

fn load_trie_stop_at_height(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
    stop_height: Option<BlockHeight>,
) -> (NightshadeRuntime, Vec<StateRoot>, BlockHeader) {
    let mut chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);

    let runtime = NightshadeRuntime::new(
        &home_dir,
        store,
        Arc::clone(&near_config.genesis),
        near_config.client_config.tracked_accounts.clone(),
        near_config.client_config.tracked_shards.clone(),
    );
    let head = chain_store.head().unwrap();
    let last_block = match stop_height {
        Some(height) => {
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
                let last_final_block_hash = chain_store
                    .get_block_header(&cur_block_hash)
                    .unwrap()
                    .inner_rest
                    .last_final_block;
                let last_final_block = chain_store.get_block(&last_final_block_hash).unwrap();
                if last_final_block.header.inner_lite.height >= height {
                    break last_final_block.clone();
                } else {
                    cur_height += 1;
                    continue;
                }
            }
        }
        None => chain_store.get_block(&head.last_block_hash).unwrap().clone(),
    };
    let state_roots = last_block.chunks.iter().map(|chunk| chunk.inner.prev_state_root).collect();
    (runtime, state_roots, last_block.header)
}

pub fn format_hash(h: CryptoHash) -> String {
    to_base(&h)[..7].to_string()
}

fn print_chain(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
    start_height: BlockHeight,
    end_height: BlockHeight,
) {
    let mut chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);
    let runtime = NightshadeRuntime::new(
        &home_dir,
        store,
        Arc::clone(&near_config.genesis),
        near_config.client_config.tracked_accounts.clone(),
        near_config.client_config.tracked_shards.clone(),
    );
    let mut account_id_to_blocks = HashMap::new();
    let mut cur_epoch_id = None;
    for height in start_height..=end_height {
        if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
            let header = chain_store.get_block_header(&block_hash).unwrap().clone();
            if height == 0 {
                println!("{: >3} {}", header.inner_lite.height, format_hash(header.hash()));
            } else {
                let parent_header = chain_store.get_block_header(&header.prev_hash).unwrap();
                let epoch_id = runtime.get_epoch_id_from_prev_block(&header.prev_hash).unwrap();
                cur_epoch_id = Some(epoch_id.clone());
                if runtime.is_next_block_epoch_start(&header.prev_hash).unwrap() {
                    println!("{:?}", account_id_to_blocks);
                    account_id_to_blocks = HashMap::new();
                    println!(
                        "Epoch {} Validators {:?}",
                        format_hash(epoch_id.0),
                        runtime
                            .get_epoch_block_producers_ordered(&epoch_id, &header.hash())
                            .unwrap()
                    );
                }
                let block_producer =
                    runtime.get_block_producer(&epoch_id, header.inner_lite.height).unwrap();
                account_id_to_blocks
                    .entry(block_producer.clone())
                    .and_modify(|e| *e += 1)
                    .or_insert(1);
                println!(
                    "{: >3} {} | {: >10} | parent: {: >3} {}",
                    header.inner_lite.height,
                    format_hash(header.hash()),
                    block_producer,
                    parent_header.inner_lite.height,
                    format_hash(parent_header.hash()),
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

fn replay_chain(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
    start_height: BlockHeight,
    end_height: BlockHeight,
) {
    let mut chain_store = ChainStore::new(store, near_config.genesis.config.genesis_height);
    let new_store = create_test_store();
    let runtime = NightshadeRuntime::new(
        &home_dir,
        new_store,
        Arc::clone(&near_config.genesis),
        near_config.client_config.tracked_accounts.clone(),
        near_config.client_config.tracked_shards.clone(),
    );
    for height in start_height..=end_height {
        if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
            let header = chain_store.get_block_header(&block_hash).unwrap().clone();
            runtime
                .add_validator_proposals(
                    header.prev_hash,
                    header.hash(),
                    header.inner_rest.random_value,
                    header.inner_lite.height,
                    chain_store.get_block_height(&header.inner_rest.last_final_block).unwrap(),
                    header.inner_rest.validator_proposals,
                    vec![],
                    header.inner_rest.chunk_mask,
                    header.inner_rest.validator_reward,
                    header.inner_rest.total_supply,
                )
                .unwrap();
        }
    }
}

fn main() {
    init_integration_logger();

    let default_home = get_default_home();
    let matches = App::new("state-viewer")
        .arg(
            Arg::with_name("home")
                .long("home")
                .default_value(&default_home)
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .subcommand(SubCommand::with_name("peers"))
        .subcommand(SubCommand::with_name("state"))
        .subcommand(
            SubCommand::with_name("dump_state").arg(
                Arg::with_name("height")
                    .long("height")
                    .help("Desired stop height of state dump")
                    .takes_value(true),
            ),
        )
        .subcommand(
            SubCommand::with_name("chain")
                .arg(
                    Arg::with_name("start_index")
                        .long("start_index")
                        .required(true)
                        .help("Start index of query")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("end_index")
                        .long("end_index")
                        .required(true)
                        .help("End index of query")
                        .takes_value(true),
                )
                .help("print chain from start_index to end_index"),
        )
        .subcommand(
            SubCommand::with_name("replay")
                .arg(
                    Arg::with_name("start_index")
                        .long("start_index")
                        .required(true)
                        .help("Start index of query")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("end_index")
                        .long("end_index")
                        .required(true)
                        .help("End index of query")
                        .takes_value(true),
                )
                .help("replay headers from chain"),
        )
        .get_matches();

    let home_dir = matches.value_of("home").map(|dir| Path::new(dir)).unwrap();
    let near_config = load_config(home_dir);

    let store = create_store(&get_store_path(&home_dir));

    match matches.subcommand() {
        ("peers", Some(_args)) => {
            let peer_store = PeerStore::new(store, &[]).unwrap();
            for (peer_id, peer_info) in peer_store.iter() {
                println!("{} {:?}", peer_id, peer_info);
            }
        }
        ("state", Some(_args)) => {
            let (runtime, state_roots, header) = load_trie(store, &home_dir, &near_config);
            println!(
                "Storage roots are {:?}, block height is {}",
                state_roots, header.inner_lite.height
            );
            for state_root in state_roots {
                let trie = TrieIterator::new(&runtime.trie, &state_root).unwrap();
                for item in trie {
                    let (key, value) = item.unwrap();
                    if let Some(state_record) = StateRecord::from_raw_key_value(key, value) {
                        println!("{}", state_record);
                    }
                }
            }
        }
        ("dump_state", Some(args)) => {
            let height = args.value_of("height").map(|s| s.parse::<u64>().unwrap());
            let (runtime, state_roots, header) =
                load_trie_stop_at_height(store.clone(), home_dir, &near_config, height);
            let height = header.inner_lite.height;
            let home_dir = PathBuf::from(&home_dir);

            let new_genesis =
                state_dump(runtime, state_roots.clone(), header, &near_config.genesis.config);

            let output_path = home_dir.join(Path::new("output.json"));
            println!(
                "Saving state at {:?} @ {} into {}",
                state_roots,
                height,
                output_path.display(),
            );
            new_genesis.to_file(&output_path);
        }
        ("chain", Some(args)) => {
            let start_index =
                args.value_of("start_index").map(|s| s.parse::<u64>().unwrap()).unwrap();
            let end_index = args.value_of("end_index").map(|s| s.parse::<u64>().unwrap()).unwrap();
            print_chain(store, home_dir, &near_config, start_index, end_index);
        }
        ("replay", Some(args)) => {
            let start_index =
                args.value_of("start_index").map(|s| s.parse::<u64>().unwrap()).unwrap();
            let end_index = args.value_of("end_index").map(|s| s.parse::<u64>().unwrap()).unwrap();
            replay_chain(store, home_dir, &near_config, start_index, end_index);
        }
        (_, _) => unreachable!(),
    }
}
