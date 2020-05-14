use std::path::Path;
use std::process;
use std::sync::Arc;

use ansi_term::Color::{Green, Red};
use clap::{App, Arg, SubCommand};

use near_chain::{ChainStore, ChainStoreAccess};
use near_logger_utils::init_integration_logger;
use near_primitives::block::BlockHeader;
use near_primitives::types::{BlockHeight, StateRoot};
use near_store::{create_store, Store, StoreValidator};
use neard::{get_default_home, get_store_path, load_config, NearConfig, NightshadeRuntime};

#[allow(unused)]
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

fn main() {
    init_integration_logger();

    let default_home = get_default_home();
    let matches = App::new("store-validator")
        .arg(
            Arg::with_name("home")
                .long("home")
                .default_value(&default_home)
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .subcommand(SubCommand::with_name("validate"))
        .get_matches();

    let home_dir = matches.value_of("home").map(|dir| Path::new(dir)).unwrap();
    let near_config = load_config(home_dir);

    let store = create_store(&get_store_path(&home_dir));

    //let (runtime, state_roots, header) = load_trie(store.clone(), &home_dir, &near_config);
    //println!("Storage roots are {:?}, block height is {}", state_roots, header.inner_lite.height);
    let mut store_validator = StoreValidator::default();
    store_validator.validate(&*store, &near_config.genesis.config);

    if store_validator.tests_done() == 0 {
        println!("{}", Red.bold().paint("No conditions has been validated"));
        process::exit(1);
    }
    println!(
        "Conditions validated: {}",
        Green.bold().paint(store_validator.tests_done().to_string())
    );
    for error in store_validator.errors.iter() {
        println!("{}: {}", Red.bold().paint(&error.col.to_string()), error.msg);
    }
    if store_validator.is_failed() {
        println!("Errors found: {}", Red.bold().paint(store_validator.failed().to_string()));
        process::exit(1);
    } else {
        println!("{}", Green.bold().paint("No errors found"));
    }
}
