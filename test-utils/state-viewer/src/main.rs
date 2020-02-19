use std::convert::TryFrom;
use std::path::Path;
use std::sync::Arc;

use borsh::BorshDeserialize;
use clap::{App, Arg, SubCommand};

use ansi_term::Color::Red;
use near::{get_default_home, get_store_path, load_config, NearConfig, NightshadeRuntime};
use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_crypto::PublicKey;
use near_network::peer_store::PeerStore;
use near_primitives::account::{AccessKey, Account};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::{Receipt, ReceivedData};
use near_primitives::serialize::{from_base64, to_base, to_base64};
use near_primitives::state_record::StateRecord;
use near_primitives::test_utils::init_integration_logger;
use near_primitives::types::{BlockHeight, StateRoot};
use near_primitives::utils::{col, ACCOUNT_DATA_SEPARATOR};
use near_store::test_utils::create_test_store;
use near_store::{create_store, Store, TrieIterator};
use std::collections::HashMap;

fn to_printable(blob: &[u8]) -> String {
    if blob.len() > 60 {
        format!("{} bytes, hash: {}", blob.len(), hash(blob))
    } else {
        let ugly = blob.iter().any(|&x| x < b' ');
        if ugly {
            return format!("0x{}", hex::encode(blob));
        }
        match String::from_utf8(blob.to_vec()) {
            Ok(v) => v,
            Err(_e) => format!("0x{}", hex::encode(blob)),
        }
    }
}

fn kv_to_state_record(key: Vec<u8>, value: Vec<u8>) -> Option<StateRecord> {
    let column = &key[0..1];
    match column {
        col::ACCOUNT => {
            let separator = (1..key.len()).find(|&x| key[x] == ACCOUNT_DATA_SEPARATOR[0]);
            if separator.is_some() {
                Some(StateRecord::Data { key: to_base64(&key), value: to_base64(&value) })
            } else {
                let mut account = Account::try_from_slice(&value).unwrap();
                // TODO(#1200): When dumping state, all accounts have to pay rent
                account.storage_paid_at = 0;
                Some(StateRecord::Account {
                    account_id: String::from_utf8(key[1..].to_vec()).unwrap(),
                    account: account.into(),
                })
            }
        }
        col::CODE => Some(StateRecord::Contract {
            account_id: String::from_utf8(key[1..].to_vec()).unwrap(),
            code: to_base64(&value),
        }),
        col::ACCESS_KEY => {
            let separator = (1..key.len()).find(|&x| key[x] == col::ACCESS_KEY[0]).unwrap();
            let access_key = AccessKey::try_from_slice(&value).unwrap();
            let account_id = String::from_utf8(key[1..separator].to_vec()).unwrap();
            let public_key = PublicKey::try_from_slice(&key[(separator + 1)..]).unwrap();
            Some(StateRecord::AccessKey { account_id, public_key, access_key: access_key.into() })
        }
        col::RECEIVED_DATA => {
            let data = ReceivedData::try_from_slice(&value).unwrap().data;
            let separator = (1..key.len()).find(|&x| key[x] == ACCOUNT_DATA_SEPARATOR[0]).unwrap();
            let account_id = String::from_utf8(key[1..separator].to_vec()).unwrap();
            let data_id = CryptoHash::try_from(&key[(separator + 1)..]).unwrap();
            Some(StateRecord::ReceivedData { account_id, data_id, data })
        }
        col::POSTPONED_RECEIPT_ID => None,
        col::PENDING_DATA_COUNT => None,
        col::POSTPONED_RECEIPT => {
            let receipt = Receipt::try_from_slice(&value).unwrap();
            Some(StateRecord::PostponedReceipt(Box::new(receipt.into())))
        }
        _ => unreachable!(),
    }
}

fn print_state_entry(key: Vec<u8>, value: Vec<u8>) {
    match kv_to_state_record(key, value) {
        Some(StateRecord::Account { account_id, account }) => {
            println!("Account {:?}: {:?}", account_id, account)
        }
        Some(StateRecord::Data { key, value }) => {
            let key = from_base64(&key).unwrap();
            let separator = (1..key.len()).find(|&x| key[x] == ACCOUNT_DATA_SEPARATOR[0]).unwrap();
            let account_id = to_printable(&key[1..separator]);
            let contract_key = to_printable(&key[(separator + 1)..]);
            println!(
                "Storage {:?},{:?}: {:?}",
                account_id,
                contract_key,
                to_printable(&from_base64(&value).unwrap())
            );
        }
        Some(StateRecord::Contract { account_id, code: _ }) => {
            println!("Code for {:?}: ...", account_id)
        }
        Some(StateRecord::AccessKey { account_id, public_key, access_key }) => {
            println!("Access key {:?},{:?}: {:?}", account_id, public_key, access_key)
        }
        Some(StateRecord::ReceivedData { account_id, data_id, data }) => {
            println!(
                "Received data {:?},{:?}: {:?}",
                account_id,
                data_id,
                data.map(|v| to_printable(&v))
            );
        }
        Some(StateRecord::PostponedReceipt(receipt)) => {
            println!("Postponed receipt {:?}", receipt);
        }
        None => (),
    }
}

fn load_trie(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
) -> (NightshadeRuntime, Vec<StateRoot>, BlockHeight) {
    let mut chain_store = ChainStore::new(store.clone());

    let runtime = NightshadeRuntime::new(
        &home_dir,
        store,
        near_config.genesis_config.clone(),
        near_config.client_config.tracked_accounts.clone(),
        near_config.client_config.tracked_shards.clone(),
    );
    let head = chain_store.head().unwrap();
    let last_block = chain_store.get_block(&head.last_block_hash).unwrap().clone();
    let mut state_roots = vec![];
    for chunk in last_block.chunks.iter() {
        state_roots.push(chunk.inner.prev_state_root.clone());
    }
    (runtime, state_roots, last_block.header.inner_lite.height)
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
    let mut chain_store = ChainStore::new(store.clone());
    let runtime = NightshadeRuntime::new(
        &home_dir,
        store,
        near_config.genesis_config.clone(),
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
    let mut chain_store = ChainStore::new(store);
    let new_store = create_test_store();
    let runtime = NightshadeRuntime::new(
        &home_dir,
        new_store,
        near_config.genesis_config.clone(),
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
                    header.inner_lite.height,
                    chain_store
                        .get_block_height(&header.inner_rest.last_quorum_pre_commit)
                        .unwrap(),
                    header.inner_rest.validator_proposals,
                    vec![],
                    header.inner_rest.chunk_mask,
                    header.inner_rest.rent_paid,
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
                Arg::with_name("output")
                    .long("output")
                    .required(true)
                    .help("Output path for new genesis given current blockchain state")
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
    let mut near_config = load_config(home_dir);

    let store = create_store(&get_store_path(&home_dir));

    match matches.subcommand() {
        ("peers", Some(_args)) => {
            let peer_store = PeerStore::new(store, &[]).unwrap();
            for (peer_id, peer_info) in peer_store.iter() {
                println!("{} {:?}", peer_id, peer_info);
            }
        }
        ("state", Some(_args)) => {
            let (runtime, state_roots, height) = load_trie(store, &home_dir, &near_config);
            println!("Storage roots are {:?}, block height is {}", state_roots, height);
            for state_root in state_roots {
                let trie = TrieIterator::new(&runtime.trie, &state_root).unwrap();
                for item in trie {
                    let (key, value) = item.unwrap();
                    print_state_entry(key, value);
                }
            }
        }
        ("dump_state", Some(args)) => {
            let (runtime, state_roots, height) = load_trie(store, home_dir, &near_config);
            let output_path = args.value_of("output").map(|path| Path::new(path)).unwrap();
            println!(
                "Saving state at {:?} @ {} into {}",
                state_roots,
                height,
                output_path.display()
            );
            near_config.genesis_config.records = vec![];
            for state_root in state_roots {
                let trie = TrieIterator::new(&runtime.trie, &state_root).unwrap();
                for item in trie {
                    let (key, value) = item.unwrap();
                    if let Some(sr) = kv_to_state_record(key, value) {
                        near_config.genesis_config.records.push(sr);
                    }
                }
            }
            near_config.genesis_config.write_to_file(&output_path);
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
