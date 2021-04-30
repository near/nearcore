use std::collections::HashSet;
use std::io::Result;
use std::iter::FromIterator;
use std::path::Path;

use clap::{App, Arg};

use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptResult};
use near_store::create_store;
use neard::{get_default_home, get_store_path, load_config, NightshadeRuntime};

fn main() -> Result<()> {
    let default_home = get_default_home();
    let matches = App::new("restored-receipts-verifier")
        .arg(
            Arg::with_name("home")
                .long("home")
                .default_value(&default_home)
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .get_matches();

    println!("Start");

    let shard_id = 0u64;
    let home_dir = matches.value_of("home").map(Path::new).unwrap();
    let near_config = load_config(&home_dir);
    let store = create_store(&get_store_path(&home_dir));
    let mut chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);
    let runtime = NightshadeRuntime::new(
        &home_dir,
        store,
        &near_config.genesis,
        near_config.client_config.tracked_accounts.clone(),
        near_config.client_config.tracked_shards.clone(),
        None,
    );

    let mut receipts_missing = Vec::<Receipt>::new();
    let height_first: u64 = 34691244; // First height for which lost receipts were found
    let height_last: u64 = 35524259; // Height for which apply_chunks was already fixed

    for height in height_first..height_last {
        let block_hash_result = chain_store.get_block_hash_by_height(height);
        let block_hash = match block_hash_result {
            Ok(it) => it,
            Err(it) => {
                println!("{} does not exist, skip", height);
                continue;
            }
        };

        let block = chain_store.get_block(&block_hash).unwrap().clone();
        if block.chunks()[shard_id as usize].height_included() == height {
            println!("{} included, skip", height);
            continue;
        }

        let chunk_extra =
            chain_store.get_chunk_extra(block.header().prev_hash(), shard_id).unwrap().clone();
        let apply_result = runtime
            .apply_transactions(
                shard_id,
                chunk_extra.state_root(),
                block.header().height(),
                block.header().raw_timestamp(),
                block.header().prev_hash(),
                &block.hash(),
                &[],
                &[],
                chunk_extra.validator_proposals(),
                block.header().gas_price(),
                chunk_extra.gas_limit(),
                &block.header().challenges_result(),
                *block.header().random_value(),
                false,
            )
            .unwrap();

        let receipts_missing_after_apply: Vec<Receipt> =
            apply_result.receipt_result.values().cloned().into_iter().flatten().collect();
        receipts_missing.extend(receipts_missing_after_apply.into_iter());
        println!("{} applied", height);
    }

    let receipt_hashes_missing: HashSet<CryptoHash> =
        HashSet::<_>::from_iter(receipts_missing.into_iter().map(|receipt| receipt.get_hash()));

    // Check that receipts from repo were actually generated
    let receipt_hashes_in_repo: HashSet<CryptoHash> = {
        let receipt_result_json =
            include_str!("../../../neard/res/fix_apply_chunks_receipts.json");
        let receipt_result = serde_json::from_str::<ReceiptResult>(receipt_result_json)
            .expect("File with receipts restored after apply_chunks fix have to be correct");
        let receipts = receipt_result.get(&shard_id).unwrap();
        HashSet::<_>::from_iter(receipts.into_iter().map(|receipt| receipt.get_hash()))
    };

    let receipt_hashes_not_verified: Vec<CryptoHash> =
        receipt_hashes_in_repo.difference(&receipt_hashes_missing).cloned().collect();
    assert!(
        receipt_hashes_not_verified.is_empty(),
        "Some of receipt hashes in repo were not verified successfully: {:?}",
        receipt_hashes_not_verified
    );
    println!("Receipt hashes in repo were verified successfully!");

    Ok(())
}
