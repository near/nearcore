use clap::{Arg, Command};
use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_chain_configs::{GenesisValidationMode, DEFAULT_GC_NUM_EPOCHS_TO_KEEP};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_store::create_store;
use nearcore::migrations::load_migration_data;
use nearcore::{get_default_home, get_store_path, load_config, NightshadeRuntime, TrackedConfig};
use std::collections::HashSet;
use std::io::Result;
use std::path::Path;

fn get_receipt_hashes_in_repo() -> Vec<CryptoHash> {
    let receipt_result = load_migration_data("mainnet").restored_receipts;
    let receipts = receipt_result.get(&0u64).unwrap();
    receipts.iter().map(|receipt| receipt.get_hash()).collect()
}

fn get_differences_with_hashes_from_repo(
    receipt_hashes_missing: Vec<CryptoHash>,
) -> (Vec<CryptoHash>, Vec<CryptoHash>) {
    let missing_hashes = HashSet::<CryptoHash>::from_iter(receipt_hashes_missing.into_iter());
    let existing_hashes = HashSet::from_iter(get_receipt_hashes_in_repo().into_iter());
    let not_verified_hashes: Vec<CryptoHash> =
        existing_hashes.difference(&missing_hashes).cloned().collect();
    let still_missing_hashes: Vec<CryptoHash> =
        missing_hashes.difference(&existing_hashes).cloned().collect();
    (not_verified_hashes, still_missing_hashes)
}

fn main() -> Result<()> {
    eprintln!("restored-receipts-verifier started");

    let default_home = get_default_home();
    let matches = Command::new("restored-receipts-verifier")
        .arg(
            Arg::new("home")
                .default_value_os(default_home.as_os_str())
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .get_matches();

    let shard_id = 0u64;
    let home_dir = matches.value_of("home").map(Path::new).unwrap();
    let near_config = load_config(home_dir, GenesisValidationMode::Full)
        .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

    let store = create_store(&get_store_path(home_dir));
    let mut chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        !near_config.client_config.archive,
    );
    let runtime = NightshadeRuntime::new(
        home_dir,
        store,
        &near_config.genesis,
        TrackedConfig::from_config(&near_config.client_config),
        None,
        near_config.client_config.max_gas_burnt_view,
        None,
        DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
    );

    let mut receipts_missing = Vec::<Receipt>::new();
    let height_first: u64 = 34691244; // First height for which lost receipts were found
    let height_last: u64 = 35524259; // Height for which apply_chunks was already fixed

    eprintln!("Collecting missing receipts from blocks...");
    for height in height_first..height_last {
        let block_hash_result = chain_store.get_block_hash_by_height(height);
        let block_hash = match block_hash_result {
            Ok(it) => it,
            Err(_) => {
                eprintln!("{} does not exist, skip", height);
                continue;
            }
        };

        let block = chain_store.get_block(&block_hash).unwrap().clone();
        if block.chunks()[shard_id as usize].height_included() == height {
            eprintln!("{} included, skip", height);
            continue;
        }
        let shard_uid = runtime.shard_id_to_uid(shard_id, block.header().epoch_id()).unwrap();

        let chunk_extra =
            chain_store.get_chunk_extra(block.header().prev_hash(), &shard_uid).unwrap().clone();
        let apply_result = runtime
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
                false, // because fix was not applied in for the blocks analyzed here
                None,
            )
            .unwrap();

        receipts_missing.extend(apply_result.outgoing_receipts.into_iter());
        eprintln!("{} applied", height);
    }

    let receipt_hashes_missing =
        receipts_missing.into_iter().map(|receipt| receipt.get_hash()).collect();

    eprintln!("Verifying receipt hashes...");
    let (receipt_hashes_not_verified, receipt_hashes_still_missing) =
        get_differences_with_hashes_from_repo(receipt_hashes_missing);
    assert!(
        receipt_hashes_not_verified.is_empty(),
        "Some of receipt hashes in repo were not verified successfully: {:?}",
        receipt_hashes_not_verified
    );
    assert!(
        receipt_hashes_still_missing.is_empty(),
        "Some of receipt hashes in repo are probably still not applied: {:?}",
        receipt_hashes_still_missing
    );

    eprintln!("Receipt hashes in repo were verified successfully!");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checking_differences() {
        let receipt_hashes_in_repo = get_receipt_hashes_in_repo();

        let receipt_hashes_missing = receipt_hashes_in_repo.clone();
        let (receipt_hashes_not_verified, receipt_hashes_still_missing) =
            get_differences_with_hashes_from_repo(receipt_hashes_missing);
        assert!(receipt_hashes_not_verified.is_empty());
        assert!(receipt_hashes_still_missing.is_empty());

        let mut receipt_hashes_missing = receipt_hashes_in_repo.clone();
        let extra_hash = receipt_hashes_missing.pop().unwrap();
        let (receipt_hashes_not_verified, receipt_hashes_still_missing) =
            get_differences_with_hashes_from_repo(receipt_hashes_missing);
        assert_eq!(receipt_hashes_not_verified, vec![extra_hash]);
        assert!(receipt_hashes_still_missing.is_empty());

        let mut receipt_hashes_missing = receipt_hashes_in_repo;
        receipt_hashes_missing.push(CryptoHash::default());
        let (receipt_hashes_not_verified, receipt_hashes_still_missing) =
            get_differences_with_hashes_from_repo(receipt_hashes_missing);
        assert!(receipt_hashes_not_verified.is_empty());
        assert_eq!(receipt_hashes_still_missing, vec![CryptoHash::default()]);
    }
}
