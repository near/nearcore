use std::collections::HashSet;
use std::io::Result;
use std::iter::FromIterator;
use std::path::Path;

use clap::{App, Arg};

use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
#[cfg(not(feature = "protocol_feature_restore_receipts_after_fix"))]
use near_primitives::receipt::ReceiptResult;
use near_store::create_store;
#[cfg(feature = "protocol_feature_restore_receipts_after_fix")]
use neard::migrations::load_migration_data;
use neard::{get_default_home, get_store_path, load_config, NightshadeRuntime};

fn get_receipt_hashes_in_repo() -> Vec<CryptoHash> {
    #[cfg(not(feature = "protocol_feature_restore_receipts_after_fix"))]
    let receipt_result = ReceiptResult::default();
    #[cfg(feature = "protocol_feature_restore_receipts_after_fix")]
    let receipt_result = load_migration_data(&"mainnet".to_string()).restored_receipts;
    let receipts = receipt_result.get(&0u64).unwrap();
    receipts.into_iter().map(|receipt| receipt.get_hash()).collect()
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
    // Script to verify that receipts being restored after apply_chunks fix were actually created.
    // Because receipt hashes are unique, we only check for their presence.
    // See https://github.com/near/nearcore/pull/4248/ for more details.
    // Requirement: mainnet archival node dump.

    eprintln!("restored-receipts-verifier started");

    let default_home = get_default_home();
    let matches = App::new("restored-receipts-verifier")
        .arg(
            Arg::new("home")
                .default_value(&default_home)
                .about("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .get_matches();

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
                false, // because fix was not applied in for the blocks analyzed here
            )
            .unwrap();

        let receipts_missing_after_apply: Vec<Receipt> =
            apply_result.receipt_result.values().cloned().into_iter().flatten().collect();
        receipts_missing.extend(receipts_missing_after_apply.into_iter());
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
    #[cfg(feature = "protocol_feature_restore_receipts_after_fix")]
    use super::*;

    #[test]
    #[cfg(feature = "protocol_feature_restore_receipts_after_fix")]
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

        let mut receipt_hashes_missing = receipt_hashes_in_repo.clone();
        receipt_hashes_missing.push(CryptoHash::default());
        let (receipt_hashes_not_verified, receipt_hashes_still_missing) =
            get_differences_with_hashes_from_repo(receipt_hashes_missing);
        assert!(receipt_hashes_not_verified.is_empty());
        assert_eq!(receipt_hashes_still_missing, vec![CryptoHash::default()]);
    }
}
