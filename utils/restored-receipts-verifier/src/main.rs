use std::io::Error;
use neard::{get_default_home, get_store_path, load_config, NightshadeRuntime};
use near_store::create_store;
use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use std::path::Path;
use near_primitives::borsh::BorshSerialize;
use near_primitives::receipt::Receipt;

fn main() -> Result<(), Error> {
    println!("Start");

    let shard_id: u64 = 0;
    // TODO parse
    let default_home = get_default_home();
    let home_dir = Path::new(&default_home); // matches.value_of("home").map(|dir| Path::new(dir)).unwrap();
    let near_config = load_config(&home_dir);
    let store = create_store(&get_store_path(&home_dir));
    let mut chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);
    let runtime = NightshadeRuntime::new(
        &home_dir,
        store,
        &near_config.genesis,
        near_config.client_config.tracked_accounts.clone(),
        near_config.client_config.tracked_shards.clone(),
    );

    let mut receipts_missing: Vec<Receipt> = vec![];
    for height in 34691244..35812045 {
        let block_hash_result = chain_store.get_block_hash_by_height(height);
        if block_hash_result.is_err() {
            println!("{} does not exist, skip", height);
            continue;
        }
        let block_hash = block_hash_result.unwrap();

        let block = chain_store.get_block(&block_hash).unwrap().clone();
        if block.chunks()[shard_id as usize].height_included() == height {
            println!("{} included, skip", height);
            continue;
        }

        let chunk_extra =
            chain_store.get_chunk_extra(block.header().prev_hash(), shard_id).unwrap().clone();
        // let apply_result = apply_transactions_for_not_included_chunk(&runtime, shard_id, &chunk_extra, &block);
        let apply_result = runtime
            .apply_transactions(
                shard_id,
                &chunk_extra.state_root,
                block.header().height(),
                block.header().raw_timestamp(),
                block.header().prev_hash(),
                &block.hash(),
                &[],
                &[],
                &chunk_extra.validator_proposals,
                block.header().gas_price(),
                chunk_extra.gas_limit,
                &block.header().challenges_result(),
                *block.header().random_value(),
                false,
            )
            .unwrap();

        let receipts_missing_after_apply: Vec<Receipt> = apply_result.receipt_result.values().cloned().into_iter().flatten().collect();
        receipts_missing.extend(receipts_missing_after_apply.into_iter());
        println!("{} applied", height);
    }

    let bytes = receipts_missing.try_to_vec().unwrap();
    std::fs::write("./utils/restored-receipts-verifier/receipts_missing.dat", bytes);

    Ok(())
}
