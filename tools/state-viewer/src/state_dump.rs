use chrono::Utc;
use near_chain::types::RuntimeAdapter;
use near_chain_configs::{Genesis, GenesisChangeConfig, GenesisConfig};
use near_crypto::PublicKey;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::EpochManagerHandle;
use near_primitives::account::id::AccountId;
use near_primitives::block::BlockHeader;
use near_primitives::receipt::ReceiptOrStateStoredReceipt;
use near_primitives::state_record::state_record_to_account_id;
use near_primitives::state_record::{DelayedReceipt, StateRecord};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountInfo, Balance, StateRoot};
use nearcore::NightshadeRuntime;
use nearcore::config::NearConfig;
use redis::Commands;
use serde::ser::{SerializeSeq, Serializer};
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Returns a `NearConfig` with genesis records taken from the current state.
/// If `records_path` argument is provided, then records will be streamed into a separate file,
/// otherwise the returned `NearConfig` will contain all the records within itself.
pub fn state_dump(
    epoch_manager: &EpochManagerHandle,
    runtime: Arc<NightshadeRuntime>,
    state_roots: &[StateRoot],
    last_block_header: BlockHeader,
    near_config: &NearConfig,
    records_path: Option<&Path>,
    change_config: &GenesisChangeConfig,
) -> NearConfig {
    println!(
        "Generating genesis from state data of #{} / {}",
        last_block_header.height(),
        last_block_header.hash()
    );
    let genesis_height = last_block_header.height() + 1;
    let block_producers =
        epoch_manager.get_epoch_block_producers_ordered(last_block_header.epoch_id()).unwrap();
    let validators = block_producers
        .into_iter()
        .map(|info| {
            let (account_id, public_key, stake) = info.destructure();
            (account_id, (public_key, stake))
        })
        .collect::<HashMap<_, _>>();

    let mut near_config = near_config.clone();

    let mut genesis_config = near_config.genesis.config.clone();
    genesis_config.genesis_height = genesis_height;
    genesis_config.genesis_time = Utc::now();
    genesis_config.validators = validators
        .iter()
        .map(|(account_id, (public_key, amount))| AccountInfo {
            account_id: account_id.clone(),
            public_key: public_key.clone(),
            amount: *amount,
        })
        .collect();
    genesis_config.validators.sort_by_key(|account_info| account_info.account_id.clone());
    // Record the protocol version of the latest block. Otherwise, the state
    // dump ignores the fact that the nodes can be running a newer protocol
    // version than the protocol version of the genesis.
    genesis_config.protocol_version = last_block_header.latest_protocol_version();
    let shard_config = epoch_manager.get_shard_config(last_block_header.epoch_id()).unwrap();
    genesis_config.shard_layout = shard_config.shard_layout;
    genesis_config.num_block_producer_seats_per_shard =
        shard_config.num_block_producer_seats_per_shard;
    genesis_config.avg_hidden_validator_seats_per_shard =
        shard_config.avg_hidden_validator_seats_per_shard;
    // Record only the filename of the records file.
    // Otherwise the absolute path is stored making it impossible to copy the dumped state to actually use it.
    match records_path {
        Some(records_path) => {
            let mut records_path_dir = records_path.to_path_buf();
            records_path_dir.pop();
            fs::create_dir_all(&records_path_dir).unwrap_or_else(|_| {
                panic!("Failed to create directory {}", records_path_dir.display())
            });
            let records_file = File::create(records_path).unwrap();
            let mut ser = serde_json::Serializer::new(records_file);
            let mut seq = ser.serialize_seq(None).unwrap();
            let total_supply = iterate_over_records(
                epoch_manager,
                runtime,
                state_roots,
                last_block_header,
                &validators,
                &genesis_config.protocol_treasury_account,
                &mut |sr| seq.serialize_element(&sr).unwrap(),
                change_config,
            );
            seq.end().unwrap();
            // `total_supply` is expected to change due to the natural processes of burning tokens and
            // minting tokens every epoch.
            genesis_config.total_supply = total_supply;
            change_genesis_config(&mut genesis_config, change_config);
            near_config.genesis = Genesis::new_with_path(genesis_config, records_path).unwrap();
            near_config.config.genesis_records_file =
                Some(records_path.file_name().unwrap().to_str().unwrap().to_string());
        }
        None => {
            let mut records: Vec<StateRecord> = vec![];
            let total_supply = iterate_over_records(
                epoch_manager,
                runtime,
                state_roots,
                last_block_header,
                &validators,
                &genesis_config.protocol_treasury_account,
                &mut |sr| records.push(sr),
                change_config,
            );
            // `total_supply` is expected to change due to the natural processes of burning tokens and
            // minting tokens every epoch.
            genesis_config.total_supply = total_supply;
            change_genesis_config(&mut genesis_config, change_config);
            near_config.genesis = Genesis::new(genesis_config, records.into()).unwrap();
        }
    }
    near_config
}

pub fn state_dump_redis(
    epoch_manager: Arc<EpochManagerHandle>,
    runtime: Arc<NightshadeRuntime>,
    state_roots: &[StateRoot],
    last_block_header: BlockHeader,
) -> redis::RedisResult<()> {
    let redis_client = redis::Client::open(
        env::var("REDIS_HOST").unwrap_or_else(|_| "redis://127.0.0.1/".to_string()),
    )?;
    let mut redis_connection = redis_client.get_connection()?;

    let block_height = last_block_header.height();
    let block_hash = last_block_header.hash();
    let epoch_id = last_block_header.epoch_id();
    let shard_layout = epoch_manager.get_shard_layout(epoch_id).unwrap();

    for (shard_index, state_root) in state_roots.iter().enumerate() {
        let shard_id = shard_layout.get_shard_id(shard_index).unwrap();
        let trie = runtime
            .get_trie_for_shard(shard_id, last_block_header.prev_hash(), *state_root, false)
            .unwrap();
        for item in trie.disk_iter().unwrap() {
            let (key, value) = item.unwrap();
            if let Some(sr) = StateRecord::from_raw_key_value(&key, value) {
                if let StateRecord::Account { account_id, account } = &sr {
                    println!("Account: {}", account_id);
                    let redis_key = account_id.as_bytes();
                    let () = redis_connection.zadd(
                        [b"account:", redis_key].concat(),
                        block_hash.as_ref(),
                        block_height,
                    )?;
                    let value = borsh::to_vec(&account).unwrap();
                    let () = redis_connection.set(
                        [b"account-data:", redis_key, b":", block_hash.as_ref()].concat(),
                        value,
                    )?;
                    println!("Account written: {}", account_id);
                }

                if let StateRecord::Data { account_id, data_key, value } = &sr {
                    println!("Data: {}", account_id);
                    let redis_key = [account_id.as_bytes(), b":", data_key.as_ref()].concat();
                    let () = redis_connection.zadd(
                        [b"data:", redis_key.as_slice()].concat(),
                        block_hash.as_ref(),
                        block_height,
                    )?;
                    let value_vec: &[u8] = value.as_ref();
                    let () = redis_connection.set(
                        [b"data-value:", redis_key.as_slice(), b":", block_hash.as_ref()].concat(),
                        value_vec,
                    )?;
                    println!("Data written: {}", account_id);
                }

                if let StateRecord::Contract { account_id, code } = &sr {
                    println!("Contract: {}", account_id);
                    let redis_key = [b"code:", account_id.as_bytes()].concat();
                    let () = redis_connection.zadd(
                        redis_key.clone(),
                        block_hash.as_ref(),
                        block_height,
                    )?;
                    let value_vec: &[u8] = code.as_ref();
                    let () = redis_connection.set(
                        [redis_key.clone(), b":".to_vec(), block_hash.0.to_vec()].concat(),
                        value_vec,
                    )?;
                    println!("Contract written: {}", account_id);
                }
            }
        }
    }

    Ok(())
}

fn should_include_record(
    record: &StateRecord,
    account_allowlist: &Option<HashSet<&AccountId>>,
) -> bool {
    match account_allowlist {
        None => true,
        Some(allowlist) => {
            let current_account_id = state_record_to_account_id(record);
            allowlist.contains(current_account_id)
        }
    }
}

// This keeps track of the delayed receipt indices we've seen in a shard.
// This is necessary because the iteration order of delayed receipts will not
// necessarily match their order in the queue. So we remember the indices with this
// type and then write them in the right order at the end.
struct DelayedReceiptsTracker {
    indices: Option<std::ops::Range<u64>>,
}

impl DelayedReceiptsTracker {
    fn new() -> Self {
        Self { indices: None }
    }

    fn index_seen(&mut self, index: u64) {
        match &mut self.indices {
            Some(indices) => {
                indices.start = std::cmp::min(indices.start, index);
                indices.end = std::cmp::max(indices.end, index + 1);
            }
            None => {
                self.indices = Some(std::ops::Range { start: index, end: index + 1 });
            }
        }
    }
}

/// Iterates over the state, calling `callback` for every record that genesis needs to contain.
fn iterate_over_records(
    epoch_manager: &EpochManagerHandle,
    runtime: Arc<NightshadeRuntime>,
    state_roots: &[StateRoot],
    last_block_header: BlockHeader,
    validators: &HashMap<AccountId, (PublicKey, Balance)>,
    protocol_treasury_account: &AccountId,
    mut callback: impl FnMut(StateRecord),
    change_config: &GenesisChangeConfig,
) -> Balance {
    let account_allowlist = match &change_config.select_account_ids {
        None => None,
        Some(select_account_id_list) => {
            let mut result = validators.keys().collect::<HashSet<&AccountId>>();
            result.extend(select_account_id_list);
            result.insert(protocol_treasury_account);
            Some(result)
        }
    };

    let epoch_id = last_block_header.epoch_id();
    let shard_layout = epoch_manager.get_shard_layout(epoch_id).unwrap();

    let mut total_supply = 0;
    for (shard_index, state_root) in state_roots.iter().enumerate() {
        let shard_id = shard_layout.get_shard_id(shard_index).unwrap();
        let trie = runtime
            .get_trie_for_shard(shard_id, last_block_header.prev_hash(), *state_root, false)
            .unwrap();
        let mut indices = DelayedReceiptsTracker::new();
        for item in trie.disk_iter().unwrap() {
            let (key, value) = item.unwrap();
            if let Some(mut sr) = StateRecord::from_raw_key_value(&key, value) {
                if !should_include_record(&sr, &account_allowlist) {
                    continue;
                }
                match &mut sr {
                    StateRecord::Account { account_id, account } => {
                        if account.locked() > 0 {
                            let mut stake =
                                *validators.get(account_id).map(|(_, s)| s).unwrap_or(&0);
                            if let Some(whitelist) = &change_config.whitelist_validators {
                                if !whitelist.contains(account_id) {
                                    stake = 0;
                                }
                            }
                            if account.locked() > stake {
                                account.set_amount(account.amount() + account.locked() - stake);
                            }
                            account.set_locked(stake);
                        }
                        total_supply += account.amount() + account.locked();
                        callback(sr);
                    }
                    StateRecord::DelayedReceipt(r) => {
                        // The index is always set when iterating over the trie
                        let index = r.index.unwrap();
                        indices.index_seen(index);
                    }
                    _ => {
                        callback(sr);
                    }
                };
            }
        }

        // Now write all delayed receipts in the right order
        if let Some(indices) = indices.indices {
            for index in indices {
                let key = TrieKey::DelayedReceipt { index };
                let value =
                    near_store::get_pure::<ReceiptOrStateStoredReceipt>(&trie, &key).unwrap();
                let Some(receipt) = value else {
                    tracing::warn!(
                        "Expected delayed receipt with index {} in shard {} not found",
                        index,
                        shard_id
                    );
                    continue;
                };
                let receipt = Box::new(receipt.into_receipt());
                let record =
                    StateRecord::DelayedReceipt(DelayedReceipt { index: Some(index), receipt });
                callback(record);
            }
        }
    }
    total_supply
}

/// Change genesis_config according to genesis_change_config.
/// 1. Kick all the non-whitelisted validators;
pub fn change_genesis_config(
    genesis_config: &mut GenesisConfig,
    genesis_change_config: &GenesisChangeConfig,
) {
    {
        // Kick validators outside of whitelist
        if let Some(whitelist) = &genesis_change_config.whitelist_validators {
            genesis_config.validators.retain(|v| whitelist.contains(&v.account_id));
        }
    }
}
