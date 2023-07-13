use borsh::BorshSerialize;
use chrono::Utc;
use near_chain::types::RuntimeAdapter;
use near_chain_configs::{Genesis, GenesisChangeConfig, GenesisConfig};
use near_crypto::PublicKey;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::EpochManagerHandle;
use near_primitives::account::id::AccountId;
use near_primitives::block::BlockHeader;
use near_primitives::state_record::state_record_to_account_id;
use near_primitives::state_record::StateRecord;
use near_primitives::types::{AccountInfo, Balance, StateRoot};
use nearcore::config::NearConfig;
use nearcore::NightshadeRuntime;
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
    let block_producers = epoch_manager
        .get_epoch_block_producers_ordered(last_block_header.epoch_id(), last_block_header.hash())
        .unwrap();
    let validators = block_producers
        .into_iter()
        .filter_map(|(info, is_slashed)| {
            if !is_slashed {
                let (account_id, public_key, stake) = info.destructure();
                Some((account_id, (public_key, stake)))
            } else {
                None
            }
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
    runtime: Arc<NightshadeRuntime>,
    state_roots: &[StateRoot],
    last_block_header: BlockHeader,
) -> redis::RedisResult<()> {
    let redis_client =
        redis::Client::open(env::var("REDIS_HOST").unwrap_or("redis://127.0.0.1/".to_string()))?;
    let mut redis_connection = redis_client.get_connection()?;

    let block_height = last_block_header.height();
    let block_hash = last_block_header.hash();

    for (shard_id, state_root) in state_roots.iter().enumerate() {
        let trie = runtime
            .get_trie_for_shard(shard_id as u64, last_block_header.prev_hash(), *state_root, false)
            .unwrap();
        for item in trie.iter().unwrap() {
            let (key, value) = item.unwrap();
            if let Some(sr) = StateRecord::from_raw_key_value(key, value) {
                if let StateRecord::Account { account_id, account } = &sr {
                    println!("Account: {}", account_id);
                    let redis_key = account_id.as_ref().as_bytes();
                    redis_connection.zadd(
                        [b"account:", redis_key].concat(),
                        block_hash.as_ref(),
                        block_height,
                    )?;
                    let value = account.try_to_vec().unwrap();
                    redis_connection.set(
                        [b"account-data:", redis_key, b":", block_hash.as_ref()].concat(),
                        value,
                    )?;
                    println!("Account written: {}", account_id);
                }

                if let StateRecord::Data { account_id, data_key, value } = &sr {
                    println!("Data: {}", account_id);
                    let redis_key =
                        [account_id.as_ref().as_bytes(), b":", data_key.as_ref()].concat();
                    redis_connection.zadd(
                        [b"data:", redis_key.as_slice()].concat(),
                        block_hash.as_ref(),
                        block_height,
                    )?;
                    let value_vec: &[u8] = value.as_ref();
                    redis_connection.set(
                        [b"data-value:", redis_key.as_slice(), b":", block_hash.as_ref()].concat(),
                        value_vec,
                    )?;
                    println!("Data written: {}", account_id);
                }

                if let StateRecord::Contract { account_id, code } = &sr {
                    println!("Contract: {}", account_id);
                    let redis_key = [b"code:", account_id.as_ref().as_bytes()].concat();
                    redis_connection.zadd(redis_key.clone(), block_hash.as_ref(), block_height)?;
                    let value_vec: &[u8] = code.as_ref();
                    redis_connection.set(
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

/// Iterates over the state, calling `callback` for every record that genesis needs to contain.
fn iterate_over_records(
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
    let mut total_supply = 0;
    for (shard_id, state_root) in state_roots.iter().enumerate() {
        let trie = runtime
            .get_trie_for_shard(shard_id as u64, last_block_header.prev_hash(), *state_root, false)
            .unwrap();
        for item in trie.iter().unwrap() {
            let (key, value) = item.unwrap();
            if let Some(mut sr) = StateRecord::from_raw_key_value(key, value) {
                if !should_include_record(&sr, &account_allowlist) {
                    continue;
                }
                if let StateRecord::Account { account_id, account } = &mut sr {
                    total_supply += account.amount() + account.locked();
                    if account.locked() > 0 {
                        let stake = *validators.get(account_id).map(|(_, s)| s).unwrap_or(&0);
                        account.set_amount(account.amount() + account.locked() - stake);
                        account.set_locked(stake);
                    }
                }
                change_state_record(&mut sr, change_config);
                callback(sr);
            }
        }
    }
    total_supply
}

/// Change record according to genesis_change_config.
/// 1. Remove stake from non-whitelisted validators;
pub fn change_state_record(record: &mut StateRecord, genesis_change_config: &GenesisChangeConfig) {
    // Kick validators outside of whitelist
    if let Some(whitelist) = &genesis_change_config.whitelist_validators {
        if let StateRecord::Account { account_id, account } = record {
            if !whitelist.contains(account_id) {
                account.set_amount(account.amount() + account.locked());
                account.set_locked(0);
            }
        }
    }
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

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::sync::Arc;

    use near_chain::{ChainGenesis, Provenance};
    use near_chain_configs::genesis_validate::validate_genesis;
    use near_chain_configs::{Genesis, GenesisChangeConfig};
    use near_client::test_utils::TestEnv;
    use near_client::ProcessTxResponse;
    use near_crypto::{InMemorySigner, KeyFile, KeyType, PublicKey, SecretKey};
    use near_epoch_manager::EpochManager;
    use near_primitives::account::id::AccountId;
    use near_primitives::state_record::StateRecord;
    use near_primitives::transaction::{Action, DeployContractAction, SignedTransaction};
    use near_primitives::types::{
        Balance, BlockHeight, BlockHeightDelta, NumBlocks, ProtocolVersion,
    };
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::genesis::initialize_genesis_state;
    use near_store::test_utils::create_test_store;
    use near_store::Store;
    use nearcore::config::GenesisExt;
    use nearcore::config::TESTING_INIT_STAKE;
    use nearcore::config::{Config, NearConfig};
    use nearcore::NightshadeRuntime;

    use crate::state_dump::state_dump;
    use near_primitives::hash::CryptoHash;
    use near_primitives::validator_signer::InMemoryValidatorSigner;

    fn setup(
        epoch_length: NumBlocks,
        protocol_version: ProtocolVersion,
        use_production_config: bool,
    ) -> (Store, Genesis, TestEnv, NearConfig) {
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.num_block_producer_seats = 2;
        genesis.config.num_block_producer_seats_per_shard = vec![2];
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = protocol_version;
        genesis.config.use_production_config = use_production_config;
        let store = create_test_store();
        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let nightshade_runtime = NightshadeRuntime::test(
            Path::new("."),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
        );
        let mut chain_genesis = ChainGenesis::test();
        chain_genesis.epoch_length = epoch_length;
        chain_genesis.gas_limit = genesis.config.gas_limit;
        let env = TestEnv::builder(chain_genesis)
            .validator_seats(2)
            .stores(vec![store.clone()])
            .epoch_managers(vec![epoch_manager])
            .runtimes(vec![nightshade_runtime])
            .build();

        let near_config = NearConfig::new(
            Config::default(),
            genesis.clone(),
            KeyFile {
                account_id: "test".parse().unwrap(),
                public_key: PublicKey::empty(KeyType::ED25519),
                secret_key: SecretKey::from_random(KeyType::ED25519),
            },
            Some(Arc::new(InMemoryValidatorSigner::from_random(
                "test".parse().unwrap(),
                KeyType::ED25519,
            ))),
        )
        .unwrap();

        (store, genesis, env, near_config)
    }

    /// Produces blocks, avoiding the potential failure where the client is not the
    /// block producer for each subsequent height (this can happen when a new validator
    /// is staked since they will also have heights where they should produce the block instead).
    fn safe_produce_blocks(
        env: &mut TestEnv,
        initial_height: BlockHeight,
        num_blocks: BlockHeightDelta,
    ) {
        let mut h = initial_height;
        for _ in 1..=num_blocks {
            let mut block = None;
            // `env.clients[0]` may not be the block producer at `h`,
            // loop until we find a height env.clients[0] should produce.
            while block.is_none() {
                block = env.clients[0].produce_block(h).unwrap();
                h += 1;
            }
            env.process_block(0, block.unwrap(), Provenance::PRODUCED);
        }
    }

    /// Test that we preserve the validators from the epoch of the state dump.
    #[test]
    fn test_dump_state_preserve_validators() {
        let epoch_length = 4;
        let (store, genesis, mut env, near_config) = setup(epoch_length, PROTOCOL_VERSION, false);
        let genesis_hash = *env.clients[0].chain.genesis().hash();
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let tx = SignedTransaction::stake(
            1,
            "test1".parse().unwrap(),
            &signer,
            TESTING_INIT_STAKE,
            signer.public_key.clone(),
            genesis_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

        safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1);

        let head = env.clients[0].chain.head().unwrap();
        let last_block_hash = head.last_block_hash;
        let cur_epoch_id = head.epoch_id;
        let block_producers = env.clients[0]
            .epoch_manager
            .get_epoch_block_producers_ordered(&cur_epoch_id, &last_block_hash)
            .unwrap();
        assert_eq!(
            block_producers.into_iter().map(|(r, _)| r.take_account_id()).collect::<HashSet<_>>(),
            HashSet::from_iter(vec!["test0".parse().unwrap(), "test1".parse().unwrap()])
        );
        let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
        let state_roots: Vec<CryptoHash> =
            last_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect();
        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime =
            NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());
        let records_file = tempfile::NamedTempFile::new().unwrap();
        let new_near_config = state_dump(
            epoch_manager.as_ref(),
            runtime,
            &state_roots,
            last_block.header().clone(),
            &near_config,
            Some(&records_file.path().to_path_buf()),
            &GenesisChangeConfig::default(),
        );
        let new_genesis = new_near_config.genesis;
        assert_eq!(new_genesis.config.validators.len(), 2);
        validate_genesis(&new_genesis).unwrap();
    }

    /// Test that we respect the specified account ID list in dump_state.
    #[test]
    fn test_dump_state_respect_select_account_ids() {
        let epoch_length = 4;
        let (store, genesis, mut env, near_config) = setup(epoch_length, PROTOCOL_VERSION, false);
        let genesis_hash = *env.clients[0].chain.genesis().hash();

        let signer0 =
            InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        let tx00 = SignedTransaction::from_actions(
            1,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer0,
            vec![Action::DeployContract(DeployContractAction {
                code: near_test_contracts::backwards_compatible_rs_contract().to_vec(),
            })],
            genesis_hash,
        );
        let tx01 = SignedTransaction::stake(
            1,
            "test0".parse().unwrap(),
            &signer0,
            TESTING_INIT_STAKE,
            signer0.public_key.clone(),
            genesis_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx00, false, false), ProcessTxResponse::ValidTx);
        assert_eq!(env.clients[0].process_tx(tx01, false, false), ProcessTxResponse::ValidTx);

        let signer1 =
            InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let tx1 = SignedTransaction::stake(
            1,
            "test1".parse().unwrap(),
            &signer1,
            TESTING_INIT_STAKE,
            signer1.public_key.clone(),
            genesis_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx1, false, false), ProcessTxResponse::ValidTx);

        safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1);

        let head = env.clients[0].chain.head().unwrap();
        let last_block_hash = head.last_block_hash;
        let cur_epoch_id = head.epoch_id;
        let block_producers = env.clients[0]
            .epoch_manager
            .get_epoch_block_producers_ordered(&cur_epoch_id, &last_block_hash)
            .unwrap();
        assert_eq!(
            block_producers.into_iter().map(|(r, _)| r.take_account_id()).collect::<HashSet<_>>(),
            HashSet::from_iter(vec!["test0".parse().unwrap(), "test1".parse().unwrap()]),
        );
        let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
        let state_roots: Vec<CryptoHash> =
            last_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect();
        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime =
            NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());
        let select_account_ids = vec!["test0".parse().unwrap()];
        let new_near_config = state_dump(
            epoch_manager.as_ref(),
            runtime,
            &state_roots,
            last_block.header().clone(),
            &near_config,
            None,
            &GenesisChangeConfig::default()
                .with_select_account_ids(Some(select_account_ids.clone())),
        );
        let new_genesis = new_near_config.genesis;
        let mut expected_accounts: HashSet<AccountId> =
            new_genesis.config.validators.iter().map(|v| v.account_id.clone()).collect();
        expected_accounts.extend(select_account_ids);
        expected_accounts.insert(new_genesis.config.protocol_treasury_account.clone());
        let mut actual_accounts: HashSet<AccountId> = HashSet::new();
        new_genesis.for_each_record(|record| {
            if let StateRecord::Account { account_id, .. } = record {
                actual_accounts.insert(account_id.clone());
            }
        });
        assert_eq!(expected_accounts, actual_accounts);
        validate_genesis(&new_genesis).unwrap();
    }

    /// Test that we preserve the validators from the epoch of the state dump.
    #[test]
    fn test_dump_state_preserve_validators_inmemory() {
        let epoch_length = 4;
        let (store, genesis, mut env, near_config) = setup(epoch_length, PROTOCOL_VERSION, false);
        let genesis_hash = *env.clients[0].chain.genesis().hash();
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let tx = SignedTransaction::stake(
            1,
            "test1".parse().unwrap(),
            &signer,
            TESTING_INIT_STAKE,
            signer.public_key.clone(),
            genesis_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

        safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1);

        let head = env.clients[0].chain.head().unwrap();
        let last_block_hash = head.last_block_hash;
        let cur_epoch_id = head.epoch_id;
        let block_producers = env.clients[0]
            .epoch_manager
            .get_epoch_block_producers_ordered(&cur_epoch_id, &last_block_hash)
            .unwrap();
        assert_eq!(
            block_producers.into_iter().map(|(r, _)| r.take_account_id()).collect::<HashSet<_>>(),
            HashSet::from_iter(vec!["test0".parse().unwrap(), "test1".parse().unwrap()])
        );
        let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
        let state_roots: Vec<CryptoHash> =
            last_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect();
        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime =
            NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());
        let new_near_config = state_dump(
            epoch_manager.as_ref(),
            runtime,
            &state_roots,
            last_block.header().clone(),
            &near_config,
            None,
            &GenesisChangeConfig::default(),
        );
        let new_genesis = new_near_config.genesis;
        assert_eq!(new_genesis.config.validators.len(), 2);
        validate_genesis(&new_genesis).unwrap();
    }

    /// Test that we return locked tokens for accounts that are not validators.
    #[test]
    fn test_dump_state_return_locked() {
        let epoch_length = 4;
        let (store, genesis, mut env, near_config) = setup(epoch_length, PROTOCOL_VERSION, false);
        let genesis_hash = *env.clients[0].chain.genesis().hash();
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let tx = SignedTransaction::stake(
            1,
            "test1".parse().unwrap(),
            &signer,
            TESTING_INIT_STAKE,
            signer.public_key.clone(),
            genesis_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        for i in 1..=epoch_length + 1 {
            env.produce_block(0, i);
        }

        let head = env.clients[0].chain.head().unwrap();
        let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
        let state_roots: Vec<CryptoHash> =
            last_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect();
        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime =
            NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());

        let records_file = tempfile::NamedTempFile::new().unwrap();
        let new_near_config = state_dump(
            epoch_manager.as_ref(),
            runtime,
            &state_roots,
            last_block.header().clone(),
            &near_config,
            Some(&records_file.path().to_path_buf()),
            &GenesisChangeConfig::default(),
        );
        let new_genesis = new_near_config.genesis;
        assert_eq!(
            new_genesis
                .config
                .validators
                .clone()
                .into_iter()
                .map(|r| r.account_id)
                .collect::<Vec<_>>(),
            vec!["test0".parse().unwrap()]
        );
        validate_genesis(&new_genesis).unwrap();
    }

    #[test]
    fn test_dump_state_shard_upgrade() {
        use near_client::test_utils::run_catchup;
        use near_primitives::shard_layout::ShardLayout;
        use near_primitives::version::ProtocolFeature::SimpleNightshade;

        let epoch_length = 4;
        let (store, genesis, mut env, near_config) =
            setup(epoch_length, SimpleNightshade.protocol_version() - 1, true);
        for i in 1..=2 * epoch_length + 1 {
            let mut block = env.clients[0].produce_block(i).unwrap().unwrap();
            block.mut_header().set_latest_protocol_version(SimpleNightshade.protocol_version());
            env.process_block(0, block, Provenance::PRODUCED);
            run_catchup(&mut env.clients[0], &[]).unwrap();
        }
        let head = env.clients[0].chain.head().unwrap();
        assert_eq!(
            env.clients[0].epoch_manager.get_shard_layout(&head.epoch_id).unwrap(),
            ShardLayout::get_simple_nightshade_layout(),
        );
        let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();

        let state_roots: Vec<CryptoHash> =
            last_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect();
        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime =
            NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());
        let records_file = tempfile::NamedTempFile::new().unwrap();
        let new_near_config = state_dump(
            epoch_manager.as_ref(),
            runtime,
            &state_roots,
            last_block.header().clone(),
            &near_config,
            Some(&records_file.path().to_path_buf()),
            &GenesisChangeConfig::default(),
        );
        let new_genesis = new_near_config.genesis;

        assert_eq!(new_genesis.config.shard_layout, ShardLayout::get_simple_nightshade_layout());
        assert_eq!(new_genesis.config.num_block_producer_seats_per_shard, vec![2; 4]);
        assert_eq!(new_genesis.config.avg_hidden_validator_seats_per_shard, vec![0; 4]);
    }

    /// If the node does not track a shard, state dump will not give the correct result.
    #[test]
    #[should_panic(expected = "MissingTrieValue")]
    fn test_dump_state_not_track_shard() {
        let epoch_length = 4;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.num_block_producer_seats = 2;
        genesis.config.num_block_producer_seats_per_shard = vec![2];
        genesis.config.epoch_length = epoch_length;
        let store1 = create_test_store();
        let store2 = create_test_store();
        initialize_genesis_state(store1.clone(), &genesis, None);
        initialize_genesis_state(store2.clone(), &genesis, None);
        let epoch_manager1 = EpochManager::new_arc_handle(store1.clone(), &genesis.config);
        let epoch_manager2 = EpochManager::new_arc_handle(store2.clone(), &genesis.config);
        let runtime1 = NightshadeRuntime::test(
            Path::new("."),
            store1.clone(),
            &genesis.config,
            epoch_manager1.clone(),
        );
        let runtime2 = NightshadeRuntime::test(
            Path::new("."),
            store2.clone(),
            &genesis.config,
            epoch_manager2.clone(),
        );
        let mut chain_genesis = ChainGenesis::test();
        chain_genesis.epoch_length = epoch_length;
        chain_genesis.gas_limit = genesis.config.gas_limit;
        let mut env = TestEnv::builder(chain_genesis)
            .clients_count(2)
            .stores(vec![store1, store2])
            .epoch_managers(vec![epoch_manager1, epoch_manager2.clone()])
            .runtimes(vec![runtime1, runtime2.clone()])
            .build();
        let genesis_hash = *env.clients[0].chain.genesis().hash();
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let tx = SignedTransaction::send_money(
            1,
            "test1".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            1,
            genesis_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

        let mut blocks = vec![];
        for i in 1..epoch_length {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            for j in 0..2 {
                let provenance = if j == 0 { Provenance::PRODUCED } else { Provenance::NONE };
                env.process_block(j, block.clone(), provenance);
            }
            blocks.push(block);
        }

        let near_config = NearConfig::new(
            Config::default(),
            genesis,
            KeyFile {
                account_id: "test".parse().unwrap(),
                public_key: PublicKey::empty(KeyType::ED25519),
                secret_key: SecretKey::from_random(KeyType::ED25519),
            },
            Some(Arc::new(InMemoryValidatorSigner::from_random(
                "test".parse().unwrap(),
                KeyType::ED25519,
            ))),
        )
        .unwrap();

        let last_block = blocks.pop().unwrap();
        let state_roots =
            last_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect::<Vec<_>>();

        let records_file = tempfile::NamedTempFile::new().unwrap();
        let _ = state_dump(
            epoch_manager2.as_ref(),
            runtime2,
            &state_roots,
            last_block.header().clone(),
            &near_config,
            Some(&records_file.path().to_path_buf()),
            &GenesisChangeConfig::default(),
        );
    }

    #[test]
    fn test_dump_state_with_delayed_receipt() {
        let epoch_length = 4;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.num_block_producer_seats = 2;
        genesis.config.num_block_producer_seats_per_shard = vec![2];
        genesis.config.epoch_length = epoch_length;
        let store = create_test_store();
        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let nightshade_runtime = NightshadeRuntime::test(
            Path::new("."),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
        );
        let mut chain_genesis = ChainGenesis::test();
        chain_genesis.epoch_length = epoch_length;
        let mut env = TestEnv::builder(chain_genesis)
            .validator_seats(2)
            .stores(vec![store.clone()])
            .epoch_managers(vec![epoch_manager])
            .runtimes(vec![nightshade_runtime])
            .build();
        let genesis_hash = *env.clients[0].chain.genesis().hash();
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let tx = SignedTransaction::stake(
            1,
            "test1".parse().unwrap(),
            &signer,
            TESTING_INIT_STAKE,
            signer.public_key.clone(),
            genesis_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

        safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1);

        let near_config = NearConfig::new(
            Config::default(),
            genesis.clone(),
            KeyFile {
                account_id: "test".parse().unwrap(),
                public_key: PublicKey::empty(KeyType::ED25519),
                secret_key: SecretKey::from_random(KeyType::ED25519),
            },
            Some(Arc::new(InMemoryValidatorSigner::from_random(
                "test".parse().unwrap(),
                KeyType::ED25519,
            ))),
        )
        .unwrap();
        let head = env.clients[0].chain.head().unwrap();
        let last_block_hash = head.last_block_hash;
        let cur_epoch_id = head.epoch_id;
        let block_producers = env.clients[0]
            .epoch_manager
            .get_epoch_block_producers_ordered(&cur_epoch_id, &last_block_hash)
            .unwrap();
        assert_eq!(
            block_producers.into_iter().map(|(r, _)| r.take_account_id()).collect::<HashSet<_>>(),
            HashSet::from_iter(vec!["test0".parse().unwrap(), "test1".parse().unwrap()])
        );
        let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
        let state_roots: Vec<CryptoHash> =
            last_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect();
        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime =
            NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());
        let records_file = tempfile::NamedTempFile::new().unwrap();
        let new_near_config = state_dump(
            epoch_manager.as_ref(),
            runtime,
            &state_roots,
            last_block.header().clone(),
            &near_config,
            Some(&records_file.path().to_path_buf()),
            &GenesisChangeConfig::default(),
        );
        let new_genesis = new_near_config.genesis;

        assert_eq!(new_genesis.config.validators.len(), 2);
        validate_genesis(&new_genesis).unwrap();
    }

    #[test]
    fn test_dump_state_respect_select_whitelist_validators() {
        let epoch_length = 4;
        let (store, genesis, mut env, near_config) = setup(epoch_length, PROTOCOL_VERSION, false);

        let genesis_hash = *env.clients[0].chain.genesis().hash();
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let tx = SignedTransaction::stake(
            1,
            "test1".parse().unwrap(),
            &signer,
            TESTING_INIT_STAKE,
            signer.public_key.clone(),
            genesis_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

        safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1);

        let head = env.clients[0].chain.head().unwrap();
        let last_block_hash = head.last_block_hash;
        let cur_epoch_id = head.epoch_id;
        let block_producers = env.clients[0]
            .epoch_manager
            .get_epoch_block_producers_ordered(&cur_epoch_id, &last_block_hash)
            .unwrap();
        assert_eq!(
            block_producers.into_iter().map(|(r, _)| r.take_account_id()).collect::<HashSet<_>>(),
            HashSet::from_iter(vec!["test0".parse().unwrap(), "test1".parse().unwrap()]),
        );

        let whitelist_validators =
            vec!["test1".parse().unwrap(), "non_validator_account".parse().unwrap()];

        let last_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
        let state_roots: Vec<CryptoHash> =
            last_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect();
        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime =
            NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());
        let new_near_config = state_dump(
            epoch_manager.as_ref(),
            runtime,
            &state_roots,
            last_block.header().clone(),
            &near_config,
            None,
            &GenesisChangeConfig::default().with_whitelist_validators(Some(whitelist_validators)),
        );
        let new_genesis = new_near_config.genesis;

        assert_eq!(
            new_genesis
                .config
                .validators
                .iter()
                .map(|x| x.account_id.clone())
                .collect::<Vec<AccountId>>(),
            vec!["test1".parse().unwrap()]
        );

        let mut stake = HashMap::<AccountId, Balance>::new();
        new_genesis.for_each_record(|record| {
            if let StateRecord::Account { account_id, account } = record {
                stake.insert(account_id.clone(), account.locked());
            }
        });

        assert_eq!(stake.get("test0").unwrap_or(&(0 as Balance)), &(0 as Balance));

        validate_genesis(&new_genesis).unwrap();
    }
}
