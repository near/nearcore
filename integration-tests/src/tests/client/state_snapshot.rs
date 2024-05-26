use near_chain::{ChainStoreAccess, Provenance};
use near_chain_configs::{Genesis, NEAR_BASE};
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::transaction::SignedTransaction;
use near_store::config::StateSnapshotType;
use near_store::flat::FlatStorageManager;
use near_store::{
    config::TrieCacheConfig, test_utils::create_test_store, Mode, ShardTries, StateSnapshotConfig,
    StoreConfig, TrieConfig,
};
use near_store::{NodeStorage, Store};
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use std::path::PathBuf;

struct StateSnaptshotTestEnv {
    home_dir: PathBuf,
    hot_store_path: PathBuf,
    state_snapshot_subdir: PathBuf,
    shard_tries: ShardTries,
}

impl StateSnaptshotTestEnv {
    fn new(
        home_dir: PathBuf,
        hot_store_path: PathBuf,
        state_snapshot_subdir: PathBuf,
        store: &Store,
    ) -> Self {
        let trie_cache_config = TrieCacheConfig {
            default_max_bytes: bytesize::ByteSize::mb(50),
            per_shard_max_bytes: Default::default(),
            shard_cache_deletions_queue_capacity: 0,
        };
        let trie_config = TrieConfig {
            shard_cache_config: trie_cache_config.clone(),
            view_shard_cache_config: trie_cache_config,
            ..TrieConfig::default()
        };
        let flat_storage_manager = FlatStorageManager::new(store.clone());
        let shard_uids = [ShardUId::single_shard()];
        let state_snapshot_config = StateSnapshotConfig {
            state_snapshot_type: StateSnapshotType::EveryEpoch,
            home_dir: home_dir.clone(),
            hot_store_path: hot_store_path.clone(),
            state_snapshot_subdir: state_snapshot_subdir.clone(),
        };
        let shard_tries = ShardTries::new(
            store.clone(),
            trie_config,
            &shard_uids,
            flat_storage_manager,
            state_snapshot_config,
        );
        Self { home_dir, hot_store_path, state_snapshot_subdir, shard_tries }
    }
}

fn set_up_test_env_for_state_snapshots(store: &Store) -> StateSnaptshotTestEnv {
    let home_dir =
        tempfile::Builder::new().prefix("storage").tempdir().unwrap().path().to_path_buf();
    let hot_store_path = PathBuf::from("data");
    let state_snapshot_subdir = PathBuf::from("state_snapshot");
    StateSnaptshotTestEnv::new(home_dir, hot_store_path, state_snapshot_subdir, store)
}

#[test]
// there's no entry in rocksdb for STATE_SNAPSHOT_KEY, maybe_open_state_snapshot should return error instead of panic
fn test_maybe_open_state_snapshot_no_state_snapshot_key_entry() {
    init_test_logger();
    let store = create_test_store();
    let test_env = set_up_test_env_for_state_snapshots(&store);
    let result =
        test_env.shard_tries.maybe_open_state_snapshot(|_| Ok(vec![ShardUId::single_shard()]));
    assert!(result.is_err());
}

#[test]
// there's no file present in the path for state snapshot, maybe_open_state_snapshot should return error instead of panic
fn test_maybe_open_state_snapshot_file_not_exist() {
    init_test_logger();
    let store = create_test_store();
    let test_env = set_up_test_env_for_state_snapshots(&store);
    let snapshot_hash = CryptoHash::new();
    test_env.shard_tries.set_state_snapshot_hash(Some(snapshot_hash)).unwrap();
    let result =
        test_env.shard_tries.maybe_open_state_snapshot(|_| Ok(vec![ShardUId::single_shard()]));
    assert!(result.is_err());
}

#[test]
// there's garbage in the path for state snapshot, maybe_open_state_snapshot should return error instead of panic
fn test_maybe_open_state_snapshot_garbage_snapshot() {
    use std::fs::{create_dir_all, File};
    use std::io::Write;
    use std::path::Path;
    init_test_logger();
    let store = create_test_store();
    let test_env = set_up_test_env_for_state_snapshots(&store);
    let snapshot_hash = CryptoHash::new();
    test_env.shard_tries.set_state_snapshot_hash(Some(snapshot_hash)).unwrap();
    let snapshot_path = ShardTries::get_state_snapshot_base_dir(
        &snapshot_hash,
        &test_env.home_dir,
        &test_env.hot_store_path,
        &test_env.state_snapshot_subdir,
    );
    if let Some(parent) = Path::new(&snapshot_path).parent() {
        create_dir_all(parent).unwrap();
    }
    let mut file = File::create(snapshot_path).unwrap();
    // write some garbage
    let data: Vec<u8> = vec![1, 2, 3, 4];
    file.write_all(&data).unwrap();

    let result =
        test_env.shard_tries.maybe_open_state_snapshot(|_| Ok(vec![ShardUId::single_shard()]));
    assert!(result.is_err());
}

fn verify_make_snapshot(
    state_snapshot_test_env: &StateSnaptshotTestEnv,
    block_hash: CryptoHash,
    block: &Block,
) -> Result<(), anyhow::Error> {
    state_snapshot_test_env.shard_tries.delete_state_snapshot();
    state_snapshot_test_env.shard_tries.create_state_snapshot(
        block_hash,
        &[ShardUId::single_shard()],
        block,
    )?;
    // check that make_state_snapshot does not panic or err out
    // assert!(res.is_ok());
    let snapshot_path = ShardTries::get_state_snapshot_base_dir(
        &block_hash,
        &state_snapshot_test_env.home_dir,
        &state_snapshot_test_env.hot_store_path,
        &state_snapshot_test_env.state_snapshot_subdir,
    );
    // check that the snapshot just made can be opened
    state_snapshot_test_env
        .shard_tries
        .maybe_open_state_snapshot(|_| Ok(vec![ShardUId::single_shard()]))?;
    // check that the entry of STATE_SNAPSHOT_KEY is the latest block hash
    let db_state_snapshot_hash = state_snapshot_test_env.shard_tries.get_state_snapshot_hash()?;
    if db_state_snapshot_hash != block_hash {
        return Err(anyhow::Error::msg(
            "the entry of STATE_SNAPSHOT_KEY does not equal to the prev block hash",
        ));
    }
    // check that the stored snapshot in file system is an actual snapshot
    let store_config = StoreConfig::default();
    let opener = NodeStorage::opener(&snapshot_path, false, &store_config, None);
    let _storage = opener.open_in_mode(Mode::ReadOnly)?;
    // check that there's only one snapshot at the parent directory of snapshot path
    let parent_path = snapshot_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{snapshot_path:?} needs to have a parent dir"))?;
    let parent_path_result = std::fs::read_dir(parent_path)?;
    if vec![parent_path_result.filter_map(Result::ok)].len() > 1 {
        return Err(anyhow::Error::msg(
            "there are more than 1 snapshot file in the snapshot parent directory",
        ));
    }
    return Ok(());
}

fn delete_content_at_path(path: &str) -> std::io::Result<()> {
    let metadata = std::fs::metadata(path)?;
    if metadata.is_dir() {
        std::fs::remove_dir_all(path)?;
    } else {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

#[test]
// Runs a validator node.
// Makes a state snapshot after processing every block. Each block contains a
// transaction creating an account.
fn test_make_state_snapshot() {
    init_test_logger();
    let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let mut env = TestEnv::builder(&genesis.config)
        .clients_count(1)
        .use_state_snapshots()
        .real_stores()
        .nightshade_runtimes(&genesis)
        .build();

    let signer: Signer =
        InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0").into();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();

    let mut blocks = vec![];

    let store = env.clients[0].chain.chain_store().store();
    let state_snapshot_test_env = set_up_test_env_for_state_snapshots(store);

    for i in 1..=5 {
        let new_account_id = format!("test_account_{i}");
        let nonce = i;
        let tx = SignedTransaction::create_account(
            nonce,
            "test0".parse().unwrap(),
            new_account_id.parse().unwrap(),
            NEAR_BASE,
            signer.public_key(),
            &signer,
            genesis_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        assert_eq!(
            format!("{:?}", Ok::<(), anyhow::Error>(())),
            format!("{:?}", verify_make_snapshot(&state_snapshot_test_env, *block.hash(), &block))
        );
    }

    // check that if the entry in DBCol::STATE_SNAPSHOT_KEY was missing while snapshot file exists, an overwrite of snapshot can succeed
    state_snapshot_test_env.shard_tries.set_state_snapshot_hash(None).unwrap();
    let head = env.clients[0].chain.head().unwrap();
    let head_block_hash = head.last_block_hash;
    let head_block = env.clients[0].chain.get_block(&head_block_hash).unwrap();
    assert_eq!(
        format!("{:?}", Ok::<(), anyhow::Error>(())),
        format!(
            "{:?}",
            verify_make_snapshot(&state_snapshot_test_env, head_block_hash, &head_block)
        )
    );

    // check that if the snapshot is deleted from file system while there's entry in DBCol::STATE_SNAPSHOT_KEY
    // recreating the snapshot will succeed
    let snapshot_hash = head.last_block_hash;
    let snapshot_path = ShardTries::get_state_snapshot_base_dir(
        &snapshot_hash,
        &state_snapshot_test_env.home_dir,
        &state_snapshot_test_env.hot_store_path,
        &state_snapshot_test_env.state_snapshot_subdir,
    );
    delete_content_at_path(snapshot_path.to_str().unwrap()).unwrap();
    assert_eq!(
        format!("{:?}", Ok::<(), anyhow::Error>(())),
        format!(
            "{:?}",
            verify_make_snapshot(&state_snapshot_test_env, head.last_block_hash, &head_block)
        )
    );
}
