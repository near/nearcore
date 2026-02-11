use crate::cli::SubCommand::CheckStateRoot;
use anyhow;
use anyhow::Context;
use borsh::BorshDeserialize;
use clap;
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_primitives::block::{Block, Tip};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::DelayedReceiptIndices;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{BlockHeight, ShardId, StateChangeCause};
use near_store::adapter::StoreAdapter;
use near_store::adapter::trie_store::{TrieStoreUpdateAdapter, get_shard_uid_mapping};
use near_store::archive::cold_storage::{copy_all_data_to_cold, update_cold_db, update_cold_head};
use near_store::db::metadata::DbKind;
use near_store::flat::FlatStorageManager;
use near_store::{
    COLD_HEAD_KEY, FINAL_HEAD_KEY, HEAD_KEY, ShardTries, ShardUId, StateSnapshotConfig, TAIL_KEY,
    Trie, TrieConfig, TrieUpdate, get_delayed_receipt_indices, get_promise_yield_indices, set,
};
use near_store::{DBCol, NodeStorage, Store, StoreOpener};
use nearcore::NearConfig;
use rand::seq::SliceRandom;
use std::ops::Deref;
use std::path::Path;
use std::time::Instant;
use strum::IntoEnumIterator;

#[derive(clap::Parser)]
pub struct ColdStoreCommand {
    /// By default state viewer opens rocks DB in the read only mode, which allows it to run
    /// multiple instances in parallel and be sure that no unintended changes get written to the DB.
    #[clap(long, short = 'w')]
    readwrite: bool,
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(clap::Parser)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
enum SubCommand {
    /// Open NodeStorage and check that is has cold storage.
    Open,
    /// Open NodeStorage and print cold head, hot head and hot final head.
    Head,
    /// Copy n blocks to cold storage and update cold HEAD. One by one.
    /// Updating of HEAD happens in every iteration.
    CopyNextBlocks(CopyNextBlocksCmd),
    /// Copy all blocks to cold storage and update cold HEAD.
    CopyAllBlocks(CopyAllBlocksCmd),
    /// Prepare a hot db from a rpc db. This command will update the db kind in
    /// the db and perform some sanity checks to make sure this db is suitable
    /// for migration to split storage.
    /// This command expects the following preconditions:
    /// - config.store.path points to an existing database with kind Hot or Archive
    /// - config.cold_store.path points to an existing database with kind Cold
    /// - store_relative_path points to an existing database with kind Rpc
    PrepareHot(PrepareHotCmd),
    /// Traverse trie and check that every node is in cold db.
    /// Can start from given state_root or compute previous roots for every chunk in provided block
    /// and use them as starting point.
    /// You can provide maximum depth and/or maximum number of vertices to traverse for each root.
    /// Trie is traversed using DFS with randomly shuffled kids for every node.
    CheckStateRoot(CheckStateRootCmd),
    /// Modifies cold db from config to be considered not initialized.
    /// Doesn't actually delete any data, except for HEAD and COLD_HEAD in BlockMisc.
    ResetCold(ResetColdCmd),
    /// Recover tries at prev state roots of the first block in a new shard layout after ReshardingV2.
    RecoverBoundaryReshardingV2,
}

impl ColdStoreCommand {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let mode =
            if self.readwrite { near_store::Mode::ReadWrite } else { near_store::Mode::ReadOnly };
        let mut near_config = nearcore::config::load_config(&home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        let opener = self.get_opener(home_dir, &mut near_config);

        let storage =
            opener.open_in_mode(mode).unwrap_or_else(|e| panic!("Error opening storage: {:#}", e));

        let epoch_manager = EpochManager::new_arc_handle(
            storage.get_hot_store(),
            &near_config.genesis.config,
            Some(home_dir),
        );
        match self.subcmd {
            SubCommand::Open => check_open(&storage),
            SubCommand::Head => print_heads(&storage),
            SubCommand::CopyNextBlocks(cmd) => {
                for _ in 0..cmd.number_of_blocks {
                    copy_next_block(&storage, &near_config, epoch_manager.as_ref());
                }
                Ok(())
            }
            SubCommand::CopyAllBlocks(cmd) => {
                copy_all_blocks(&storage, cmd.batch_size, !cmd.no_check_after);
                Ok(())
            }
            SubCommand::PrepareHot(cmd) => cmd.run(&storage, &home_dir, &near_config),
            SubCommand::CheckStateRoot(cmd) => cmd.run(&storage, &epoch_manager),
            SubCommand::ResetCold(cmd) => cmd.run(&storage),
            SubCommand::RecoverBoundaryReshardingV2 => {
                RecoverBoundaryReshardingV2Cmd::run(&storage, &home_dir, &near_config)
            }
        }
    }

    /// Returns opener suitable for subcommand.
    /// If subcommand is  CheckStateRoot, creates checkpoint for cold db
    /// and modifies `near_config.config.cold_store.path` to path to that checkpoint.
    /// Then returns opener for dbs at `store.path` and `cold_store.path`.
    pub fn get_opener<'a>(
        &'a self,
        home_dir: &Path,
        near_config: &'a mut NearConfig,
    ) -> StoreOpener<'a> {
        if !near_config.config.archive {
            tracing::warn!("expected archive option in config to be set to true");
        }

        let opener = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
            near_config.cloud_storage_context(),
        );

        match &self.subcmd {
            CheckStateRoot(cmd) if !cmd.skip_checkpoint => {
                let (hot_snapshot, cold_snapshot) = opener
                    .create_snapshots(near_store::Mode::ReadOnly)
                    .expect("Failed to create snapshots");
                if let Some(_) = &hot_snapshot.0 {
                    hot_snapshot.remove().expect("Failed to remove unnecessary hot snapshot");
                }
                if let Some(cold_store_config) = near_config.config.cold_store.as_mut() {
                    cold_store_config.path =
                        Some(cold_snapshot.0.clone().expect("cold_snapshot should be Some"));
                }
            }
            _ => {}
        }

        NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
            near_config.cloud_storage_context(),
        )
    }
}

#[derive(clap::Parser)]
struct CopyNextBlocksCmd {
    #[clap(short, long, default_value_t = 1)]
    number_of_blocks: usize,
}

#[derive(clap::Parser)]
struct CopyAllBlocksCmd {
    /// Threshold size of the write transaction.
    #[clap(short = 'b', long, default_value_t = 500_000_000)]
    batch_size: usize,
    /// Flag to not check correctness of cold db after copying.
    #[clap(long = "nc")]
    no_check_after: bool,
}

fn check_open(store: &NodeStorage) -> anyhow::Result<()> {
    assert!(store.has_cold());
    Ok(())
}

fn print_heads(store: &NodeStorage) -> anyhow::Result<()> {
    let hot_store = store.get_hot_store();
    let cold_store = store.get_cold_store();
    // TODO(cloud_archival) Handle cloud head

    // hot store
    {
        let kind = hot_store.get_db_kind();
        let head = hot_store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?;
        let final_head = hot_store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?;
        let cold_head = hot_store.get_ser::<Tip>(DBCol::BlockMisc, COLD_HEAD_KEY)?;
        println!("HOT STORE KIND is {:#?}", kind);
        println!("HOT STORE HEAD is at {:#?}", head);
        println!("HOT STORE FINAL_HEAD is at {:#?}", final_head);
        println!("HOT STORE COLD_HEAD is at {:#?}", cold_head);
    }

    // cold store
    if let Some(cold_store) = cold_store {
        let kind = cold_store.get_db_kind();
        let head_in_cold = cold_store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?;
        println!("COLD STORE KIND is {:#?}", kind);
        println!("COLD STORE HEAD is at {:#?}", head_in_cold);
    }
    Ok(())
}

fn copy_next_block(store: &NodeStorage, config: &NearConfig, epoch_manager: &EpochManagerHandle) {
    // Cold HEAD can be not set in testing.
    // It should be set before the copying of a block in prod,
    // but we should default it to genesis height here.
    let cold_head_height = store
        .get_cold_store()
        .unwrap()
        .get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)
        .unwrap_or_else(|e| panic!("Error reading cold HEAD: {:#}", e))
        .map_or(config.genesis.config.genesis_height, |t| t.height);

    // If FINAL_HEAD is not set for hot storage though, we default it to 0.
    // And subsequently fail in assert!(next_height <= hot_final_height).
    let hot_final_head = store
        .get_hot_store()
        .get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)
        .unwrap_or_else(|e| panic!("Error reading hot FINAL_HEAD: {:#}", e))
        .map(|t| t.height)
        .unwrap_or(0);

    let next_height = cold_head_height + 1;
    println!("Height: {}", next_height);
    assert!(next_height <= hot_final_head, "Should not copy non final blocks");

    // Here it should be sufficient to just read from hot storage.
    // Because BlockHeight is never garbage collectable and is not even copied to cold.
    let next_height_block_hash = get_ser_from_store::<CryptoHash>(
        &store.get_hot_store(),
        DBCol::BlockHeight,
        &next_height.to_le_bytes(),
    )
    .unwrap_or_else(|| panic!("No block hash in hot storage for height {}", next_height));

    // For copying block we need to have shard_layout.
    // For that we need epoch_id.
    // For that we might use the hash of the block.
    let epoch_id = &epoch_manager.get_epoch_id(&next_height_block_hash).unwrap();
    let shard_layout = &epoch_manager.get_shard_layout(epoch_id).unwrap();
    let shard_uids = shard_layout.shard_uids().collect();
    let block_info = epoch_manager.get_block_info(&next_height_block_hash).unwrap();
    let is_resharding_boundary =
        epoch_manager.is_resharding_boundary(block_info.prev_hash()).unwrap();
    update_cold_db(
        &*store.cold_db().unwrap(),
        &store.get_hot_store(),
        &shard_layout,
        &shard_uids,
        &next_height,
        is_resharding_boundary,
        1,
    )
    .unwrap_or_else(|_| panic!("Failed to copy block at height {} to cold db", next_height));

    update_cold_head(&*store.cold_db().unwrap(), &store.get_hot_store(), &next_height)
        .unwrap_or_else(|_| panic!("Failed to update cold HEAD to {}", next_height));
}

fn copy_all_blocks(storage: &NodeStorage, batch_size: usize, check: bool) {
    // If FINAL_HEAD is not set for hot storage we default it to 0
    // not genesis_height, because hot db needs to contain genesis block for that
    let hot_final_head = storage
        .get_hot_store()
        .get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)
        .unwrap_or_else(|e| panic!("Error reading hot FINAL_HEAD: {:#}", e))
        .map(|t| t.height)
        .unwrap_or(0);

    let keep_going = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));

    copy_all_data_to_cold(
        (*storage.cold_db().unwrap()).clone(),
        &storage.get_hot_store(),
        batch_size,
        &keep_going,
    )
    .expect("Failed to do migration to cold db");

    // Setting cold head to hot_final_head captured BEFORE the start of initial migration.
    // Doesn't really matter here, but very important in case of migration during `neard run`.
    update_cold_head(&*storage.cold_db().unwrap(), &storage.get_hot_store(), &hot_final_head)
        .unwrap_or_else(|_| panic!("Failed to update cold HEAD to {}", hot_final_head));

    if check {
        for col in DBCol::iter() {
            if col.is_cold() {
                println!(
                    "Performed {} {:?} checks",
                    check_iter(&storage.get_hot_store(), &storage.get_cold_store().unwrap(), col),
                    col
                );
            }
        }
    }
}

fn check_key(
    first_store: &near_store::Store,
    second_store: &near_store::Store,
    col: DBCol,
    key: &[u8],
) {
    let first_res = first_store.get(col, key);
    let second_res = second_store.get(col, key);

    assert_eq!(first_res.unwrap(), second_res.unwrap());
}

/// Checks that `first_store`'s column `col` is fully included in `second_store`
/// with same values for every key.
/// Return number of checks performed == number of keys in column `col` of the `first_store`.
fn check_iter(
    first_store: &near_store::Store,
    second_store: &near_store::Store,
    col: DBCol,
) -> u64 {
    let mut num_checks = 0;
    for (key, _value) in first_store.iter(col) {
        check_key(first_store, second_store, col, &key);
        num_checks += 1;
    }
    num_checks
}

/// Calls get_ser on Store with provided temperature from provided NodeStorage.
/// Expects read to not result in errors.
fn get_ser_from_store<T: near_primitives::borsh::BorshDeserialize>(
    store: &Store,
    col: DBCol,
    key: &[u8],
) -> Option<T> {
    store.get_ser(col, key).unwrap_or_else(|_| panic!("Error reading {} {:?} from store", col, key))
}

#[derive(clap::Parser)]
struct PrepareHotCmd {
    /// The relative path to the rpc store that will be converted to a hot store.
    /// The path should be relative to the home_dir e.g. hot_data
    #[clap(short, long)]
    store_relative_path: String,
}

impl PrepareHotCmd {
    pub fn run(
        &self,
        storage: &NodeStorage,
        home_dir: &Path,
        near_config: &NearConfig,
    ) -> anyhow::Result<()> {
        let _span = tracing::info_span!(target: "prepare-hot", "run");

        let path = Path::new(&self.store_relative_path);
        tracing::info!(target: "prepare-hot", rpc_path=?path, "preparing a hot db from the rpc db");

        tracing::info!(target: "prepare-hot", "opening hot and cold");
        let hot_store = storage.get_hot_store();
        let cold_store = storage.get_cold_store();
        let cold_store =
            cold_store.ok_or_else(|| anyhow::anyhow!("The cold store is not configured!"))?;

        tracing::info!(target: "prepare-hot", "opening rpc");
        // Open the rpc_storage using the near_config with the path swapped.
        let mut rpc_store_config = near_config.config.store.clone();
        rpc_store_config.path = Some(path.to_path_buf());
        let rpc_opener = NodeStorage::opener(home_dir, &rpc_store_config, None, None);
        let rpc_storage = rpc_opener.open()?;
        let rpc_store = rpc_storage.get_hot_store();

        tracing::info!(target: "prepare-hot", "checking db kind");
        Self::check_db_kind(&hot_store, &cold_store, &rpc_store)?;

        tracing::info!(target: "prepare-hot", "checking up to date");
        Self::check_up_to_date(&cold_store, &rpc_store)?;

        // TODO may be worth doing some simple sanity check that the rpc store
        // and the cold store contain the same chain. Keep in mind that the
        // responsibility of ensuring that the rpc backup can be trusted lies
        // with the node owner still. We don't want to do a full check here
        // as it would take too long.

        tracing::info!(target: "prepare-hot", "the hot, cold and RPC stores are suitable for cold storage migration");
        tracing::info!(target: "prepare-hot", "changing the db kind of the rpc store to hot");
        rpc_store.set_db_kind(DbKind::Hot);

        tracing::info!(target: "prepare-hot", ?path, "successfully prepared the hot store for migration, you can now set the `config.store.path` in neard config");

        Ok(())
    }

    /// Check that the DbKind of each of the stores is as expected.
    fn check_db_kind(
        hot_store: &Store,
        cold_store: &Store,
        rpc_store: &Store,
    ) -> anyhow::Result<()> {
        let hot_db_kind = hot_store.get_db_kind();
        if hot_db_kind != Some(DbKind::Hot) && hot_db_kind != Some(DbKind::Archive) {
            return Err(anyhow::anyhow!(
                "Unexpected hot_store DbKind, expected: DbKind::Hot or DbKind::Archive, got: {:?}",
                hot_db_kind,
            ));
        }

        let cold_db_kind = cold_store.get_db_kind();
        if cold_db_kind != Some(DbKind::Cold) {
            return Err(anyhow::anyhow!(
                "Unexpected cold_store DbKind, expected: DbKind::Cold, got: {:?}",
                cold_db_kind,
            ));
        }

        let rpc_db_kind = rpc_store.get_db_kind();
        if rpc_db_kind != Some(DbKind::RPC) {
            return Err(anyhow::anyhow!(
                "Unexpected rpc_store DbKind, expected: DbKind::RPC, got: {:?}",
                rpc_db_kind,
            ));
        }

        Ok(())
    }

    /// Check that the cold store and rpc store are sufficiently up to date.
    fn check_up_to_date(cold_store: &Store, rpc_store: &Store) -> anyhow::Result<()> {
        let rpc_head = rpc_store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?;
        let rpc_head = rpc_head.ok_or_else(|| anyhow::anyhow!("The rpc head is missing!"))?;
        let rpc_tail = rpc_store.get_ser::<u64>(DBCol::BlockMisc, TAIL_KEY)?;
        let rpc_tail = rpc_tail.ok_or_else(|| anyhow::anyhow!("The rpc tail is missing!"))?;
        let cold_head = cold_store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?;
        let cold_head = cold_head.ok_or_else(|| anyhow::anyhow!("The cold head is missing"))?;

        // Ideally it should look like this:
        // RPC     T . .  . . . . H
        // COLD  . . . ES . . H

        if cold_head.height < rpc_tail {
            return Err(anyhow::anyhow!(
                "The cold head is behind the rpc tail. cold head height: {} rpc tail height: {}",
                cold_head.height,
                rpc_tail
            ));
        }

        let cold_head_hash = cold_head.last_block_hash;
        let cold_head_block_info =
            rpc_store.get_ser::<BlockInfo>(DBCol::BlockInfo, cold_head_hash.as_ref())?;
        let cold_head_block_info = cold_head_block_info
            .ok_or_else(|| anyhow::anyhow!("Cold head block info is not in rpc db"))?;
        let cold_epoch_first_block = *cold_head_block_info.epoch_first_block();
        let cold_epoch_first_block_info =
            rpc_store.get_ser::<BlockInfo>(DBCol::BlockInfo, cold_epoch_first_block.as_ref())?;

        if cold_epoch_first_block_info.is_none() {
            return Err(anyhow::anyhow!(
                "The start of the latest epoch in cold db is behind the rpc tail.\
                cold head height: {}, rpc tail height: {}",
                cold_head.height,
                rpc_tail
            ));
        }

        // More likely the cold store head will actually be ahead of the rpc
        // head, since rpc will be downloaded from S3 and a bit behind.
        // It should be fine and we just end up copying a few blocks from hot to
        // cold that already are present in cold. Just in case something doesn't
        // work let's tell the user that we're in that state.
        // RPC     T . . . . . H
        // COLD  . . . . . . . . . H

        if cold_head.height > rpc_head.height {
            tracing::warn!(target: "prepare-hot",
                cold_head_height = cold_head.height,
                rpc_head_height = rpc_head.height,
                "the cold head is ahead of the RPC head, this should fix itself when the node catches up and becomes in sync"
            );
        }

        Ok(())
    }
}

/// The StateRootSelector is a subcommand that allows the user to select the state root(s) either by block height(s) or by the state root hash.
#[derive(clap::Subcommand)]
enum StateRootSelector {
    Heights { from: BlockHeight, to: Option<BlockHeight> },
    Hash { hash: CryptoHash, shard_uid: ShardUId },
}

fn get_block_hash_key(hot_store: &Store, height: BlockHeight) -> anyhow::Result<Vec<u8>> {
    let height_key = height.to_le_bytes();
    let hash_key = hot_store
        .get(DBCol::BlockHeight, &height_key)
        .ok_or_else(|| anyhow::anyhow!("Failed to find block hash for height {:?}", height))?
        .as_slice()
        .to_vec();
    Ok(hash_key)
}

fn get_block(store: &Store, hash_key: Vec<u8>) -> anyhow::Result<Block> {
    store
        .get_ser::<Block>(DBCol::Block, &hash_key)?
        .ok_or_else(|| anyhow::anyhow!("Failed to find Block: {:?}", hash_key))
}

/// Calculate previous state roots for chunks at the given block.
fn get_prev_state_roots(
    cold_store: &Store,
    epoch_manager: &EpochManagerHandle,
    block_hash_key: Vec<u8>,
) -> anyhow::Result<Vec<(CryptoHash, ShardUId)>> {
    let block = get_block(cold_store, block_hash_key)?;
    let shard_layout = epoch_manager.read().get_shard_layout(block.header().epoch_id())?;
    let mut hashes = vec![];
    for chunk in block.chunks().iter() {
        let state_root_hash = cold_store
            .get_ser::<near_primitives::sharding::ShardChunk>(
                DBCol::Chunks,
                chunk.chunk_hash().as_bytes(),
            )?
            .ok_or_else(|| anyhow::anyhow!("Failed to find Chunk: {:?}", chunk.chunk_hash()))?
            .take_header()
            .prev_state_root();
        let shard_uid = ShardUId::from_shard_id_and_layout(chunk.shard_id(), &shard_layout);
        hashes.push((state_root_hash, shard_uid));
    }
    Ok(hashes)
}

/// Struct that holds all conditions for node in Trie
/// to be checked by CheckStateRootCmd::check_trie.
#[derive(Clone, Debug)]
struct PruneCondition {
    /// Maximum depth (measured in number of nodes, not trie key length).
    max_depth: Option<u64>,
    /// Maximum number of nodes checked for each state_root.
    max_count: Option<u64>,
}

/// Struct that holds data related to iterating the trie in CheckStateRootCmd::check_trie.
#[derive(Debug)]
struct IterState {
    /// Depth of node in trie (measured in number of nodes, not trie key length).
    depth: u64,
    /// Number of already checked nodes.
    count: u64,
    /// Consecutive parts of the trie path that led to the current state.
    trie_path: Vec<Vec<u8>>,
}

impl IterState {
    pub fn new() -> Self {
        Self { depth: 0, count: 0, trie_path: vec![] }
    }

    /// Return `true` if node should be pruned.
    pub fn should_prune(&self, condition: &PruneCondition) -> bool {
        if let Some(md) = condition.max_depth {
            if self.depth > md {
                return true;
            }
        }
        if let Some(mc) = condition.max_count {
            if self.count > mc {
                return true;
            }
        }
        false
    }

    /// Modify self to reflect going down a tree.
    /// We increment node count, because we are visiting a new node.
    pub fn down(&mut self, path_extension: Vec<u8>) {
        self.count += 1;
        self.depth += 1;
        self.trie_path.push(path_extension);
    }

    /// Modify self to reflect going up a tree.
    /// We do not change node count, because we already visited parent node before.
    pub fn up(&mut self) {
        self.depth -= 1;
        let _ = self.trie_path.pop();
    }

    pub fn print_path(&self) {
        if self.trie_path.is_empty() {
            println!("\n--> Missing trie root node");
            return;
        }
        println!(
            "\n--> Trie path to missing node: {}",
            self.trie_path.iter().map(|v| format!("{:?}", v)).collect::<Vec<_>>().join(" | ")
        );
    }
}

#[derive(clap::Args)]
struct CheckStateRootCmd {
    /// Maximum depth (measured in number of nodes, not trie key length) for checking trie.
    #[clap(long)]
    max_depth: Option<u64>,
    /// Maximum number of nodes checked for each state_root.
    #[clap(long)]
    max_count: Option<u64>,
    #[clap(subcommand)]
    state_root_selector: StateRootSelector,
    /// Skip creating a checkpoint before running checks.
    #[clap(long)]
    skip_checkpoint: bool,
}

struct CheckTrieContext {
    store: Store,
    prune_condition: PruneCondition,
    shard_uid: ShardUId,
}

impl CheckStateRootCmd {
    pub fn run(
        self,
        storage: &NodeStorage,
        epoch_manager: &EpochManagerHandle,
    ) -> anyhow::Result<()> {
        let cold_store = storage
            .get_cold_store()
            .ok_or_else(|| anyhow::anyhow!("Cold storage is not configured"))?;
        let prune_condition =
            PruneCondition { max_depth: self.max_depth, max_count: self.max_count };
        let heights = match self.state_root_selector {
            StateRootSelector::Hash { hash, shard_uid } => {
                return Self::check_trie_root(&cold_store, &prune_condition, &shard_uid, &hash);
            }
            StateRootSelector::Heights { from, to } => {
                if let Some(to) = to {
                    from..=to
                } else {
                    from..=from
                }
            }
        };
        for height in heights {
            let block_hash_key = match get_block_hash_key(&storage.get_hot_store(), height) {
                Ok(key) => key,
                Err(err) => {
                    println!(
                        "\n## Skipping block at height {height} as it is missing. Error: {err}\n"
                    );
                    continue;
                }
            };
            println!(
                "# Running checks for prev state roots of chunks from block height {}",
                height
            );
            let roots_with_shard_uid =
                get_prev_state_roots(&cold_store, epoch_manager, block_hash_key)?;
            for (hash, shard_uid) in roots_with_shard_uid {
                if let Err(error) =
                    Self::check_trie_root(&cold_store, &prune_condition, &shard_uid, &hash)
                {
                    // Just display the error and continue checking other state roots.
                    println!("{error}\n");
                }
            }
        }
        Ok(())
    }

    fn check_trie_root(
        cold_store: &Store,
        prune_condition: &PruneCondition,
        shard_uid: &ShardUId,
        hash: &CryptoHash,
    ) -> anyhow::Result<()> {
        let context = CheckTrieContext {
            store: cold_store.clone(),
            prune_condition: prune_condition.clone(),
            shard_uid: *shard_uid,
        };
        let mut iter_state = IterState::new();
        println!("* Checking shard {}, subtree of {}", shard_uid, hash);
        let timer = Instant::now();
        Self::check_trie(&context, &hash, &mut iter_state)?;
        println!(
            "Checked {} trie nodes, elapsed_sec: {}\n",
            iter_state.count,
            timer.elapsed().as_secs_f64()
        );
        Ok(())
    }

    /// Check that subtree of `hash` is fully present in store.
    fn check_trie(
        context: &CheckTrieContext,
        hash: &CryptoHash,
        iter_state: &mut IterState,
    ) -> anyhow::Result<()> {
        tracing::debug!(target: "check_trie", ?hash, ?iter_state, "checking trie");
        let CheckTrieContext { store, shard_uid, prune_condition } = &context;
        if iter_state.should_prune(prune_condition) {
            tracing::debug!(target: "check_trie", ?prune_condition, "reached prune condition");
            return Ok(());
        }

        let bytes = Self::read_state(store, shard_uid, hash);
        if !matches!(bytes, Ok(Some(_))) {
            iter_state.print_path();
        }
        let bytes = bytes
            .with_context(|| format!("Failed to read raw bytes for hash {:?}", hash))?
            .with_context(|| format!("Failed to find raw bytes for hash {:?}", hash))?;

        let node = near_store::RawTrieNodeWithSize::try_from_slice(&bytes)?;
        match node.node {
            near_store::RawTrieNode::Leaf(..) => {
                tracing::debug!(target: "check_trie", "reached leaf node");
                return Ok(());
            }
            near_store::RawTrieNode::BranchNoValue(children)
            | near_store::RawTrieNode::BranchWithValue(_, children) => {
                let mut children: Vec<_> = children.iter().collect();
                children.shuffle(&mut rand::thread_rng());
                for (idx, child) in children {
                    // Record in iter state that we are visiting a child node
                    iter_state.down(vec![idx]);
                    // Visit a child node
                    Self::check_trie(context, child, iter_state)?;
                    // Record in iter state that we are returning from a child node
                    iter_state.up();
                }
            }
            near_store::RawTrieNode::Extension(key, child) => {
                // Record in iter state that we are visiting a child node
                iter_state.down(key);
                // Visit a child node
                Self::check_trie(context, &child, iter_state)?;
                // Record in iter state that we are returning from a child node
                iter_state.up();
            }
        }
        Ok(())
    }

    fn read_state<'a>(
        store: &'a Store,
        shard_uid: &ShardUId,
        trie_key: &CryptoHash,
    ) -> std::io::Result<Option<near_store::db::DBSlice<'a>>> {
        let mapped_shard_uid = get_shard_uid_mapping(&store, *shard_uid);
        let cold_state_key = [mapped_shard_uid.to_bytes().as_ref(), trie_key.as_bytes()].concat();
        Ok(store.get(DBCol::State, &cold_state_key))
    }
}

#[derive(clap::Args)]
struct ResetColdCmd {}

impl ResetColdCmd {
    pub fn run(self, storage: &NodeStorage) -> anyhow::Result<()> {
        let cold_store = storage
            .get_cold_store()
            .ok_or_else(|| anyhow::anyhow!("Cold storage is not configured"))?;

        let mut store_update = cold_store.store_update();
        store_update.delete(DBCol::BlockMisc, HEAD_KEY);
        store_update.delete(DBCol::BlockMisc, COLD_HEAD_KEY);
        store_update.commit();
        Ok(())
    }
}

struct RecoverBoundaryReshardingV2Cmd;

impl RecoverBoundaryReshardingV2Cmd {
    const BLOCK_HEIGHT: BlockHeight = 115185108;
    const CHILD_SHARD_ID: ShardId = ShardId::new(1);

    pub fn run(
        storage: &NodeStorage,
        home_dir: &Path,
        near_config: &NearConfig,
    ) -> anyhow::Result<()> {
        let hot_store = storage.get_hot_store();
        let cold_store = storage
            .get_cold_store()
            .ok_or_else(|| anyhow::anyhow!("Cold storage is not configured"))?;

        let split_store = storage.get_split_store().expect("Split store expected on archival node");
        let epoch_manager =
            EpochManager::new_arc_handle(split_store, &near_config.genesis.config, Some(home_dir));
        let tries = ShardTries::new(
            cold_store.trie_store(),
            TrieConfig::from_store_config(&near_config.config.store),
            FlatStorageManager::new(cold_store.flat_store()),
            StateSnapshotConfig::Disabled,
        );

        let block_hash_key = get_block_hash_key(&hot_store, Self::BLOCK_HEIGHT)?;
        let block = get_block(&cold_store, block_hash_key)?;
        let new_shard_layout = epoch_manager.read().get_shard_layout(block.header().epoch_id())?;
        let parent_shard_id = new_shard_layout.get_parent_shard_id(Self::CHILD_SHARD_ID)?;
        let child_shard_uid =
            ShardUId::from_shard_id_and_layout(Self::CHILD_SHARD_ID, &new_shard_layout);
        // Chunk which prev_state_root is the trie we want to repair.
        let chunk = extract_chunk_from_block(&block, &Self::CHILD_SHARD_ID)?;
        // That should be the outcome of our backfilling. If it matches, we are confident we backfilled properly.
        let expected_new_state_root = chunk.prev_state_root();
        println!(
            "Expected child previous state root after resharding: {}",
            expected_new_state_root
        );

        let prev_block_hash = chunk.prev_block_hash();
        let prev_block = cold_store.chain_store().get_block(prev_block_hash)?;
        let old_shard_layout =
            epoch_manager.read().get_shard_layout(prev_block.header().epoch_id())?;
        // The tool is supposed to be called with the first block of the new shard layout.
        assert_ne!(old_shard_layout, new_shard_layout);
        let is_shard_split =
            old_shard_layout.get_children_shards_ids(parent_shard_id).is_some_and(|v| v.len() > 1);
        // The current version of the recovery tool supports only shards that have not been split.
        assert!(!is_shard_split);
        let parent_shard_uid =
            ShardUId::from_shard_id_and_layout(parent_shard_id, &old_shard_layout);
        let chunk_extra =
            cold_store.chunk_store().get_chunk_extra(prev_block_hash, &parent_shard_uid)?;
        // We expect the trie under `prev_state_root` to be fully available in State.
        let prev_state_root = chunk_extra.state_root();
        println!("Parent state root at the end of the resharding epoch: {}", prev_state_root);
        let prev_trie = tries.get_trie_for_shard(parent_shard_uid, *prev_state_root);
        let prev_delayed_receipt_indices = get_delayed_receipt_indices(&prev_trie)?;
        let prev_promise_yield_indices = get_promise_yield_indices(&prev_trie)?;
        // We use the assumptions below to simplify the code.
        // These are valid for the height `115185108` for which this recovery tool was written.
        assert_eq!(prev_delayed_receipt_indices.len(), 0);
        assert_eq!(prev_promise_yield_indices.len(), 0);

        let prev_block_info = cold_store.epoch_store().get_block_info(prev_block_hash)?;
        let prev_epoch_start =
            cold_store.chain_store().get_block(prev_block_info.epoch_first_block())?;
        let prev_epoch_start_chunk = extract_chunk_from_block(&prev_epoch_start, &parent_shard_id)?;
        let prev_epoch_start_trie =
            tries.get_trie_for_shard(parent_shard_uid, prev_epoch_start_chunk.prev_state_root());
        // We know what were delayed receipt indices in the parent shard at the moment before shard split started.
        let prev_epoch_start_delayed_receipt_indices =
            get_delayed_receipt_indices(&prev_epoch_start_trie)?;
        println!(
            "Parent delayed receipt indices at the start of the resharding epoch: {:?}",
            prev_epoch_start_delayed_receipt_indices
        );
        println!(
            "Parent delayed receipt indices at the end of the resharding epoch: {:?}",
            prev_delayed_receipt_indices
        );
        assert!(
            prev_epoch_start_delayed_receipt_indices.first_index
                <= prev_delayed_receipt_indices.first_index
        );

        // ReshardingV2 implementation set delayed receipt first index to 0 at children shards.
        // ReshardingV2 split started one epoch before the new shard layout.
        // We calculate what was the number of delayed receipts that were applied to the child shard over the epoch.
        let prev_epoch_processed_delayed_receipt_count = prev_delayed_receipt_indices.first_index
            - prev_epoch_start_delayed_receipt_indices.first_index;

        let child_delayed_receipt_indices = DelayedReceiptIndices {
            first_index: prev_epoch_processed_delayed_receipt_count,
            next_available_index: prev_epoch_processed_delayed_receipt_count,
        };
        println!(
            "Child delayed receipt indices after resharding: {:?}",
            child_delayed_receipt_indices
        );
        let mut cold_store_update = cold_store.trie_store().store_update();
        // We use child shard uid because this is where we want the backfilling to happen.
        let new_state_root = Self::apply_delayed_receipt_indices(
            &tries,
            prev_trie,
            &child_shard_uid,
            &mut cold_store_update,
            child_delayed_receipt_indices,
        )?;
        println!("New state root: {}", new_state_root);
        // The check below gives us confidence that everything went correctly.
        assert_eq!(new_state_root, expected_new_state_root);
        cold_store_update.commit()?;
        Ok(())
    }

    fn apply_delayed_receipt_indices(
        tries: &ShardTries,
        trie: Trie,
        shard_uid: &ShardUId,
        store_update: &mut TrieStoreUpdateAdapter,
        delayed_receipt_indices: DelayedReceiptIndices,
    ) -> anyhow::Result<CryptoHash> {
        let mut trie_update = TrieUpdate::new(trie);
        set(&mut trie_update, TrieKey::DelayedReceiptIndices, &delayed_receipt_indices);
        trie_update.commit(StateChangeCause::_UnusedReshardingV2);
        let trie_changes = trie_update.finalize()?.trie_changes;
        let new_state_root = tries.apply_all(&trie_changes, *shard_uid, store_update);
        Ok(new_state_root)
    }
}

fn extract_chunk_from_block(block: &Block, shard_id: &ShardId) -> anyhow::Result<ShardChunkHeader> {
    let chunk = block
        .chunks()
        .iter()
        .find(|chunk| &chunk.shard_id() == shard_id)
        .ok_or_else(|| anyhow::anyhow!("No chunk with given shard and height"))?
        .deref()
        .clone();
    Ok(chunk)
}
