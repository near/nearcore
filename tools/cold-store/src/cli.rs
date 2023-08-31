use crate::cli::SubCommand::CheckStateRoot;
use anyhow;
use anyhow::Context;
use borsh::BorshDeserialize;
use clap;
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_primitives::block::Tip;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::hash::CryptoHash;
use near_store::cold_storage::{copy_all_data_to_cold, update_cold_db, update_cold_head};
use near_store::metadata::DbKind;
use near_store::{DBCol, NodeStorage, Store, StoreOpener};
use near_store::{COLD_HEAD_KEY, FINAL_HEAD_KEY, HEAD_KEY, TAIL_KEY};
use nearcore::NearConfig;
use rand::seq::SliceRandom;
use std::io::Result;
use std::path::Path;
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
    /// - config.store.path points to an existing database with kind Archive
    /// - config.cold_store.path points to an existing database with kind Cold
    /// - store_relative_path points to an existing database with kind Rpc
    PrepareHot(PrepareHotCmd),
    /// Traverse trie and check that every node is in cold db.
    /// Can start from given state_root or compute previous roots for every chunk in provided block
    /// and use them as starting point.
    /// You can provide maximum depth and/or maximum number of vertices to traverse for each root.
    /// Trie is traversed using DFS with randomly shuffled kids for every node.
    CheckStateRoot(CheckStateRootCmd),
}

impl ColdStoreCommand {
    pub fn run(self, home_dir: &Path) -> anyhow::Result<()> {
        let mode =
            if self.readwrite { near_store::Mode::ReadWrite } else { near_store::Mode::ReadOnly };
        let mut near_config = nearcore::config::load_config(
            &home_dir,
            near_chain_configs::GenesisValidationMode::Full,
        )
        .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        let opener = self.get_opener(home_dir, &mut near_config);

        let storage =
            opener.open_in_mode(mode).unwrap_or_else(|e| panic!("Error opening storage: {:#}", e));

        let epoch_manager =
            EpochManager::new_arc_handle(storage.get_hot_store(), &near_config.genesis.config);
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
            SubCommand::CheckStateRoot(cmd) => cmd.run(&storage),
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
            tracing::warn!("Expected archive option in config to be set to true.");
        }

        let opener = NodeStorage::opener(
            home_dir,
            near_config.config.archive,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
        );

        match self.subcmd {
            CheckStateRoot(_) => {
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
            near_config.config.archive,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
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

    // hot store
    {
        let kind = hot_store.get_db_kind()?;
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
        let kind = cold_store.get_db_kind()?;
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
    let cold_head_hash = get_ser_from_store::<CryptoHash>(
        &store.get_hot_store(),
        DBCol::BlockHeight,
        &cold_head_height.to_le_bytes(),
    )
    .unwrap_or_else(|| panic!("No block hash in hot storage for height {}", cold_head_height));

    // For copying block we need to have shard_layout.
    // For that we need epoch_id.
    // For that we might use prev_block_hash, and because next_hight = cold_head_height + 1,
    // we use cold_head_hash.
    update_cold_db(
        &*store.cold_db().unwrap(),
        &store.get_hot_store(),
        &epoch_manager
            .get_shard_layout(&epoch_manager.get_epoch_id_from_prev_block(&cold_head_hash).unwrap())
            .unwrap(),
        &next_height,
    )
    .expect(&std::format!("Failed to copy block at height {} to cold db", next_height));

    update_cold_head(&*store.cold_db().unwrap(), &store.get_hot_store(), &next_height)
        .expect(&std::format!("Failed to update cold HEAD to {}", next_height));
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
        .expect(&std::format!("Failed to update cold HEAD to {}", hot_final_head));

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
    for (key, _value) in first_store.iter(col).map(Result::unwrap) {
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
    store.get_ser(col, key).expect(&std::format!("Error reading {} {:?} from store", col, key,))
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
        tracing::info!(target : "prepare-hot", "Preparing a hot db from a rpc db at path {path:#?}.");

        tracing::info!(target : "prepare-hot", "Opening hot and cold.");
        let hot_store = storage.get_hot_store();
        let cold_store = storage.get_cold_store();
        let cold_store = cold_store.ok_or(anyhow::anyhow!("The cold store is not configured!"))?;

        tracing::info!(target : "prepare-hot", "Opening rpc.");
        // Open the rpc_storage using the near_config with the path swapped.
        let mut rpc_store_config = near_config.config.store.clone();
        rpc_store_config.path = Some(path.to_path_buf());
        let rpc_opener = NodeStorage::opener(home_dir, false, &rpc_store_config, None);
        let rpc_storage = rpc_opener.open()?;
        let rpc_store = rpc_storage.get_hot_store();

        tracing::info!(target : "prepare-hot", "Checking db kind");
        Self::check_db_kind(&hot_store, &cold_store, &rpc_store)?;

        tracing::info!(target : "prepare-hot", "Checking up to date");
        Self::check_up_to_date(&cold_store, &rpc_store)?;

        // TODO may be worth doing some simple sanity check that the rpc store
        // and the cold store contain the same chain. Keep in mind that the
        // responsibility of ensuring that the rpc backupd can be trusted lies
        // with the node owner still. We don't want to do a full check here
        // as it would take too long.

        tracing::info!(target : "prepare-hot", "The hot, cold and RPC stores are suitable for cold storage migration");
        tracing::info!(target : "prepare-hot", "Changing the DbKind of the RPC store to Hot");
        rpc_store.set_db_kind(DbKind::Hot)?;

        tracing::info!(target : "prepare-hot", "Successfully prepared the hot store for migration. You can now set the config.store.path in neard config to {:#?}", path);

        Ok(())
    }

    /// Check that the DbKind of each of the stores is as expected.
    fn check_db_kind(
        hot_store: &Store,
        cold_store: &Store,
        rpc_store: &Store,
    ) -> anyhow::Result<()> {
        let hot_db_kind = hot_store.get_db_kind()?;
        if hot_db_kind != Some(DbKind::Archive) {
            return Err(anyhow::anyhow!(
                "Unexpected hot_store DbKind, expected: {:?}, got: {:?}",
                DbKind::Archive,
                hot_db_kind,
            ));
        }

        let cold_db_kind = cold_store.get_db_kind()?;
        if cold_db_kind != Some(DbKind::Cold) {
            return Err(anyhow::anyhow!(
                "Unexpected cold_store DbKind, expected: {:?}, got: {:?}",
                DbKind::Cold,
                cold_db_kind,
            ));
        }

        let rpc_db_kind = rpc_store.get_db_kind()?;
        if rpc_db_kind != Some(DbKind::RPC) {
            return Err(anyhow::anyhow!(
                "Unexpected rpc_store DbKind, expected: {:?}, got: {:?}",
                DbKind::RPC,
                rpc_db_kind,
            ));
        }

        Ok(())
    }

    /// Check that the cold store and rpc store are sufficiently up to date.
    fn check_up_to_date(cold_store: &Store, rpc_store: &Store) -> anyhow::Result<()> {
        let rpc_head = rpc_store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?;
        let rpc_head = rpc_head.ok_or(anyhow::anyhow!("The rpc head is missing!"))?;
        let rpc_tail = rpc_store.get_ser::<u64>(DBCol::BlockMisc, TAIL_KEY)?;
        let rpc_tail = rpc_tail.ok_or(anyhow::anyhow!("The rpc tail is missing!"))?;
        let cold_head = cold_store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?;
        let cold_head = cold_head.ok_or(anyhow::anyhow!("The cold head is missing"))?;

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
        let cold_head_block_info =
            cold_head_block_info.ok_or(anyhow::anyhow!("Cold head block info is not in rpc db"))?;
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
                "The cold head is ahead of the RPC head. This should fix itself when the node catches up and becomes in sync"
            );
        }

        Ok(())
    }
}

/// The StateRootSelector is a subcommand that allows the user to select the state root either by block height or by the state root hash.
#[derive(clap::Subcommand)]
enum StateRootSelector {
    Height { height: near_primitives::types::BlockHeight },
    Hash { hash: CryptoHash },
}

impl StateRootSelector {
    pub fn get_hashes(
        &self,
        storage: &NodeStorage,
        cold_store: &Store,
    ) -> anyhow::Result<Vec<CryptoHash>> {
        match self {
            // If height is provided, calculate previous state roots for this block's chunks.
            StateRootSelector::Height { height } => {
                let hash_key = {
                    let height_key = height.to_le_bytes();
                    storage
                        .get_hot_store()
                        .get(DBCol::BlockHeight, &height_key)?
                        .ok_or(anyhow::anyhow!(
                            "Failed to find block hash for height {:?}",
                            height
                        ))?
                        .as_slice()
                        .to_vec()
                };
                let block = cold_store
                    .get_ser::<near_primitives::block::Block>(DBCol::Block, &hash_key)?
                    .ok_or(anyhow::anyhow!("Failed to find Block: {:?}", hash_key))?;
                let mut hashes = vec![];
                for chunk in block.chunks().iter() {
                    hashes.push(
                        cold_store
                            .get_ser::<near_primitives::sharding::ShardChunk>(
                                DBCol::Chunks,
                                chunk.chunk_hash().as_bytes(),
                            )?
                            .ok_or(anyhow::anyhow!(
                                "Failed to find Chunk: {:?}",
                                chunk.chunk_hash()
                            ))?
                            .take_header()
                            .prev_state_root(),
                    );
                }
                Ok(hashes)
            }
            // If state root is provided, then just use it.
            StateRootSelector::Hash { hash } => Ok(vec![*hash]),
        }
    }
}

/// Struct that holds all conditions for node in Trie
/// to be checked by CheckStateRootCmd::check_trie.
#[derive(Debug)]
struct PruneCondition {
    /// Maximum depth (measured in number of nodes, not trie key length).
    max_depth: Option<u64>,
    /// Maximum number of nodes checked for each state_root.
    max_count: Option<u64>,
}

/// Struct that holds data related to pruning of node in CheckStateRootCmd::check_trie.
#[derive(Debug)]
struct PruneState {
    /// Depth of node in trie (measured in number of nodes, not trie key length).
    depth: u64,
    /// Number of already checked nodes.
    count: u64,
}

impl PruneState {
    pub fn new() -> Self {
        Self { depth: 0, count: 0 }
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
    pub fn down(&mut self) {
        self.count += 1;
        self.depth += 1;
    }

    /// Modify self to reflect going up a tree.
    /// We do not change node count, because we already visited parent node before.
    pub fn up(&mut self) {
        self.depth -= 1;
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
}

impl CheckStateRootCmd {
    pub fn run(self, storage: &NodeStorage) -> anyhow::Result<()> {
        let cold_store =
            storage.get_cold_store().ok_or(anyhow::anyhow!("Cold storage is not configured"))?;

        let hashes = self.state_root_selector.get_hashes(storage, &cold_store)?;
        for hash in hashes.iter() {
            Self::check_trie(
                &cold_store,
                &hash,
                &mut PruneState::new(),
                &PruneCondition { max_depth: self.max_depth, max_count: self.max_count },
            )?;
        }

        Ok(())
    }

    /// Check that trie subtree of `hash` is fully present in `store`.
    fn check_trie(
        store: &Store,
        hash: &CryptoHash,
        prune_state: &mut PruneState,
        prune_condition: &PruneCondition,
    ) -> anyhow::Result<()> {
        tracing::debug!(target: "check_trie", "Checking {:?} at {:?}", hash, prune_state);
        if prune_state.should_prune(prune_condition) {
            tracing::debug!(target: "check_trie", "Reached prune condition: {:?}", prune_condition);
            return Ok(());
        }

        let bytes = Self::read_state(store, hash.as_ref())
            .with_context(|| format!("Failed to read raw bytes for hash {:?}", hash))?
            .with_context(|| format!("Failed to find raw bytes for hash {:?}", hash))?;
        let node = near_store::RawTrieNodeWithSize::try_from_slice(&bytes)?;
        match node.node {
            near_store::RawTrieNode::Leaf(..) => {
                tracing::debug!(target: "check_trie", "Reached leaf node");
                return Ok(());
            }
            near_store::RawTrieNode::BranchNoValue(mut children)
            | near_store::RawTrieNode::BranchWithValue(_, mut children) => {
                children.0.shuffle(&mut rand::thread_rng());
                for (_, child) in children.iter() {
                    // Record in prune state that we are visiting a child node
                    prune_state.down();
                    // Visit a child node
                    Self::check_trie(store, child, prune_state, prune_condition)?;
                    // Record in prune state that we are returning from a child node
                    prune_state.up();
                }
            }
            near_store::RawTrieNode::Extension(_, child) => {
                // Record in prune state that we are visiting a child node
                prune_state.down();
                // Visit a child node
                Self::check_trie(store, &child, prune_state, prune_condition)?;
                // Record in prune state that we are returning from a child node
                prune_state.up();
            }
        }
        Ok(())
    }

    fn read_state<'a>(
        store: &'a Store,
        trie_key: &'a [u8],
    ) -> std::io::Result<Option<near_store::db::DBSlice<'a>>> {
        // As cold db strips shard_uid at the beginning of State key, we can add any 8 u8s as prefix.
        let cold_state_key = [&[1; 8], trie_key.as_ref()].concat();
        store.get(DBCol::State, &cold_state_key)
    }
}
