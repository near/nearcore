use crate::commands::*;
use crate::dump_state_parts::dump_state_parts;
use crate::rocksdb_stats::get_rocksdb_stats;
use crate::{dump_state_parts, epoch_info};
use clap::{Args, Parser, Subcommand};
use near_chain_configs::{GenesisChangeConfig, GenesisValidationMode};
use near_primitives::account::id::AccountId;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::{Mode, Store};
use nearcore::{load_config, NearConfig};
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Subcommand)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
pub enum StateViewerSubCommand {
    Peers,
    State,
    /// Generate a genesis file from the current state of the DB.
    #[clap(alias = "dump_state")]
    DumpState(DumpStateCmd),
    #[clap(alias = "dump_state_redis")]
    DumpStateRedis(DumpStateRedisCmd),
    /// Generate a file that contains all transactions from a block.
    #[clap(alias = "dump_tx")]
    DumpTx(DumpTxCmd),
    /// Print chain from start_index to end_index.
    Chain(ChainCmd),
    /// Replay headers from chain.
    Replay(ReplayCmd),
    /// Apply blocks at a range of heights for a single shard.
    #[clap(alias = "apply_range")]
    ApplyRange(ApplyRangeCmd),
    /// Apply block at some height for shard.
    Apply(ApplyCmd),
    /// View head of the storage.
    #[clap(alias = "view_chain")]
    ViewChain(ViewChainCmd),
    /// Check whether the node has all the blocks up to its head.
    #[clap(alias = "check_block")]
    CheckBlock,
    /// Dump deployed contract code of given account to wasm file.
    #[clap(alias = "dump_code")]
    DumpCode(DumpCodeCmd),
    /// Dump contract data in storage of given account to binary file.
    #[clap(alias = "dump_account_storage")]
    DumpAccountStorage(DumpAccountStorageCmd),
    /// Print `EpochInfo` of an epoch given by `--epoch_id` or by `--epoch_height`.
    #[clap(alias = "epoch_info")]
    EpochInfo(EpochInfoCmd),
    /// Dump stats for the RocksDB storage.
    #[clap(name = "rocksdb-stats", alias = "rocksdb_stats")]
    RocksDBStats(RocksDBStatsCmd),
    Receipts(ReceiptsCmd),
    Chunks(ChunksCmd),
    #[clap(alias = "partial_chunks")]
    PartialChunks(PartialChunksCmd),
    /// Apply a chunk, even if it's not included in any block on disk
    #[clap(alias = "apply_chunk")]
    ApplyChunk(ApplyChunkCmd),
    /// Apply a transaction if it occurs in some chunk we know about,
    /// even if it's not included in any block on disk
    #[clap(alias = "apply_tx")]
    ApplyTx(ApplyTxCmd),
    /// Apply a receipt if it occurs in some chunk we know about,
    /// even if it's not included in any block on disk
    #[clap(alias = "apply_receipt")]
    ApplyReceipt(ApplyReceiptCmd),
    /// View trie structure.
    #[clap(alias = "view_trie")]
    ViewTrie(ViewTrieCmd),
    /// Dump all or a single state part of a shard.
    DumpStateParts(DumpStatePartsCmd),
}

impl StateViewerSubCommand {
    pub fn run(self, home_dir: &Path, genesis_validation: GenesisValidationMode, mode: Mode) {
        let near_config = load_config(home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
        let store_opener =
            near_store::NodeStorage::opener(home_dir, &near_config.config.store, None);
        let store = store_opener.open_in_mode(mode).unwrap();
        let hot = store.get_store(near_store::Temperature::Hot);
        match self {
            StateViewerSubCommand::Peers => peers(store),
            StateViewerSubCommand::State => state(home_dir, near_config, hot),
            StateViewerSubCommand::DumpState(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::DumpStateParts(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::DumpStateRedis(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::DumpTx(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::Chain(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::Replay(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::ApplyRange(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::Apply(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::ViewChain(cmd) => cmd.run(near_config, hot),
            StateViewerSubCommand::CheckBlock => check_block_chunk_existence(near_config, hot),
            StateViewerSubCommand::DumpCode(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::DumpAccountStorage(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::EpochInfo(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::RocksDBStats(cmd) => cmd.run(store_opener.path()),
            StateViewerSubCommand::Receipts(cmd) => cmd.run(near_config, hot),
            StateViewerSubCommand::Chunks(cmd) => cmd.run(near_config, hot),
            StateViewerSubCommand::PartialChunks(cmd) => cmd.run(near_config, hot),
            StateViewerSubCommand::ApplyChunk(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::ApplyTx(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::ApplyReceipt(cmd) => cmd.run(home_dir, near_config, hot),
            StateViewerSubCommand::ViewTrie(cmd) => cmd.run(hot),
        }
    }
}

#[derive(Parser)]
pub struct DumpStateCmd {
    /// Optionally, can specify at which height to dump state.
    #[clap(long)]
    height: Option<BlockHeight>,
    /// Dumps state records and genesis config into separate files.
    /// Has reasonable RAM requirements.
    /// Use for chains with large state, such as mainnet and testnet.
    /// If false - writes all information into a single file, which is useful for smaller networks,
    /// such as betanet.
    #[clap(long)]
    stream: bool,
    /// Location of the dumped state.
    /// This is a directory if --stream is set, and a file otherwise.
    #[clap(long, parse(from_os_str))]
    file: Option<PathBuf>,
    /// List of account IDs to dump.
    /// Note: validators will always be dumped.
    /// If not set, all account IDs will be dumped.
    #[clap(long)]
    account_ids: Option<Vec<AccountId>>,
    /// List of validators to remain validators.
    /// All other validators will be kicked, but still dumped.
    /// Their stake will be returned to balance.
    #[clap(long)]
    include_validators: Option<Vec<AccountId>>,
}

impl DumpStateCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        dump_state(
            self.height,
            self.stream,
            self.file,
            home_dir,
            near_config,
            store,
            &GenesisChangeConfig::default()
                .with_select_account_ids(self.account_ids)
                .with_whitelist_validators(self.include_validators),
        );
    }
}

#[derive(Parser)]
pub struct DumpStateRedisCmd {
    /// Optionally, can specify at which height to dump state.
    #[clap(long)]
    height: Option<BlockHeight>,
}

impl DumpStateRedisCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        dump_state_redis(self.height, home_dir, near_config, store);
    }
}

#[derive(Parser)]
pub struct DumpTxCmd {
    /// Specify the start block by height to begin dumping transactions from, inclusive.
    #[clap(long)]
    start_height: BlockHeight,
    /// Specify the end block by height to stop dumping transactions at, inclusive.
    #[clap(long)]
    end_height: BlockHeight,
    /// List of account IDs to dump.
    /// If not set, all account IDs will be dumped.
    #[clap(long)]
    account_ids: Option<Vec<AccountId>>,
    /// Optionally, can specify the path of the output.
    #[clap(long)]
    output_path: Option<String>,
}

impl DumpTxCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        dump_tx(
            self.start_height,
            self.end_height,
            home_dir,
            near_config,
            store,
            self.account_ids.as_deref(),
            self.output_path,
        )
        .expect("Failed to dump transaction...")
    }
}

#[derive(Parser)]
pub struct ChainCmd {
    #[clap(long)]
    start_index: BlockHeight,
    #[clap(long)]
    end_index: BlockHeight,
    // If true, show the full hash (block hash and chunk hash) when printing.
    // If false, show only first couple chars.
    #[clap(long)]
    show_full_hashes: bool,
}

impl ChainCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        print_chain(
            self.start_index,
            self.end_index,
            home_dir,
            near_config,
            store,
            self.show_full_hashes,
        );
    }
}

#[derive(Parser)]
pub struct ReplayCmd {
    #[clap(long)]
    start_index: BlockHeight,
    #[clap(long)]
    end_index: BlockHeight,
}

impl ReplayCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        replay_chain(self.start_index, self.end_index, home_dir, near_config, store);
    }
}

#[derive(Parser)]
pub struct ApplyRangeCmd {
    #[clap(long)]
    start_index: Option<BlockHeight>,
    #[clap(long)]
    end_index: Option<BlockHeight>,
    #[clap(long, default_value = "0")]
    shard_id: ShardId,
    #[clap(long)]
    verbose_output: bool,
    #[clap(long, parse(from_os_str))]
    csv_file: Option<PathBuf>,
    #[clap(long)]
    only_contracts: bool,
    #[clap(long)]
    sequential: bool,
}

impl ApplyRangeCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        apply_range(
            self.start_index,
            self.end_index,
            self.shard_id,
            self.verbose_output,
            self.csv_file,
            home_dir,
            near_config,
            store,
            self.only_contracts,
            self.sequential,
        );
    }
}

#[derive(Parser)]
pub struct ApplyCmd {
    #[clap(long)]
    height: BlockHeight,
    #[clap(long)]
    shard_id: ShardId,
}

impl ApplyCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        apply_block_at_height(self.height, self.shard_id, home_dir, near_config, store);
    }
}

#[derive(Parser)]
pub struct ViewChainCmd {
    #[clap(long)]
    height: Option<BlockHeight>,
    #[clap(long)]
    block: bool,
    #[clap(long)]
    chunk: bool,
}

impl ViewChainCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        view_chain(self.height, self.block, self.chunk, near_config, store);
    }
}

#[derive(Parser)]
pub struct DumpCodeCmd {
    #[clap(long)]
    account_id: String,
    #[clap(long, parse(from_os_str))]
    output: PathBuf,
}

impl DumpCodeCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        dump_code(self.account_id, &self.output, home_dir, near_config, store);
    }
}

#[derive(Parser)]
pub struct DumpAccountStorageCmd {
    #[clap(long)]
    account_id: String,
    #[clap(long)]
    storage_key: String,
    #[clap(long, parse(from_os_str))]
    output: PathBuf,
    #[clap(long)]
    block_height: String,
}

impl DumpAccountStorageCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        dump_account_storage(
            self.account_id,
            self.storage_key,
            &self.output,
            self.block_height,
            home_dir,
            near_config,
            store,
        );
    }
}
#[derive(Args)]
pub struct EpochInfoCmd {
    #[clap(subcommand)]
    epoch_selection: epoch_info::EpochSelection,
    /// Displays kickouts of the given validator and expected and missed blocks and chunks produced.
    #[clap(long)]
    validator_account_id: Option<String>,
}

impl EpochInfoCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        print_epoch_info(
            self.epoch_selection,
            self.validator_account_id.map(|s| AccountId::from_str(&s).unwrap()),
            home_dir,
            near_config,
            store,
        );
    }
}

#[derive(Parser)]
pub struct RocksDBStatsCmd {
    /// Location of the dumped Rocks DB stats.
    #[clap(long, parse(from_os_str))]
    file: Option<PathBuf>,
}

impl RocksDBStatsCmd {
    pub fn run(self, store_dir: &Path) {
        get_rocksdb_stats(store_dir, self.file).expect("Couldn't get RocksDB stats");
    }
}

#[derive(Parser)]
pub struct ReceiptsCmd {
    #[clap(long)]
    receipt_id: String,
}

impl ReceiptsCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        get_receipt(CryptoHash::from_str(&self.receipt_id).unwrap(), near_config, store)
    }
}

#[derive(Parser)]
pub struct ChunksCmd {
    #[clap(long)]
    chunk_hash: String,
}

impl ChunksCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        let chunk_hash = ChunkHash::from(CryptoHash::from_str(&self.chunk_hash).unwrap());
        get_chunk(chunk_hash, near_config, store)
    }
}
#[derive(Parser)]
pub struct PartialChunksCmd {
    #[clap(long)]
    partial_chunk_hash: String,
}

impl PartialChunksCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        let partial_chunk_hash =
            ChunkHash::from(CryptoHash::from_str(&self.partial_chunk_hash).unwrap());
        get_partial_chunk(partial_chunk_hash, near_config, store)
    }
}

#[derive(Parser)]
pub struct ApplyChunkCmd {
    #[clap(long)]
    chunk_hash: String,
    #[clap(long)]
    target_height: Option<u64>,
}

impl ApplyChunkCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        let hash = ChunkHash::from(CryptoHash::from_str(&self.chunk_hash).unwrap());
        apply_chunk(home_dir, near_config, store, hash, self.target_height).unwrap()
    }
}

#[derive(Parser)]
pub struct ApplyTxCmd {
    #[clap(long)]
    hash: String,
}

impl ApplyTxCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        let hash = CryptoHash::from_str(&self.hash).unwrap();
        apply_tx(home_dir, near_config, store, hash).unwrap();
    }
}

#[derive(Parser)]
pub struct ApplyReceiptCmd {
    #[clap(long)]
    hash: String,
}

impl ApplyReceiptCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        let hash = CryptoHash::from_str(&self.hash).unwrap();
        apply_receipt(home_dir, near_config, store, hash).unwrap();
    }
}

#[derive(Parser)]
pub struct ViewTrieCmd {
    #[clap(long)]
    hash: String,
    #[clap(long)]
    shard_id: u32,
    #[clap(long)]
    shard_version: u32,
    #[clap(long)]
    max_depth: u32,
}

impl ViewTrieCmd {
    pub fn run(self, store: Store) {
        let hash = CryptoHash::from_str(&self.hash).unwrap();
        view_trie(store, hash, self.shard_id, self.shard_version, self.max_depth).unwrap();
    }
}

#[derive(Parser)]
pub struct DumpStatePartsCmd {
    /// Selects an epoch. The dump will be of the state at the beginning of this epoch.
    #[clap(subcommand)]
    epoch_selection: dump_state_parts::EpochSelection,
    /// Shard id.
    #[clap(long)]
    shard_id: ShardId,
    /// State part id. Leave empty to go through every part in the shard.
    #[clap(long)]
    part_id: Option<u64>,
    /// Where to write the state parts to.
    #[clap(long)]
    output_dir: PathBuf,
}

impl DumpStatePartsCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        dump_state_parts(
            self.epoch_selection,
            self.shard_id,
            self.part_id,
            home_dir,
            near_config,
            store,
            &self.output_dir,
        );
    }
}
