use crate::commands::*;
use crate::contract_accounts::ContractAccountFilter;
use crate::rocksdb_stats::get_rocksdb_stats;
use near_chain_configs::{GenesisChangeConfig, GenesisValidationMode};
use near_primitives::account::id::AccountId;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::{Mode, NodeStorage, Store, Temperature};
use nearcore::{load_config, NearConfig};
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(clap::Subcommand)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
pub enum StateViewerSubCommand {
    /// Apply block at some height for shard.
    Apply(ApplyCmd),
    /// Apply a chunk, even if it's not included in any block on disk
    #[clap(alias = "apply_chunk")]
    ApplyChunk(ApplyChunkCmd),
    /// Apply blocks at a range of heights for a single shard.
    #[clap(alias = "apply_range")]
    ApplyRange(ApplyRangeCmd),
    /// Apply a receipt if it occurs in some chunk we know about,
    /// even if it's not included in any block on disk
    #[clap(alias = "apply_receipt")]
    ApplyReceipt(ApplyReceiptCmd),
    /// Apply a transaction if it occurs in some chunk we know about,
    /// even if it's not included in any block on disk
    #[clap(alias = "apply_tx")]
    ApplyTx(ApplyTxCmd),
    /// Print chain from start_index to end_index.
    Chain(ChainCmd),
    /// Check whether the node has all the blocks up to its head.
    #[clap(alias = "check_block")]
    CheckBlock,
    /// Looks up a certain chunk.
    Chunks(ChunksCmd),
    /// List account names with contracts deployed.
    #[clap(alias = "contract_accounts")]
    ContractAccounts(ContractAccountsCmd),
    /// Dump contract data in storage of given account to binary file.
    #[clap(alias = "dump_account_storage")]
    DumpAccountStorage(DumpAccountStorageCmd),
    /// Dump deployed contract code of given account to wasm file.
    #[clap(alias = "dump_code")]
    DumpCode(DumpCodeCmd),
    /// Generate a genesis file from the current state of the DB.
    #[clap(alias = "dump_state")]
    DumpState(DumpStateCmd),
    /// Writes state to a remote redis server.
    #[clap(alias = "dump_state_redis")]
    DumpStateRedis(DumpStateRedisCmd),
    /// Generate a file that contains all transactions from a block.
    #[clap(alias = "dump_tx")]
    DumpTx(DumpTxCmd),
    /// Print `EpochInfo` of an epoch given by `--epoch_id` or by `--epoch_height`.
    #[clap(alias = "epoch_info")]
    EpochInfo(EpochInfoCmd),
    /// Looks up a certain partial chunk.
    #[clap(alias = "partial_chunks")]
    PartialChunks(PartialChunksCmd),
    /// Looks up a certain receipt.
    Receipts(ReceiptsCmd),
    /// Replay headers from chain.
    Replay(ReplayCmd),
    /// Dump stats for the RocksDB storage.
    #[clap(name = "rocksdb-stats", alias = "rocksdb_stats")]
    RocksDBStats(RocksDBStatsCmd),
    /// Iterates over a trie and prints the StateRecords.
    State,
    /// Dumps or applies StateChanges.
    /// Experimental tool for shard shadowing development.
    StateChanges(StateChangesCmd),
    /// Dump or apply state parts.
    StateParts(StatePartsCmd),
    /// View head of the storage.
    #[clap(alias = "view_chain")]
    ViewChain(ViewChainCmd),
    /// View trie structure.
    #[clap(alias = "view_trie")]
    ViewTrie(ViewTrieCmd),
}

impl StateViewerSubCommand {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
        mode: Mode,
        temperature: Temperature,
    ) {
        let near_config = load_config(home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        let cold_config: Option<&near_store::StoreConfig> = near_config.config.cold_store.as_ref();
        let store_opener = NodeStorage::opener(
            home_dir,
            near_config.config.archive,
            &near_config.config.store,
            cold_config,
        );

        let storage = store_opener.open_in_mode(mode).unwrap();
        let store = match temperature {
            Temperature::Hot => storage.get_hot_store(),
            // Cold store on it's own is useless in majority of subcommands
            Temperature::Cold => storage.get_split_store().unwrap(),
        };

        match self {
            StateViewerSubCommand::Apply(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::ApplyChunk(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::ApplyRange(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::ApplyReceipt(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::ApplyTx(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::Chain(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::CheckBlock => check_block_chunk_existence(near_config, store),
            StateViewerSubCommand::Chunks(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::ContractAccounts(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::DumpAccountStorage(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::DumpCode(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::DumpState(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::DumpStateRedis(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::DumpTx(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::EpochInfo(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::PartialChunks(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::Receipts(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::Replay(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::RocksDBStats(cmd) => cmd.run(store_opener.path()),
            StateViewerSubCommand::State => state(home_dir, near_config, store),
            StateViewerSubCommand::StateChanges(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::StateParts(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::ViewChain(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::ViewTrie(cmd) => cmd.run(store),
        }
    }
}

#[derive(clap::Parser)]
pub struct ApplyCmd {
    #[clap(long)]
    height: BlockHeight,
    #[clap(long)]
    shard_id: ShardId,
}

impl ApplyCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        apply_block_at_height(self.height, self.shard_id, home_dir, near_config, store).unwrap();
    }
}

#[derive(clap::Parser)]
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

#[derive(clap::Parser)]
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

#[derive(clap::Parser)]
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

#[derive(clap::Parser)]
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

#[derive(clap::Parser)]
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

#[derive(clap::Parser)]
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

#[derive(clap::Parser)]
pub struct ContractAccountsCmd {
    #[clap(flatten)]
    filter: ContractAccountFilter,
}

impl ContractAccountsCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        contract_accounts(home_dir, store, near_config, self.filter).unwrap();
    }
}

#[derive(clap::Parser)]
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

#[derive(clap::Parser)]
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

#[derive(clap::Parser)]
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

#[derive(clap::Parser)]
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

#[derive(clap::Parser)]
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

#[derive(clap::Args)]
pub struct EpochInfoCmd {
    #[clap(subcommand)]
    epoch_selection: crate::epoch_info::EpochSelection,
    /// Displays kickouts of the given validator and expected and missed blocks and chunks produced.
    #[clap(long)]
    validator_account_id: Option<String>,
}

impl EpochInfoCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        print_epoch_info(
            self.epoch_selection,
            self.validator_account_id.map(|s| AccountId::from_str(&s).unwrap()),
            near_config,
            store,
        );
    }
}

#[derive(clap::Parser)]
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

#[derive(clap::Parser)]
pub struct ReceiptsCmd {
    #[clap(long)]
    receipt_id: String,
}

impl ReceiptsCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        get_receipt(CryptoHash::from_str(&self.receipt_id).unwrap(), near_config, store)
    }
}

#[derive(clap::Parser)]
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

#[derive(clap::Parser)]
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

#[derive(clap::Parser)]
pub struct StateChangesCmd {
    #[clap(subcommand)]
    command: crate::state_changes::StateChangesSubCommand,
}

impl StateChangesCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        self.command.run(home_dir, near_config, store)
    }
}

#[derive(clap::Parser)]
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

pub enum ViewTrieFormat {
    Full,
    Pretty,
}

impl std::str::FromStr for ViewTrieFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let s = s.to_lowercase();
        match s.as_str() {
            "full" => Ok(ViewTrieFormat::Full),
            "pretty" => Ok(ViewTrieFormat::Pretty),
            _ => Err(format!("invalid view trie format string {s}")),
        }
    }
}

#[derive(clap::Parser)]
pub struct ViewTrieCmd {
    /// The format of the output. This can be either `full` or `pretty`.
    /// The full format will print all the trie nodes and can be rooted anywhere in the trie.
    /// The pretty format will only print leaf nodes and must be rooted in the state root but is more human friendly.
    #[clap(long, default_value = "pretty")]
    format: ViewTrieFormat,
    /// The hash of the trie node.
    /// For format=full this can be any node in the trie.
    /// For format=pretty this must the state root node.
    /// You can find the state root hash using the `view-state view-chain` command.
    #[clap(long)]
    hash: String,
    /// The id of the shard, a number between [0-NUM_SHARDS). When looking for particular
    /// account you will need to know on which shard it's located.
    #[clap(long)]
    shard_id: u32,
    /// The current shard version based on the shard layout.
    /// You can find the shard version by using the `view-state view-chain` command.
    /// It's typically equal to 0 for single shard localnet or the most recent near_primitives::shard_layout::ShardLayout for prod.
    #[clap(long)]
    shard_version: u32,
    /// The max depth of trie iteration. It's recommended to keep that value small,
    /// otherwise the output may be really large.
    /// For format=full this measures depth in terms of number of trie nodes.
    /// For format=pretty this measures depth in terms of key nibbles.
    #[clap(long)]
    max_depth: u32,
}

impl ViewTrieCmd {
    pub fn run(self, store: Store) {
        let hash = CryptoHash::from_str(&self.hash).unwrap();

        match self.format {
            ViewTrieFormat::Full => {
                view_trie(store, hash, self.shard_id, self.shard_version, self.max_depth).unwrap();
            }
            ViewTrieFormat::Pretty => {
                view_trie_leaves(store, hash, self.shard_id, self.shard_version, self.max_depth)
                    .unwrap();
            }
        }
    }
}

#[derive(clap::Parser)]
pub struct StatePartsCmd {
    /// Shard id.
    #[clap(long)]
    shard_id: ShardId,
    /// Location of serialized state parts.
    #[clap(long)]
    root_dir: Option<PathBuf>,
    /// Store state parts in an S3 bucket.
    #[clap(long)]
    s3_bucket: Option<String>,
    /// Store state parts in an S3 bucket.
    #[clap(long)]
    s3_region: Option<String>,
    /// Dump or Apply state parts.
    #[clap(subcommand)]
    command: crate::state_parts::StatePartsSubCommand,
}

impl StatePartsCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        self.command.run(
            self.shard_id,
            self.root_dir,
            self.s3_bucket,
            self.s3_region,
            home_dir,
            near_config,
            store,
        );
    }
}
