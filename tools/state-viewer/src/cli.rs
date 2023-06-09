use crate::commands::*;
use crate::contract_accounts::ContractAccountFilter;
use crate::rocksdb_stats::get_rocksdb_stats;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::{GenesisChangeConfig, GenesisValidationMode};
use near_epoch_manager::EpochManager;
use near_primitives::account::id::AccountId;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::state_record::{state_record_to_account_id, StateRecord};
use near_primitives::trie_key::col;
use near_primitives::trie_key::trie_key_parsers::{
    parse_account_id_from_access_key_key, parse_account_id_from_trie_key_with_separator,
};
use near_primitives::types::{BlockHeight, ShardId};
use near_store::{Mode, NodeStorage, ShardUId, Store, Temperature, Trie, TrieDBStorage};
use nearcore::{load_config, NearConfig};
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;
use std::time::Instant;

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
    /// Benchmark how long does it take to iterate the trie.
    TrieIterationBenchmark(TrieIterationBenchmarkCmd),
    /// View head of the storage.
    #[clap(alias = "view_chain")]
    ViewChain(ViewChainCmd),
    /// View trie structure.
    #[clap(alias = "view_trie")]
    ViewTrie(ViewTrieCmd),
    /// Clear recoverable data in CachedContractCode column.
    #[clap(alias = "clear_cache")]
    ClearCache,
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
            StateViewerSubCommand::Chain(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::CheckBlock => check_block_chunk_existence(near_config, store),
            StateViewerSubCommand::Chunks(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::ClearCache => clear_cache(store),
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
            StateViewerSubCommand::TrieIterationBenchmark(cmd) => {
                cmd.run(home_dir, near_config, store)
            }
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
    #[clap(long, value_parser)]
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
    pub fn run(self, near_config: NearConfig, store: Store) {
        print_chain(self.start_index, self.end_index, near_config, store, self.show_full_hashes);
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
    #[clap(long, value_parser)]
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
    #[clap(long, value_parser)]
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
    #[clap(long, value_parser)]
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
    /// Which EpochInfos to process.
    #[clap(subcommand)]
    epoch_selection: crate::epoch_info::EpochSelection,
    /// Displays kickouts of the given validator and expected and missed blocks and chunks produced.
    #[clap(long)]
    validator_account_id: Option<String>,
    /// Show only information about kickouts.
    #[clap(long)]
    kickouts_summary: bool,
}

impl EpochInfoCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        print_epoch_info(
            self.epoch_selection,
            self.validator_account_id.map(|s| AccountId::from_str(&s).unwrap()),
            self.kickouts_summary,
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
    #[clap(long, value_parser)]
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

#[derive(Clone)]
pub enum ViewTrieFormat {
    Full,
    Pretty,
}

impl clap::ValueEnum for ViewTrieFormat {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Full, Self::Pretty]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        match self {
            Self::Full => Some(clap::builder::PossibleValue::new("full")),
            Self::Pretty => Some(clap::builder::PossibleValue::new("pretty")),
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

#[derive(Clone)]
pub enum TrieIterationType {
    Full,
    Shallow,
}

impl clap::ValueEnum for TrieIterationType {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Full, Self::Shallow]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        match self {
            Self::Full => Some(clap::builder::PossibleValue::new("full")),
            Self::Shallow => Some(clap::builder::PossibleValue::new("shallow")),
        }
    }
}

#[derive(Default)]
struct ColumnCountMap(HashMap<u8, usize>);

impl ColumnCountMap {
    fn col_to_string(col: u8) -> &'static str {
        match col {
            col::ACCOUNT => "ACCOUNT",
            col::CONTRACT_CODE => "CONTRACT_CODE",
            col::DELAYED_RECEIPT => "DELAYED_RECEIPT",
            col::DELAYED_RECEIPT_INDICES => "DELAYED_RECEIPT_INDICES",
            col::ACCESS_KEY => "ACCESS KEY",
            col::CONTRACT_DATA => "CONTRACT DATA",
            col::RECEIVED_DATA => "RECEIVED DATA",
            col::POSTPONED_RECEIPT_ID => "POSTPONED RECEIPT ID",
            col::PENDING_DATA_COUNT => "PENDING DATA COUNT",
            col::POSTPONED_RECEIPT => "POSTPONED RECEIPT",
            _ => unreachable!(),
        }
    }
}

impl std::fmt::Debug for ColumnCountMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        for (col, count) in &self.0 {
            map.entry(&Self::col_to_string(*col), &count);
        }
        map.finish()
    }
}

#[derive(Debug)]
pub struct TrieIterationBenchmarkStats {
    visited_map: ColumnCountMap,
    pruned_map: ColumnCountMap,
}

impl TrieIterationBenchmarkStats {
    pub fn new() -> Self {
        Self { visited_map: ColumnCountMap::default(), pruned_map: ColumnCountMap::default() }
    }

    pub fn bump_visited(&mut self, col: u8) {
        let entry = self.visited_map.0.entry(col).or_insert(0);
        *entry += 1;
    }

    pub fn bump_pruned(&mut self, col: u8) {
        let entry = self.pruned_map.0.entry(col).or_insert(0);
        *entry += 1;
    }
}

#[derive(clap::Parser)]
pub struct TrieIterationBenchmarkCmd {
    /// The type of trie iteration.
    /// - Full will iterate over all trie keys.
    /// - Shallow will only iterate until full account id prefix can be parsed
    ///   in the trie key. Most notably this will skip any keys or data
    ///   belonging to accounts.
    #[clap(long, default_value = "full")]
    iteration_type: TrieIterationType,

    /// Limit the number of trie nodes to be iterated.
    #[clap(long)]
    limit: Option<u32>,

    /// Print the trie nodes to stdout.
    #[clap(long, default_value = "false")]
    print: bool,
}

impl TrieIterationBenchmarkCmd {
    pub fn run(self, _home_dir: &Path, near_config: NearConfig, store: Store) {
        let genesis_config = &near_config.genesis.config;
        let chain_store = ChainStore::new(
            store.clone(),
            genesis_config.genesis_height,
            near_config.client_config.save_trie_changes,
        );
        let head = chain_store.head().unwrap();
        let block = chain_store.get_block(&head.last_block_hash).unwrap();
        let epoch_manager =
            EpochManager::new_from_genesis_config(store.clone(), &genesis_config).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();

        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            if chunk_header.height_included() != block.header().height() {
                println!("chunk for shard {shard_id} is missing and will be skipped");
            }
        }

        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if chunk_header.height_included() != block.header().height() {
                println!("chunk for shard {shard_id} is missing, skipping it");
                continue;
            }
            let trie = self.get_trie(shard_id, &shard_layout, &chunk_header, &store);

            println!("shard id    {shard_id:#?} benchmark starting");
            self.iter_trie(&trie);
            println!("shard id    {shard_id:#?} benchmark finished");
        }
    }

    fn get_trie(
        &self,
        shard_id: ShardId,
        shard_layout: &near_primitives::shard_layout::ShardLayout,
        chunk_header: &near_primitives::sharding::ShardChunkHeader,
        store: &Store,
    ) -> Trie {
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, shard_layout);
        // Note: here we get the previous state root but the shard layout
        // corresponds to the current epoch id. In practice shouldn't
        // matter as the shard layout doesn't change.
        let state_root = chunk_header.prev_state_root();
        let storage = TrieDBStorage::new(store.clone(), shard_uid);
        let flat_storage_chunk_view = None;
        Trie::new(Rc::new(storage), state_root, flat_storage_chunk_view)
    }

    fn iter_trie(&self, trie: &Trie) {
        let stats = Rc::new(RefCell::new(TrieIterationBenchmarkStats::new()));
        let stats_clone = Rc::clone(&stats);

        let prune_condition: Option<Box<dyn Fn(&Vec<u8>) -> bool>> = match &self.iteration_type {
            TrieIterationType::Full => None,
            TrieIterationType::Shallow => Some(Box::new(move |key_nibbles| -> bool {
                Self::shallow_iter_prune_condition(key_nibbles, &stats_clone)
            })),
        };

        let start = Instant::now();
        let mut node_count = 0;
        let mut error_count = 0;
        let iter = trie.iter_with_prune_condition(prune_condition);
        let iter = match iter {
            Ok(iter) => iter,
            Err(err) => {
                println!("iter error {err:#?}");
                return;
            }
        };
        for item in iter {
            node_count += 1;

            let (key, value) = match item {
                Ok((key, value)) => (key, value),
                Err(err) => {
                    println!("Failed to iterate node with error: {err}");
                    error_count += 1;
                    continue;
                }
            };

            stats.borrow_mut().bump_visited(key[0]);

            if self.print {
                let state_record = StateRecord::from_raw_key_value(key.clone(), value);
                Self::print_state_record(&state_record);
            }

            if let Some(limit) = self.limit {
                if limit < node_count {
                    break;
                }
            }
        }
        let duration = start.elapsed();
        println!("node count  {node_count}");
        println!("error count {error_count}");
        println!("time        {duration:?}");
        println!("stats\n{:#?}", stats.borrow());
    }

    fn shallow_iter_prune_condition(
        key_nibbles: &Vec<u8>,
        stats: &Rc<RefCell<TrieIterationBenchmarkStats>>,
    ) -> bool {
        // Need at least 2 nibbles for the column type byte.
        if key_nibbles.len() < 2 {
            return false;
        }

        // The key method will drop the last nibble if there is an odd number of
        // them. This is on purpose because the interesting keys have even length.
        let key = Self::key(key_nibbles);
        let col: u8 = key[0];
        let result = match col {
            // key for account only contains account id, nothing to prune
            col::ACCOUNT => false,
            // key for contract code only contains account id, nothing to prune
            col::CONTRACT_CODE => false,
            // key for delayed receipt only contains account id, nothing to prune
            col::DELAYED_RECEIPT => false,
            // key for delayed receipt indices is a shard singleton, nothing to prune
            col::DELAYED_RECEIPT_INDICES => false,

            // Most columns use the ACCOUNT_DATA_SEPARATOR to indicate the end
            // of the accound id in the trie key. For those columns the
            // partial_parse_account_id method should be used.
            // The only exception is the ACCESS_KEY and dedicated method
            // partial_parse_account_id_from_access_key should be used.
            col::ACCESS_KEY => Self::partial_parse_account_id_from_access_key(&key, "ACCESS KEY"),
            col::CONTRACT_DATA => Self::partial_parse_account_id(col, &key, "CONTRACT DATA"),
            col::RECEIVED_DATA => Self::partial_parse_account_id(col, &key, "RECEIVED DATA"),
            col::POSTPONED_RECEIPT_ID => {
                Self::partial_parse_account_id(col, &key, "POSTPONED RECEIPT ID")
            }
            col::PENDING_DATA_COUNT => {
                Self::partial_parse_account_id(col, &key, "PENDING DATA COUNT")
            }
            col::POSTPONED_RECEIPT => {
                Self::partial_parse_account_id(col, &key, "POSTPONED RECEIPT")
            }
            _ => unreachable!(),
        };

        if result {
            stats.borrow_mut().bump_pruned(col);
        }

        result

        // TODO - this can be optimized, we really only need to look at the last
        // byte of the key and check if it is the separator. This only works
        // when doing full iteration as seeking inside of the trie would break
        // the invariant that parent node key was already checked.
    }

    fn key(key_nibbles: &Vec<u8>) -> Vec<u8> {
        // Intentionally ignoring the odd nibble at the end.
        let mut result = <Vec<u8>>::with_capacity(key_nibbles.len() / 2);
        for i in (1..key_nibbles.len()).step_by(2) {
            result.push(key_nibbles[i - 1] * 16 + key_nibbles[i]);
        }
        result
    }

    fn partial_parse_account_id(col: u8, key: &Vec<u8>, col_name: &str) -> bool {
        match parse_account_id_from_trie_key_with_separator(col, &key, "") {
            Ok(account_id) => {
                tracing::trace!(target: "trie-iteration-benchmark", "pruning column {col_name} account id {account_id:?}");
                true
            }
            Err(_) => false,
        }
    }

    // returns true if the partial key contains full account id
    fn partial_parse_account_id_from_access_key(key: &Vec<u8>, col_name: &str) -> bool {
        match parse_account_id_from_access_key_key(&key) {
            Ok(account_id) => {
                tracing::trace!(target: "trie-iteration-benchmark", "pruning column {col_name} account id {account_id:?}");
                true
            }
            Err(_) => false,
        }
    }

    fn print_state_record(state_record: &Option<StateRecord>) {
        let state_record_string = match state_record {
            None => "none".to_string(),
            Some(state_record) => {
                format!(
                    "{} {:?}",
                    &state_record.get_type_string(),
                    state_record_to_account_id(&state_record)
                )
            }
        };
        println!("{state_record_string}");
    }
}
