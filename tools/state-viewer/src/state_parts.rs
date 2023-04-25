use crate::epoch_info::iterate_and_filter;
use borsh::BorshDeserialize;
use near_chain::{Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode};
use near_client::sync::state::{
    get_num_parts_from_filename, is_part_filename, location_prefix, part_filename, StateSync,
};
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::state_part::PartId;
use near_primitives::state_record::StateRecord;
use near_primitives::syncing::get_num_state_parts;
use near_primitives::types::{EpochId, StateRoot};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{BlockHeight, EpochHeight, ShardId};
use near_store::{PartialStorage, Store, Trie};
use nearcore::{NearConfig, NightshadeRuntime};
use s3::serde_types::ListBucketResult;
use std::fs::DirEntry;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;

#[derive(clap::Subcommand, Debug, Clone)]
pub(crate) enum StatePartsSubCommand {
    /// Apply all or a single state part of a shard.
    Apply {
        /// If true, validate the state part but don't write it to the DB.
        #[clap(long)]
        dry_run: bool,
        /// If provided, this value will be used instead of looking it up in the headers.
        /// Use if those headers or blocks are not available.
        #[clap(long)]
        state_root: Option<StateRoot>,
        /// Choose a single part id.
        /// If None - affects all state parts.
        #[clap(long)]
        part_id: Option<u64>,
        /// Select an epoch to work on.
        #[clap(subcommand)]
        epoch_selection: EpochSelection,
    },
    /// Dump all or a single state part of a shard.
    Dump {
        /// Dump part ids starting from this part.
        #[clap(long)]
        part_from: Option<u64>,
        /// Dump part ids up to this part (exclusive).
        #[clap(long)]
        part_to: Option<u64>,
        /// Select an epoch to work on.
        #[clap(subcommand)]
        epoch_selection: EpochSelection,
    },
    /// Read State Header from the DB
    ReadStateHeader {
        /// Select an epoch to work on.
        #[clap(subcommand)]
        epoch_selection: EpochSelection,
    },
}

impl StatePartsSubCommand {
    pub(crate) fn run(
        self,
        shard_id: ShardId,
        root_dir: Option<PathBuf>,
        s3_bucket: Option<String>,
        s3_region: Option<String>,
        home_dir: &Path,
        near_config: NearConfig,
        store: Store,
    ) {
        let runtime = NightshadeRuntime::from_config(home_dir, store.clone(), &near_config);
        let chain_genesis = ChainGenesis::new(&near_config.genesis);
        let mut chain = Chain::new_for_view_client(
            runtime,
            &chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            false,
        )
        .unwrap();
        let chain_id = &near_config.genesis.config.chain_id;
        match self {
            StatePartsSubCommand::Apply { dry_run, state_root, part_id, epoch_selection } => {
                apply_state_parts(
                    epoch_selection,
                    shard_id,
                    part_id,
                    dry_run,
                    state_root,
                    &mut chain,
                    chain_id,
                    store,
                    Location::new(root_dir, (s3_bucket, s3_region)),
                );
            }
            StatePartsSubCommand::Dump { part_from, part_to, epoch_selection } => {
                dump_state_parts(
                    epoch_selection,
                    shard_id,
                    part_from,
                    part_to,
                    &chain,
                    chain_id,
                    store,
                    Location::new(root_dir, (s3_bucket, s3_region)),
                );
            }
            StatePartsSubCommand::ReadStateHeader { epoch_selection } => {
                read_state_header(epoch_selection, shard_id, &chain, store)
            }
        }
    }
}

#[derive(clap::Subcommand, Debug, Clone)]
pub(crate) enum EpochSelection {
    /// Current epoch.
    Current,
    /// Fetch the given epoch.
    EpochId { epoch_id: String },
    /// Fetch epochs at the given height.
    EpochHeight { epoch_height: EpochHeight },
    /// Fetch an epoch containing the given block hash.
    BlockHash { block_hash: String },
    /// Fetch an epoch containing the given block height.
    BlockHeight { block_height: BlockHeight },
}

impl EpochSelection {
    fn to_epoch_id(&self, store: Store, chain: &Chain) -> EpochId {
        match self {
            EpochSelection::Current => {
                chain.runtime_adapter.get_epoch_id(&chain.head().unwrap().last_block_hash).unwrap()
            }
            EpochSelection::EpochId { epoch_id } => {
                EpochId(CryptoHash::from_str(epoch_id).unwrap())
            }
            EpochSelection::EpochHeight { epoch_height } => {
                // Fetch epochs at the given height.
                // There should only be one epoch at a given height. But this is a debug tool, let's check
                // if there are multiple epochs at a given height.
                let epoch_ids = iterate_and_filter(store, |epoch_info| {
                    epoch_info.epoch_height() == *epoch_height
                });
                assert_eq!(epoch_ids.len(), 1, "{:#?}", epoch_ids);
                epoch_ids[0].clone()
            }
            EpochSelection::BlockHash { block_hash } => {
                let block_hash = CryptoHash::from_str(block_hash).unwrap();
                chain.runtime_adapter.get_epoch_id(&block_hash).unwrap()
            }
            EpochSelection::BlockHeight { block_height } => {
                // Fetch an epoch containing the given block height.
                let block_hash = chain.store().get_block_hash_by_height(*block_height).unwrap();
                chain.runtime_adapter.get_epoch_id(&block_hash).unwrap()
            }
        }
    }
}

enum Location {
    Files(PathBuf),
    S3 { bucket: String, region: String },
}

impl Location {
    fn new(
        root_dir: Option<PathBuf>,
        s3_bucket_and_region: (Option<String>, Option<String>),
    ) -> Self {
        let (s3_bucket, s3_region) = s3_bucket_and_region;
        assert_eq!(
            s3_bucket.is_some(),
            s3_region.is_some(),
            "None or both of --s3-bucket and --s3-region need to be set"
        );
        assert_ne!(
            root_dir.is_some(),
            s3_bucket.is_some(),
            "Only one of --root-dir and --s3 flags can be set"
        );
        if let Some(root_dir) = root_dir {
            Location::Files(root_dir)
        } else {
            Location::S3 { bucket: s3_bucket.unwrap(), region: s3_region.unwrap() }
        }
    }
}

/// Returns block hash of some block of the given `epoch_info` epoch.
fn get_any_block_hash_of_epoch(epoch_info: &EpochInfo, chain: &Chain) -> CryptoHash {
    let head = chain.store().head().unwrap();
    let mut cur_block_info = chain.runtime_adapter.get_block_info(&head.last_block_hash).unwrap();
    // EpochManager doesn't have an API that maps EpochId to Blocks, and this function works
    // around that limitation by iterating over the epochs.
    // This workaround is acceptable because:
    // 1) Extending EpochManager's API is a major change.
    // 2) This use case is not critical at all.
    loop {
        let cur_epoch_info =
            chain.runtime_adapter.get_epoch_info(cur_block_info.epoch_id()).unwrap();
        let cur_epoch_height = cur_epoch_info.epoch_height();
        assert!(
            cur_epoch_height >= epoch_info.epoch_height(),
            "cur_block_info: {:#?}, epoch_info.epoch_height: {}",
            cur_block_info,
            epoch_info.epoch_height()
        );
        let epoch_first_block_info =
            chain.runtime_adapter.get_block_info(cur_block_info.epoch_first_block()).unwrap();
        let prev_epoch_last_block_info =
            chain.runtime_adapter.get_block_info(epoch_first_block_info.prev_hash()).unwrap();

        if cur_epoch_height == epoch_info.epoch_height() {
            return *cur_block_info.hash();
        }

        cur_block_info = prev_epoch_last_block_info;
    }
}

fn apply_state_parts(
    epoch_selection: EpochSelection,
    shard_id: ShardId,
    part_id: Option<u64>,
    dry_run: bool,
    maybe_state_root: Option<StateRoot>,
    chain: &mut Chain,
    chain_id: &str,
    store: Store,
    location: Location,
) {
    let (state_root, epoch_height, epoch_id, sync_hash) =
        if let (Some(state_root), EpochSelection::EpochHeight { epoch_height }) =
            (maybe_state_root, &epoch_selection)
        {
            (state_root, *epoch_height, None, None)
        } else {
            let epoch_id = epoch_selection.to_epoch_id(store, chain);
            let epoch = chain.runtime_adapter.get_epoch_info(&epoch_id).unwrap();

            let sync_hash = get_any_block_hash_of_epoch(&epoch, chain);
            let sync_hash = StateSync::get_epoch_start_sync_hash(chain, &sync_hash).unwrap();

            let state_header = chain.get_state_response_header(shard_id, sync_hash).unwrap();
            let state_root = state_header.chunk_prev_state_root();

            (state_root, epoch.epoch_height(), Some(epoch_id), Some(sync_hash))
        };

    let part_storage = get_state_part_reader(location, chain_id, epoch_height, shard_id);

    let num_parts = part_storage.num_parts();
    assert_ne!(num_parts, 0, "Too few num_parts: {}", num_parts);
    let part_ids = get_part_ids(part_id, part_id.map(|x| x + 1), num_parts);
    tracing::info!(
        target: "state-parts",
        epoch_height,
        shard_id,
        num_parts,
        ?sync_hash,
        ?part_ids,
        "Applying state as seen at the beginning of the specified epoch.",
    );

    let timer = Instant::now();
    for part_id in part_ids {
        let timer = Instant::now();
        assert!(part_id < num_parts, "part_id: {}, num_parts: {}", part_id, num_parts);
        let part = part_storage.read(part_id, num_parts);

        if dry_run {
            assert!(chain.runtime_adapter.validate_state_part(
                &state_root,
                PartId::new(part_id, num_parts),
                &part
            ));
            tracing::info!(target: "state-parts", part_id, part_length = part.len(), elapsed_sec = timer.elapsed().as_secs_f64(), "Validated a state part");
        } else {
            chain
                .set_state_part(
                    shard_id,
                    sync_hash.unwrap(),
                    PartId::new(part_id, num_parts),
                    &part,
                )
                .unwrap();
            chain
                .runtime_adapter
                .apply_state_part(
                    shard_id,
                    &state_root,
                    PartId::new(part_id, num_parts),
                    &part,
                    epoch_id.as_ref().unwrap(),
                )
                .unwrap();
            tracing::info!(target: "state-parts", part_id, part_length = part.len(), elapsed_sec = timer.elapsed().as_secs_f64(), "Applied a state part");
        }
    }
    tracing::info!(target: "state-parts", total_elapsed_sec = timer.elapsed().as_secs_f64(), "Applied all requested state parts");
}

fn dump_state_parts(
    epoch_selection: EpochSelection,
    shard_id: ShardId,
    part_from: Option<u64>,
    part_to: Option<u64>,
    chain: &Chain,
    chain_id: &str,
    store: Store,
    location: Location,
) {
    let epoch_id = epoch_selection.to_epoch_id(store, chain);
    let epoch = chain.runtime_adapter.get_epoch_info(&epoch_id).unwrap();
    let sync_hash = get_any_block_hash_of_epoch(&epoch, chain);
    let sync_hash = StateSync::get_epoch_start_sync_hash(chain, &sync_hash).unwrap();
    let sync_block = chain.get_block_header(&sync_hash).unwrap();
    let sync_prev_hash = sync_block.prev_hash();

    let state_header = chain.compute_state_response_header(shard_id, sync_hash).unwrap();
    let state_root = state_header.chunk_prev_state_root();
    let num_parts = get_num_state_parts(state_header.state_root_node().memory_usage);
    let part_ids = get_part_ids(part_from, part_to, num_parts);

    tracing::info!(
        target: "state-parts",
        epoch_height = epoch.epoch_height(),
        epoch_id = ?epoch_id.0,
        shard_id,
        num_parts,
        ?sync_hash,
        ?part_ids,
        ?state_root,
        "Dumping state as seen at the beginning of the specified epoch.",
    );

    let part_storage = get_state_part_writer(location, chain_id, epoch.epoch_height(), shard_id);

    let timer = Instant::now();
    for part_id in part_ids {
        let timer = Instant::now();
        assert!(part_id < num_parts, "part_id: {}, num_parts: {}", part_id, num_parts);
        let state_part = chain
            .runtime_adapter
            .obtain_state_part(
                shard_id,
                &sync_prev_hash,
                &state_root,
                PartId::new(part_id, num_parts),
            )
            .unwrap();
        part_storage.write(&state_part, part_id, num_parts);
        let elapsed_sec = timer.elapsed().as_secs_f64();
        let first_state_record = get_first_state_record(&state_root, &state_part);
        tracing::info!(
            target: "state-parts",
            part_id,
            part_length = state_part.len(),
            elapsed_sec,
            ?first_state_record,
            "Wrote a state part");
    }
    tracing::info!(target: "state-parts", total_elapsed_sec = timer.elapsed().as_secs_f64(), "Wrote all requested state parts");
}

/// Returns the first `StateRecord` encountered while iterating over a sub-trie in the state part.
fn get_first_state_record(state_root: &StateRoot, data: &[u8]) -> Option<StateRecord> {
    let trie_nodes = BorshDeserialize::try_from_slice(data).unwrap();
    let trie = Trie::from_recorded_storage(PartialStorage { nodes: trie_nodes }, *state_root);

    for (key, value) in trie.iter().unwrap().flatten() {
        if let Some(sr) = StateRecord::from_raw_key_value(key, value) {
            return Some(sr);
        }
    }
    None
}

/// Reads `StateHeader` stored in the DB.
fn read_state_header(
    epoch_selection: EpochSelection,
    shard_id: ShardId,
    chain: &Chain,
    store: Store,
) {
    let epoch_id = epoch_selection.to_epoch_id(store, chain);
    let epoch = chain.runtime_adapter.get_epoch_info(&epoch_id).unwrap();

    let sync_hash = get_any_block_hash_of_epoch(&epoch, chain);
    let sync_hash = StateSync::get_epoch_start_sync_hash(chain, &sync_hash).unwrap();

    let state_header = chain.store().get_state_header(shard_id, sync_hash);
    tracing::info!(target: "state-parts", ?epoch_id, ?sync_hash, ?state_header);
}

fn get_part_ids(part_from: Option<u64>, part_to: Option<u64>, num_parts: u64) -> Range<u64> {
    part_from.unwrap_or(0)..part_to.unwrap_or(num_parts)
}

trait StatePartWriter {
    fn write(&self, state_part: &[u8], part_id: u64, num_parts: u64);
}

trait StatePartReader {
    fn read(&self, part_id: u64, num_parts: u64) -> Vec<u8>;
    fn num_parts(&self) -> u64;
}

fn get_state_part_reader(
    location: Location,
    chain_id: &str,
    epoch_height: u64,
    shard_id: ShardId,
) -> Box<dyn StatePartReader> {
    match location {
        Location::Files(root_dir) => {
            Box::new(FileSystemStorage::new(root_dir, false, chain_id, epoch_height, shard_id))
        }
        Location::S3 { bucket, region } => {
            Box::new(S3Storage::new(&bucket, &region, chain_id, epoch_height, shard_id))
        }
    }
}

fn get_state_part_writer(
    location: Location,
    chain_id: &str,
    epoch_height: u64,
    shard_id: ShardId,
) -> Box<dyn StatePartWriter> {
    match location {
        Location::Files(root_dir) => {
            Box::new(FileSystemStorage::new(root_dir, true, chain_id, epoch_height, shard_id))
        }
        Location::S3 { bucket, region } => {
            Box::new(S3Storage::new(&bucket, &region, chain_id, epoch_height, shard_id))
        }
    }
}

struct FileSystemStorage {
    state_parts_dir: PathBuf,
}

impl FileSystemStorage {
    fn new(
        root_dir: PathBuf,
        create_dir: bool,
        chain_id: &str,
        epoch_height: u64,
        shard_id: u64,
    ) -> Self {
        let prefix = location_prefix(chain_id, epoch_height, shard_id);
        let state_parts_dir = root_dir.join(&prefix);
        if create_dir {
            tracing::info!(target: "state-parts", ?root_dir, ?prefix, ?state_parts_dir, "Ensuring the directory exists");
            std::fs::create_dir_all(&state_parts_dir).unwrap();
        }
        tracing::info!(target: "state-parts", ?state_parts_dir, "Initialized FileSystemStorage");
        Self { state_parts_dir }
    }

    fn get_location(&self, part_id: u64, num_parts: u64) -> PathBuf {
        self.state_parts_dir.join(part_filename(part_id, num_parts))
    }
}

impl StatePartWriter for FileSystemStorage {
    fn write(&self, state_part: &[u8], part_id: u64, num_parts: u64) {
        let filename = self.get_location(part_id, num_parts);
        std::fs::write(&filename, state_part).unwrap();
        tracing::info!(target: "state-parts", part_id, part_length = state_part.len(), ?filename, "Wrote a state part to disk");
    }
}

impl StatePartReader for FileSystemStorage {
    fn read(&self, part_id: u64, num_parts: u64) -> Vec<u8> {
        let filename = self.get_location(part_id, num_parts);
        tracing::debug!(target: "state-parts", part_id, num_parts, ?filename, "Reading state part file");
        std::fs::read(filename).unwrap()
    }

    fn num_parts(&self) -> u64 {
        let paths = std::fs::read_dir(&self.state_parts_dir).unwrap();
        let mut known_num_parts = None;
        let num_files = paths
            .filter(|path| {
                let full_path = path.as_ref().unwrap();
                tracing::debug!(target: "state-parts", ?full_path);
                let filename = full_path.file_name().to_str().unwrap().to_string();
                if let Some(num_parts) = get_num_parts_from_filename(&filename) {
                    if let Some(known_num_parts) = known_num_parts {
                        assert_eq!(known_num_parts, num_parts);
                    }
                    known_num_parts = Some(num_parts);
                }
                is_part_filename(&filename)
            })
            .collect::<Vec<std::io::Result<DirEntry>>>()
            .len();
        if known_num_parts != Some(num_files as u64) {
            // This is expected when a user saves time and downloads a few parts instead of all parts.
            tracing::warn!(target: "state-parts",
                dir = ?self.state_parts_dir,
                ?known_num_parts,
                num_files,
                "Filename indicates that number of files expected doesn't match the number of files available");
        }
        known_num_parts.unwrap()
    }
}

struct S3Storage {
    location: String,
    bucket: s3::Bucket,
}

impl S3Storage {
    fn new(
        s3_bucket: &str,
        s3_region: &str,
        chain_id: &str,
        epoch_height: u64,
        shard_id: u64,
    ) -> Self {
        let location = location_prefix(chain_id, epoch_height, shard_id);
        let bucket = s3::Bucket::new(
            s3_bucket,
            s3_region.parse::<s3::Region>().unwrap(),
            s3::creds::Credentials::default().unwrap(),
        )
        .unwrap();

        tracing::info!(target: "state-parts", s3_bucket, s3_region, location, "Initialized an S3 bucket");
        Self { location, bucket }
    }

    fn get_location(&self, part_id: u64, num_parts: u64) -> String {
        format!("{}/{}", self.location, part_filename(part_id, num_parts))
    }
}

impl StatePartWriter for S3Storage {
    fn write(&self, state_part: &[u8], part_id: u64, num_parts: u64) {
        let location = self.get_location(part_id, num_parts);
        self.bucket.put_object_blocking(&location, state_part).unwrap();
        tracing::info!(target: "state-parts", part_id, part_length = state_part.len(), ?location, "Wrote a state part to S3");
    }
}

impl StatePartReader for S3Storage {
    fn read(&self, part_id: u64, num_parts: u64) -> Vec<u8> {
        let location = self.get_location(part_id, num_parts);
        let response = self.bucket.get_object_blocking(location.clone()).unwrap();
        tracing::info!(target: "state-parts", part_id, location, response_code = response.status_code(), "Got an object from S3");
        assert_eq!(response.status_code(), 200);
        response.into()
    }

    fn num_parts(&self) -> u64 {
        let location = format!("{}/", self.location);
        tracing::info!(target: "state-parts", location, "Reading an S3 bucket location to get the number of parts");
        let list: Vec<ListBucketResult> =
            self.bucket.list_blocking(location, Some("/".to_string())).unwrap();
        assert_eq!(list.len(), 1);
        let mut known_num_parts = None;
        let num_objects = list[0]
            .contents
            .iter()
            .filter(|object| {
                let filename = Path::new(&object.key);
                let filename = filename.file_name().unwrap().to_str().unwrap();
                tracing::debug!(target: "state-parts", object_key = ?object.key, ?filename);
                if let Some(num_parts) = get_num_parts_from_filename(filename) {
                    if let Some(known_num_parts) = known_num_parts {
                        assert_eq!(known_num_parts, num_parts);
                    }
                    known_num_parts = Some(num_parts);
                }
                is_part_filename(filename)
            })
            .collect::<Vec<&s3::serde_types::Object>>()
            .len();
        if let Some(known_num_parts) = known_num_parts {
            assert_eq!(known_num_parts, num_objects as u64);
        }
        num_objects as u64
    }
}
