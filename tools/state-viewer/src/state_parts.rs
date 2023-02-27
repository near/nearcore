use crate::epoch_info::iterate_and_filter;
use clap::Subcommand;
use near_chain::types::RuntimeAdapter;
use near_chain::{ChainStore, ChainStoreAccess};
use near_epoch_manager::EpochManager;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::state_part::PartId;
use near_primitives::syncing::get_num_state_parts;
use near_primitives::types::EpochId;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{BlockHeight, EpochHeight, ShardId};
use near_store::Store;
use nearcore::{NearConfig, NightshadeRuntime};
use s3::serde_types::ListBucketResult;
use std::fs::DirEntry;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

#[derive(Subcommand, Debug, Clone)]
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
    pub fn to_epoch_id(
        &self,
        store: Store,
        chain_store: &ChainStore,
        epoch_manager: &EpochManager,
    ) -> EpochId {
        match self {
            EpochSelection::Current => {
                epoch_manager.get_epoch_id(&chain_store.head().unwrap().last_block_hash).unwrap()
            }
            EpochSelection::EpochId { epoch_id } => {
                EpochId(CryptoHash::from_str(&epoch_id).unwrap())
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
                let block_hash = CryptoHash::from_str(&block_hash).unwrap();
                epoch_manager.get_epoch_id(&block_hash).unwrap()
            }
            EpochSelection::BlockHeight { block_height } => {
                // Fetch an epoch containing the given block height.
                let block_hash = chain_store.get_block_hash_by_height(*block_height).unwrap();
                epoch_manager.get_epoch_id(&block_hash).unwrap()
            }
        }
    }
}

pub(crate) enum Location {
    Files(PathBuf),
    S3 { bucket: String, region: String },
}

impl Location {
    pub(crate) fn new(
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

/// Returns block hash of the last block of an epoch preceding the given `epoch_info`.
fn get_prev_hash_of_epoch(
    epoch_info: &EpochInfo,
    chain_store: &ChainStore,
    epoch_manager: &EpochManager,
) -> CryptoHash {
    let head = chain_store.head().unwrap();
    let mut cur_block_info = epoch_manager.get_block_info(&head.last_block_hash).unwrap();
    // EpochManager doesn't have an API that maps EpochId to Blocks, and this function works
    // around that limitation by iterating over the epochs.
    // This workaround is acceptable because:
    // 1) Extending EpochManager's API is a major change.
    // 2) This use case is not critical at all.
    loop {
        let cur_epoch_info = epoch_manager.get_epoch_info(cur_block_info.epoch_id()).unwrap();
        let cur_epoch_height = cur_epoch_info.epoch_height();
        assert!(
            cur_epoch_height >= epoch_info.epoch_height(),
            "cur_block_info: {:#?}, epoch_info.epoch_height: {}",
            cur_block_info,
            epoch_info.epoch_height()
        );
        let epoch_first_block_info =
            epoch_manager.get_block_info(cur_block_info.epoch_first_block()).unwrap();
        let prev_epoch_last_block_info =
            epoch_manager.get_block_info(epoch_first_block_info.prev_hash()).unwrap();

        if cur_epoch_height == epoch_info.epoch_height() {
            return *prev_epoch_last_block_info.hash();
        }

        cur_block_info = prev_epoch_last_block_info;
    }
}

pub(crate) fn apply_state_parts(
    epoch_selection: EpochSelection,
    shard_id: ShardId,
    part_id: Option<u64>,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    location: Location,
) {
    let runtime_adapter: Arc<dyn RuntimeAdapter> =
        Arc::new(NightshadeRuntime::from_config(home_dir, store.clone(), &near_config));
    let epoch_manager =
        EpochManager::new_from_genesis_config(store.clone(), &near_config.genesis.config)
            .expect("Failed to start Epoch Manager");
    let chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );

    let epoch_id = epoch_selection.to_epoch_id(store, &chain_store, &epoch_manager);
    let epoch = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    let sync_prev_hash = get_prev_hash_of_epoch(&epoch, &chain_store, &epoch_manager);
    let sync_prev_block = chain_store.get_block(&sync_prev_hash).unwrap();

    assert!(epoch_manager.is_next_block_epoch_start(&sync_prev_hash).unwrap());
    assert!(
        shard_id < sync_prev_block.chunks().len() as u64,
        "shard_id: {}, #shards: {}",
        shard_id,
        sync_prev_block.chunks().len()
    );
    let state_root = sync_prev_block.chunks()[shard_id as usize].prev_state_root();

    let part_storage = get_state_part_reader(
        location,
        &near_config.client_config.chain_id,
        epoch.epoch_height(),
        shard_id,
    );

    let num_parts = part_storage.num_parts();
    let part_ids = get_part_ids(part_id, part_id.map(|x| x + 1), num_parts);
    tracing::info!(
        target: "state-parts",
        epoch_height = epoch.epoch_height(),
        epoch_id = ?epoch_id.0,
        shard_id,
        num_parts,
        ?sync_prev_hash,
        ?part_ids,
        "Applying state as seen at the beginning of the specified epoch.",
    );

    let timer = Instant::now();
    for part_id in part_ids {
        let timer = Instant::now();
        assert!(part_id < num_parts, "part_id: {}, num_parts: {}", part_id, num_parts);
        let part = part_storage.read(part_id);
        runtime_adapter
            .apply_state_part(
                shard_id,
                &state_root,
                PartId::new(part_id, num_parts),
                &part,
                &epoch_id,
            )
            .unwrap();
        tracing::info!(target: "state-parts", part_id, part_length = part.len(), elapsed_sec = timer.elapsed().as_secs_f64(), "Applied a state part");
    }
    tracing::info!(target: "state-parts", total_elapsed_sec = timer.elapsed().as_secs_f64(), "Applied all requested state parts");
}

pub(crate) fn dump_state_parts(
    epoch_selection: EpochSelection,
    shard_id: ShardId,
    part_from: Option<u64>,
    part_to: Option<u64>,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    location: Location,
) {
    let runtime_adapter: Arc<dyn RuntimeAdapter> =
        Arc::new(NightshadeRuntime::from_config(home_dir, store.clone(), &near_config));
    let epoch_manager =
        EpochManager::new_from_genesis_config(store.clone(), &near_config.genesis.config)
            .expect("Failed to start Epoch Manager");
    let chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );

    let epoch_id = epoch_selection.to_epoch_id(store, &chain_store, &epoch_manager);
    let epoch = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    let sync_prev_hash = get_prev_hash_of_epoch(&epoch, &chain_store, &epoch_manager);
    let sync_prev_block = chain_store.get_block(&sync_prev_hash).unwrap();

    assert!(epoch_manager.is_next_block_epoch_start(&sync_prev_hash).unwrap());
    assert!(
        shard_id < sync_prev_block.chunks().len() as u64,
        "shard_id: {}, #shards: {}",
        shard_id,
        sync_prev_block.chunks().len()
    );
    let state_root = sync_prev_block.chunks()[shard_id as usize].prev_state_root();
    let state_root_node =
        runtime_adapter.get_state_root_node(shard_id, &sync_prev_hash, &state_root).unwrap();

    let num_parts = get_num_state_parts(state_root_node.memory_usage);
    let part_ids = get_part_ids(part_from, part_to, num_parts);

    tracing::info!(
        target: "state-parts",
        epoch_height = epoch.epoch_height(),
        epoch_id = ?epoch_id.0,
        shard_id,
        num_parts,
        ?sync_prev_hash,
        ?part_ids,
        "Dumping state as seen at the beginning of the specified epoch.",
    );

    let part_storage = get_state_part_writer(
        location,
        &near_config.client_config.chain_id,
        epoch.epoch_height(),
        shard_id,
    );

    let timer = Instant::now();
    for part_id in part_ids {
        let timer = Instant::now();
        assert!(part_id < num_parts, "part_id: {}, num_parts: {}", part_id, num_parts);
        let state_part = runtime_adapter
            .obtain_state_part(
                shard_id,
                &sync_prev_hash,
                &state_root,
                PartId::new(part_id, num_parts),
            )
            .unwrap();
        part_storage.write(&state_part, part_id);
        tracing::info!(target: "state-parts", part_id, part_length = state_part.len(), elapsed_sec = timer.elapsed().as_secs_f64(), "Wrote a state part");
    }
    tracing::info!(target: "state-parts", total_elapsed_sec = timer.elapsed().as_secs_f64(), "Wrote all requested state parts");
}

fn get_part_ids(part_from: Option<u64>, part_to: Option<u64>, num_parts: u64) -> Range<u64> {
    part_from.unwrap_or(0)..part_to.unwrap_or(num_parts)
}

fn location_prefix(chain_id: &str, epoch_height: u64, shard_id: u64) -> String {
    format!("chain_id={}/epoch_height={}/shard_id={}", chain_id, epoch_height, shard_id)
}

fn is_part_filename(s: &str) -> bool {
    let re = regex::Regex::new(r"^state_part_(\d{6})$").unwrap();
    re.is_match(s)
}

fn part_filename(part_id: u64) -> String {
    format!("state_part_{:06}", part_id)
}

trait StatePartWriter {
    fn write(&self, state_part: &[u8], part_id: u64);
}

trait StatePartReader {
    fn read(&self, part_id: u64) -> Vec<u8>;
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
        Self { state_parts_dir }
    }

    fn get_location(&self, part_id: u64) -> PathBuf {
        (&self.state_parts_dir).join(part_filename(part_id))
    }
}

impl StatePartWriter for FileSystemStorage {
    fn write(&self, state_part: &[u8], part_id: u64) {
        let filename = self.get_location(part_id);
        std::fs::write(&filename, state_part).unwrap();
        tracing::info!(target: "state-parts", part_id, part_length = state_part.len(), ?filename, "Wrote a state part to disk");
    }
}

impl StatePartReader for FileSystemStorage {
    fn read(&self, part_id: u64) -> Vec<u8> {
        let filename = self.get_location(part_id);
        let part = std::fs::read(filename).unwrap();
        part
    }

    fn num_parts(&self) -> u64 {
        let paths = std::fs::read_dir(&self.state_parts_dir).unwrap();
        let num_parts = paths
            .filter(|path| {
                let full_path = path.as_ref().unwrap();
                tracing::debug!(target: "state-parts", ?full_path);
                is_part_filename(full_path.file_name().to_str().unwrap())
            })
            .collect::<Vec<std::io::Result<DirEntry>>>()
            .len();
        num_parts as u64
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
            &s3_bucket,
            s3_region.parse().unwrap(),
            s3::creds::Credentials::default().unwrap(),
        )
        .unwrap();

        tracing::info!(target: "state-parts", s3_bucket, s3_region, location, "Initialized an S3 bucket");
        Self { location, bucket }
    }

    fn get_location(&self, part_id: u64) -> String {
        format!("{}/{}", self.location, part_filename(part_id))
    }
}

impl StatePartWriter for S3Storage {
    fn write(&self, state_part: &[u8], part_id: u64) {
        let location = self.get_location(part_id);
        self.bucket.put_object_blocking(&location, &state_part).unwrap();
        tracing::info!(target: "state-parts", part_id, part_length = state_part.len(), ?location, "Wrote a state part to S3");
    }
}

impl StatePartReader for S3Storage {
    fn read(&self, part_id: u64) -> Vec<u8> {
        let location = self.get_location(part_id);
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
        let num_parts = list[0]
            .contents
            .iter()
            .filter(|object| {
                let filename = Path::new(&object.key);
                let filename = filename.file_name().unwrap().to_str().unwrap();
                tracing::debug!(target: "state-parts", object_key = ?object.key, ?filename);
                is_part_filename(filename)
            })
            .collect::<Vec<&s3::serde_types::Object>>()
            .len();
        num_parts as u64
    }
}
