use actix::clock::{sleep, timeout};
use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use futures_util::StreamExt;
use near_chain::{Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode, Error};
use near_chain_configs::{GenesisValidationMode, SyncConcurrency};
use near_client::sync::external::{
    ExternalConnection, StateFileType, create_bucket_read_write, create_bucket_readonly,
    external_storage_location, external_storage_location_directory, get_num_parts_from_filename,
};
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::state_part::{PartId, StatePart};
use near_primitives::state_sync::StatePartKey;
use near_primitives::types::{EpochId, ProtocolVersion, StateRoot};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{BlockHeight, EpochHeight, ShardId};
use near_store::{DBCol, NodeStorage, Store};
use near_time::{Clock, Duration, Instant};
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt, load_config};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

#[derive(clap::Parser)]
pub struct CloudArchivalCommand {
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
    /// Store state parts in an GCS bucket.
    #[clap(long)]
    gcs_bucket: Option<String>,
    #[clap(long)]
    pv: Option<ProtocolVersion>,
    /// Dump or Apply state parts.
    #[clap(subcommand)]
    command: CloudArchivalSubCommand,
}

impl CloudArchivalCommand {
    pub fn run(self, home_dir: &Path, genesis_validation: GenesisValidationMode) {
        let near_config = load_config(home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        let store_opener = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
            near_config.config.cloud_storage.as_ref(),
        );

        let store = store_opener.open().unwrap().get_hot_store();
        self.command.run(
            self.shard_id,
            self.root_dir,
            self.s3_bucket,
            self.s3_region,
            self.gcs_bucket,
            home_dir,
            near_config,
            store,
            self.pv,
        );
    }
}

#[derive(clap::Subcommand, Debug, Clone)]
pub(crate) enum CloudArchivalSubCommand {
    /// Load all or a single state part of a shard and perform an action over those parts.
    Download {
        /// If provided, this value will be used instead of looking it up in the headers.
        /// Use if those headers or blocks are not available.
        #[clap(long)]
        state_root: Option<StateRoot>,
        /// If provided, this value will be used instead of looking it up in the headers.
        /// Use if those headers or blocks are not available.
        #[clap(long)]
        sync_hash: Option<CryptoHash>,
        /// Choose a single part id.
        /// If None - affects all state parts.
        #[clap(long)]
        part_id: Option<u64>,
        /// Select an epoch to work on.
        #[clap(subcommand)]
        epoch_selection: EpochSelection,
    },
    /// Dump all or a single state part of a shard.
    Upload {
        /// Dump part ids starting from this part.
        #[clap(long)]
        part_from: Option<u64>,
        /// Dump part ids up to this part (exclusive).
        #[clap(long)]
        part_to: Option<u64>,
        /// Dump state sync header.
        #[clap(long, short, action)]
        dump_header: bool,
        /// Location of a file with write permissions to the bucket.
        #[clap(long)]
        credentials_file: Option<PathBuf>,
        /// Select an epoch to work on.
        #[clap(subcommand)]
        epoch_selection: EpochSelection,
    },
}

impl CloudArchivalSubCommand {
    pub(crate) fn run(
        self,
        shard_id: ShardId,
        root_dir: Option<PathBuf>,
        s3_bucket: Option<String>,
        s3_region: Option<String>,
        gcs_bucket: Option<String>,
        home_dir: &Path,
        near_config: NearConfig,
        store: Store,
        pv: Option<ProtocolVersion>,
    ) {
        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );
        let shard_tracker = ShardTracker::new(
            near_config.client_config.tracked_shards_config.clone(),
            epoch_manager.clone(),
            near_config.validator_signer.clone(),
        );
        let runtime = NightshadeRuntime::from_config(
            home_dir,
            store.clone(),
            &near_config,
            epoch_manager.clone(),
        )
        .expect("could not create the transaction runtime");
        let chain_genesis = ChainGenesis::new(&near_config.genesis.config);
        let mut chain = Chain::new_for_view_client(
            Clock::real(),
            epoch_manager,
            shard_tracker,
            runtime,
            &chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            false,
            near_config.validator_signer.clone(),
        )
        .unwrap();
        let chain_id = &near_config.genesis.config.chain_id;


        let sys = actix::System::with_tokio_rt(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .max_blocking_threads(4)
                .enable_all()
                .build()
                .unwrap()
        });

        let concurrency_limit = SyncConcurrency::default().per_shard;
        let max_attempts = 5;
        sys.block_on(async move {
            match self {
                Self::Download { state_root, sync_hash, part_id, epoch_selection } => {
                    let external = create_external_connection(
                        root_dir,
                        s3_bucket,
                        s3_region,
                        gcs_bucket,
                        None,
                        Mode::ReadOnly,
                    );

                    download_state_parts(
                        epoch_selection,
                        shard_id,
                        part_id,
                        state_root,
                        sync_hash,
                        &mut chain,
                        chain_id,
                        store,
                        &external,
                        concurrency_limit,
                        max_attempts,
                        near_config.client_config.state_sync_retry_backoff,
                        near_config.client_config.state_sync_external_timeout,
                        pv,
                    )
                    .await
                    .unwrap()
                }
                Self::Upload {
                    part_from,
                    part_to,
                    dump_header,
                    epoch_selection,
                    credentials_file,
                } => {
                    let external = create_external_connection(
                        root_dir,
                        s3_bucket,
                        s3_region,
                        gcs_bucket,
                        credentials_file,
                        Mode::ReadWrite,
                    );
                    upload_state_parts(
                        epoch_selection,
                        shard_id,
                        part_from,
                        part_to,
                        dump_header,
                        &chain,
                        chain_id,
                        store,
                        &external,
                        pv,
                        concurrency_limit,
                        max_attempts,
                        near_config.client_config.state_sync_retry_backoff,
                        near_config.client_config.state_sync_external_timeout,
                    )
                    .await
                    .unwrap()
                }
            }
            near_async::shutdown_all_actors();
        });
        sys.run().unwrap();
    }
}

enum Mode {
    ReadOnly,
    ReadWrite,
}

fn create_external_connection(
    root_dir: Option<PathBuf>,
    bucket: Option<String>,
    region: Option<String>,
    gcs_bucket: Option<String>,
    credentials_file: Option<PathBuf>,
    mode: Mode,
) -> ExternalConnection {
    if let Some(root_dir) = root_dir {
        ExternalConnection::Filesystem { root_dir }
    } else if let (Some(bucket), Some(region)) = (bucket, region) {
        let bucket = match mode {
            Mode::ReadOnly => {
                create_bucket_readonly(&bucket, &region, std::time::Duration::from_secs(5))
            }
            Mode::ReadWrite => create_bucket_read_write(
                &bucket,
                &region,
                std::time::Duration::from_secs(5),
                credentials_file,
            ),
        }
        .expect("Failed to create an S3 bucket");
        ExternalConnection::S3 { bucket: Arc::new(bucket) }
    } else if let Some(bucket) = gcs_bucket {
        if let Some(credentials_file) = credentials_file {
            unsafe { std::env::set_var("SERVICE_ACCOUNT", &credentials_file) };
        }
        let client = reqwest::Client::builder()
            .http2_adaptive_window(true)
            .pool_max_idle_per_host(128)
            .tcp_nodelay(true)
            .build().unwrap();
        ExternalConnection::GCS {
            gcs_client: Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(&bucket)
                    .build()
                    .unwrap(),
            ),
            reqwest_client: Arc::new(client),
            bucket,
        }
    } else {
        panic!(
            "Please provide --root-dir, or both of --s3-bucket and --s3-region, or --gcs-bucket"
        );
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
                chain.epoch_manager.get_epoch_id(&chain.head().unwrap().last_block_hash).unwrap()
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
                epoch_ids[0]
            }
            EpochSelection::BlockHash { block_hash } => {
                let block_hash = CryptoHash::from_str(block_hash).unwrap();
                chain.epoch_manager.get_epoch_id(&block_hash).unwrap()
            }
            EpochSelection::BlockHeight { block_height } => {
                // Fetch an epoch containing the given block height.
                let block_hash =
                    chain.chain_store().get_block_hash_by_height(*block_height).unwrap();
                chain.epoch_manager.get_epoch_id(&block_hash).unwrap()
            }
        }
    }
}

/// Returns block hash of some block of the given `epoch_info` epoch.
fn get_any_block_hash_of_epoch(epoch_info: &EpochInfo, chain: &Chain) -> CryptoHash {
    let head = chain.chain_store().head().unwrap();
    let mut cur_block_info = chain.epoch_manager.get_block_info(&head.last_block_hash).unwrap();
    // EpochManager doesn't have an API that maps EpochId to Blocks, and this function works
    // around that limitation by iterating over the epochs.
    // This workaround is acceptable because:
    // 1) Extending EpochManager's API is a major change.
    // 2) This use case is not critical at all.
    loop {
        let cur_epoch_info = chain.epoch_manager.get_epoch_info(cur_block_info.epoch_id()).unwrap();
        let cur_epoch_height: u64 = cur_epoch_info.epoch_height();
        assert!(
            cur_epoch_height >= epoch_info.epoch_height(),
            "cur_block_info: {:#?}, epoch_info.epoch_height: {}",
            cur_block_info,
            epoch_info.epoch_height()
        );
        let epoch_first_block_info =
            chain.epoch_manager.get_block_info(cur_block_info.epoch_first_block()).unwrap();
        let prev_epoch_last_block_info =
            chain.epoch_manager.get_block_info(epoch_first_block_info.prev_hash()).unwrap();

        if cur_epoch_height == epoch_info.epoch_height() {
            return *cur_block_info.hash();
        }

        cur_block_info = prev_epoch_last_block_info;
    }
}

async fn download_state_parts(
    epoch_selection: EpochSelection,
    shard_id: ShardId,
    part_id: Option<u64>,
    maybe_state_root: Option<StateRoot>,
    maybe_sync_hash: Option<CryptoHash>,
    chain: &Chain,
    chain_id: &str,
    store: Store,
    external: &ExternalConnection,
    concurrency_limit: u8,
    max_attempts: u32,
    retry_backoff: Duration,
    request_timeout: Duration,
    pv: Option<ProtocolVersion>,
) -> Result<(), Error> {
    let epoch_id = epoch_selection.to_epoch_id(store.clone(), chain);
    let (state_root, epoch_height, epoch_id, sync_hash) =
        if let (Some(state_root), Some(sync_hash), EpochSelection::EpochHeight { epoch_height }) =
            (maybe_state_root, maybe_sync_hash, &epoch_selection)
        {
            (state_root, *epoch_height, epoch_id, sync_hash)
        } else {
            let epoch = chain.epoch_manager.get_epoch_info(&epoch_id).unwrap();
            let sync_base = get_any_block_hash_of_epoch(&epoch, chain);
            let sync_hash = chain
                .get_sync_hash(&sync_base)
                .unwrap()
                .ok_or_else(|| Error::Other(format!("sync hash not yet known for {epoch_id:?}")))?;
            let state_header =
                chain.state_sync_adapter.get_state_response_header(shard_id, sync_hash).unwrap();
            (state_header.chunk_prev_state_root(), epoch.epoch_height(), epoch_id, sync_hash)
        };
    let protocol_version =
        pv.unwrap_or(chain.epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap());

    let directory_path = external_storage_location_directory(
        chain_id,
        &epoch_id,
        epoch_height,
        shard_id,
        &StateFileType::StatePart { part_id: 0, num_parts: 0 },
    );
    let part_file_names = external
        .list_objects(shard_id, &directory_path)
        .await
        .map_err(|e| Error::Other(format!("list_objects: {e}")))?;
    assert!(!part_file_names.is_empty());
    let num_parts = get_num_parts_from_filename(&part_file_names[0]).unwrap();
    let part_ids = get_part_ids(part_id, part_id.map(|x| x + 1), num_parts);

    let t_total = Instant::now();
    let mut success = 0;
    let mut failed = 0;
    let mut sum_dl = 0.0;
    let mut sum_val = 0.0;
    let mut sum_store = 0.0;
    let mut total_bytes: u64 = 0;
    let mut min_bytes: u64 = u64::MAX;
    let mut max_bytes: u64 = 0;

    let mut stream = tokio_stream::iter(part_ids.clone())
        .map(|pid| {
            let external = external.clone();
            let store = store.clone();
            let epoch_id = epoch_id.clone();
            let state_root = state_root.clone();
            let runtime = chain.runtime_adapter.clone();
            async move {
                let mut attempts = 0u32;
                loop {
                    attempts += 1;
                    let file_type = StateFileType::StatePart { part_id: pid, num_parts };
                    let location = external_storage_location(
                        chain_id,
                        &epoch_id,
                        epoch_height,
                        shard_id,
                        &file_type,
                    );
                    let t_dl = Instant::now();
                    let request_timeout =
                        std::time::Duration::from_secs_f64(request_timeout.as_seconds_f64());
                    let retry_backoff =
                        std::time::Duration::from_secs_f64(retry_backoff.as_seconds_f64());
                    let bytes = match timeout(
                        request_timeout,
                        external.get_file(shard_id, &location, &file_type),
                    )
                    .await
                    {
                        Ok(Ok(b)) => b,
                        _ => {
                            panic!("download");
                            if attempts >= max_attempts {
                                return Err(Error::Other("download failed".into()));
                            }
                            sleep(retry_backoff).await;
                            continue;
                        }
                    };
                    let dl_sec = t_dl.elapsed().as_secs_f64();
                    println!("Download took {}", dl_sec);
                    let part_size = bytes.len() as u64;

                    let (part, valid, decode_sec, val_sec) = tokio::task::spawn_blocking(move || {
                        use std::time::Instant;

                        let t = Instant::now();
                        let part = StatePart::from_bytes(bytes, protocol_version)
                            .map_err(|e| Error::Other(format!("decode: {e}")))?;
                        let decode_sec = t.elapsed().as_secs_f64();

                        let t = Instant::now();
                        let valid = runtime.validate_state_part(
                            shard_id,
                            &state_root,
                            PartId::new(pid, num_parts),
                            &part,
                        );
                        let val_sec = t.elapsed().as_secs_f64();

                        Ok::<_, Error>((part, valid, decode_sec, val_sec))
                    })
                    .await
                    .map_err(|e| Error::Other(format!("join error: {e}")))??;
                    println!("Decode took {}", decode_sec);

                    let val_sec = decode_sec + val_sec;

                    if !valid {
                        panic!("validation");
                    }

                    let (store_sec, ()) = tokio::task::spawn_blocking({
                        let store = store.clone();
                        let key = borsh::to_vec(&StatePartKey(sync_hash, shard_id, pid))
                            .map_err(|e| Error::Other(format!("key ser: {e}")))?;
                        let part = part.clone();

                        move || {
                            use std::time::Instant;
                            let t = Instant::now();

                            let raw = part.to_bytes(protocol_version);
                            let mut su = store.store_update();
                            su.set(DBCol::StateParts, &key, &raw);
                            su.commit().map_err(|e| Error::Other(format!("store commit: {e}")))?;

                            Ok::<_, Error>((t.elapsed().as_secs_f64(), ()))
                        }
                    })
                    .await
                    .map_err(|e| Error::Other(format!("join error: {e}")))??;

                    return Ok((dl_sec, val_sec, store_sec, part_size));
                }
            }
        })
        .buffer_unordered(concurrency_limit as usize);

    let mut done = 0u64;
    let mut next_report = num_parts / 100;
    if next_report == 0 { next_report = 1; }

    while let Some(res) = stream.next().await {
        done += 1;
        match res {
            Ok((dl, val, st, size)) => {
                success += 1;
                sum_dl += dl;
                sum_val += val;
                sum_store += st;
                total_bytes = total_bytes.saturating_add(size);
                if size < min_bytes {
                    min_bytes = size;
                }
                if size > max_bytes {
                    max_bytes = size;
                }
            }
            e => {
                println!("Fail {e:?}");
                failed += 1;
            }
        }
        if done % next_report == 0 || done == num_parts {
            let percent = (done * 100) / num_parts;
            println!("Progress: {done}/{num_parts} parts ({percent}%)");
        }
    }

    let total_sec = t_total.elapsed().as_secs();
    println!("\n=== State Part Download Report ===");
    println!("shard_id: {shard_id}");
    println!("num_parts: {num_parts}");
    println!("succeeded: {success}, failed: {failed}");
    println!("total_time_sec: {}", total_sec);
    if success > 0 {
        println!("avg_download_ms: {:.2}", (sum_dl / success as f64) * 1000.0);
        println!("avg_validate_ms: {:.2}", (sum_val / success as f64) * 1000.0);
        println!("avg_store_ms: {:.2}", (sum_store / success as f64) * 1000.0);
        let avg_bytes = total_bytes / success as u64;
        println!("total_size: {}", ByteSize::b(total_bytes));
        println!("avg_part_size: {}", ByteSize::b(avg_bytes));
        println!("min_part_size: {}", ByteSize::b(min_bytes));
        println!("max_part_size: {}", ByteSize::b(max_bytes));
    }

    Ok(())
}

async fn upload_state_parts(
    epoch_selection: EpochSelection,
    shard_id: ShardId,
    part_from: Option<u64>,
    part_to: Option<u64>,
    dump_header: bool,
    chain: &Chain,
    chain_id: &str,
    store: Store,
    external: &ExternalConnection,
    pv: Option<ProtocolVersion>,
    concurrency_limit: u8,
    max_attempts: u32,
    retry_backoff: Duration,
    request_timeout: Duration,
) -> Result<(), Error> {
    let epoch_id = epoch_selection.to_epoch_id(store, chain);
    let epoch = chain.epoch_manager.get_epoch_info(&epoch_id).unwrap();
    let protocol_version =
        pv.unwrap_or(chain.epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap());

    let sync_hash0 = get_any_block_hash_of_epoch(&epoch, chain);
    let Some(sync_hash) = chain.get_sync_hash(&sync_hash0).unwrap() else {
        tracing::warn!(target: "state-parts", ?epoch_id, "sync hash not yet known");
        return Ok(());
    };
    let sync_block_header = chain.get_block_header(&sync_hash).unwrap();
    let sync_prev_header = chain.get_previous_header(&sync_block_header).unwrap();
    let sync_prev_prev_hash = sync_prev_header.prev_hash();

    let state_header =
        chain.state_sync_adapter.compute_state_response_header(shard_id, sync_hash).unwrap();
    let state_root = state_header.chunk_prev_state_root();
    let num_parts = state_header.num_state_parts();
    let part_ids = get_part_ids(part_from, part_to, num_parts);
    let epoch_height = epoch.epoch_height();

    tracing::info!(target: "state-parts", %shard_id, num_parts, ?sync_hash, ?state_root, ?part_ids, epoch_height, epoch_id=?epoch_id.0, "Uploading state parts (parallel)");

    let t_total = Instant::now();

    if dump_header {
        let mut state_sync_header_buf = Vec::new();
        state_header.serialize(&mut state_sync_header_buf).unwrap();
        let file_type = StateFileType::StateHeader;
        let location =
            external_storage_location(&chain_id, &epoch_id, epoch_height, shard_id, &file_type);
        external
            .put_file(file_type, &state_sync_header_buf, shard_id, &location)
            .await
            .map_err(|e| Error::Other(format!("put header: {e}")))?;
    }

    let mut stream = tokio_stream::iter(part_ids.clone())
        .map(|part_id| {
            let external = external.clone();
            let epoch_id = epoch_id.clone();
            let chain_id = chain_id.to_string();
            async move {
                let file_type = StateFileType::StatePart { part_id, num_parts };
                let location = external_storage_location(&chain_id, &epoch_id, epoch_height, shard_id, &file_type);

                let request_timeout_std = std::time::Duration::from_secs_f64(request_timeout.as_seconds_f64());
                let retry_backoff_std = std::time::Duration::from_secs_f64(retry_backoff.as_seconds_f64());

                let mut attempts = 0u32;
                loop {
                    attempts += 1;

                    let obtain_res = chain.runtime_adapter.obtain_state_part(
                        shard_id,
                        sync_prev_prev_hash,
                        &state_root,
                        PartId::new(part_id, num_parts),
                        pv,
                    );

                    let state_part = match obtain_res {
                        Ok(p) => p,
                        Err(e) => {
                            panic!("obtain failed");
                            if attempts >= max_attempts { return Err(Error::Other(format!("obtain_state_part: {e}"))); }
                            sleep(retry_backoff_std).await;
                            continue;
                        }
                    };

                    let bytes = state_part.to_bytes(protocol_version);

                    match timeout(request_timeout_std, external.put_file(file_type.clone(), &bytes, shard_id, &location)).await {
                        Ok(Ok(())) => {
                            tracing::info!(target: "state-parts", part_id, part_length = bytes.len(), "Uploaded state part");
                            return Ok(());
                        }
                        err => {
                            println!("fail {err:?}");
                            //panic!("upload failed");
                            if attempts >= max_attempts {
                                return Err(Error::Other("upload failed".into()));
                            }
                            sleep(retry_backoff_std).await;
                            continue;
                        }
                    }
                }
            }
        })
        .buffer_unordered(concurrency_limit as usize);

    let mut done = 0u64;
    let mut next_report = num_parts / 100;
    if next_report == 0 { next_report = 1; }

    let mut ok = 0usize;
    while let Some(res) = stream.next().await {
        done += 1;
        if res.is_ok() {
            ok += 1;
        }
        if done % next_report == 0 || done == num_parts {
            let percent = (done * 100) / num_parts;
            println!("Upload progress: {done}/{num_parts} parts ({percent}%)");
        }
    }
    tracing::info!(
        target: "state-parts",
        elapsed_sec = t_total.elapsed().as_secs_f64(),
        succeeded = ok,
        failed = num_parts - ok as u64,
        "Upload state parts complete"
    );

    Ok(())
}

fn get_part_ids(part_from: Option<u64>, part_to: Option<u64>, num_parts: u64) -> Range<u64> {
    part_from.unwrap_or(0)..part_to.unwrap_or(num_parts)
}

// Iterates over the DBCol::EpochInfo column, ignores AGGREGATOR_KEY and returns deserialized EpochId
// for EpochInfos that satisfy the given predicate.
pub(crate) fn iterate_and_filter(
    store: Store,
    predicate: impl Fn(EpochInfo) -> bool,
) -> Vec<EpochId> {
    store
        .iter(DBCol::EpochInfo)
        .map(Result::unwrap)
        .filter_map(|(key, value)| {
            if key.as_ref() == AGGREGATOR_KEY {
                None
            } else {
                let epoch_info = EpochInfo::try_from_slice(value.as_ref()).unwrap();
                if predicate(epoch_info) {
                    Some(EpochId::try_from_slice(key.as_ref()).unwrap())
                } else {
                    None
                }
            }
        })
        .collect()
}
