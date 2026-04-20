use anyhow::Context;
use borsh::BorshDeserialize;
use near_chain_configs::GenesisValidationMode;
use near_primitives::block::Tip;
use near_primitives::types::BlockHeight;
use near_primitives::types::ShardId;
use near_store::DBCol;
use near_store::Mode;
use near_store::NodeStorage;
use near_store::Store;
use near_store::archive::cloud_storage::CloudStorage;
use near_store::archive::cloud_storage::ListableCloudDir;
use near_store::archive::cloud_storage::bucket_config::BucketConfig;
use near_store::archive::cloud_storage::opener::CloudStorageOpener;
use near_store::db::CLOUD_BLOCK_HEAD_KEY;
use near_store::db::CLOUD_MIN_HEAD_KEY;
use near_store::db::CLOUD_SHARD_HEAD_PREFIX;
use near_store::db::FINAL_HEAD_KEY;
use near_store::db::HEAD_KEY;
use std::path::Path;
use std::sync::Arc;

struct ExternalStatus {
    block_head: Option<BlockHeight>,
    shard_heads: Vec<(ShardId, BlockHeight)>,
}

struct LocalStatus {
    cloud_min_head: Option<Tip>,
    cloud_block_head: Option<BlockHeight>,
    cloud_shard_heads: Vec<(ShardId, BlockHeight)>,
    chain_head: BlockHeight,
    chain_final_head: BlockHeight,
}

fn collect_external(cloud_storage: &Arc<CloudStorage>) -> anyhow::Result<ExternalStatus> {
    let runtime = tokio::runtime::Runtime::new()?;

    let block_head = runtime
        .block_on(cloud_storage.retrieve_cloud_block_head_if_exists())
        .context("failed to retrieve block head from cloud")?;

    let shard_files = runtime
        .block_on(cloud_storage.list_dir(&ListableCloudDir::ShardHeads))
        .context("failed to list shard heads in cloud")?;

    let mut shard_heads = Vec::new();
    for filename in &shard_files {
        let shard_id: ShardId = match filename.parse() {
            Ok(id) => id,
            Err(_) => continue,
        };
        let head = runtime
            .block_on(cloud_storage.retrieve_cloud_shard_head_if_exists(shard_id))
            .context("failed to retrieve shard head from cloud")?;
        if let Some(height) = head {
            shard_heads.push((shard_id, height));
        }
    }
    shard_heads.sort_by_key(|(shard_id, _)| *shard_id);

    Ok(ExternalStatus { block_head, shard_heads })
}

fn collect_local(store: &Store) -> anyhow::Result<LocalStatus> {
    let cloud_min_head = store.get_ser::<Tip>(DBCol::BlockMisc, CLOUD_MIN_HEAD_KEY);
    let cloud_block_head = store.get_ser(DBCol::BlockMisc, CLOUD_BLOCK_HEAD_KEY);
    let cloud_shard_heads = read_local_shard_heads(store);
    let chain_head =
        store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY).context("HEAD not found in DB")?.height;
    let chain_final_head = store
        .get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)
        .context("FINAL_HEAD not found in DB")?
        .height;
    Ok(LocalStatus {
        cloud_min_head,
        cloud_block_head,
        cloud_shard_heads,
        chain_head,
        chain_final_head,
    })
}

fn read_local_shard_heads(store: &Store) -> Vec<(ShardId, BlockHeight)> {
    let mut shard_heads = Vec::new();
    for (key, value) in store.iter_prefix(DBCol::BlockMisc, CLOUD_SHARD_HEAD_PREFIX) {
        let shard_id_bytes = &key[CLOUD_SHARD_HEAD_PREFIX.len()..];
        let Ok(shard_id) = ShardId::try_from_slice(shard_id_bytes) else {
            continue;
        };
        let Ok(height) = BlockHeight::try_from_slice(&value) else {
            continue;
        };
        shard_heads.push((shard_id, height));
    }
    shard_heads.sort_by_key(|(shard_id, _)| *shard_id);
    shard_heads
}

fn print_external(external: &ExternalStatus) {
    println!("=== External storage (cloud) ===");
    println!("  block head:  {:?}", external.block_head);
    for (shard_id, height) in &external.shard_heads {
        println!("  shard {} head: {}", shard_id, height);
    }
}

fn print_local(local: &LocalStatus) {
    println!("=== Local storage (DB) ===");
    println!("  cloud min head:   {:?}", local.cloud_min_head.as_ref().map(|t| t.height));
    println!("  cloud block head: {:?}", local.cloud_block_head);
    for (shard_id, height) in &local.cloud_shard_heads {
        println!("  cloud shard {} head: {}", shard_id, height);
    }
    println!();
    println!("  chain HEAD:       {}", local.chain_head);
    println!("  chain FINAL_HEAD: {}", local.chain_final_head);
}

#[derive(clap::Parser)]
pub(crate) struct StatusCmd {}

impl StatusCmd {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let near_config = nearcore::config::load_config(home_dir, genesis_validation)
            .context("failed to load config")?;

        let cloud_storage_context = near_config
            .cloud_storage_context()
            .context("cloud_archival is not configured in config.json")?;
        let cloud_storage =
            CloudStorageOpener::new(cloud_storage_context, BucketConfig::canonical())
                .open()
                .context("failed to open cloud storage")?;

        let store = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
            near_config.cloud_storage_context(),
        )
        .open_in_mode(Mode::ReadOnly)
        .context("failed to open local DB")?
        .get_hot_store();

        match collect_external(&cloud_storage) {
            Ok(external) => print_external(&external),
            Err(err) => println!("=== External storage (cloud) ===\n  error: {err:#}"),
        }
        println!();

        let local = collect_local(&store)?;
        print_local(&local);

        Ok(())
    }
}
