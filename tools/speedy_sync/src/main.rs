use borsh::{BorshDeserialize, BorshSerialize};
use near_chain::types::{ChainConfig, Tip};
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::types::EpochInfoAggregator;
use near_epoch_manager::EpochManager;
use near_primitives::block::Block;
use near_primitives::block_header::BlockHeader;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::types::EpochId;
use near_primitives::utils::index_to_bytes;
use near_store::HEADER_HEAD_KEY;
use near_store::{DBCol, Mode, NodeStorage, Store, StoreUpdate};
use nearcore::NightshadeRuntime;
use std::fs;
use std::path::Path;

#[derive(serde::Serialize, BorshSerialize, BorshDeserialize)]
pub struct BlockCheckpoint {
    pub header: BlockHeader,
    pub info: BlockInfo,
    pub merkle_tree: PartialMerkleTree,
}

#[derive(serde::Serialize, BorshSerialize, BorshDeserialize)]
pub struct EpochCheckpoint {
    pub id: EpochId,
    pub info: EpochInfo,
}

#[derive(serde::Serialize, BorshSerialize, BorshDeserialize)]
pub struct SpeedyCheckpoint {
    pub prev_epoch: EpochCheckpoint,
    pub current_epoch: EpochCheckpoint,
    pub next_epoch: EpochCheckpoint,

    pub block: BlockCheckpoint,
    pub prev_block: BlockCheckpoint,
    pub final_block: BlockCheckpoint,
    pub first_block: BlockCheckpoint,
}

#[derive(clap::Parser)]
pub struct CreateCmd {
    #[clap(long)]
    home: String,

    #[clap(long)]
    destination_dir: String,
}

#[derive(clap::Parser)]
pub struct LoadCmd {
    #[clap(long)]
    target_home: String,

    #[clap(long)]
    source_dir: String,
}

#[derive(clap::Parser)]
enum CliSubcmd {
    Create(CreateCmd),
    Load(LoadCmd),
}

#[derive(clap::Parser)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
struct Cli {
    #[clap(subcommand)]
    subcmd: CliSubcmd,
}

fn read_block_checkpoint(store: &Store, block_hash: &CryptoHash) -> BlockCheckpoint {
    let block: Block = store
        .get_ser(DBCol::Block, block_hash.as_ref())
        .expect(format!("DB error Block {:?}", block_hash).as_str())
        .expect(format!("Key missing Block {}", block_hash).as_str());

    let info: BlockInfo = store
        .get_ser(DBCol::BlockInfo, block_hash.as_ref())
        .expect(format!("DB error BlockInfo {:?}", block_hash).as_str())
        .expect(format!("Key missing BlockInfo {}", block_hash).as_str());

    let merkle_tree: PartialMerkleTree = store
        .get_ser(DBCol::BlockMerkleTree, block_hash.as_ref())
        .expect(format!("DB error BlockMerkleTree {:?}", block_hash).as_str())
        .expect(format!("Key missing BlockMerkleTree {}", block_hash).as_str());

    BlockCheckpoint { header: block.header().clone(), info, merkle_tree }
}

fn write_block_checkpoint(store_update: &mut StoreUpdate, block_checkpoint: &BlockCheckpoint) {
    let hash = block_checkpoint.header.hash();
    store_update
        .set_ser(DBCol::BlockHeader, hash.as_ref(), &block_checkpoint.header)
        .expect("Failed writing a header");

    store_update
        .insert_ser(DBCol::BlockInfo, hash.as_ref(), &block_checkpoint.info)
        .expect("Failed writing a block info");

    store_update
        .set_ser(DBCol::BlockMerkleTree, hash.as_ref(), &block_checkpoint.merkle_tree)
        .expect("Failed writing merkle tree");
    store_update
        .set_ser(
            DBCol::BlockHeight,
            &index_to_bytes(block_checkpoint.header.height()),
            block_checkpoint.header.hash(),
        )
        .unwrap();
}

fn write_epoch_checkpoint(store_update: &mut StoreUpdate, epoch_checkpoint: &EpochCheckpoint) {
    store_update
        .set_ser(DBCol::EpochInfo, epoch_checkpoint.id.as_ref(), &epoch_checkpoint.info)
        .expect("Failed to write epoch info");
}

fn create_snapshot(create_cmd: CreateCmd) {
    let path = Path::new(&create_cmd.home);
    let store = NodeStorage::opener(path, false, &Default::default(), None)
        .open_in_mode(Mode::ReadOnly)
        .unwrap()
        .get_hot_store();

    // Get epoch information:
    let mut epochs = store
        .iter(DBCol::EpochInfo)
        .filter_map(|result| {
            if let Ok((key, value)) = result {
                if key.as_ref() == AGGREGATOR_KEY {
                    None
                } else {
                    let info = EpochInfo::try_from_slice(value.as_ref()).unwrap();
                    let id = EpochId::try_from_slice(key.as_ref()).unwrap();
                    Some(EpochCheckpoint { id, info })
                }
            } else {
                None
            }
        })
        .collect::<Vec<EpochCheckpoint>>();

    assert!(epochs.len() > 4, "Number of epochs must be greater than 4.");

    epochs.sort_by(|a, b| a.info.epoch_height().partial_cmp(&b.info.epoch_height()).unwrap());
    // Take last two epochs
    let next_epoch = epochs.pop().unwrap();
    let current_epoch = epochs.pop().unwrap();
    let prev_epoch = epochs.pop().unwrap();

    // We need information about 4 blocks to start the chain:
    //
    // 'block' - we'll always pick the last block of a given epoch.
    // 'prev_block' - its predecessor
    // 'final_block' - the block with finality (usually 2 blocks behind)
    // 'first_block' - the first block of this epoch (usualy epoch_length behind).

    let block_hash = next_epoch.id.0;
    let block = read_block_checkpoint(&store, &block_hash);
    let block_header = block.header.clone();
    let prev_block = read_block_checkpoint(&store, block_header.prev_hash());
    let final_block = read_block_checkpoint(&store, block_header.last_final_block());
    let first_block = read_block_checkpoint(&store, block.info.epoch_first_block());

    let checkpoint = SpeedyCheckpoint {
        prev_epoch,
        current_epoch,
        next_epoch,
        block,
        prev_block,
        final_block,
        first_block,
    };

    let serialized = serde_json::to_string(&checkpoint).unwrap();

    fs::write(Path::new(&create_cmd.destination_dir).join("snapshot.json"), serialized)
        .expect("Failed writing to destination file");

    fs::write(
        Path::new(&create_cmd.destination_dir).join("snapshot.borsh"),
        checkpoint.try_to_vec().unwrap(),
    )
    .expect("Failed writing to destination file");

    fs::copy(
        Path::new(&create_cmd.home).join("genesis.json"),
        Path::new(&create_cmd.destination_dir).join("genesis.json"),
    )
    .unwrap();
    fs::copy(
        Path::new(&create_cmd.home).join("config.json"),
        Path::new(&create_cmd.destination_dir).join("config.json"),
    )
    .unwrap();
}

fn load_snapshot(load_cmd: LoadCmd) {
    let data = fs::read(Path::new(&load_cmd.source_dir).join("snapshot.borsh"))
        .expect("Failed reading snapshot.borsh");

    let snapshot: SpeedyCheckpoint = SpeedyCheckpoint::try_from_slice(data.as_ref()).unwrap();

    let home_dir = Path::new(&load_cmd.target_home);
    fs::copy(
        Path::new(&load_cmd.source_dir).join("genesis.json"),
        Path::new(&load_cmd.target_home).join("genesis.json"),
    )
    .unwrap();
    fs::copy(
        Path::new(&load_cmd.source_dir).join("config.json"),
        Path::new(&load_cmd.target_home).join("config.json"),
    )
    .unwrap();

    let config = nearcore::config::load_config(&home_dir, GenesisValidationMode::UnsafeFast)
        .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
    let store = NodeStorage::opener(home_dir, config.config.archive, &Default::default(), None)
        .open()
        .unwrap()
        .get_hot_store();
    let chain_genesis = ChainGenesis::new(&config.genesis);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &config.genesis.config);
    let shard_tracker =
        ShardTracker::new(TrackedConfig::from_config(&config.client_config), epoch_manager.clone());
    let runtime =
        NightshadeRuntime::from_config(home_dir, store.clone(), &config, epoch_manager.clone());
    // This will initialize the database (add genesis block etc)
    let _chain = Chain::new(
        epoch_manager,
        shard_tracker,
        runtime,
        &chain_genesis,
        DoomslugThresholdMode::TwoThirds,
        ChainConfig {
            save_trie_changes: config.client_config.save_trie_changes,
            background_migration_threads: 1,
            state_snapshot_every_n_blocks: None,
        },
        None,
    )
    .unwrap();

    let mut store_update = store.store_update();
    // Store epoch information.
    write_epoch_checkpoint(&mut store_update, &snapshot.current_epoch);
    write_epoch_checkpoint(&mut store_update, &snapshot.prev_epoch);
    write_epoch_checkpoint(&mut store_update, &snapshot.next_epoch);

    // Store blocks.
    write_block_checkpoint(&mut store_update, &snapshot.block);
    write_block_checkpoint(&mut store_update, &snapshot.prev_block);
    write_block_checkpoint(&mut store_update, &snapshot.final_block);
    write_block_checkpoint(&mut store_update, &snapshot.first_block);

    // Store the HEADER_KEY (used in header sync).
    store_update
        .set_ser(DBCol::BlockMisc, HEADER_HEAD_KEY, &Tip::from_header(&snapshot.block.header))
        .unwrap();

    // TODO: confirm if this aggregator can be empty.
    // If not - we'll have to compute one and put it in the checkpoint.
    let aggregator =
        EpochInfoAggregator::new(snapshot.prev_epoch.id, *snapshot.final_block.header.hash());
    store_update.set_ser(DBCol::EpochInfo, AGGREGATOR_KEY, &aggregator).unwrap();
    store_update.commit().unwrap();
}

fn main() {
    let args: Cli = clap::Parser::parse();
    match args.subcmd {
        CliSubcmd::Create(create_cmd) => create_snapshot(create_cmd),
        CliSubcmd::Load(load_cmd) => load_snapshot(load_cmd),
    }
}
