use std::fs;
use std::path::Path;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use clap::Parser;
use near_chain::types::{BlockHeaderInfo, Tip};
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode, RuntimeAdapter};
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::types::EpochInfoAggregator;
use near_primitives::block::Block;
use near_primitives::block_header::BlockHeader;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::types::EpochId;
use near_primitives::utils::index_to_bytes;
use near_store::{db::RocksDB, DBCol, Store, StoreConfig, TrieChanges, HEAD_KEY};
use near_store::{StoreOpener, HEADER_HEAD_KEY};

use nearcore::{init_and_migrate_store, NightshadeRuntime};
use serde::{Deserialize, Serialize};

#[derive(Serialize, BorshSerialize, BorshDeserialize)]
pub struct SpeedyCheckpoint {
    pub current_epoch_info: EpochInfo,
    pub current_epoch_id: EpochId,
    pub last_block_header_prev_epoch: BlockHeader,
    pub block_info: BlockInfo,

    pub block_merkle_tree: PartialMerkleTree,

    // probably not needed
    pub prev_last_block_header_prev_epoch: BlockHeader,
    pub prev_last_block_header_prev_epoch_block_info: BlockInfo,
    pub prev_last_block_header_prev_epoch_block_merkle_tree: PartialMerkleTree,

    // probably not needed
    pub final_last_block_header_prev_epoch: BlockHeader,
    pub final_last_block_header_prev_epoch_block_info: BlockInfo,
    pub final_last_block_header_prev_epoch_block_merkle_tree: PartialMerkleTree,

    // maybe not needed
    pub next_epoch_info: EpochInfo,
    // maybe not needed
    pub next_epoch_id: EpochId,

    pub prev_epoch_info: EpochInfo,
    pub prev_epoch_id: EpochId,
    pub first_block_header_prev_epoch: BlockHeader,
    pub first_block_prev_epoch_block_info: BlockInfo,
}

#[derive(Parser)]
pub struct CreateCmd {
    #[clap(long)]
    home: String,

    #[clap(long)]
    destination_dir: String,
}

#[derive(Parser)]
pub struct LoadCmd {
    #[clap(long)]
    target_home: String,

    #[clap(long)]
    source_dir: String,
}

#[derive(Parser)]
enum CliSubcmd {
    Create(CreateCmd),
    Load(LoadCmd),
}

#[derive(Parser)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
struct Cli {
    #[clap(subcommand)]
    subcmd: CliSubcmd,
}

fn create_snapshot(create_cmd: CreateCmd) {
    println!("Starting");
    let path = Path::new(&create_cmd.home);

    let store = StoreOpener::with_default_config().read_only(true).home(path).open();

    // Get epoch information:
    let mut epochs = store
        .iter(DBCol::EpochInfo)
        .filter_map(|(key, value)| {
            if key.as_ref() == AGGREGATOR_KEY {
                None
            } else {
                let epoch_info = EpochInfo::try_from_slice(value.as_ref()).unwrap();
                let epoch_id = EpochId::try_from_slice(key.as_ref()).unwrap();
                Some((epoch_id, epoch_info))
            }
        })
        .collect::<Vec<(EpochId, EpochInfo)>>();

    epochs.sort_by(|a, b| a.1.epoch_height().partial_cmp(&b.1.epoch_height()).unwrap());
    ///print!("{:?}", epochs);
    // Take last two epochs
    let next_epoch = &epochs[epochs.len() - 1];
    let current_epoch = &epochs[epochs.len() - 2];
    let prev_epoch = &epochs[epochs.len() - 3];
    println!("{:?}", current_epoch);

    // Now let's fetch the last block of the prev_epoch (it's hash is exactly the 'next_epoch' id).

    let block: Block = store.get_ser(DBCol::Block, next_epoch.0.as_ref()).unwrap().unwrap();
    let prev_block: Block =
        store.get_ser(DBCol::Block, block.header().prev_hash().as_ref()).unwrap().unwrap();

    let prev_last_block_header_prev_epoch_block_info =
        store.get_ser(DBCol::BlockInfo, block.header().prev_hash().as_ref()).unwrap().unwrap();
    let final_last_block_header_prev_epoch_block_info = store
        .get_ser(DBCol::BlockInfo, block.header().last_final_block().as_ref())
        .unwrap()
        .unwrap();

    let final_block: Block =
        store.get_ser(DBCol::Block, block.header().last_final_block().as_ref()).unwrap().unwrap();

    let block_info: BlockInfo =
        store.get_ser(DBCol::BlockInfo, next_epoch.0.as_ref()).unwrap().unwrap();

    let first_block_prev_epoch: Block =
        store.get_ser(DBCol::Block, block_info.epoch_first_block().as_ref()).unwrap().unwrap();
    let first_block_prev_epoch_block_info: BlockInfo =
        store.get_ser(DBCol::BlockInfo, block_info.epoch_first_block().as_ref()).unwrap().unwrap();

    let block_merkle_tree: PartialMerkleTree =
        store.get_ser(DBCol::BlockMerkleTree, next_epoch.0.as_ref()).unwrap().unwrap();
    let prev_last_block_header_prev_epoch_block_merkle_tree = store
        .get_ser(DBCol::BlockMerkleTree, block.header().prev_hash().as_ref())
        .unwrap()
        .unwrap();

    let final_last_block_header_prev_epoch_block_merkle_tree = store
        .get_ser(DBCol::BlockMerkleTree, block.header().last_final_block().as_ref())
        .unwrap()
        .unwrap();

    let checkpoint = SpeedyCheckpoint {
        current_epoch_info: current_epoch.1.clone(),
        current_epoch_id: current_epoch.0.clone(),
        last_block_header_prev_epoch: block.header().clone(),
        final_last_block_header_prev_epoch: final_block.header().clone(),
        prev_last_block_header_prev_epoch: prev_block.header().clone(),
        block_info,
        next_epoch_info: next_epoch.1.clone(),
        next_epoch_id: next_epoch.0.clone(),
        prev_epoch_info: prev_epoch.1.clone(),
        prev_epoch_id: prev_epoch.0.clone(),
        first_block_header_prev_epoch: first_block_prev_epoch.header().clone(),
        first_block_prev_epoch_block_info,
        block_merkle_tree,
        prev_last_block_header_prev_epoch_block_info,
        final_last_block_header_prev_epoch_block_info,
        prev_last_block_header_prev_epoch_block_merkle_tree,
        final_last_block_header_prev_epoch_block_merkle_tree,
    };

    println!("{:?}", block);

    let serialized = serde_json::to_string(&checkpoint).unwrap();

    println!("{}", serialized);
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
    /*     fs::copy(
        Path::new(&load_cmd.source_dir).join("config.json"),
        Path::new(&load_cmd.target_home).join("config.json"),
    )
    .unwrap();*/

    let mut config = nearcore::config::load_config(&home_dir, GenesisValidationMode::UnsafeFast)
        .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
    let store = init_and_migrate_store(home_dir, &config).unwrap();
    let chain_genesis = ChainGenesis::from(&config.genesis);
    let runtime = Arc::new(NightshadeRuntime::with_config(
        home_dir,
        store.clone(),
        &config,
        config.client_config.trie_viewer_state_size_limit,
        config.client_config.max_gas_burnt_view,
    ));
    // This will initialize the database (add genesis block etc)
    let chain = Chain::new(
        runtime.clone(),
        &chain_genesis,
        DoomslugThresholdMode::TwoThirds,
        !config.client_config.archive,
    )
    .unwrap();

    let mut store_update = store.store_update();
    // Store epoch information.
    store_update
        .set_ser(DBCol::EpochInfo, snapshot.current_epoch_id.as_ref(), &snapshot.current_epoch_info)
        .unwrap();

    store_update
        .set_ser(DBCol::EpochInfo, snapshot.next_epoch_id.as_ref(), &snapshot.next_epoch_info)
        .unwrap();
    store_update
        .set_ser(DBCol::EpochInfo, snapshot.prev_epoch_id.as_ref(), &snapshot.prev_epoch_info)
        .unwrap();
    // Now let's insert our blocks.

    println!("Adding header: {:?}", snapshot.last_block_header_prev_epoch.hash());
    store_update
        .set_ser(
            DBCol::BlockHeader,
            snapshot.last_block_header_prev_epoch.hash().as_ref(),
            &snapshot.last_block_header_prev_epoch,
        )
        .unwrap();
    println!("Adding header: {:?}", snapshot.prev_last_block_header_prev_epoch.hash());
    store_update
        .set_ser(
            DBCol::BlockHeader,
            snapshot.prev_last_block_header_prev_epoch.hash().as_ref(),
            &snapshot.prev_last_block_header_prev_epoch,
        )
        .unwrap();
    println!("Adding header: {:?}", snapshot.final_last_block_header_prev_epoch.hash());
    store_update
        .set_ser(
            DBCol::BlockHeader,
            snapshot.final_last_block_header_prev_epoch.hash().as_ref(),
            &snapshot.final_last_block_header_prev_epoch,
        )
        .unwrap();

    println!(
        "Adding header (first block prev epoch): {:?}",
        snapshot.first_block_header_prev_epoch.hash()
    );
    store_update
        .set_ser(
            DBCol::BlockHeader,
            snapshot.first_block_header_prev_epoch.hash().as_ref(),
            &snapshot.first_block_header_prev_epoch,
        )
        .unwrap();

    store_update
        .insert_ser(
            DBCol::BlockInfo,
            snapshot.last_block_header_prev_epoch.hash().as_ref(),
            &snapshot.block_info,
        )
        .unwrap();
    store_update
        .insert_ser(
            DBCol::BlockInfo,
            snapshot.first_block_header_prev_epoch.hash().as_ref(),
            &snapshot.first_block_prev_epoch_block_info,
        )
        .unwrap();

    store_update
        .insert_ser(
            DBCol::BlockInfo,
            snapshot.prev_last_block_header_prev_epoch.hash().as_ref(),
            &snapshot.prev_last_block_header_prev_epoch_block_info,
        )
        .unwrap();

    store_update
        .insert_ser(
            DBCol::BlockInfo,
            snapshot.final_last_block_header_prev_epoch.hash().as_ref(),
            &snapshot.final_last_block_header_prev_epoch_block_info,
        )
        .unwrap();
    store_update
        .set_ser(
            DBCol::BlockMisc,
            HEADER_HEAD_KEY,
            &Tip::from_header(&snapshot.last_block_header_prev_epoch),
        )
        .unwrap();

    store_update
        .set_ser(
            DBCol::BlockMerkleTree,
            snapshot.last_block_header_prev_epoch.hash().as_ref(),
            &snapshot.block_merkle_tree,
        )
        .unwrap();

    store_update
        .set_ser(
            DBCol::BlockMerkleTree,
            snapshot.prev_last_block_header_prev_epoch.hash().as_ref(),
            &snapshot.prev_last_block_header_prev_epoch_block_merkle_tree,
        )
        .unwrap();

    store_update
        .set_ser(
            DBCol::BlockMerkleTree,
            snapshot.final_last_block_header_prev_epoch.hash().as_ref(),
            &snapshot.final_last_block_header_prev_epoch_block_merkle_tree,
        )
        .unwrap();

    // Maybe broken -- sending empty aggregator.
    let foo = EpochInfoAggregator::new(
        snapshot.prev_epoch_id,
        *snapshot.final_last_block_header_prev_epoch.hash(),
    );
    store_update.set_ser(DBCol::EpochInfo, AGGREGATOR_KEY, &foo).unwrap();

    store_update
        .set_ser(
            DBCol::BlockHeight,
            &index_to_bytes(snapshot.last_block_header_prev_epoch.height()),
            snapshot.last_block_header_prev_epoch.hash(),
        )
        .unwrap();

    store_update.commit().unwrap();
    /*
    let another_store_update = runtime
        .add_validator_proposals(BlockHeaderInfo::new(
            &snapshot.last_block_header_prev_epoch,
            snapshot.final_last_block_header_prev_epoch.height(),
        ))
        .unwrap();

    another_store_update.commit().unwrap();*/

    /*
    let last_finalized_height =
        chain_update.chain_store_update.get_block_height(header.last_final_block())?;
    let epoch_manager_update = chain_update
        .runtime_adapter
        .add_validator_proposals(BlockHeaderInfo::new(header, last_finalized_height))?;
    */
}

fn main() {
    let args = Cli::parse();

    match args.subcmd {
        CliSubcmd::Create(create_cmd) => create_snapshot(create_cmd),
        CliSubcmd::Load(load_cmd) => load_snapshot(load_cmd),
    }
}
