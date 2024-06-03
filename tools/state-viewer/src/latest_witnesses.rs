use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use clap::{Parser, Subcommand};
use near_async::time::Clock;
use near_chain::runtime::NightshadeRuntime;
use near_chain::{Chain, ChainGenesis, ChainStore, DoomslugThresholdMode};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_primitives::stateless_validation::ChunkStateWitness;
use near_primitives::types::EpochId;
use near_store::Store;
use nearcore::NearConfig;
use nearcore::NightshadeRuntimeExt;

#[derive(clap::Subcommand)]
pub enum StateWitnessCmd {
    /// Prints latest state witnesses saved to DB.
    Latest(LatestWitnessesCmd),
    /// Validates given state witness and hangs.
    Validate(ValidateWitnessCmd),
}

impl StateWitnessCmd {
    pub(crate) fn run(&self, home_dir: &Path, near_config: NearConfig, store: Store) {
        match self {
            StateWitnessCmd::Latest(cmd) => cmd.run(near_config, store),
            StateWitnessCmd::Validate(cmd) => cmd.run(home_dir, near_config, store),
        }
    }
}

#[derive(clap::Parser)]
struct LatestWitnessesCmd {
    /// Block height
    #[arg(long)]
    height: Option<u64>,

    /// Shard id
    #[arg(long)]
    shard_id: Option<u64>,

    /// Epoch Id
    #[arg(long)]
    epoch_id: Option<EpochId>,

    #[clap(subcommand)]
    /// Save mode.
    mode: LatestWitnessesMode,
    // /// Pretty-print using the "{:#?}" formatting.
    // #[arg(long)]
    // pretty: bool,
    //
    // /// Print the raw &[u8], can be pasted into rust code
    // #[arg(long)]
    // binary: bool,
}

#[derive(clap::Subcommand)]
enum LatestWitnessesMode {
    /// Pretty-print on screen using the "{:#?}" formatting.
    Pretty,
    /// Saves the raw &[u8] of each witness to the given directory.
    Binary { output_dir: PathBuf },
}

impl LatestWitnessesCmd {
    pub(crate) fn run(&self, near_config: NearConfig, store: Store) {
        let chain_store =
            Rc::new(ChainStore::new(store, near_config.genesis.config.genesis_height, false));

        let witnesses = chain_store
            .get_latest_witnesses(self.height, self.shard_id, self.epoch_id.clone())
            .unwrap();
        println!("Found {} witnesses:", witnesses.len());
        if let LatestWitnessesMode::Binary(ref dir) = self.mode {
            if !dir.exists() {
                std::fs::create_dir_all(dir).unwrap();
            }
        }

        for (i, witness) in witnesses.iter().enumerate() {
            println!(
                "#{} (height: {}, shard_id: {}, epoch_id: {:?})",
                i,
                witness.chunk_header.height_created(),
                witness.chunk_header.shard_id(),
                witness.epoch_id
            );
            match self.mode {
                LatestWitnessesMode::Pretty => {
                    println!("{:#?}", witness);
                    println!("");
                }
                LatestWitnessesMode::Binary(ref dir) => {
                    let file_name = format!(
                        "witness_{}_{}_{}.bin",
                        witness.chunk_header.height_created(),
                        witness.chunk_header.shard_id(),
                        witness.epoch_id.0
                    );
                    let file_path = dir.join(file_name);
                    std::fs::write(&file_path, borsh::to_vec(witness).unwrap()).unwrap();
                    println!("Saved to {:?}", file_path);
                }
            }
        }
    }
}

#[derive(clap::Parser)]
struct ValidateWitnessCmd {
    /// File with state witness saved as vector in JSON.
    #[arg(long)]
    input_file: PathBuf,
}

impl ValidateWitnessCmd {
    pub(crate) fn run(&self, home_dir: &Path, near_config: NearConfig, store: Store) {
        let encoded_witness: Vec<u8> =
            serde_json::from_reader(BufReader::new(std::fs::File::open(&self.input_file).unwrap()))
                .unwrap();
        let witness: ChunkStateWitness = borsh::BorshDeserialize::try_from_slice(&encoded_witness)
            .expect("Failed to deserialize witness");
        let chain_genesis = ChainGenesis::new(&near_config.genesis.config);
        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);
        let runtime_adapter =
            NightshadeRuntime::from_config(home_dir, store, &near_config, epoch_manager.clone())
                .expect("could not create the transaction runtime");
        let shard_tracker = ShardTracker::new(
            TrackedConfig::from_config(&near_config.client_config),
            epoch_manager.clone(),
        );
        // TODO(stateless_validation): consider using `ChainStore` instead of
        // `Chain`.
        let chain = Chain::new_for_view_client(
            Clock::real(),
            epoch_manager.clone(),
            shard_tracker,
            runtime_adapter.clone(),
            &chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            false,
        )
        .unwrap();
        chain
            .shadow_validate_state_witness(
                witness,
                epoch_manager.as_ref(),
                runtime_adapter.as_ref(),
            )
            .unwrap();
        // Not optimal, but needed to wait for potential `validate_state_witness` failure.
        loop {
            std::thread::sleep(core::time::Duration::from_secs(60));
        }
    }
}
