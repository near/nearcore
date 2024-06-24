use std::path::{Path, PathBuf};
use std::rc::Rc;

use near_async::time::Clock;
use near_chain::runtime::NightshadeRuntime;
use near_chain::stateless_validation::processing_tracker::ProcessingDoneTracker;
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
    /// Dumps some of the latest stored state witnesses.
    Dump(DumpWitnessesCmd),
    /// Validates given state witness.
    Validate(ValidateWitnessCmd),
}

impl StateWitnessCmd {
    pub(crate) fn run(&self, home_dir: &Path, near_config: NearConfig, store: Store) {
        match self {
            StateWitnessCmd::Dump(cmd) => cmd.run(near_config, store),
            StateWitnessCmd::Validate(cmd) => cmd.run(home_dir, near_config, store),
        }
    }
}

#[derive(clap::Parser)]
pub struct DumpWitnessesCmd {
    /// Select received witnesses only with given block height.
    #[arg(long)]
    height: Option<u64>,
    /// Select only witnesses for given shard id.
    #[arg(long)]
    shard_id: Option<u64>,
    /// Select only witnesses for given epoch.
    #[arg(long)]
    epoch_id: Option<EpochId>,
    #[clap(subcommand)]
    /// Mode of dumping state witnesses.
    mode: DumpWitnessesMode,
}

#[derive(clap::Subcommand)]
enum DumpWitnessesMode {
    /// Pretty-print on screen using the "{:#?}" formatting.
    Pretty,
    /// Saves the raw &[u8] of each witness to the given directory.
    Binary { output_dir: PathBuf },
}

impl DumpWitnessesCmd {
    pub(crate) fn run(&self, near_config: NearConfig, store: Store) {
        let chain_store =
            Rc::new(ChainStore::new(store, near_config.genesis.config.genesis_height, false));

        let witnesses =
            chain_store.get_latest_witnesses(self.height, self.shard_id, self.epoch_id).unwrap();
        println!("Found {} witnesses:", witnesses.len());
        if let DumpWitnessesMode::Binary { ref output_dir } = self.mode {
            if !output_dir.exists() {
                std::fs::create_dir_all(output_dir).unwrap();
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
                DumpWitnessesMode::Pretty => {
                    println!("{:#?}", witness);
                    println!("");
                }
                DumpWitnessesMode::Binary { ref output_dir } => {
                    let file_name = format!(
                        "witness_{}_{}_{}_{}.bin",
                        witness.chunk_header.height_created(),
                        witness.chunk_header.shard_id(),
                        witness.epoch_id.0,
                        i
                    );
                    let file_path = output_dir.join(file_name);
                    std::fs::write(&file_path, borsh::to_vec(witness).unwrap()).unwrap();
                    println!("Saved to {:?}", file_path);
                }
            }
        }
    }
}

#[derive(clap::Parser)]
pub struct ValidateWitnessCmd {
    /// File with state witness saved as raw &[u8].
    #[arg(long)]
    input_file: PathBuf,
}

impl ValidateWitnessCmd {
    pub(crate) fn run(&self, home_dir: &Path, near_config: NearConfig, store: Store) {
        let encoded_witness: Vec<u8> =
            std::fs::read(&self.input_file).expect("Failed to read file");
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
        let processing_done_tracker = ProcessingDoneTracker::new();
        let waiter = processing_done_tracker.make_waiter();
        chain
            .shadow_validate_state_witness(
                witness,
                epoch_manager.as_ref(),
                runtime_adapter.as_ref(),
                Some(processing_done_tracker),
            )
            .unwrap();
        waiter.wait();
        println!("Validation finished. Use `RUST_LOG=debug` to see validation result");
    }
}
