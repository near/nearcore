use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;

use crate::commands::apply_range;
use near_chain::runtime::NightshadeRuntime;
use near_chain::stateless_validation::processing_tracker::ProcessingDoneTracker;
use near_chain::stateless_validation::state_witness::CreateWitnessResult;
use near_chain::{Chain, ChainGenesis, ChainStore, DoomslugThresholdMode};
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_assignment::shard_id_to_index;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use near_primitives::types::{AccountId, BlockHeight, EpochId, ShardId};
use near_store::Store;
use near_time::Clock;
use nearcore::NearConfig;
use nearcore::NightshadeRuntimeExt;

pub enum DumpWitnessesSource {
    /// Dumps latest saved witnesses.
    Latest,
    /// Dumps saved invalid witnesses.
    Invalid,
}

#[derive(clap::Subcommand)]
pub enum StateWitnessCmd {
    /// Generates and validates resulting state witnesses for the given range
    /// of blocks.
    Generate(GenerateWitnessesCmd),
    /// Dumps some of the stored latest state witnesses.
    DumpLatest(DumpWitnessesCmd),
    /// Dumps some of the stored invalid state witnesses.
    DumpInvalid(DumpWitnessesCmd),
    /// Validates given state witness.
    Validate(ValidateWitnessCmd),
}

impl StateWitnessCmd {
    pub(crate) fn run(&self, home_dir: &Path, near_config: NearConfig, store: Store) {
        match self {
            StateWitnessCmd::Generate(cmd) => cmd.run(home_dir, near_config, store),
            StateWitnessCmd::DumpLatest(cmd) => {
                cmd.run(near_config, store, DumpWitnessesSource::Latest)
            }
            StateWitnessCmd::DumpInvalid(cmd) => {
                cmd.run(near_config, store, DumpWitnessesSource::Invalid)
            }
            StateWitnessCmd::Validate(cmd) => cmd.run(home_dir, near_config, store),
        }
    }
}

/// TODO(stateless_validation): consider using `ChainStore` instead of
/// `Chain`.
fn setup_chain(home_dir: &Path, near_config: NearConfig, store: Store) -> Chain {
    let chain_genesis = ChainGenesis::new(&near_config.genesis.config);
    let epoch_manager =
        EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config, Some(home_dir));
    let runtime_adapter =
        NightshadeRuntime::from_config(home_dir, store, &near_config, epoch_manager.clone())
            .expect("could not create the transaction runtime");
    let shard_tracker = ShardTracker::new(
        near_config.client_config.tracked_shards_config,
        epoch_manager.clone(),
        near_config.validator_signer.clone(),
    );
    Chain::new_for_view_client(
        Clock::real(),
        epoch_manager,
        shard_tracker,
        runtime_adapter,
        &chain_genesis,
        DoomslugThresholdMode::TwoThirds,
        false,
        near_config.validator_signer,
    )
    .unwrap()
}

#[derive(clap::Parser)]
pub struct GenerateWitnessesCmd {
    /// Start block height.
    #[arg(long)]
    start_height: BlockHeight,
    /// End block height.
    #[arg(long)]
    end_height: BlockHeight,
    /// Shard id.
    #[arg(long)]
    shard_id: ShardId,
}

impl GenerateWitnessesCmd {
    pub(crate) fn run(&self, home_dir: &Path, near_config: NearConfig, store: Store) {
        let chain = setup_chain(home_dir, near_config.clone(), store.clone());
        let epoch_manager = chain.epoch_manager.clone();
        let chain_store = chain.chain_store();

        let last_block_hash = chain_store.get_block_hash_by_height(self.end_height).unwrap();
        let last_block = chain_store.get_block(&last_block_hash).unwrap();
        let shard_index = shard_id_to_index(
            epoch_manager.as_ref(),
            self.shard_id,
            last_block.header().epoch_id(),
        )
        .unwrap();

        // Find the last chunk's height in the range.
        let last_block_chunks = last_block.chunks();
        let last_chunk_header = last_block_chunks.get(shard_index).unwrap();
        let last_chunk_prev_hash = last_chunk_header.prev_block_hash();
        let last_chunk_hash = chain_store.get_next_block_hash(last_chunk_prev_hash).unwrap();
        let end_height = chain_store.get_block_height(&last_chunk_hash).unwrap();

        // To generate witnesses for the first chunk in the range, we need to
        // save state transitions since the previous chunk.
        let first_block_hash = chain_store.get_block_hash_by_height(self.start_height).unwrap();
        let first_block = chain_store.get_block(&first_block_hash).unwrap();
        let first_block_chunks = first_block.chunks();
        let first_chunk_header = first_block_chunks.get(shard_index).unwrap();
        let first_chunk_prev_hash = first_chunk_header.prev_block_hash();
        let first_chunk_prev_block = chain_store.get_block(&first_chunk_prev_hash).unwrap();
        let first_chunk_prev_block_chunks = first_chunk_prev_block.chunks();
        let prev_first_chunk_header = first_chunk_prev_block_chunks.get(shard_index).unwrap();
        let prev_first_chunk_prev_hash = prev_first_chunk_header.prev_block_hash();
        let prev_first_chunk_prev_block =
            chain_store.get_block(&prev_first_chunk_prev_hash).unwrap();
        let start_height = prev_first_chunk_prev_block.header().height() + 1;

        apply_range(
            crate::cli::ApplyRangeMode::Sequential { save_state_transitions: true },
            crate::cli::StorageSource::TrieFree,
            Some(start_height),
            Some(end_height),
            self.shard_id,
            false,
            None,
            home_dir,
            near_config,
            store,
            None,
            false,
        );

        for height in self.start_height..=self.end_height {
            let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
            let block = chain_store.get_block(&block_hash).unwrap();
            let block_chunks = block.chunks();
            let chunk_header = block_chunks.get(shard_index).unwrap();
            if !chunk_header.is_new_chunk(height) {
                continue;
            }

            let prev_block_hash = block.header().prev_hash();
            let prev_block = chain_store.get_block(&prev_block_hash).unwrap();
            let prev_block_chunks = prev_block.chunks();
            let prev_chunk_header = prev_block_chunks.get(shard_index).unwrap();
            let chunk_hash = chunk_header.chunk_hash();
            let chunk = chain_store.get_chunk(&chunk_hash).unwrap();

            let CreateWitnessResult { state_witness, .. } = chain_store
                .create_state_witness(
                    epoch_manager.as_ref(),
                    AccountId::from_str("alice.near").unwrap(),
                    prev_block.header(),
                    prev_chunk_header,
                    &chunk,
                )
                .unwrap();
            let processing_done_tracker = ProcessingDoneTracker::new();
            let waiter = processing_done_tracker.make_waiter();
            chain
                .shadow_validate_state_witness(
                    state_witness,
                    chain.epoch_manager.as_ref(),
                    Some(processing_done_tracker),
                )
                .unwrap();
            waiter.wait();
        }
        println!("Validation finished. Use `RUST_LOG=debug` to see validation result");
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
    pub(crate) fn run(&self, near_config: NearConfig, store: Store, source: DumpWitnessesSource) {
        let chain_store = Rc::new(ChainStore::new(
            store,
            false,
            near_config.genesis.config.transaction_validity_period,
        ));

        let witnesses = match source {
            DumpWitnessesSource::Latest => {
                chain_store.get_latest_witnesses(self.height, self.shard_id, self.epoch_id).unwrap()
            }
            DumpWitnessesSource::Invalid => chain_store
                .get_invalid_witnesses(self.height, self.shard_id, self.epoch_id)
                .unwrap(),
        };
        println!("Found {} witnesses:", witnesses.len());
        if let DumpWitnessesMode::Binary { ref output_dir } = self.mode {
            if !output_dir.exists() {
                std::fs::create_dir_all(output_dir).unwrap();
            }
        }

        for (i, witness) in witnesses.iter().enumerate() {
            let ChunkProductionKey { shard_id, height_created, epoch_id } =
                witness.chunk_production_key();

            println!(
                "#{} (height: {}, shard_id: {}, epoch_id: {:?})",
                i, height_created, shard_id, epoch_id
            );
            match self.mode {
                DumpWitnessesMode::Pretty => {
                    println!("{:#?}", witness);
                    println!("");
                }
                DumpWitnessesMode::Binary { ref output_dir } => {
                    let file_name =
                        format!("witness_{}_{}_{}_{}.bin", height_created, shard_id, epoch_id.0, i);
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
        let chain = setup_chain(home_dir, near_config, store);
        let processing_done_tracker = ProcessingDoneTracker::new();
        let waiter = processing_done_tracker.make_waiter();
        chain
            .shadow_validate_state_witness(
                witness,
                chain.epoch_manager.as_ref(),
                Some(processing_done_tracker),
            )
            .unwrap();
        waiter.wait();
        println!("Validation finished. Use `RUST_LOG=debug` to see validation result");
    }
}
