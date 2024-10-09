use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use borsh::BorshDeserialize as _;
use near_chain::runtime::NightshadeRuntime;
use near_chain::stateless_validation::processing_tracker::ProcessingDoneTracker;
use near_chain::{Chain, ChainGenesis, ChainStore, DoomslugThresholdMode};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_primitives::challenge::PartialState;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use near_primitives::types::EpochId;
use near_store::{RawTrieNodeWithSize, Store};
use near_time::Clock;
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
    /// Pretty-print the contents on screen.
    Pretty {
        #[clap(long)]
        decode_tries: bool,
    },
    /// Saves the raw &[u8] of each witness to the given directory.
    Binary { output_dir: PathBuf },
}

impl DumpWitnessesCmd {
    fn pretty_print_partial_state(&self, partial_state: &PartialState) {
        let PartialState::TrieValues(nodes) = partial_state;
        let mut nmap = nodes.iter().map(|n| (hash(&n), (None, &**n))).collect::<BTreeMap<_, _>>();
        fn compute_size(nmap: &mut BTreeMap<CryptoHash, (Option<u64>, &[u8])>, h: &CryptoHash) {
            let Some((sz, val)) = nmap.get(h) else { return };
            if sz.is_some() {
                return;
            }
            let self_size = val.len() as u64;
            let Ok(node) = RawTrieNodeWithSize::try_from_slice(val) else {
                nmap.get_mut(h).unwrap().0.replace(self_size);
                return;
            };
            let size = match node.node {
                near_store::RawTrieNode::Leaf(_, r) => r.length as u64,
                near_store::RawTrieNode::BranchNoValue(ch) => ch
                    .iter()
                    .map(|(_, c)| {
                        compute_size(nmap, c);
                        nmap.get(c).map(|v| v.0.unwrap()).unwrap_or(0)
                    })
                    .sum(),
                near_store::RawTrieNode::BranchWithValue(v, ch) => {
                    ch.iter()
                        .map(|(_, c)| {
                            compute_size(nmap, c);
                            nmap.get(c).map(|v| v.0.unwrap()).unwrap_or(0)
                        })
                        .sum::<u64>()
                        + v.length as u64
                }
                near_store::RawTrieNode::Extension(_, c) => {
                    compute_size(nmap, &c);
                    nmap.get(&c).map(|v| v.0.unwrap()).unwrap_or(0)
                }
            };
            nmap.get_mut(h).unwrap().0.replace(self_size + size);
        }

        for n in nmap.keys().copied().collect::<Vec<_>>() {
            compute_size(&mut nmap, &n);
        }

        for node in nodes {
            let hash = hash(node);
            let Ok(node) = RawTrieNodeWithSize::try_from_slice(node) else {
                println!("  {}: Not RawTrieNodeWithSize (size: {})", hash, node.len());
                continue;
            };

            println!(
                "  {}: .memory_usage: {}, .actual_cumulative_memory: {}, .node: {:?}",
                hash,
                bytesize::ByteSize(node.memory_usage),
                bytesize::ByteSize(nmap.get(&hash).unwrap().0.unwrap()),
                node.node,
            );
        }
    }

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
                DumpWitnessesMode::Pretty { decode_tries } => {
                    let ChunkStateWitness {
                        chunk_producer,
                        epoch_id,
                        chunk_header,
                        main_state_transition,
                        source_receipt_proofs,
                        applied_receipts_hash,
                        transactions,
                        implicit_transitions,
                        new_transactions,
                        new_transactions_validation_state,
                        signature_differentiator,
                    } = witness;
                    println!("chunk_producer: {:#?}", chunk_producer);
                    println!("epoch_id: {:#?}", epoch_id);
                    println!("chunk_header: {:#?}", chunk_header);
                    println!(
                        "main_state_transition.block_hash: {:#?}",
                        main_state_transition.block_hash
                    );
                    println!(
                        "main_state_transition.post_state_root: {:#?}",
                        main_state_transition.post_state_root
                    );
                    if decode_tries {
                        println!("main_state_transition.base_state: (continued)",);
                        self.pretty_print_partial_state(&main_state_transition.base_state);
                    } else {
                        println!(
                            "main_state_transition.base_state: {:#?}",
                            main_state_transition.base_state
                        );
                    }
                    println!("source_receipt_proofs: {:#?}", source_receipt_proofs);
                    println!("applied_receipts_hash: {:#?}", applied_receipts_hash);
                    println!("transactions: {:#?}", transactions);
                    println!("implicit_transitions: {:#?}", implicit_transitions);
                    println!("new_transactions: {:#?}", new_transactions);
                    if decode_tries {
                        println!("new_transactions_validation_state: (continued)",);
                        self.pretty_print_partial_state(new_transactions_validation_state);
                    } else {
                        println!(
                            "new_transactions_validation_state: {:#?}",
                            new_transactions_validation_state
                        );
                    }
                    println!("signature_differentiator: {:#?}", signature_differentiator);
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
