use rand::Rng;
use std::borrow::Cow;
use std::path::Path;

use near_chain::types::RuntimeAdapter;
use near_chain::{ChainStore, ChainStoreAccess};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    DataReceipt, Receipt, ReceiptEnum, ReceiptOrStateStoredReceipt, ReceiptV1,
};
use near_primitives::types::{ShardId, StateChangeCause, StateRoot};
use near_store::trie::receipts_column_helper::{DelayedReceiptQueue, TrieQueue};
use near_store::{ShardTries, ShardUId, Store, TrieUpdate};
use nearcore::NearConfig;
use node_runtime::bootstrap_congestion_info;

use crate::util::load_trie;

/// A set of commands for inspecting and debugging the congestion control
/// feature. The typical scenarios are:
/// 1) Run the print command and the bootstrap command and compare the results.
/// 2) Run the prepare-benchmark command and the bootstrap command to see how
///    long it takes to bootstrap the congestion info. Please note that this
///    will corrupt the database and it should not be used on production nodes.
#[derive(clap::Subcommand)]
pub enum CongestionControlCmd {
    /// Print the congestion information.
    Print(PrintCmd),
    /// Run the congestion info bootstrapping.
    Bootstrap(BootstrapCmd),
    /// Load the trie with receipts and print new state roots that can be used
    /// for benchmarking the bootstrapping logic. Please note that running this
    /// command will corrupt the database.
    PrepareBenchmark(PrepareBenchmarkCmd),
}

impl CongestionControlCmd {
    pub(crate) fn run(&self, home_dir: &Path, near_config: NearConfig, store: Store) {
        match self {
            CongestionControlCmd::Print(cmd) => cmd.run(&near_config, store),
            CongestionControlCmd::Bootstrap(cmd) => cmd.run(home_dir, &near_config, store),
            CongestionControlCmd::PrepareBenchmark(cmd) => cmd.run(home_dir, &near_config, store),
        }
    }
}

#[derive(clap::Parser)]
pub struct PrintCmd {}

impl PrintCmd {
    pub(crate) fn run(&self, near_config: &NearConfig, store: Store) {
        let chain_store = ChainStore::new(
            store,
            near_config.genesis.config.genesis_height,
            near_config.client_config.save_trie_changes,
        );
        let head = chain_store.head().unwrap();
        let block = chain_store.get_block(&head.last_block_hash).unwrap();

        for chunk_header in block.chunks().iter() {
            let congestion_info = chunk_header.congestion_info();
            println!(
                "{:?} - {:?} - {:?}",
                chunk_header.shard_id(),
                chunk_header.prev_state_root(),
                congestion_info
            );
        }
    }
}

#[derive(clap::Parser)]
pub struct BootstrapCmd {
    #[arg(long)]
    shard_id: Option<ShardId>,
    #[arg(long)]
    state_root: Option<StateRoot>,
}

impl BootstrapCmd {
    pub(crate) fn run(&self, home_dir: &Path, near_config: &NearConfig, store: Store) {
        let (epoch_manager, runtime, state_roots, block_header) =
            load_trie(store, home_dir, near_config);

        let epoch_id = block_header.epoch_id();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

        let shard_id_state_root_list = match (self.shard_id, self.state_root) {
            (None, None) => {
                let mut result = vec![];
                for (shard_index, state_root) in state_roots.into_iter().enumerate() {
                    let shard_id = shard_layout.get_shard_id(shard_index);
                    result.push((shard_id, state_root));
                }
                result
            }
            (Some(shard_id), Some(state_root)) => vec![(shard_id, state_root)],
            _ => {
                panic!("Both shard_id and state_root must be provided");
            }
        };

        let &prev_hash = block_header.prev_hash();
        for (shard_id, state_root) in shard_id_state_root_list {
            Self::run_impl(
                epoch_manager.as_ref(),
                runtime.as_ref(),
                prev_hash,
                shard_id,
                state_root,
            );
        }
    }

    fn run_impl(
        epoch_manager: &dyn EpochManagerAdapter,
        runtime: &dyn RuntimeAdapter,
        prev_hash: CryptoHash,
        shard_id: ShardId,
        state_root: StateRoot,
    ) {
        let shard_id = shard_id as u64;
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_hash).unwrap();
        let protocol_config = runtime.get_protocol_config(&epoch_id).unwrap();
        let runtime_config = protocol_config.runtime_config;
        let trie = runtime.get_trie_for_shard(shard_id, &prev_hash, state_root, true).unwrap();

        let start_time = std::time::Instant::now();
        let congestion_info = bootstrap_congestion_info(&trie, &runtime_config, shard_id).unwrap();
        let duration = start_time.elapsed();

        println!("{:?} - {:?} - {:?}", shard_id, congestion_info, duration);
    }
}

#[derive(clap::Parser)]
pub struct PrepareBenchmarkCmd {
    // How many receipts should be added to the delayed receipts queue.
    // By default insert 10k receipts.
    #[arg(long, default_value = "10000")]
    receipt_count: u32,
    // The size in bytes of each receipt.
    // By default each receipts is 10kB.
    #[arg(long, default_value = "10000")]
    receipt_size: u32,
    // If set to true the command will not ask for confirmation.
    #[arg(long, default_value = "false")]
    yes: bool,
}

impl PrepareBenchmarkCmd {
    fn run(&self, home_dir: &Path, near_config: &NearConfig, store: Store) {
        if !self.should_run() {
            println!("aborted");
            return;
        }

        let (epoch_manager, runtime, state_roots, block_header) =
            load_trie(store, home_dir, near_config);

        let prev_hash = block_header.prev_hash();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_hash).unwrap();

        for (shard_id, &state_root) in state_roots.iter().enumerate() {
            println!("old - {:?} - {:?}", shard_id, state_root);
            let shard_id = shard_id as u64;
            let shard_uid = epoch_manager.shard_id_to_uid(shard_id, &epoch_id).unwrap();

            let tries = runtime.get_tries();

            let state_root = self.add_receipts(tries, shard_uid, state_root);
            println!("new - {:?} - {:?}", shard_id, state_root);
        }
    }

    fn should_run(&self) -> bool {
        println!("WARNING: This command will corrupt the database.");

        if self.yes {
            return true;
        }
        println!("WARNING: To bypass this confirmation use the --yes flag.");
        println!("WARNING: Do you want to continue? [y/N]");

        let mut input = String::new();
        std::io::stdin().read_line(&mut input).unwrap();
        let input = input.trim().to_lowercase();
        input == "y" || input == "yes"
    }

    fn add_receipts(
        &self,
        tries: ShardTries,
        shard_uid: ShardUId,
        state_root: StateRoot,
    ) -> StateRoot {
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        let mut trie_update = TrieUpdate::new(trie);
        let mut queue = DelayedReceiptQueue::load(&trie_update).unwrap();

        for _ in 0..self.receipt_count {
            let receipt = self.create_receipt();
            let receipt = Cow::Borrowed(&receipt);
            let receipt = ReceiptOrStateStoredReceipt::Receipt(receipt);
            queue.push(&mut trie_update, &receipt).unwrap();
        }

        trie_update.commit(StateChangeCause::UpdatedDelayedReceipts);
        let (_, trie_changes, _) = trie_update.finalize().unwrap();

        let mut store_update = tries.store_update();
        let new_state_root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit().unwrap();

        new_state_root
    }

    fn create_receipt(&self) -> Receipt {
        let predecessor_id = "predecessor".parse().unwrap();
        let receiver_id = "receiver".parse().unwrap();

        let mut rng = rand::thread_rng();

        let mut data = vec![0; self.receipt_size as usize];
        rng.fill(&mut data[..]);

        // Use the hash of the data as the id for things.
        let id = CryptoHash::hash_bytes(&data);
        let data_id = id;
        let receipt_id = id;

        let receipt = DataReceipt { data_id, data: Some(data) };
        let receipt = ReceiptEnum::Data(receipt);
        let receipt = ReceiptV1 { predecessor_id, receiver_id, receipt_id, receipt, priority: 0 };

        Receipt::V1(receipt)
    }
}
