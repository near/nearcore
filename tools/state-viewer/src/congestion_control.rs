use std::path::Path;

use near_chain::types::RuntimeAdapter;
use near_chain::{ChainStore, ChainStoreAccess};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{DataReceipt, Receipt, ReceiptEnum, ReceiptV1};
use near_primitives::types::{StateChangeCause, StateRoot};
use near_store::trie::receipts_column_helper::{DelayedReceiptQueue, TrieQueue};
use near_store::{ShardTries, ShardUId, Store, TrieUpdate};
use nearcore::NearConfig;
use node_runtime::bootstrap_congestion_info;

use crate::util::load_trie;

#[derive(clap::Parser)]
pub enum CongestionControlCmd {
    /// Print the congestion information.
    Print,
    /// Run the congestion info bootstrapping.
    Bootstrap,
    /// Load the trie with receipts and run the congestion info bootstrapping.
    /// TODO - doing both steps in single invocation may keep everything in
    /// memory. Split it into two steps.
    Benchmark,
}

impl CongestionControlCmd {
    pub(crate) fn run(&self, home_dir: &Path, near_config: NearConfig, store: Store) {
        match self {
            CongestionControlCmd::Print => self.run_print(&near_config, store),
            CongestionControlCmd::Bootstrap => self.run_bootstrap(home_dir, &near_config, store),
            CongestionControlCmd::Benchmark => self.run_benchmark(home_dir, &near_config, store),
        }
    }

    fn run_print(&self, near_config: &NearConfig, store: Store) {
        let chain_store = ChainStore::new(
            store,
            near_config.genesis.config.genesis_height,
            near_config.client_config.save_trie_changes,
        );
        let head = chain_store.head().unwrap();
        let block = chain_store.get_block(&head.last_block_hash).unwrap();

        for chunk_header in block.chunks().iter() {
            let congestion_info = chunk_header.congestion_info();
            println!("{:?} - {:?}", chunk_header.shard_id(), congestion_info);
        }
    }

    fn run_bootstrap(&self, home_dir: &Path, near_config: &NearConfig, store: Store) {
        let (epoch_manager, runtime, state_roots, block_header) =
            load_trie(store, home_dir, near_config);

        let prev_hash = block_header.prev_hash();

        for (shard_id, state_root) in state_roots.iter().enumerate() {
            let shard_id = shard_id as u64;
            let trie = runtime.get_trie_for_shard(shard_id, prev_hash, *state_root, true).unwrap();
            let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_hash).unwrap();
            let protocol_config = runtime.get_protocol_config(&epoch_id).unwrap();
            let runtime_config = protocol_config.runtime_config;

            let congestion_info =
                bootstrap_congestion_info(&trie, &runtime_config, shard_id).unwrap();

            println!("{:?} - {:?}", shard_id, congestion_info);
        }
    }

    fn run_benchmark(&self, home_dir: &Path, near_config: &NearConfig, store: Store) {
        let (epoch_manager, runtime, state_roots, block_header) =
            load_trie(store, home_dir, near_config);

        let prev_hash = block_header.prev_hash();

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_hash).unwrap();
        let protocol_config = runtime.get_protocol_config(&epoch_id).unwrap();
        let runtime_config = protocol_config.runtime_config;

        for (shard_id, &state_root) in state_roots.iter().enumerate() {
            let shard_id = shard_id as u64;
            let shard_uid = epoch_manager.shard_id_to_uid(shard_id, &epoch_id).unwrap();

            let tries = runtime.get_tries();

            let state_root = Self::add_receipts(tries, shard_uid, state_root);
            let trie = runtime.get_trie_for_shard(shard_id, prev_hash, state_root, true).unwrap();

            let now = std::time::Instant::now();
            let congestion_info =
                bootstrap_congestion_info(&trie, &runtime_config, shard_id).unwrap();
            let duration = now.elapsed();

            println!("{:?} - {:?}", shard_id, congestion_info);
            println!("Boostrapping took {:?}", duration);
        }
    }

    fn add_receipts(tries: ShardTries, shard_uid: ShardUId, state_root: StateRoot) -> StateRoot {
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        let mut trie_update = TrieUpdate::new(trie);
        let mut queue = DelayedReceiptQueue::load(&trie_update).unwrap();

        for _ in 0..10_000 {
            let receipt = Self::create_receipt();
            queue.push(&mut trie_update, &receipt).unwrap();
        }

        trie_update.commit(StateChangeCause::UpdatedDelayedReceipts);
        let (_, trie_changes, _) = trie_update.finalize().unwrap();

        let mut store_update = tries.store_update();
        tries.apply_all(&trie_changes, shard_uid, &mut store_update);

        trie_changes.new_root
    }

    fn create_receipt() -> Receipt {
        let predecessor_id = "predecessor".parse().unwrap();
        let receiver_id = "receiver".parse().unwrap();

        // TOOD - generate random data
        let data = vec![0, 1, 2, 3];

        let receipt = DataReceipt { data_id: CryptoHash::default(), data: Some(data) };
        let receipt = ReceiptEnum::Data(receipt);
        let receipt = ReceiptV1 {
            predecessor_id,
            receiver_id,
            receipt_id: CryptoHash::default(),
            receipt,
            priority: 0,
        };

        Receipt::V1(receipt)
    }
}
