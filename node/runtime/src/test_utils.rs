use state_viewer::StateDbViewer;
use chain_spec::ChainSpec;
use primitives::signature::{PublicKey, get_keypair};
use primitives::types::Transaction;
use std::sync::Arc;
use storage::test_utils::create_memory_db;
use storage::StateDb;
use shard::SignedShardBlock;
use chain::BlockChain;
use byteorder::{ByteOrder, LittleEndian};
use super::{Runtime, ApplyResult, ApplyState};

pub fn generate_test_chain_spec() -> ChainSpec {
    let genesis_wasm = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm").to_vec();
    let public_keys: Vec<PublicKey> = (0..3).map(|_| get_keypair().0).collect();
    ChainSpec {
        accounts: vec![
            ("alice".to_string(), public_keys[0].to_string(), 100),
            ("bob".to_string(), public_keys[1].to_string(), 0),
            ("system".to_string(), public_keys[2].to_string(), 0),
        ],
        initial_authorities: vec![(public_keys[0].to_string(), 50)],
        genesis_wasm,
        beacon_chain_epoch_length: 2,
        beacon_chain_num_seats_per_slot: 10,
    }
}

pub fn get_runtime_and_state_db_viewer() -> (Runtime, StateDbViewer) {
    let chain_spec = generate_test_chain_spec();
    let storage = Arc::new(create_memory_db());
    let state_db = Arc::new(StateDb::new(storage.clone()));
    let runtime = Runtime::new(state_db.clone());
    let genesis_root = runtime.apply_genesis_state(
        &chain_spec.accounts,
        &chain_spec.genesis_wasm,
        &chain_spec.initial_authorities
    );

    let shard_genesis = SignedShardBlock::genesis(genesis_root);
    let shard_chain = Arc::new(BlockChain::new(shard_genesis, storage));

    let state_db_viewer = StateDbViewer::new(
        shard_chain.clone(),
        state_db.clone(),
    );
    (runtime, state_db_viewer)
}

pub fn get_test_state_db_viewer() -> StateDbViewer {
    let (_, state_db_viewer) = get_runtime_and_state_db_viewer();
    state_db_viewer
}

pub fn encode_int(val: i32) -> [u8; 4] {
    let mut tmp = [0u8; 4];
    LittleEndian::write_i32(&mut tmp, val);
    tmp
}

impl Runtime {
    pub fn apply_all(
        &mut self,
        apply_state: ApplyState,
        transactions: Vec<Transaction>,
    ) -> ApplyResult {
        let mut cur_apply_state = apply_state;
        let mut cur_transactions = transactions;
        loop {
            let mut apply_result = self.apply(&cur_apply_state, &[], cur_transactions);
            if apply_result.new_receipts.is_empty() {
                return apply_result;
            }
            self.state_db.commit(&mut apply_result.transaction).unwrap();
            cur_apply_state = ApplyState {
                root: apply_result.root,
                shard_id: cur_apply_state.shard_id,
                block_index: cur_apply_state.block_index,
                parent_block_hash: cur_apply_state.parent_block_hash,
            };
            cur_transactions = apply_result.new_receipts;
        }
    }
}