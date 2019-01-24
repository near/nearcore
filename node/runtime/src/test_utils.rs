use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};

use primitives::types::MerkleHash;
use primitives::signer::InMemorySigner;
use primitives::test_utils::get_key_pair_from_seed;
use storage::StateDb;
use storage::test_utils::create_memory_db;
use transaction::Transaction;

use configs::ChainSpec;
use crate::state_viewer::StateDbViewer;

use super::{ApplyResult, ApplyState, Runtime};

pub fn generate_test_chain_spec() -> (ChainSpec, InMemorySigner) {
    let genesis_wasm = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm").to_vec();
    let account_id = "alice.near";
    let key_pair = get_key_pair_from_seed(account_id);
    let signer = InMemorySigner {
        account_id: account_id.to_string(),
        public_key: key_pair.0,
        secret_key: key_pair.1,
    };
    (ChainSpec {
        accounts: vec![
            ("alice.near".to_string(), get_key_pair_from_seed("alice.near").0.to_string(), 100, 10),
            ("bob.near".to_string(), get_key_pair_from_seed("bob.near").0.to_string(), 0, 10),
            ("system".to_string(), get_key_pair_from_seed("system").0.to_string(), 0, 0),
        ],
        initial_authorities: vec![(account_id.to_string(), key_pair.0.to_string(), 50)],
        genesis_wasm,
        beacon_chain_epoch_length: 2,
        beacon_chain_num_seats_per_slot: 10,
        boot_nodes: vec![],
    }, signer)
}

pub fn get_runtime_and_state_db_viewer_from_chain_spec(chain_spec: &ChainSpec) -> (Runtime, StateDbViewer, MerkleHash) {
    let storage = Arc::new(create_memory_db());
    let state_db = Arc::new(StateDb::new(storage.clone()));
    let runtime = Runtime::new(state_db.clone());
    let genesis_root = runtime.apply_genesis_state(
        &chain_spec.accounts,
        &chain_spec.genesis_wasm,
        &chain_spec.initial_authorities
    );

    let state_db_viewer = StateDbViewer::new(
        state_db.clone(),
    );
    (runtime, state_db_viewer, genesis_root)
}

pub fn get_runtime_and_state_db_viewer() -> (Runtime, StateDbViewer, MerkleHash) {
    let (chain_spec, _) = generate_test_chain_spec();
    get_runtime_and_state_db_viewer_from_chain_spec(&chain_spec)
}

pub fn get_test_state_db_viewer() -> (StateDbViewer, MerkleHash) {
    let (_, state_db_viewer, root) = get_runtime_and_state_db_viewer();
    (state_db_viewer, root)
}

pub fn encode_int(val: i32) -> [u8; 4] {
    let mut tmp = [0u8; 4];
    LittleEndian::write_i32(&mut tmp, val);
    tmp
}

impl Runtime {
    pub fn apply_all_vec(
        &mut self,
        apply_state: ApplyState,
        transactions: Vec<Transaction>,
    ) -> Vec<ApplyResult> {
        let mut cur_apply_state = apply_state;
        let mut cur_transactions = transactions;
        let mut results = vec![];
        loop {
            let apply_result = self.apply(&cur_apply_state, &[], cur_transactions);
            results.push(apply_result.clone());
            if apply_result.new_receipts.is_empty() {
                return results;
            }
            self.state_db.commit(apply_result.transaction).unwrap();
            cur_apply_state = ApplyState {
                root: apply_result.root,
                shard_id: cur_apply_state.shard_id,
                block_index: cur_apply_state.block_index,
                parent_block_hash: cur_apply_state.parent_block_hash,
            };
            cur_transactions = apply_result.new_receipts;
        }
    }

    pub fn apply_all(
        &mut self,
        apply_state: ApplyState,
        transactions: Vec<Transaction>,
    ) -> ApplyResult {
        self.apply_all_vec(apply_state, transactions).pop().unwrap()
    }
}
