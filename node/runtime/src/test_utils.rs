use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};

use primitives::types::MerkleHash;
use primitives::signature::{get_key_pair, DEFAULT_SIGNATURE};
use primitives::signer::InMemorySigner;
use primitives::hash::CryptoHash;
use primitives::test_utils::get_key_pair_from_seed;
use storage::StateDb;
use storage::test_utils::create_memory_db;
use transaction::{Transaction, TransactionBody, SignedTransaction, DeployContractTransaction, TransactionStatus, FunctionCallTransaction, SendMoneyTransaction};

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
            let apply_result = self.apply(&cur_apply_state, &[], &cur_transactions);
            results.push(apply_result.clone());
            if apply_result.new_receipts.is_empty() {
                return results;
            }
            self.state_db.commit(apply_result.db_changes).unwrap();
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

pub struct User {
    runtime: Runtime,
    account_id: String,
    nonce: u64,
}

impl User {
    pub fn new(runtime: Runtime, account_id: &str) -> Self {
        User {
            runtime, account_id: account_id.to_string(), nonce: 1
        }
    }

    fn send_tx(&mut self, root: CryptoHash, tx_body: TransactionBody) -> MerkleHash {
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let apply_results = self.runtime.apply_all_vec(
            apply_state, vec![Transaction::SignedTransaction(transaction)]
        );
        for apply_result in apply_results.iter() {
            assert_eq!(apply_result.tx_result[0].status, TransactionStatus::Completed, "{:?}", apply_result);
        }
        let last_apply_result = apply_results[apply_results.len() - 1].clone();
        self.runtime.state_db.commit(last_apply_result.db_changes).unwrap();
        last_apply_result.root
    }

    pub fn send_money(&mut self, root: MerkleHash, destination: &str, amount: u64) -> MerkleHash {
        let tx_body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: self.nonce,
            originator: self.account_id.clone(),
            receiver: destination.to_string(),
            amount,
        });
        self.nonce += 1;
        self.send_tx(root, tx_body)
    }

    pub fn deploy_contract(&mut self, root: MerkleHash, contract_id: &str, wasm_binary: &[u8]) -> MerkleHash {
        let (pk, _) = get_key_pair();
        let tx_body = TransactionBody::DeployContract(DeployContractTransaction {
            nonce: self.nonce,
            originator: self.account_id.clone(),
            contract_id: contract_id.to_string(),
            public_key: pk.0[..].to_vec(),
            wasm_byte_array: wasm_binary.to_vec(),
        });
        self.nonce += 1;
        self.send_tx(root, tx_body)
    }

    pub fn call_function(&mut self, root: MerkleHash, contract_id: &str, method_name: &str, args: &str) -> MerkleHash {
        let tx_body = TransactionBody::FunctionCall(FunctionCallTransaction {
                nonce: self.nonce,
                originator: self.account_id.clone(),
                contract_id: contract_id.to_string(),
                method_name: method_name.as_bytes().to_vec(),
                args: args.as_bytes().to_vec(),
                amount: 0
        });
        self.nonce += 1;
        self.send_tx(root, tx_body)
    }
}

pub fn setup_test_contract(wasm_binary: &[u8]) -> (User, CryptoHash) {
    let (runtime, _, genesis_root) = get_runtime_and_state_db_viewer();
    let mut user = User::new(runtime, "alice.near");
    let root = user.deploy_contract(genesis_root, "test", wasm_binary);
    assert_ne!(root, genesis_root);
    (user, root)
}
