use near_crypto::{InMemorySigner, KeyType};
use near_primitives::account::AccessKey;
use near_primitives::hash::hash;
use near_primitives::receipt::Receipt;
use near_primitives::serialize::to_base64;
use near_primitives::transaction::{Action, FunctionCallAction, SignedTransaction, TransactionLog};
use near_primitives::types::{Balance, MerkleHash, ShardId};
use near_primitives::views::AccountView;
use near_store::test_utils::create_trie;
use near_store::{Trie, TrieUpdate};
use node_runtime::config::RuntimeConfig;
use node_runtime::ethereum::EthashProvider;
use node_runtime::{ApplyState, Runtime, StateRecord};
use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;
use tempdir::TempDir;

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000_000_000;
/// Validator's stake used in tests.
pub const TESTING_INIT_STAKE: Balance = 50_000_000;

pub struct StandaloneRuntime {
    // We hold the reference to the temporary folder to avoid it being cleaned up by the drop.
    #[allow(dead_code)]
    ethash_dir: TempDir,
    apply_state: ApplyState,
    runtime: Runtime,
    trie: Arc<Trie>,
    signer: InMemorySigner,
    shard_id: u64,
}

impl StandaloneRuntime {
    /// Get records that initialize the runtime.
    fn get_state_records(
        num_runtimes: u64,
        contract_code: &[u8],
    ) -> (Vec<StateRecord>, Vec<InMemorySigner>) {
        let code_hash = hash(contract_code);
        let mut state_records = vec![];
        let mut signers = vec![];
        for i in 0..num_runtimes {
            let account_id = format!("near.{}", i);
            let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
            state_records.push(StateRecord::Account {
                account_id: account_id.to_string(),
                account: AccountView {
                    amount: TESTING_INIT_BALANCE,
                    staked: TESTING_INIT_STAKE,
                    code_hash: code_hash.clone().into(),
                    storage_usage: 0,
                    storage_paid_at: 0,
                },
            });
            state_records.push(StateRecord::AccessKey {
                account_id: account_id.to_string(),
                public_key: signer.public_key.into(),
                access_key: AccessKey::full_access().into(),
            });
            state_records
                .push(StateRecord::Contract { account_id, code: to_base64(contract_code) });
            signers.push(signer);
        }
        (state_records, signers)
    }

    pub fn new(runtime_idx: u64, num_runtimes: u64, contract_code: &[u8]) -> Self {
        let trie = create_trie();
        let ethash_dir = TempDir::new(format!("ethash_dir{}", runtime_idx).as_str()).unwrap();
        let ethash_provider =
            Arc::new(std::sync::Mutex::new(EthashProvider::new(ethash_dir.path())));
        let runtime_config = RuntimeConfig::default();

        let runtime = Runtime::new(runtime_config, ethash_provider);
        let trie_update = TrieUpdate::new(trie.clone(), MerkleHash::default());
        let (state_records, signers) = Self::get_state_records(num_runtimes, contract_code);
        let signer = signers[runtime_idx as usize].clone();

        let (store_update, genesis_root) =
            runtime.apply_genesis_state(trie_update, &[], &state_records);
        store_update.commit().unwrap();

        let apply_state = ApplyState {
            root: genesis_root,
            // Put each runtime into a separate shard.
            block_index: 0,
            // Parent block hash is not updated, because we are not producing actual blocks.
            parent_block_hash: Default::default(),
            // Epoch length is long enough to avoid corner cases.
            epoch_length: 4,
        };

        Self { ethash_dir, apply_state, runtime, trie, signer, shard_id: runtime_idx }
    }

    pub fn process_block(
        &mut self,
        receipts: Vec<Receipt>,
        transactions: Vec<SignedTransaction>,
    ) -> (Vec<Receipt>, Vec<TransactionLog>) {
        let state_update = TrieUpdate::new(self.trie.clone(), self.apply_state.root);
        let apply_result =
            self.runtime.apply(state_update, &self.apply_state, &receipts, &transactions).unwrap();

        let (store_update, _) = apply_result.trie_changes.into(self.trie.clone()).unwrap();
        store_update.commit().unwrap();

        self.apply_state.root = apply_result.root.clone();
        self.apply_state.block_index += 1;

        (apply_result.new_receipts, apply_result.tx_result)
    }
}

#[derive(Default)]
pub struct RuntimeMailbox {
    pub incoming_transactions: Vec<SignedTransaction>,
    pub incoming_receipts: Vec<Receipt>,
}

impl RuntimeMailbox {
    pub fn is_emtpy(&self) -> bool {
        self.incoming_receipts.is_empty() && self.incoming_transactions.is_empty()
    }
}

pub struct RuntimeGroup {
    mailboxes: (Mutex<HashMap<ShardId, RuntimeMailbox>>, Condvar),
    runtimes: Vec<Arc<Mutex<StandaloneRuntime>>>,
}

impl Default for RuntimeGroup {
    fn default() -> Self {
        Self { mailboxes: (Mutex::new(HashMap::new()), Condvar::new()), runtimes: vec![] }
    }
}

impl RuntimeGroup {
    pub fn new(num_runtimes: u64, contract_code: &[u8]) -> Arc<Self> {
        let mut res = Self::default();
        for i in 0..num_runtimes {
            res.mailboxes.0.lock().unwrap().insert(i, Default::default());
        }

        for i in 0..num_runtimes {
            let runtime =
                Arc::new(Mutex::new(StandaloneRuntime::new(i, num_runtimes, contract_code)));
            res.runtimes.push(runtime);
        }
        Arc::new(res)
    }

    pub fn start_runtimes(
        group: Arc<Self>,
        transactions: HashMap<ShardId, Vec<SignedTransaction>>,
    ) -> Vec<JoinHandle<()>> {
        for (i, v) in transactions {
            group.mailboxes.0.lock().unwrap().get_mut(&i).unwrap().incoming_transactions = v;
        }

        let mut handles = vec![];
        for runtime in &group.runtimes {
            handles.push(Self::start_runtime_in_thread(group.clone(), runtime.clone()));
        }
        handles
    }

    fn start_runtime_in_thread(
        group: Arc<Self>,
        runtime: Arc<Mutex<StandaloneRuntime>>,
    ) -> JoinHandle<()> {
        thread::spawn(move || loop {
            let shard_id = runtime.lock().unwrap().shard_id;
            println!("Shard {} started", shard_id);
            std::io::stdout().flush().ok().unwrap();

            let mut mailboxes = group.mailboxes.0.lock().unwrap();
            loop {
                if !mailboxes.get(&shard_id).unwrap().is_emtpy() {
                    println!("Shard {} got transactions/receipts", shard_id);
                    std::io::stdout().flush().ok().unwrap();
                    break;
                }
                if mailboxes.values().all(|m| m.is_emtpy()) {
                    println!("Shard {}. All mailboxed are empty. Exiting.", shard_id);
                    std::io::stdout().flush().ok().unwrap();
                    return;
                }
                mailboxes = group.mailboxes.1.wait(mailboxes).unwrap();
            }

            let mailbox = mailboxes.get_mut(&shard_id).unwrap();
            let (new_receipts, transaction_results) = runtime.lock().unwrap().process_block(
                mailbox.incoming_receipts.drain(..).collect(),
                mailbox.incoming_transactions.drain(..).collect(),
            );
            println!(
                "Processed transactions/receipts {} on shard_id {}",
                transaction_results.len(),
                shard_id
            );
            std::io::stdout().flush().ok().unwrap();
            let locked_other_mailbox = mailboxes.get_mut(&1).unwrap();
            locked_other_mailbox.incoming_receipts.extend(new_receipts);
            group.mailboxes.1.notify_all();
        })
    }
}

#[test]
fn test_simple() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let mut standalone_runtime = StandaloneRuntime::new(0, 1, wasm_binary);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        standalone_runtime.signer.account_id.clone(),
        standalone_runtime.signer.account_id.clone(),
        &standalone_runtime.signer.clone(),
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "sum_n".to_string(),
            args: 10u64.to_le_bytes().to_vec(),
            gas: 1_000_000,
            deposit: 0,
        })],
        standalone_runtime.apply_state.parent_block_hash,
    );

    let mut receipts_to_process = vec![];
    let mut transactions_to_process = vec![signed_transaction];
    loop {
        let (mut new_receipts, transaction_results) =
            standalone_runtime.process_block(receipts_to_process, transactions_to_process);
        receipts_to_process = new_receipts;
        transactions_to_process = vec![];
        println!("{:#?}", transaction_results);
        if receipts_to_process.is_empty() && transactions_to_process.is_empty() {
            break;
        }
    }
}

#[test]
fn test_three_shards() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(2, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id.clone(),
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "sum_n".to_string(),
            args: 10u64.to_le_bytes().to_vec(),
            gas: 1_000_000,
            deposit: 0,
        })],
        group.runtimes[0].lock().unwrap().apply_state.parent_block_hash,
    );

    let mut transactions = HashMap::new();
    transactions.insert(0, vec![signed_transaction]);
    let handles = RuntimeGroup::start_runtimes(group.clone(), transactions);
    for h in handles {
        h.join().unwrap();
    }
}
