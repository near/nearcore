use near_chain_configs::{get_initial_supply, Genesis, GenesisConfig, GenesisRecords};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::account::{AccessKey, Account};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::Receipt;
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use near_primitives::shard_layout::ShardUId;
use near_primitives::state_record::{state_record_to_account_id, StateRecord};
use near_primitives::test_utils::MockEpochInfoProvider;
use near_primitives::transaction::{ExecutionOutcomeWithId, SignedTransaction};
use near_primitives::types::{AccountId, AccountInfo, Balance};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::genesis::GenesisStateApplier;
use near_store::test_utils::create_tries;
use near_store::ShardTries;
use near_vm_runner::logic::ActionCosts;
use node_runtime::{ApplyState, Runtime};
use random_config::random_config;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;

pub mod random_config;

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000 * NEAR_BASE;

/// Validator's stake used in tests.
pub const TESTING_INIT_STAKE: Balance = 50_000_000 * NEAR_BASE;

/// One NEAR, divisible by 10^24.
pub const NEAR_BASE: Balance = 1_000_000_000_000_000_000_000_000;

pub struct StandaloneRuntime {
    pub apply_state: ApplyState,
    pub runtime: Runtime,
    pub tries: ShardTries,
    pub signer: InMemorySigner,
    pub root: CryptoHash,
    pub epoch_info_provider: MockEpochInfoProvider,
}

impl StandaloneRuntime {
    pub fn account_id(&self) -> AccountId {
        self.signer.account_id.clone()
    }

    pub fn new(
        signer: InMemorySigner,
        state_records: &[StateRecord],
        tries: ShardTries,
        validators: Vec<AccountInfo>,
    ) -> Self {
        let mut runtime_config = random_config();
        // Bumping costs to avoid inflation overflows.
        runtime_config.wasm_config.limit_config.max_total_prepaid_gas = 10u64.pow(15);
        runtime_config.fees.action_fees[ActionCosts::new_action_receipt].execution =
            runtime_config.wasm_config.limit_config.max_total_prepaid_gas / 64;
        runtime_config.fees.action_fees[ActionCosts::new_data_receipt_base].execution =
            runtime_config.wasm_config.limit_config.max_total_prepaid_gas / 64;

        let runtime = Runtime::new();
        let genesis = Genesis::new(
            GenesisConfig {
                validators,
                total_supply: get_initial_supply(state_records),
                epoch_length: 60,
                ..Default::default()
            },
            GenesisRecords(state_records.to_vec()),
        )
        .unwrap();

        let mut account_ids: HashSet<AccountId> = HashSet::new();
        genesis.for_each_record(|record: &StateRecord| {
            account_ids.insert(state_record_to_account_id(record).clone());
        });
        let writers = std::sync::atomic::AtomicUsize::new(0);
        let root = GenesisStateApplier::apply(
            &writers,
            tries.clone(),
            0,
            &[],
            &runtime_config.fees.storage_usage_config,
            &genesis,
            account_ids,
        );

        let apply_state = ApplyState {
            block_height: 1,
            prev_block_hash: Default::default(),
            block_hash: Default::default(),
            epoch_id: Default::default(),
            epoch_height: 0,
            gas_price: 100,
            block_timestamp: 0,
            gas_limit: None,
            random_seed: Default::default(),
            current_protocol_version: PROTOCOL_VERSION,
            config: Arc::new(runtime_config),
            cache: None,
            is_new_chunk: true,
            migration_data: Arc::new(MigrationData::default()),
            migration_flags: MigrationFlags::default(),
        };

        Self {
            apply_state,
            runtime,
            tries,
            signer,
            root,
            epoch_info_provider: MockEpochInfoProvider::default(),
        }
    }

    pub fn process_block(
        &mut self,
        receipts: &[Receipt],
        transactions: &[SignedTransaction],
    ) -> (Vec<Receipt>, Vec<ExecutionOutcomeWithId>) {
        let apply_result = self
            .runtime
            .apply(
                self.tries.get_trie_for_shard(ShardUId::single_shard(), self.root),
                &None,
                &self.apply_state,
                receipts,
                transactions,
                &self.epoch_info_provider,
                Default::default(),
            )
            .unwrap();

        let mut store_update = self.tries.store_update();
        self.root = self.tries.apply_all(
            &apply_result.trie_changes,
            ShardUId::single_shard(),
            &mut store_update,
        );
        store_update.commit().unwrap();
        self.apply_state.block_height += 1;

        (apply_result.outgoing_receipts, apply_result.outcomes)
    }
}

#[derive(Default)]
pub struct RuntimeMailbox {
    pub incoming_transactions: Vec<SignedTransaction>,
    pub incoming_receipts: Vec<Receipt>,
}

impl RuntimeMailbox {
    pub fn is_empty(&self) -> bool {
        self.incoming_receipts.is_empty() && self.incoming_transactions.is_empty()
    }
}

#[derive(Default)]
pub struct RuntimeGroup {
    pub mailboxes: (Mutex<HashMap<AccountId, RuntimeMailbox>>, Condvar),
    pub state_records: Arc<Vec<StateRecord>>,
    pub signers: Vec<InMemorySigner>,
    pub validators: Vec<AccountInfo>,

    /// Account id of the runtime on which the transaction was executed mapped to the transactions.
    pub executed_transactions: Mutex<HashMap<AccountId, Vec<SignedTransaction>>>,
    /// Account id of the runtime on which the receipt was executed mapped to the list of the receipts.
    pub executed_receipts: Mutex<HashMap<AccountId, Vec<Receipt>>>,
    /// List of the transaction logs.
    pub transaction_logs: Mutex<Vec<ExecutionOutcomeWithId>>,
}

impl RuntimeGroup {
    pub fn new_with_account_ids(
        account_ids: Vec<AccountId>,
        num_existing_accounts: u64,
        contract_code: &[u8],
    ) -> Arc<Self> {
        let mut res = Self::default();
        assert!(num_existing_accounts <= account_ids.len() as u64);
        let (state_records, signers, validators) =
            Self::state_records_signers(account_ids, num_existing_accounts, contract_code);
        Arc::make_mut(&mut res.state_records).extend(state_records);

        for signer in signers {
            res.signers.push(signer.clone());
            res.mailboxes.0.lock().unwrap().insert(signer.account_id, Default::default());
        }
        res.validators = validators;
        Arc::new(res)
    }

    pub fn new(num_runtimes: u64, num_existing_accounts: u64, contract_code: &[u8]) -> Arc<Self> {
        let account_ids = (0..num_runtimes)
            .map(|i| AccountId::try_from(format!("near_{}", i)).unwrap())
            .collect();
        Self::new_with_account_ids(account_ids, num_existing_accounts, contract_code)
    }

    /// Get state records and signers for standalone runtimes.
    fn state_records_signers(
        account_ids: Vec<AccountId>,
        num_existing_accounts: u64,
        contract_code: &[u8],
    ) -> (Vec<StateRecord>, Vec<InMemorySigner>, Vec<AccountInfo>) {
        let code_hash = hash(contract_code);
        let mut state_records = vec![];
        let mut signers = vec![];
        let mut validators = vec![];
        for (i, account_id) in account_ids.into_iter().enumerate() {
            let signer = InMemorySigner::from_seed(
                account_id.clone(),
                KeyType::ED25519,
                account_id.as_ref(),
            );
            if (i as u64) < num_existing_accounts {
                state_records.push(StateRecord::Account {
                    account_id: account_id.clone(),
                    account: Account::new(TESTING_INIT_BALANCE, TESTING_INIT_STAKE, code_hash, 0),
                });
                state_records.push(StateRecord::AccessKey {
                    account_id: account_id.clone(),
                    public_key: signer.public_key.clone(),
                    access_key: AccessKey::full_access(),
                });
                state_records
                    .push(StateRecord::Contract { account_id, code: contract_code.to_vec() });
                validators.push(AccountInfo {
                    account_id: signer.account_id.clone(),
                    public_key: signer.public_key.clone(),
                    amount: TESTING_INIT_STAKE,
                });
            }
            signers.push(signer);
        }
        (state_records, signers, validators)
    }

    pub fn start_runtimes(
        group: Arc<Self>,
        transactions: Vec<SignedTransaction>,
    ) -> Vec<JoinHandle<()>> {
        for transaction in transactions {
            group
                .mailboxes
                .0
                .lock()
                .unwrap()
                .get_mut(&transaction.transaction.signer_id)
                .unwrap()
                .incoming_transactions
                .push(transaction);
        }

        let mut handles = vec![];
        for signer in &group.signers {
            let signer = signer.clone();
            let state_records = Arc::clone(&group.state_records);
            let validators = group.validators.clone();
            let runtime_factory =
                move || StandaloneRuntime::new(signer, &state_records, create_tries(), validators);
            handles.push(Self::start_runtime_in_thread(group.clone(), runtime_factory));
        }
        handles
    }

    fn start_runtime_in_thread<F>(group: Arc<Self>, runtime_factory: F) -> JoinHandle<()>
    where
        F: FnOnce() -> StandaloneRuntime + Send + 'static,
    {
        thread::spawn(move || {
            let mut runtime = runtime_factory();
            loop {
                let account_id = runtime.account_id();

                let mut mailboxes = group.mailboxes.0.lock().unwrap();
                loop {
                    if !mailboxes.get(&account_id).unwrap().is_empty() {
                        break;
                    }
                    if mailboxes.values().all(|m| m.is_empty()) {
                        return;
                    }
                    mailboxes = group.mailboxes.1.wait(mailboxes).unwrap();
                }

                let mailbox = mailboxes.get_mut(&account_id).unwrap();
                group
                    .executed_receipts
                    .lock()
                    .unwrap()
                    .entry(account_id.clone())
                    .or_insert_with(Vec::new)
                    .extend(mailbox.incoming_receipts.clone());
                group
                    .executed_transactions
                    .lock()
                    .unwrap()
                    .entry(account_id.clone())
                    .or_insert_with(Vec::new)
                    .extend(mailbox.incoming_transactions.clone());

                let (new_receipts, transaction_results) = runtime
                    .process_block(&mailbox.incoming_receipts, &mailbox.incoming_transactions);
                mailbox.incoming_receipts.clear();
                mailbox.incoming_transactions.clear();
                group.transaction_logs.lock().unwrap().extend(transaction_results);
                for new_receipt in new_receipts {
                    let locked_other_mailbox = mailboxes.get_mut(&new_receipt.receiver_id).unwrap();
                    locked_other_mailbox.incoming_receipts.push(new_receipt);
                }
                group.mailboxes.1.notify_all();
            }
        })
    }

    /// Get receipt that was executed by the given runtime based on hash.
    pub fn get_receipt(&self, executing_runtime: &str, hash: &CryptoHash) -> Receipt {
        self.executed_receipts
            .lock()
            .unwrap()
            .get(executing_runtime)
            .expect("Runtime not found")
            .iter()
            .find_map(|r| if &r.get_hash() == hash { Some(r.clone()) } else { None })
            .expect("Runtime does not contain the receipt with the given hash.")
    }

    /// Get transaction log produced by the execution of given transaction/receipt
    /// identified by `producer_hash`.
    pub fn get_transaction_log(&self, producer_hash: &CryptoHash) -> ExecutionOutcomeWithId {
        self.transaction_logs
            .lock()
            .unwrap()
            .iter()
            .find_map(|tl| if &tl.id == producer_hash { Some(tl.clone()) } else { None })
            .expect("The execution log of the given receipt is missing")
    }

    pub fn get_receipt_debug(&self, hash: &CryptoHash) -> (AccountId, Receipt) {
        for (executed_runtime, tls) in self.executed_receipts.lock().unwrap().iter() {
            if let Some(res) =
                tls.iter().find_map(|r| if &r.get_hash() == hash { Some(r.clone()) } else { None })
            {
                return (executed_runtime.clone(), res);
            }
        }
        unimplemented!()
    }
}

/// Binds a tuple to a vector.
/// # Examples:
///
/// ```
/// let v = vec![1,2,3];
/// tuplet!((a,b,c) = v);
/// assert_eq!(a, &1);
/// assert_eq!(b, &2);
/// assert_eq!(c, &3);
/// ```
#[macro_export]
macro_rules! tuplet {
    {() = $v:expr, $message:expr } => {
        assert!($v.is_empty(), "{}", $message);
    };
    {($y:ident) = $v:expr, $message:expr } => {
        assert_eq!($v.len(), 1, "{}", $message);
        let $y = &$v[0];
    };
    { ($y:ident $(, $x:ident)*) = $v:expr, $message:expr } => {
        let ($y, $($x),*) = tuplet!($v ; 1 ; ($($x),*) ; (&$v[0]), $message );
    };
    { $v:expr ; $j:expr ; ($y:ident $(, $x:ident)*) ; ($($a:expr),*), $message:expr } => {
        tuplet!( $v ; $j+1 ; ($($x),*) ; ($($a),*,&$v[$j]), $message )
    };
    { $v:expr ; $j:expr ; () ; $accu:expr, $message:expr } => { {
            assert_eq!($v.len(), $j, "{}", $message);
            $accu
    } }
}

#[macro_export]
macro_rules! assert_receipts {
    ($group:ident, $transaction:ident => [ $($receipt:ident),* ] ) => {
        let transaction_log = $group.get_transaction_log(&$transaction.get_hash());
        tuplet!(( $($receipt),* ) = transaction_log.outcome.receipt_ids, "Incorrect number of produced receipts for transaction");
    };
    ($group:ident, $from:expr => $receipt:ident @ $to:expr,
    $receipt_pat:pat,
    $receipt_assert:block,
    $actions_name:ident,
    $($action_name:ident, $action_pat:pat, $action_assert:block ),+
     => [ $($produced_receipt:ident),*] ) => {
        let r = $group.get_receipt($to, $receipt);
        assert_eq!(r.predecessor_id.as_ref(), $from);
        assert_eq!(r.receiver_id.as_ref(), $to);
        match &r.receipt {
            $receipt_pat => {
                $receipt_assert
                tuplet!(( $($action_name),* ) = $actions_name, "Incorrect number of actions");
                $(
                    match $action_name {
                        $action_pat => {
                            $action_assert
                        }
                        _ => panic!("Action {:#?} does not satisfy the pattern {}", $action_name, stringify!($action_pat)),
                    }
                )*
            }
            _ => panic!("Receipt {:#?} does not satisfy the pattern {}", r, stringify!($receipt_pat)),
        }
       let receipt_log = $group.get_transaction_log(&r.get_hash());
       tuplet!(( $($produced_receipt),* ) = receipt_log.outcome.receipt_ids, "Incorrect number of produced receipts for a receipt");
    };
}

/// A short form for refunds.
/// ```
/// assert_refund!(group, ref1 @ "near_0");
/// ```
/// expands into:
/// ```
/// assert_receipts!(group, "system" => ref1 @ "near_0",
///                  ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
///                  actions,
///                  a0, Action::Transfer(TransferAction{..}), {}
///                  => []);
/// ```
#[macro_export]
macro_rules! assert_refund {
 ($group:ident, $receipt:ident @ $to:expr) => {
        assert_receipts!($group, "system" => $receipt @ $to,
                         ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                         actions,
                         a0, Action::Transfer(TransferAction{..}), {}
                         => []);
 }
}
