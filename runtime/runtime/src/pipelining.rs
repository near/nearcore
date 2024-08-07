#![allow(dead_code)]

use crate::ext::RuntimeContractExt;
use near_parameters::RuntimeConfig;
use near_primitives::account::Account;
use near_primitives::action::Action;
use near_primitives::config::ViewConfig;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::types::{AccountId, Gas};
use near_store::contract::ContractStorage;
use near_vm_runner::logic::{GasCounter, ProtocolVersion};
use near_vm_runner::{ContractRuntimeCache, PreparedContract};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Condvar, Mutex};

pub(crate) struct ReceiptPreparationPipeline {
    /// Mapping from a Receipt's ID to a parallel "task" to prepare the receipt's data.
    ///
    /// The task itself may be run in the current thread, a separate thread or forced in any other
    /// way.
    map: BTreeMap<PrepareTaskKey, Arc<PrepareTask>>,

    /// List of Receipt receiver IDs that must not be prepared for this chunk.
    ///
    /// This solves an issue wherein the pipelining implementation only has access to the committed
    /// storage (read: data as a result of applying the previous chunk,) and not the state that has
    /// been built up as a result of processing the current chunk. One notable thing that may have
    /// occurred there is a contract deployment. Once that happens, we can no longer get the
    /// "current" contract code for the account.
    ///
    /// However, even if we had access to the transaction of the current chunk and were able to
    /// access the new code, there's a risk of a race between when the deployment is executed
    /// and when a parallel preparation may occur, leading back to needing to hold prefetching of
    /// that account's contracts until the deployment is executed.
    ///
    /// As deployments are a relatively rare event, it is probably just fine to entirely disable
    /// pipelining for the account in question for that particular block. This field implements
    /// exactly that.
    ///
    /// In the future, however, it may make sense to either move the responsibility of executing
    /// deployment actions to this pipelining thingy OR, even better, modify the protocol such that
    /// contract deployments in block N only take effect in the block N+1 as that, among other
    /// things, would give the runtime more time to compile the contract.
    block_accounts: BTreeSet<AccountId>,

    /// The Runtime config for these pipelining  requests.
    config: Arc<RuntimeConfig>,

    /// The contract cache.
    contract_cache: Option<Box<dyn ContractRuntimeCache>>,

    /// The chain ID being processed.
    chain_id: String,

    /// Protocol version for this chunk.
    protocol_version: u32,

    /// Storage for WASM code.
    storage: ContractStorage,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct PrepareTaskKey {
    receipt_id: CryptoHash,
    action_index: usize,
}

struct PrepareTask {
    status: Mutex<PrepareTaskStatus>,
    condvar: Condvar,
}

enum PrepareTaskStatus {
    Pending,
    Working,
    Prepared(Box<dyn PreparedContract>),
    Finished,
}

impl ReceiptPreparationPipeline {
    pub(crate) fn new(
        config: Arc<RuntimeConfig>,
        contract_cache: Option<Box<dyn ContractRuntimeCache>>,
        chain_id: String,
        protocol_version: u32,
        storage: ContractStorage,
    ) -> Self {
        Self {
            map: Default::default(),
            block_accounts: Default::default(),
            config,
            contract_cache,
            chain_id,
            protocol_version,
            storage,
        }
    }

    /// Submit a receipt to the "pipeline" for preparation of likely eventual execution.
    ///
    /// Note that not all receipts submitted here must be actually handled in some way. That said,
    /// while it is perfectly fine to not use the results of submitted work (e.g. because a
    /// applying a chunk ran out of gas or compute cost,) this work would eventually get lost, so
    /// for the most part it is best to submit work with limited look-ahead.
    ///
    /// Returns `true` if the receipt is interesting and that pipelining has acted on it in some
    /// way. Currently `true` is returned for any receipts containing `Action::DeployContract` (in
    /// which case no further processing for the receiver account will be done), and
    /// `Action::FunctionCall` (provided the account has not been blocked.)
    pub(crate) fn submit(
        &mut self,
        receipt: &Receipt,
        account: &Account,
        view_config: Option<ViewConfig>,
    ) -> bool {
        let account_id = receipt.receiver_id();
        if self.block_accounts.contains(account_id) {
            return false;
        }
        let actions = match receipt.receipt() {
            ReceiptEnum::Action(a) | ReceiptEnum::PromiseYield(a) => &a.actions,
            ReceiptEnum::Data(_) | ReceiptEnum::PromiseResume(_) => return false,
        };
        let mut any_function_calls = false;
        for (action_index, action) in actions.iter().enumerate() {
            let account_id = account_id.clone();
            match action {
                Action::DeployContract(_) => {
                    // FIXME: instead of blocking these accounts, move the handling of
                    // deploy action into here, so that the necessary data dependencies can be
                    // established.
                    return self.block_accounts.insert(account_id);
                }
                Action::FunctionCall(function_call) => {
                    let key = PrepareTaskKey { receipt_id: receipt.get_hash(), action_index };
                    let gas_counter = self.gas_counter(view_config.as_ref(), function_call.gas);
                    let entry = match self.map.entry(key) {
                        std::collections::btree_map::Entry::Vacant(v) => v,
                        // Already been submitted.
                        // TODO: Warning?
                        std::collections::btree_map::Entry::Occupied(_) => continue,
                    };
                    let config = Arc::clone(&self.config.wasm_config);
                    let cache = self.contract_cache.as_ref().map(|c| c.handle());
                    let storage = self.storage.clone();
                    let chain_id = self.chain_id.clone();
                    let protocol_version = self.protocol_version;
                    let code_hash = account.code_hash();
                    let status = Mutex::new(PrepareTaskStatus::Pending);
                    let task = Arc::new(PrepareTask { status, condvar: Condvar::new() });
                    let method_name = function_call.method_name.clone();
                    entry.insert(Arc::clone(&task));
                    // FIXME: don't spawn all tasks at once. We want to keep some capacity for
                    // other things and also to control (in a way) the concurrency here.
                    rayon::spawn_fifo(move || {
                        let task_status = {
                            let mut status = task.status.lock().expect("mutex lock");
                            std::mem::replace(&mut *status, PrepareTaskStatus::Working)
                        };
                        match &task_status {
                            PrepareTaskStatus::Pending => {}
                            PrepareTaskStatus::Working => return,
                            // TODO: seeing Prepared here may mean there's double spawning for the
                            // same receipt index. Maybe output a warning?
                            PrepareTaskStatus::Prepared(..) => return,
                            PrepareTaskStatus::Finished => return,
                        };
                        let contract = prepare_function_call(
                            &storage,
                            cache.as_deref(),
                            &chain_id,
                            protocol_version,
                            config,
                            gas_counter,
                            code_hash,
                            &account_id,
                            &method_name,
                        );

                        let mut status = task.status.lock().expect("mutex lock");
                        *status = PrepareTaskStatus::Prepared(contract);
                        task.condvar.notify_all();
                    });
                    any_function_calls = true;
                }
                // No need to handle this receipt as it only generates other new receipts.
                Action::Delegate(_) => {}
                // No handling for these.
                Action::CreateAccount(_)
                | Action::Transfer(_)
                | Action::Stake(_)
                | Action::AddKey(_)
                | Action::DeleteKey(_)
                | Action::DeleteAccount(_) => {}
                #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
                Action::NonrefundableStorageTransfer(_) => {}
            }
        }
        return any_function_calls;
    }

    /// Obtain the prepared contract for the provided receipt.
    ///
    /// If the contract is currently being prepared this function will block waiting for the
    /// preparation to complete.
    ///
    /// If the preparation hasn't been started yet (either because it hasn't been scheduled for any
    /// reason, or because the pipeline didn't make it in time), this function will prepare the
    /// contract in the calling thread.
    pub(crate) fn get_contract(
        &self,
        receipt: &Receipt,
        code_hash: CryptoHash,
        action_index: usize,
        view_config: Option<ViewConfig>,
    ) -> Box<dyn PreparedContract> {
        let account_id = receipt.receiver_id();
        let action = match receipt.receipt() {
            ReceiptEnum::Action(r) | ReceiptEnum::PromiseYield(r) => r
                .actions
                .get(action_index)
                .expect("indexing receipt actions by an action_index failed!"),
            ReceiptEnum::Data(_) | ReceiptEnum::PromiseResume(_) => {
                panic!("attempting to get_contract with a non-action receipt!?")
            }
        };
        let Action::FunctionCall(function_call) = action else {
            panic!("referenced receipt action is not a function call!");
        };
        let key = PrepareTaskKey { receipt_id: receipt.get_hash(), action_index };
        let Some(task) = self.map.get(&key) else {
            let gas_counter = self.gas_counter(view_config.as_ref(), function_call.gas);
            tracing::debug!(
                target: "runtime::pipelining",
                message="function call task was not submitted for preparation",
                receipt=%receipt.get_hash(),
                action_index,
                backtrace = %std::backtrace::Backtrace::force_capture()
            );
            return prepare_function_call(
                &self.storage,
                self.contract_cache.as_deref(),
                &self.chain_id,
                self.protocol_version,
                Arc::clone(&self.config.wasm_config),
                gas_counter,
                code_hash,
                &account_id,
                &function_call.method_name,
            );
        };
        let mut status_guard = task.status.lock().unwrap();
        loop {
            let current = std::mem::replace(&mut *status_guard, PrepareTaskStatus::Working);
            match current {
                PrepareTaskStatus::Pending => {
                    *status_guard = PrepareTaskStatus::Finished;
                    drop(status_guard);

                    let gas_counter = self.gas_counter(view_config.as_ref(), function_call.gas);
                    tracing::trace!(
                        target: "runtime::pipelining",
                        message="function call preparation on the main thread",
                        receipt=%receipt.get_hash(),
                        action_index
                    );
                    let contract = prepare_function_call(
                        &self.storage,
                        self.contract_cache.as_deref(),
                        &self.chain_id,
                        self.protocol_version,
                        Arc::clone(&self.config.wasm_config),
                        gas_counter,
                        code_hash,
                        &account_id,
                        &function_call.method_name,
                    );
                    return contract;
                }
                PrepareTaskStatus::Working => {
                    status_guard = task.condvar.wait(status_guard).unwrap();
                    continue;
                }
                PrepareTaskStatus::Prepared(c) => {
                    *status_guard = PrepareTaskStatus::Finished;
                    return c;
                }
                PrepareTaskStatus::Finished => {
                    *status_guard = PrepareTaskStatus::Finished;
                    // Don't poison the lock.
                    drop(status_guard);
                    panic!("attempting to get_contract that has already been taken");
                }
            }
        }
    }

    fn gas_counter(&self, view_config: Option<&ViewConfig>, gas: Gas) -> GasCounter {
        let max_gas_burnt = match view_config {
            Some(ViewConfig { max_gas_burnt }) => *max_gas_burnt,
            None => self.config.wasm_config.limit_config.max_gas_burnt,
        };
        GasCounter::new(
            self.config.wasm_config.ext_costs.clone(),
            max_gas_burnt,
            self.config.wasm_config.regular_op_cost,
            gas,
            view_config.is_some(),
        )
    }
}

fn prepare_function_call(
    contract_storage: &ContractStorage,
    cache: Option<&dyn ContractRuntimeCache>,

    chain_id: &str,
    protocol_version: ProtocolVersion,
    config: Arc<near_parameters::vm::Config>,
    gas_counter: GasCounter,

    code_hash: CryptoHash,
    account_id: &AccountId,
    method_name: &str,
) -> Box<dyn PreparedContract> {
    let code_ext = RuntimeContractExt {
        storage: contract_storage.clone(),
        account_id,
        code_hash,
        chain_id,
        current_protocol_version: protocol_version,
    };
    let contract = near_vm_runner::prepare(&code_ext, config, cache, gas_counter, method_name);
    contract
}
