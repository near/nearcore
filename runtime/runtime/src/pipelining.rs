#![allow(dead_code)]

use crate::ext::RuntimeContractExt;
use near_parameters::RuntimeConfig;
use near_primitives::account::Account;
use near_primitives::action::Action;
use near_primitives::config::ViewConfig;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::types::AccountId;
use near_vm_runner::logic::GasCounter;
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
    storage: near_store::contract::Storage,
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
    FunctionCallTask { method: String },
    Working,
    Done(Box<dyn PreparedContract>),
}

impl ReceiptPreparationPipeline {
    pub(crate) fn new(
        config: Arc<RuntimeConfig>,
        contract_cache: Option<Box<dyn ContractRuntimeCache>>,
        chain_id: String,
        protocol_version: u32,
        storage: near_store::contract::Storage,
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
    /// Returns `true` if the receipt is interesting and work has been scheduled for its
    /// preparation.
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
            match action {
                Action::DeployContract(_) => {
                    self.block_accounts.insert(account_id.clone());
                    break;
                }
                Action::FunctionCall(function_call) => {
                    let key = PrepareTaskKey { receipt_id: receipt.get_hash(), action_index };
                    let entry = match self.map.entry(key) {
                        std::collections::btree_map::Entry::Vacant(v) => v,
                        // Already been submitted.
                        std::collections::btree_map::Entry::Occupied(_) => continue,
                    };
                    let max_gas_burnt = match view_config {
                        Some(ViewConfig { max_gas_burnt }) => max_gas_burnt,
                        None => self.config.wasm_config.limit_config.max_gas_burnt,
                    };
                    let gas_counter = GasCounter::new(
                        self.config.wasm_config.ext_costs.clone(),
                        max_gas_burnt,
                        self.config.wasm_config.regular_op_cost,
                        function_call.gas,
                        view_config.is_some(),
                    );
                    // TODO: This part needs to be executed in a thread eventually.
                    let code_ext = RuntimeContractExt {
                        storage: self.storage.clone(),
                        account_id,
                        account,
                        chain_id: &self.chain_id,
                        current_protocol_version: self.protocol_version,
                    };
                    let contract = near_vm_runner::prepare(
                        &code_ext,
                        Arc::clone(&self.config.wasm_config),
                        self.contract_cache.as_deref(),
                        gas_counter,
                        &function_call.method_name,
                    );
                    let task = Arc::new(PrepareTask {
                        status: Mutex::new(PrepareTaskStatus::Done(contract)),
                        condvar: Condvar::new(),
                    });
                    // let task = Arc::new(Mutex::new(PrepareTaskStatus::FunctionCallTask {
                    //     method: function_call.method_name.clone(),
                    // }));
                    entry.insert(Arc::clone(&task));
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
            }
        }
        return any_function_calls;
    }
}
