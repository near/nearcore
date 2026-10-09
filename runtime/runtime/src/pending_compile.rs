//! Budget admission of cold contract code.
//!
//! A contract is cold when the chain has not admitted its code under the
//! current VM generation. A chunk admits a bounded number of bytes of cold
//! code; receipts that would run more wait in the pending-compile queue.

use crate::cache_warming::spawn_lazy_cache_warming;
use crate::contract_code::RuntimeContractIdentifier;
use near_parameters::vm::Config as VmConfig;
use near_primitives::account::AccountContract;
use near_primitives::action::{Action, GlobalContractIdentifier};
use near_primitives::errors::{IntegerOverflowError, StorageError};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, VersionedReceiptEnum};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, ProtocolVersion};
use near_store::trie::AccessOptions;
use near_store::trie::receipts_column_helper::{
    PendingCompileAccountQueue, PendingCompileReceiptQueue, TrieQueue,
};
use near_store::{TrieUpdate, get, get_account, set};
use near_vm_runner::ContractRuntimeCache;
use std::collections::HashSet;
use std::sync::Arc;

/// Bytes of cold contract code a chunk admits after its first cold contract.
// TODO(async-compilation): make this a runtime parameter sized from measured
// compile time per MiB.
pub(crate) const COLD_BYTES_PER_CHUNK: u64 = 8 * 1024 * 1024;

/// Bytes of contract code a chunk marks warm for the next VM generation.
pub(crate) const NEXT_GENERATION_MARK_BYTES_PER_CHUNK: u64 = 8 * 1024 * 1024;

/// Accounts the pending-compile queue holds. A cold receipt for an account
/// that is not queued yet goes to the delayed-receipt queue above this.
// TODO(async-compilation): count the queue in congestion info instead.
pub(crate) const MAX_PENDING_COMPILE_ACCOUNTS: u64 = 1_000;

/// VM generations of the chunk's epoch and of the next epoch. `next` equals
/// `current` unless the next epoch changes the VM.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VmGenerations {
    pub current: ProtocolVersion,
    pub next: ProtocolVersion,
}

/// What a receipt about to execute may do.
pub(crate) enum Admission {
    /// Execute now.
    Execute,
    /// Wait in the pending-compile queue.
    Defer,
}

/// The code the first `FunctionCall` of a receipt runs.
struct CalledCode {
    identifier: RuntimeContractIdentifier,
    warmth_key: TrieKey,
    code_len: u64,
}

/// Per-chunk admission state.
#[derive(Default)]
pub(crate) struct ColdAdmission {
    admitted_code: HashSet<CryptoHash>,
    admitted_bytes: u64,
    marked_bytes: u64,
    /// The receipt just taken off the front of the pending-compile queue,
    /// already admitted.
    admitted_from_queue: Option<CryptoHash>,
}

impl ColdAdmission {
    /// Decides whether `receipt` executes now, and records the admission of
    /// its code in state if it does.
    pub(crate) fn admit(
        &mut self,
        state_update: &mut TrieUpdate,
        receipt: &Receipt,
        generations: VmGenerations,
        chain_id: &str,
        protocol_version: ProtocolVersion,
    ) -> Result<Admission, StorageError> {
        if has_pending_compile_receipts(state_update, receipt.receiver_id())? {
            return Ok(Admission::Defer);
        }
        self.admit_code(state_update, receipt, generations, chain_id, protocol_version)
    }

    /// Like [`Self::admit`], for the receipt at the front of the
    /// pending-compile queue.
    pub(crate) fn admit_code(
        &mut self,
        state_update: &mut TrieUpdate,
        receipt: &Receipt,
        generations: VmGenerations,
        chain_id: &str,
        protocol_version: ProtocolVersion,
    ) -> Result<Admission, StorageError> {
        let Some(called) = called_code(state_update, receipt, chain_id, protocol_version)? else {
            return Ok(Admission::Execute);
        };
        let warm_generation: ProtocolVersion =
            get(state_update, &called.warmth_key)?.unwrap_or_default();
        if warm_generation < generations.current {
            if !self.try_admit_cold(called.identifier.hash(), called.code_len) {
                return Ok(Admission::Defer);
            }
            set(state_update, called.warmth_key.clone(), &generations.current);
        }
        if warm_generation < generations.next && self.try_mark_next(called.code_len) {
            set(state_update, called.warmth_key, &generations.next);
        }
        Ok(Admission::Execute)
    }

    fn try_admit_cold(&mut self, code_hash: CryptoHash, code_len: u64) -> bool {
        if self.admitted_code.contains(&code_hash) {
            return true;
        }
        let fits = self.admitted_code.is_empty()
            || self.admitted_bytes.saturating_add(code_len) <= COLD_BYTES_PER_CHUNK;
        if fits {
            self.admitted_code.insert(code_hash);
            self.admitted_bytes = self.admitted_bytes.saturating_add(code_len);
        }
        fits
    }

    fn try_mark_next(&mut self, code_len: u64) -> bool {
        let marked_bytes = self.marked_bytes.saturating_add(code_len);
        let fits = marked_bytes <= NEXT_GENERATION_MARK_BYTES_PER_CHUNK;
        if fits {
            self.marked_bytes = marked_bytes;
        }
        fits
    }

    pub(crate) fn set_admitted_from_queue(&mut self, receipt_id: CryptoHash) {
        self.admitted_from_queue = Some(receipt_id);
    }

    /// Returns whether `receipt_id` was just admitted from the pending-compile
    /// queue, and forgets it.
    pub(crate) fn take_admitted_from_queue(&mut self, receipt_id: &CryptoHash) -> bool {
        let admitted = self.admitted_from_queue.as_ref() == Some(receipt_id);
        if admitted {
            self.admitted_from_queue = None;
        }
        admitted
    }
}

fn has_pending_compile_receipts(
    state_update: &TrieUpdate,
    receiver_id: &AccountId,
) -> Result<bool, StorageError> {
    Ok(PendingCompileReceiptQueue::load(state_update, receiver_id)?.len() > 0)
}

/// Returns the code the first `FunctionCall` of `receipt` runs, or `None` if
/// the receipt runs no code that can be cold.
///
/// A `DeployContract` before the call compiles the code in the same receipt
/// and writes its warmth, so the call is not gated.
// TODO(async-compilation): resolve the global code a `DeterministicStateInit`
// or `UniversalStateInit` binds; until then such receipts are not gated.
fn called_code(
    state_update: &TrieUpdate,
    receipt: &Receipt,
    chain_id: &str,
    protocol_version: ProtocolVersion,
) -> Result<Option<CalledCode>, StorageError> {
    let action_receipt = match receipt.versioned_receipt() {
        VersionedReceiptEnum::Action(action_receipt)
        | VersionedReceiptEnum::PromiseYield(action_receipt) => action_receipt,
        _ => return Ok(None),
    };
    let receiver_id = receipt.receiver_id();
    let Some(account) = get_account(state_update, receiver_id)? else {
        return Ok(None);
    };
    let mut contract = account.contract().into_owned();
    for action in action_receipt.actions() {
        match action {
            Action::DeployContract(_)
            | Action::DeterministicStateInit(_)
            | Action::UniversalStateInit(_) => return Ok(None),
            Action::UseGlobalContract(use_global) => {
                contract = match &use_global.contract_identifier {
                    GlobalContractIdentifier::CodeHash(hash) => AccountContract::Global(*hash),
                    GlobalContractIdentifier::AccountId(account_id) => {
                        AccountContract::GlobalByAccount(account_id.clone())
                    }
                };
            }
            Action::FunctionCall(_) => {
                return resolve_called_code(
                    state_update,
                    receiver_id,
                    contract,
                    chain_id,
                    protocol_version,
                );
            }
            _ => {}
        }
    }
    Ok(None)
}

fn resolve_called_code(
    state_update: &TrieUpdate,
    receiver_id: &AccountId,
    contract: AccountContract,
    chain_id: &str,
    protocol_version: ProtocolVersion,
) -> Result<Option<CalledCode>, StorageError> {
    let access = AccessOptions::DEFAULT;
    let identifier = RuntimeContractIdentifier::resolve(
        receiver_id,
        contract,
        state_update,
        chain_id,
        access,
        protocol_version,
    )?;
    let warmth_key = match &identifier {
        RuntimeContractIdentifier::None => return Ok(None),
        RuntimeContractIdentifier::AccountLocal { account_id, .. } => {
            TrieKey::ContractWarmth { account_id: account_id.clone() }
        }
        RuntimeContractIdentifier::Global { identifier, .. } => {
            TrieKey::GlobalContractWarmth { identifier: identifier.clone().into() }
        }
    };
    let Some(code_len) =
        identifier.resolve_code_len(state_update, access, chain_id, protocol_version)?
    else {
        return Ok(None);
    };
    Ok(Some(CalledCode { identifier, warmth_key, code_len }))
}

/// Puts `receipt` at the back of its receiver's pending-compile receipts.
/// Returns `false`, leaving state untouched, if the receiver is not queued and
/// the queue is full.
pub(crate) fn push_pending_compile_receipt(
    state_update: &mut TrieUpdate,
    receipt: &Receipt,
) -> Result<bool, StorageError> {
    let receiver_id = receipt.receiver_id();
    let mut receipts = PendingCompileReceiptQueue::load(state_update, receiver_id)?;
    if receipts.len() == 0 {
        let mut accounts = PendingCompileAccountQueue::load(state_update)?;
        if accounts.len() >= MAX_PENDING_COMPILE_ACCOUNTS {
            return Ok(false);
        }
        accounts.push_back(state_update, receiver_id).map_err(overflow)?;
    }
    receipts.push_back(state_update, receipt).map_err(overflow)?;
    Ok(true)
}

/// Starts compiling the code `receipt` calls on the warming pool, so that the
/// artifact is cached by the time the receipt is admitted.
pub(crate) fn warm_pending_compile_code(
    state_update: &TrieUpdate,
    receipt: &Receipt,
    config: Arc<VmConfig>,
    cache: Option<&dyn ContractRuntimeCache>,
    chain_id: &str,
    protocol_version: ProtocolVersion,
) -> Result<(), StorageError> {
    let Some(cache) = cache else {
        return Ok(());
    };
    let Some(called) = called_code(state_update, receipt, chain_id, protocol_version)? else {
        return Ok(());
    };
    spawn_lazy_cache_warming(
        state_update.contract_storage().clone(),
        called.identifier,
        config,
        cache.handle(),
    );
    Ok(())
}

/// The front of the pending-compile queue: the first receiver and its first
/// receipt. Receivers without receipts are dropped from the front on the way;
/// after resharding a child holds receivers of the other child.
pub(crate) fn peek_pending_compile_receipt(
    state_update: &mut TrieUpdate,
) -> Result<Option<Receipt>, StorageError> {
    let mut accounts = PendingCompileAccountQueue::load(state_update)?;
    loop {
        let Some(receiver_id) = accounts.iter(state_update, false).next().transpose()? else {
            return Ok(None);
        };
        let receipts = PendingCompileReceiptQueue::load(state_update, &receiver_id)?;
        if let Some(receipt) = receipts.iter(state_update, false).next().transpose()? {
            return Ok(Some(receipt));
        }
        accounts.pop_front(state_update)?;
    }
}

/// Removes the front receipt of the pending-compile queue, and its receiver if
/// that was the receiver's last receipt.
pub(crate) fn pop_pending_compile_receipt(
    state_update: &mut TrieUpdate,
    receiver_id: &AccountId,
) -> Result<(), StorageError> {
    let mut receipts = PendingCompileReceiptQueue::load(state_update, receiver_id)?;
    receipts.pop_front(state_update)?;
    if receipts.len() == 0 {
        PendingCompileAccountQueue::load(state_update)?.pop_front(state_update)?;
    }
    Ok(())
}

fn overflow(_: IntegerOverflowError) -> StorageError {
    StorageError::StorageInconsistentState("pending-compile queue index overflow".to_string())
}

/// Writes the warmth of code deployed on `account_id` under `generation`.
pub(crate) fn set_contract_warmth(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
    generation: ProtocolVersion,
) {
    set(state_update, TrieKey::ContractWarmth { account_id: account_id.clone() }, &generation);
}
