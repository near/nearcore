// cspell:words wycheproof
use super::context::VMContext;
use super::errors::FunctionCallError;
use super::gas_counter::GasCounter;
use super::types::{ReceiptIndex, ReturnData};
use super::{HostError, VMLogicError};
use crate::ProfileDataV3;
use near_parameters::vm::Config;
use near_primitives_core::types::{Balance, Compute, Gas, StorageUsage};
use std::sync::Arc;

pub type Result<T, E = VMLogicError> = ::std::result::Result<T, E>;

/// Structure representing the results and outcomes of a contract execution.
///
/// This is the subset of the VM host state that's strictly necessary to produce `VMOutcome`s.
pub struct ExecutionResultState {
    /// All gas and economic parameters required during contract execution.
    pub(crate) config: Arc<Config>,
    /// Gas tracking for the current contract execution.
    pub(crate) gas_counter: GasCounter,
    /// Logs written by the runtime.
    pub(crate) logs: Vec<String>,
    /// Tracks the total log length. The sum of length of all logs.
    pub(crate) total_log_length: u64,
    /// What method returns.
    pub(crate) return_data: ReturnData,
    /// Keeping track of the current account balance, which can decrease when we create promises
    /// and attach balance to them.
    pub(crate) current_account_balance: Balance,
    /// Total amount subsidized by skipping balance deduction for 1 yoctoNEAR
    /// attached deposits on zero-balance contract promise calls.
    pub(crate) subsidized_amount: Balance,
    /// Storage usage of the current account at the moment
    pub(crate) current_storage_usage: StorageUsage,
}

impl ExecutionResultState {
    /// Create a new state.
    ///
    /// # Panics
    ///
    /// Note that `context.account_balance + context.attached_deposit` must not overflow `u128`,
    /// otherwise this function will panic.
    pub fn new(context: &VMContext, gas_counter: GasCounter, config: Arc<Config>) -> Self {
        let current_account_balance = context
            .account_balance
            .checked_add(context.attached_deposit)
            .expect("current_account_balance overflowed");
        let current_storage_usage = context.storage_usage;
        Self {
            config,
            gas_counter,
            logs: vec![],
            total_log_length: 0,
            return_data: ReturnData::None,
            current_account_balance,
            subsidized_amount: Balance::ZERO,
            current_storage_usage,
        }
    }

    /// A helper function to subtract balance on transfer or attached deposit for promises.
    ///
    /// ### Args
    ///
    /// * `amount`: the amount to deduct from the current account balance.
    pub(crate) fn deduct_balance(&mut self, amount: Balance) -> Result<()> {
        self.current_account_balance =
            self.current_account_balance.checked_sub(amount).ok_or(HostError::BalanceExceeded)?;
        Ok(())
    }

    /// Checks that the current log number didn't reach the limit yet, so we can add a new message.
    pub(crate) fn check_can_add_a_log_message(&self) -> Result<()> {
        if self.logs.len() as u64 >= self.config.limit_config.max_number_logs {
            Err(HostError::NumberOfLogsExceeded { limit: self.config.limit_config.max_number_logs }
                .into())
        } else {
            Ok(())
        }
    }

    pub(crate) fn checked_push_log(&mut self, message: String) -> Result<()> {
        let len = u64::try_from(message.len()).unwrap_or(u64::MAX);
        let Some(total_log_length) = self.total_log_length.checked_add(len) else {
            return self.total_log_length_exceeded(len);
        };
        self.total_log_length = total_log_length;
        if self.total_log_length > self.config.limit_config.max_total_log_length {
            return self.total_log_length_exceeded(len);
        }
        self.logs.push(message);
        Ok(())
    }

    pub(crate) fn total_log_length_exceeded<T>(&self, add_len: u64) -> Result<T> {
        Err(HostError::TotalLogLengthExceeded {
            length: self.total_log_length.saturating_add(add_len),
            limit: self.config.limit_config.max_total_log_length,
        }
        .into())
    }

    /// Computes the outcome of the execution.
    ///
    /// If `FunctionCallWeight` protocol feature is enabled, unused gas will be
    /// distributed to functions that specify a gas weight. If there are no functions with
    /// a gas weight, the outcome will contain unused gas as usual.
    pub fn compute_outcome(self) -> VMOutcome {
        let burnt_gas = self.gas_counter.burnt_gas();
        let used_gas = self.gas_counter.used_gas();

        let mut profile = self.gas_counter.profile_data();
        profile.compute_wasm_instruction_cost(burnt_gas);
        let compute_usage = profile.total_compute_usage(
            &self.config.ext_costs,
            self.gas_counter.send_action_compute_usage,
        );

        VMOutcome {
            balance: self.current_account_balance,
            storage_usage: self.current_storage_usage,
            return_data: self.return_data,
            burnt_gas,
            used_gas,
            compute_usage,
            logs: self.logs,
            profile,
            aborted: None,
            subsidized_amount: self.subsidized_amount,
        }
    }
}

/// Promises API allows to create a DAG-structure that defines dependencies between smart contract
/// calls. A single promise can be created with zero or several dependencies on other promises.
/// * If a promise was created from a receipt (using `promise_create` or `promise_then`) it's a
///   `Receipt`;
/// * If a promise was created by merging several promises (using `promise_and`) then
///   it's a `NotReceipt`, but has receipts of all promises it depends on.
#[derive(Debug)]
pub(crate) enum Promise {
    Receipt(ReceiptIndex),
    NotReceipt(Vec<ReceiptIndex>),
}

/// A wrapper for reading public key.
///
/// This exists for historical reasons because we must maintain when errors are
/// returned.  In the old days, between reading the public key and decoding it
/// we could return unrelated error.  Because of that we cannot change the code
/// to return deserialization errors immediately after reading the public key.
///
/// This struct abstracts away the fact that we’re deserializing the key
/// immediately.  Decoding errors are detected as soon as this object is created
/// but they are communicated to the user only once they call [`Self::decode`].
///
/// Why not just keep the old ways without this noise? By doing deserialization
/// immediately we’re copying the data onto the stack without having to allocate
/// a temporary vector.
pub(crate) struct PublicKeyBuffer(Result<near_crypto::PublicKey, ()>);

impl PublicKeyBuffer {
    pub(crate) fn new(data: &[u8], post_quantum_keys_enabled: bool) -> Self {
        use near_crypto::PublicKey;

        let deserialize_res: Result<PublicKey, ()> =
            borsh::BorshDeserialize::try_from_slice(data).map_err(|_| ());

        let final_res = match deserialize_res {
            Ok(PublicKey::ED25519(_)) | Ok(PublicKey::SECP256K1(_)) | Err(_) => deserialize_res,
            Ok(PublicKey::MLDSA65(_)) => {
                if post_quantum_keys_enabled {
                    deserialize_res
                } else {
                    // Post quantum keys not enabled, simulate serialization failure
                    Err(())
                }
            }
        };
        Self(final_res)
    }

    pub(crate) fn decode(self) -> Result<near_crypto::PublicKey> {
        self.0.map_err(|_| HostError::InvalidPublicKey.into())
    }
}

/// Public-key byte length for a gas-key EXEC (storage) fee computation. Once the
/// fix is enabled this is the on-trie identifier length; otherwise it falls back
/// to `pk_len` (the decoded key's wire length, same as the send fee), preserving
/// the pre-fix behavior.
pub(crate) fn gas_key_exec_pk_len(
    public_key_res: &Result<near_crypto::PublicKey>,
    config: &Config,
    pk_len: usize,
) -> usize {
    match public_key_res {
        // Exec (storage) fee should reflect how many bytes the key occupies in
        // storage, not on the wire.
        Ok(pk) if config.fix_ml_dsa_cost_charging => pk.trie_id_len(),
        // Preserve the existing behavior if the fix is not enabled (or the key
        // failed to decode); changing it would break protocol consensus.
        _ => pk_len,
    }
}

#[derive(PartialEq)]
pub struct VMOutcome {
    pub balance: Balance,
    pub storage_usage: StorageUsage,
    pub return_data: ReturnData,
    pub burnt_gas: Gas,
    pub used_gas: Gas,
    pub compute_usage: Compute,
    pub logs: Vec<String>,
    /// Data collected from making a contract call
    pub profile: ProfileDataV3,
    pub aborted: Option<FunctionCallError>,
    /// Amount of balance subsidized (minted) by skipping deduction for
    /// 1 yoctoNEAR attached deposits on zero-balance contracts.
    pub subsidized_amount: Balance,
}

impl VMOutcome {
    /// Consumes the [`ExecutionResultState`] and computes the final outcome with the
    /// given error that stopped execution from finishing successfully.
    pub fn abort(state: ExecutionResultState, error: FunctionCallError) -> VMOutcome {
        let mut outcome = state.compute_outcome();
        outcome.aborted = Some(error);
        outcome
    }

    /// Consumes the [`ExecutionResultState`] and computes the final outcome for a
    /// successful execution.
    pub fn ok(state: ExecutionResultState) -> VMOutcome {
        state.compute_outcome()
    }

    /// Creates an outcome with a no-op outcome.
    pub fn nop_outcome(error: FunctionCallError) -> VMOutcome {
        VMOutcome {
            // Note: Balance and storage fields are ignored on a failed outcome.
            balance: Balance::ZERO,
            storage_usage: 0,
            // Note: Fields below are added or merged when processing the
            // outcome. With 0 or the empty set, those are no-ops.
            return_data: ReturnData::None,
            burnt_gas: Gas::ZERO,
            used_gas: Gas::ZERO,
            compute_usage: 0,
            logs: Vec::new(),
            profile: ProfileDataV3::default(),
            aborted: Some(error),
            subsidized_amount: Balance::ZERO,
        }
    }

    /// Like `Self::abort()` but without feature `FixContractLoadingCost` it
    /// will return a NOP outcome. This is used for backwards-compatibility only.
    pub fn abort_but_nop_outcome_in_old_protocol(
        state: ExecutionResultState,
        error: FunctionCallError,
    ) -> VMOutcome {
        if state.config.fix_contract_loading_cost {
            Self::abort(state, error)
        } else {
            Self::nop_outcome(error)
        }
    }
}

impl std::fmt::Debug for VMOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let return_data_str = match &self.return_data {
            ReturnData::None => "None".to_string(),
            ReturnData::ReceiptIndex(_) => "Receipt".to_string(),
            ReturnData::Value(v) => format!("Value [{} bytes]", v.len()),
        };
        write!(
            f,
            "VMOutcome: balance {} storage_usage {} return data {} burnt gas {} used gas {}",
            self.balance.as_yoctonear(),
            self.storage_usage,
            return_data_str,
            self.burnt_gas.as_gas(),
            self.used_gas.as_gas()
        )?;
        if let Some(err) = &self.aborted {
            write!(f, " failed with {err}")?;
        }
        Ok(())
    }
}

pub(crate) enum GlobalContractIdentifierPtrData {
    CodeHash { code_hash_len: u64, code_hash_ptr: u64 },
    AccountId { account_id_len: u64, account_id_ptr: u64 },
}
