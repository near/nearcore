use crate::logic::errors::InconsistentStateError;
use crate::logic::gas_counter::FreeGasCounter;
use crate::logic::logic::{Promise, PublicKeyBuffer};
use crate::logic::types::{
    ActionIndex, GlobalContractDeployMode, GlobalContractIdentifier, PromiseIndex, PromiseResult,
    ReceiptIndex,
};
use crate::logic::{GasCounter, HostError, ReturnData, alt_bn128, bls12381};
use crate::wasmtime_runner::ErrorContainer;
use crate::wasmtime_runner::component::Ctx;
use crate::wasmtime_runner::component::bindings::near::nearcore::{finite_wasm, runtime};
use core::array;
use near_crypto::{PublicKey, Secp256K1Signature};
use near_parameters::ExtCosts::*;
use near_parameters::{
    ActionCosts, ExtCosts, RuntimeFeesConfig, transfer_exec_fee, transfer_send_fee,
};
use near_primitives_core::account::AccountContract;
use near_primitives_core::config::INLINE_DISK_VALUE_THRESHOLD;
use near_primitives_core::gas::Gas;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, Balance, GasWeight};
use std::rc::Rc;
use wasmtime::component::Resource;

impl From<u128> for runtime::U128 {
    fn from(value: u128) -> Self {
        Self { lo: value as _, hi: (value >> 64) as _ }
    }
}

impl From<runtime::U128> for u128 {
    fn from(runtime::U128 { lo, hi }: runtime::U128) -> Self {
        u128::from(lo) | (u128::from(hi) << 64)
    }
}

impl runtime::U128 {
    fn to_le_bytes(&self) -> [u8; 16] {
        let lo = self.lo.to_le_bytes();
        let hi = self.hi.to_le_bytes();
        array::from_fn(|i| if i < 8 { lo[i] } else { hi[i - 8] })
    }

    fn from_le_bytes(bytes: [u8; 16]) -> Self {
        u128::from_le_bytes(bytes).into()
    }
}

impl runtime::U160 {
    #[expect(unused)]
    fn to_le_bytes(&self) -> [u8; 20] {
        let lo = self.lo.to_le_bytes();
        let hi = self.hi.to_le_bytes();
        array::from_fn(|i| if i < 10 { lo[i] } else { hi[i - 10] })
    }

    fn from_le_bytes(
        [v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19]: [u8; 20],
    ) -> Self {
        Self {
            lo: runtime::U128::from_le_bytes([
                v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15,
            ]),
            hi: u32::from_le_bytes([v16, v17, v18, v19]),
        }
    }
}

impl runtime::U256 {
    fn to_le_bytes(&self) -> [u8; 32] {
        let lo = self.lo.to_le_bytes();
        let hi = self.hi.to_le_bytes();
        array::from_fn(|i| if i < 16 { lo[i] } else { hi[i - 16] })
    }

    fn from_le_bytes(
        [
            v0,
            v1,
            v2,
            v3,
            v4,
            v5,
            v6,
            v7,
            v8,
            v9,
            v10,
            v11,
            v12,
            v13,
            v14,
            v15,
            v16,
            v17,
            v18,
            v19,
            v20,
            v21,
            v22,
            v23,
            v24,
            v25,
            v26,
            v27,
            v28,
            v29,
            v30,
            v31,
        ]: [u8; 32],
    ) -> Self {
        Self {
            lo: runtime::U128::from_le_bytes([
                v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15,
            ]),
            hi: runtime::U128::from_le_bytes([
                v16, v17, v18, v19, v20, v21, v22, v23, v24, v25, v26, v27, v28, v29, v30, v31,
            ]),
        }
    }
}

impl runtime::U512 {
    fn to_le_bytes(&self) -> [u8; 64] {
        let lo = self.lo.to_le_bytes();
        let hi = self.hi.to_le_bytes();
        array::from_fn(|i| if i < 32 { lo[i] } else { hi[i - 32] })
    }

    fn from_le_bytes(bytes: [u8; 64]) -> Self {
        let ([lo, hi], []) = bytes.as_chunks::<32>() else {
            unreachable!();
        };
        Self { lo: runtime::U256::from_le_bytes(*lo), hi: runtime::U256::from_le_bytes(*hi) }
    }
}

impl From<PublicKey> for runtime::PublicKey {
    fn from(key: PublicKey) -> Self {
        match key {
            PublicKey::ED25519(key) => Self::Ed25519(runtime::U256::from_le_bytes(key.0)),
            PublicKey::SECP256K1(key) => Self::Secp256k1(runtime::U512::from_le_bytes(key.0)),
        }
    }
}

impl From<runtime::PublicKey> for PublicKey {
    fn from(key: runtime::PublicKey) -> Self {
        match key {
            runtime::PublicKey::Ed25519(key) => Self::ED25519(key.to_le_bytes().into()),
            runtime::PublicKey::Secp256k1(key) => Self::SECP256K1(key.to_le_bytes().into()),
        }
    }
}

fn pay_base(gas_counter: &mut GasCounter, cost: ExtCosts) -> wasmtime::Result<()> {
    gas_counter.pay_base(cost).map_err(ErrorContainer::new)?;
    Ok(())
}

fn pay_per(gas_counter: &mut GasCounter, cost: ExtCosts, num: u64) -> wasmtime::Result<()> {
    gas_counter.pay_per(cost, num).map_err(ErrorContainer::new)?;
    Ok(())
}

/// A helper function to pay base cost gas fee for batching an action.
fn pay_action_accumulated(
    gas_counter: &mut GasCounter,
    burn_gas: Gas,
    use_gas: Gas,
    action: ActionCosts,
) -> wasmtime::Result<()> {
    gas_counter.pay_action_accumulated(burn_gas, use_gas, action).map_err(ErrorContainer::new)?;
    Ok(())
}

/// A helper function to pay base cost gas fee for batching an action.
fn pay_action_base(
    gas_counter: &mut GasCounter,
    fees_config: &RuntimeFeesConfig,
    action: ActionCosts,
    sir: bool,
) -> wasmtime::Result<()> {
    let base_fee = fees_config.fee(action);
    let burn_gas = base_fee.send_fee(sir);
    let use_gas = burn_gas
        .checked_add(base_fee.exec_fee())
        .ok_or(HostError::IntegerOverflow)
        .map_err(ErrorContainer::new)?;
    pay_action_accumulated(gas_counter, burn_gas, use_gas, action)?;
    Ok(())
}

/// A helper function to pay per byte gas fee for batching an action.
fn pay_action_per_byte(
    gas_counter: &mut GasCounter,
    fees_config: &RuntimeFeesConfig,
    action: ActionCosts,
    num_bytes: u64,
    sir: bool,
) -> wasmtime::Result<()> {
    let per_byte_fee = fees_config.fee(action);
    let burn_gas = per_byte_fee
        .send_fee(sir)
        .checked_mul(num_bytes)
        .ok_or(HostError::IntegerOverflow)
        .map_err(ErrorContainer::new)?;

    let charge = per_byte_fee
        .exec_fee()
        .checked_mul(num_bytes)
        .ok_or(HostError::IntegerOverflow)
        .map_err(ErrorContainer::new)?;
    let use_gas = burn_gas
        .checked_add(charge)
        .ok_or(HostError::IntegerOverflow)
        .map_err(ErrorContainer::new)?;
    pay_action_accumulated(gas_counter, burn_gas, use_gas, action)?;
    Ok(())
}

fn pay_for_writing_bytes(gas_counter: &mut GasCounter, n: usize) -> wasmtime::Result<()> {
    pay_base(gas_counter, write_memory_base)?;
    pay_per(gas_counter, write_memory_byte, n as _)?;
    Ok(())
}

impl runtime::U128 {
    fn read(self, gas_counter: &mut GasCounter) -> wasmtime::Result<u128> {
        pay_base(gas_counter, read_memory_base)?;
        pay_per(gas_counter, read_memory_byte, 16)?;
        Ok(u128::from(self))
    }

    fn write(v: u128, gas_counter: &mut GasCounter) -> wasmtime::Result<Self> {
        pay_base(gas_counter, write_memory_base)?;
        pay_per(gas_counter, write_memory_byte, 16)?;
        Ok(Self::from(v))
    }
}

/// A helper function to pay gas fee for creating a new receipt without actions.
/// # Args:
/// * `sir`: whether contract call is addressed to itself;
/// * `data_dependencies`: other contracts that this execution will be waiting on (or rather
///   their data receipts), where bool indicates whether this is sender=receiver communication.
///
/// # Cost
///
/// This is a convenience function that encapsulates several costs:
/// `burnt_gas := dispatch cost of the receipt + base dispatch cost of the data receipt`
/// `used_gas := burnt_gas + exec cost of the receipt + base exec cost of the data receipt`
/// Notice that we prepay all base cost upon the creation of the data dependency, we are going to
/// pay for the content transmitted through the dependency upon the actual creation of the
/// DataReceipt.
fn pay_gas_for_new_receipt(
    gas_counter: &mut GasCounter,
    fees_config: &RuntimeFeesConfig,
    sir: bool,
    data_dependencies: &[bool],
) -> wasmtime::Result<()> {
    let mut burn_gas = fees_config.fee(ActionCosts::new_action_receipt).send_fee(sir);
    let mut use_gas = fees_config.fee(ActionCosts::new_action_receipt).exec_fee();
    for dep in data_dependencies {
        // Both creation and execution for data receipts are considered burnt gas.
        burn_gas = burn_gas
            .checked_add(fees_config.fee(ActionCosts::new_data_receipt_base).send_fee(*dep))
            .ok_or(HostError::IntegerOverflow)
            .map_err(ErrorContainer::new)?
            .checked_add(fees_config.fee(ActionCosts::new_data_receipt_base).exec_fee())
            .ok_or(HostError::IntegerOverflow)
            .map_err(ErrorContainer::new)?;
    }
    use_gas = use_gas
        .checked_add(burn_gas)
        .ok_or(HostError::IntegerOverflow)
        .map_err(ErrorContainer::new)?;
    // This should go to `new_data_receipt_base` and `new_action_receipt` in parts.
    // But we have to keep charing these two together unless we make a protocol change.
    pay_action_accumulated(gas_counter, burn_gas, use_gas, ActionCosts::new_action_receipt)?;
    Ok(())
}

impl Ctx {
    fn pay_base(&mut self, cost: ExtCosts) -> wasmtime::Result<()> {
        pay_base(&mut self.result_state.gas_counter, cost)?;
        Ok(())
    }

    fn pay_per(&mut self, cost: ExtCosts, num: u64) -> wasmtime::Result<()> {
        pay_per(&mut self.result_state.gas_counter, cost, num)?;
        Ok(())
    }

    fn pay_for_reading_bytes(&mut self, n: usize) -> wasmtime::Result<()> {
        self.pay_base(read_memory_base)?;
        let n =
            u64::try_from(n).map_err(|_| ErrorContainer::new(HostError::MemoryAccessViolation))?;
        self.pay_per(read_memory_byte, n)?;
        Ok(())
    }

    fn pay_for_writing_bytes(&mut self, n: usize) -> wasmtime::Result<()> {
        pay_for_writing_bytes(&mut self.result_state.gas_counter, n)
    }

    fn pay_for_reading_string(&mut self, n: usize) -> wasmtime::Result<()> {
        self.pay_for_reading_bytes(n)?;
        self.pay_base(utf8_decoding_base)?;
        self.pay_per(utf8_decoding_byte, n as _)?;
        Ok(())
    }

    fn deduct_balance(&mut self, amount: Balance) -> wasmtime::Result<()> {
        self.result_state.deduct_balance(amount).map_err(ErrorContainer::new)?;
        Ok(())
    }

    fn load_action(&mut self, action: &Resource<ActionIndex>) -> wasmtime::Result<ActionIndex> {
        self.pay_base(read_register_base)?;
        self.pay_per(read_register_byte, 8)?;
        let action = self.table.get(action)?;
        Ok(*action)
    }

    fn store_action(&mut self, action: ActionIndex) -> wasmtime::Result<Resource<ActionIndex>> {
        self.pay_base(write_register_base)?;
        self.pay_per(write_register_byte, 8)?;
        let action = self.table.push(action)?;
        Ok(action)
    }

    fn load_promise_idx(
        &mut self,
        promise: &Resource<PromiseIndex>,
    ) -> wasmtime::Result<PromiseIndex> {
        self.pay_base(read_register_base)?;
        self.pay_per(read_register_byte, 8)?;
        let promise_idx = self.table.get(promise)?;
        Ok(*promise_idx)
    }

    fn store_promise_idx(
        &mut self,
        promise_idx: PromiseIndex,
    ) -> wasmtime::Result<Resource<PromiseIndex>> {
        self.pay_base(write_register_base)?;
        self.pay_per(write_register_byte, 8)?;
        let promise_idx = self.table.push(promise_idx)?;
        Ok(promise_idx)
    }

    fn load_account_id(&mut self, account_id: &Resource<AccountId>) -> wasmtime::Result<AccountId> {
        self.pay_base(read_register_base)?;
        let account_id = self.table.get(account_id)?;
        pay_per(&mut self.result_state.gas_counter, read_register_byte, account_id.len() as _)?;
        Ok(account_id.clone())
    }

    fn store_account_id(&mut self, account_id: AccountId) -> wasmtime::Result<Resource<AccountId>> {
        self.pay_base(write_register_base)?;
        self.pay_per(write_register_byte, account_id.len() as _)?;
        let account_id = self.table.push(account_id)?;
        Ok(account_id)
    }

    fn read_public_key(&mut self, public_key: &runtime::PublicKey) -> wasmtime::Result<PublicKey> {
        match public_key {
            runtime::PublicKey::Ed25519(key) => {
                self.pay_for_reading_bytes(32)?;
                Ok(PublicKey::ED25519(key.to_le_bytes().into()))
            }
            runtime::PublicKey::Secp256k1(key) => {
                self.pay_for_reading_bytes(64)?;
                Ok(PublicKey::SECP256K1(key.to_le_bytes().into()))
            }
        }
    }

    /// Adds a given promise to the vector of promises and returns a new promise index.
    /// Throws `NumberPromisesExceeded` if the total number of promises exceeded the limit.
    fn checked_push_promise(
        &mut self,
        promise: Promise,
    ) -> wasmtime::Result<Resource<PromiseIndex>> {
        let new_promise_idx = self.promises.len() as PromiseIndex;
        self.promises.push(promise);
        if self.promises.len() as u64
            > self.config.limit_config.max_promises_per_function_call_action
        {
            Err(ErrorContainer::new(HostError::NumberPromisesExceeded {
                number_of_promises: self.promises.len() as u64,
                limit: self.config.limit_config.max_promises_per_function_call_action,
            })
            .into())
        } else {
            self.store_promise_idx(new_promise_idx)
        }
    }

    /// Helper function to return the receipt index corresponding to the given promise index.
    /// It also pulls account ID for the given receipt and compares it with the current account ID
    /// to return whether the receipt's account ID is the same.
    fn promise_idx_to_receipt_idx_with_sir(
        &self,
        promise_idx: u64,
    ) -> wasmtime::Result<(ReceiptIndex, bool)> {
        let promise = self
            .promises
            .get(promise_idx as usize)
            .ok_or(HostError::InvalidPromiseIndex { promise_idx })
            .map_err(ErrorContainer::new)?;
        let receipt_idx = match &promise {
            Promise::Receipt(receipt_idx) => Ok(*receipt_idx),
            Promise::NotReceipt(_) => {
                Err(ErrorContainer::new(HostError::CannotAppendActionToJointPromise))
            }
        }?;

        let account_id = self.ext.get_receipt_receiver(receipt_idx);
        let sir = account_id == &self.context.current_account_id;
        Ok((receipt_idx, sir))
    }

    fn promise_batch_action_deploy_global_contract_impl(
        &mut self,
        promise: Resource<PromiseIndex>,
        code: Vec<u8>,
        mode: GlobalContractDeployMode,
        method_name: &str,
    ) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: method_name.to_owned(),
            })
            .into());
        }
        self.pay_for_reading_bytes(code.len())?;
        let code_len = code.len() as u64;
        let limit = self.config.limit_config.max_contract_size;
        if code_len > limit {
            return Err(ErrorContainer::new(HostError::ContractSizeExceeded {
                size: code_len,
                limit,
            })
            .into());
        }

        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::deploy_global_contract_base,
            sir,
        )?;
        pay_action_per_byte(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::deploy_global_contract_byte,
            code_len,
            sir,
        )?;

        self.ext
            .append_action_deploy_global_contract(receipt_idx, code, mode)
            .map_err(ErrorContainer::new)?;
        Ok(())
    }
}

macro_rules! bls12381_impl {
    (
        $fn_name:ident,
        $ITEM_SIZE:expr,
        $bls12381_base:ident,
        $bls12381_element:ident,
        $impl_fn_name:ident
    ) => {
        fn $fn_name(&mut self, value: Vec<u8>) -> wasmtime::Result<Result<Vec<u8>, ()>> {
            self.pay_base($bls12381_base)?;
            self.pay_for_reading_bytes(value.len())?;

            let elements_count = value.len() / $ITEM_SIZE;
            self.pay_per($bls12381_element, elements_count as u64)?;

            let res_option = bls12381::$impl_fn_name(&value).map_err(ErrorContainer::new)?;

            if let Some(res) = res_option {
                self.pay_for_writing_bytes(res.len())?;
                Ok(Ok(res))
            } else {
                Ok(Err(()))
            }
        }
    };
}

impl finite_wasm::Host for Ctx {
    fn gas_exhausted(&mut self) -> wasmtime::Result<()> {
        // Burn all remaining gas
        self.result_state
            .gas_counter
            .burn_gas(self.result_state.gas_counter.remaining_gas())
            .map_err(ErrorContainer::new)?;
        // This function will only ever be called by instrumentation on overflow, otherwise
        // `finite_wasm_gas` will be called with the out-of-budget charge

        Err(ErrorContainer::new(HostError::IntegerOverflow).into())
    }

    fn stack_exhausted(&mut self) -> wasmtime::Result<()> {
        Err(ErrorContainer::new(HostError::MemoryAccessViolation).into())
    }

    fn burn_gas(&mut self, gas: u64) -> wasmtime::Result<()> {
        self.result_state.gas_counter.burn_gas(Gas::from_gas(gas)).map_err(ErrorContainer::new)?;
        Ok(())
    }
}

impl runtime::Host for Ctx {
    fn write_register(&mut self, register_id: u64, data: Vec<u8>) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        self.pay_for_reading_bytes(data.len())?;
        self.registers
            .set(&mut self.result_state.gas_counter, &self.config.limit_config, register_id, data)
            .map_err(ErrorContainer::new)?;
        Ok(())
    }

    fn read_register(&mut self, register_id: u64) -> wasmtime::Result<Vec<u8>> {
        self.pay_base(base)?;
        let buf = self
            .registers
            .get(&mut self.result_state.gas_counter, register_id)
            .map_err(ErrorContainer::new)?;
        pay_base(&mut self.result_state.gas_counter, write_memory_base)?;
        pay_per(&mut self.result_state.gas_counter, write_memory_byte, buf.len() as _)?;
        Ok(buf.into())
    }

    fn register_len(&mut self, register_id: u64) -> wasmtime::Result<Option<u64>> {
        self.pay_base(base)?;
        Ok(self.registers.get_len(register_id))
    }

    fn current_account_id(&mut self) -> wasmtime::Result<Resource<AccountId>> {
        self.pay_base(base)?;
        self.store_account_id(self.context.current_account_id.clone())
    }

    fn signer_account_id(&mut self) -> wasmtime::Result<Resource<AccountId>> {
        self.pay_base(base)?;

        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "signer_account_id".to_string(),
            })
            .into());
        }
        self.store_account_id(self.context.signer_account_id.clone())
    }

    fn signer_account_pk(&mut self) -> wasmtime::Result<runtime::PublicKey> {
        self.pay_base(base)?;

        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "signer_account_pk".to_string(),
            })
            .into());
        }
        let public_key = PublicKeyBuffer::new(&self.context.signer_account_pk)
            .decode()
            .map_err(ErrorContainer::new)?;
        self.pay_base(write_register_base)?;
        self.pay_per(write_register_byte, public_key.len() as _)?;
        Ok(public_key.into())
    }

    fn predecessor_account_id(&mut self) -> wasmtime::Result<Resource<AccountId>> {
        self.pay_base(base)?;

        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "predecessor_account_id".to_string(),
            })
            .into());
        }
        self.store_account_id(self.context.predecessor_account_id.clone())
    }

    fn refund_to_account_id(&mut self) -> wasmtime::Result<Resource<AccountId>> {
        self.pay_base(base)?;

        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "refund_to_account_id".to_string(),
            })
            .into());
        }
        self.store_account_id(self.context.refund_to_account_id.clone())
    }

    fn input(&mut self) -> wasmtime::Result<Vec<u8>> {
        self.pay_base(base)?;
        self.pay_for_writing_bytes(self.context.input.len())?;
        Ok(self.context.input.to_vec())
    }

    fn block_height(&mut self) -> wasmtime::Result<u64> {
        self.pay_base(base)?;
        Ok(self.context.block_height)
    }

    fn block_timestamp(&mut self) -> wasmtime::Result<u64> {
        self.pay_base(base)?;
        Ok(self.context.block_timestamp)
    }

    fn epoch_height(&mut self) -> wasmtime::Result<u64> {
        self.pay_base(base)?;
        Ok(self.context.epoch_height)
    }

    fn validator_stake(
        &mut self,
        account_id: Resource<AccountId>,
    ) -> wasmtime::Result<runtime::U128> {
        self.pay_base(base)?;
        self.pay_base(validator_stake_base)?;
        let account_id = self.load_account_id(&account_id)?;
        let balance =
            self.ext.validator_stake(&account_id).map_err(ErrorContainer::new)?.unwrap_or_default();
        let v = runtime::U128::write(balance.as_yoctonear(), &mut self.result_state.gas_counter)?;
        Ok(v)
    }

    fn validator_total_stake(&mut self) -> wasmtime::Result<runtime::U128> {
        self.pay_base(base)?;
        self.pay_base(validator_total_stake_base)?;
        let total_stake = self.ext.validator_total_stake().map_err(ErrorContainer::new)?;
        let v =
            runtime::U128::write(total_stake.as_yoctonear(), &mut self.result_state.gas_counter)?;
        Ok(v)
    }

    fn storage_usage(&mut self) -> wasmtime::Result<u64> {
        self.pay_base(base)?;
        Ok(self.result_state.current_storage_usage)
    }

    fn account_balance(&mut self) -> wasmtime::Result<runtime::U128> {
        self.pay_base(base)?;
        let v = runtime::U128::write(
            self.result_state.current_account_balance.as_yoctonear(),
            &mut self.result_state.gas_counter,
        )?;
        Ok(v)
    }

    fn account_locked_balance(&mut self) -> wasmtime::Result<runtime::U128> {
        self.pay_base(base)?;
        let v = runtime::U128::write(
            self.current_account_locked_balance.as_yoctonear(),
            &mut self.result_state.gas_counter,
        )?;
        Ok(v)
    }

    fn attached_deposit(&mut self) -> wasmtime::Result<runtime::U128> {
        self.pay_base(base)?;
        let v = runtime::U128::write(
            self.context.attached_deposit.as_yoctonear(),
            &mut self.result_state.gas_counter,
        )?;
        Ok(v)
    }

    fn prepaid_gas(&mut self) -> wasmtime::Result<u64> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "prepaid_gas".to_string(),
            })
            .into());
        }
        Ok(self.context.prepaid_gas.as_gas())
    }

    fn used_gas(&mut self) -> wasmtime::Result<u64> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "used_gas".to_string(),
            })
            .into());
        }
        Ok(self.result_state.gas_counter.used_gas().as_gas())
    }

    fn alt_bn128_g1_multiexp(&mut self, value: Vec<u8>) -> wasmtime::Result<runtime::U512> {
        self.pay_base(alt_bn128_g1_multiexp_base)?;
        self.pay_for_reading_bytes(value.len())?;

        let elements = alt_bn128::split_elements(&value).map_err(ErrorContainer::new)?;
        self.pay_per(alt_bn128_g1_multiexp_element, elements.len() as u64)?;

        let res = alt_bn128::g1_multiexp(elements).map_err(ErrorContainer::new)?;
        self.pay_for_writing_bytes(64)?;
        Ok(runtime::U512::from_le_bytes(res))
    }

    fn alt_bn128_g1_sum(&mut self, value: Vec<u8>) -> wasmtime::Result<runtime::U512> {
        self.pay_base(alt_bn128_g1_sum_base)?;
        self.pay_for_reading_bytes(value.len())?;

        let elements = alt_bn128::split_elements(&value).map_err(ErrorContainer::new)?;
        self.pay_per(alt_bn128_g1_sum_element, elements.len() as u64)?;

        let res = alt_bn128::g1_sum(elements).map_err(ErrorContainer::new)?;
        self.pay_for_writing_bytes(64)?;
        Ok(runtime::U512::from_le_bytes(res))
    }

    fn alt_bn128_pairing_check(&mut self, value: Vec<u8>) -> wasmtime::Result<bool> {
        self.pay_base(alt_bn128_pairing_check_base)?;
        self.pay_for_reading_bytes(value.len())?;

        let elements = alt_bn128::split_elements(&value).map_err(ErrorContainer::new)?;
        self.pay_per(alt_bn128_pairing_check_element, elements.len() as u64)?;

        let res = alt_bn128::pairing_check(elements).map_err(ErrorContainer::new)?;
        Ok(res)
    }

    bls12381_impl!(bls12381_p1_sum, 97, bls12381_p1_sum_base, bls12381_p1_sum_element, p1_sum);

    bls12381_impl!(bls12381_p2_sum, 193, bls12381_p2_sum_base, bls12381_p2_sum_element, p2_sum);

    bls12381_impl!(
        bls12381_g1_multiexp,
        128,
        bls12381_g1_multiexp_base,
        bls12381_g1_multiexp_element,
        g1_multiexp
    );

    bls12381_impl!(
        bls12381_g2_multiexp,
        224,
        bls12381_g2_multiexp_base,
        bls12381_g2_multiexp_element,
        g2_multiexp
    );

    bls12381_impl!(
        bls12381_map_fp_to_g1,
        48,
        bls12381_map_fp_to_g1_base,
        bls12381_map_fp_to_g1_element,
        map_fp_to_g1
    );

    bls12381_impl!(
        bls12381_map_fp2_to_g2,
        96,
        bls12381_map_fp2_to_g2_base,
        bls12381_map_fp2_to_g2_element,
        map_fp2_to_g2
    );

    fn bls12381_pairing_check(&mut self, value: Vec<u8>) -> wasmtime::Result<Result<bool, ()>> {
        self.pay_base(bls12381_pairing_base)?;

        const BLS_P1_SIZE: usize = 96;
        const BLS_P2_SIZE: usize = 192;
        const ITEM_SIZE: usize = BLS_P1_SIZE + BLS_P2_SIZE;

        self.pay_for_reading_bytes(value.len())?;
        let elements_count = value.len() / ITEM_SIZE;

        self.pay_per(bls12381_pairing_element, elements_count as u64)?;

        match bls12381::pairing_check(&value)? {
            0 => Ok(Ok(true)),
            1 => Ok(Err(())),
            2 => Ok(Ok(false)),
            n => panic!("unexpected pairing check result: {n}"),
        }
    }

    bls12381_impl!(
        bls12381_p1_decompress,
        48,
        bls12381_p1_decompress_base,
        bls12381_p1_decompress_element,
        p1_decompress
    );

    bls12381_impl!(
        bls12381_p2_decompress,
        96,
        bls12381_p2_decompress_base,
        bls12381_p2_decompress_element,
        p2_decompress
    );

    fn random_seed(&mut self) -> wasmtime::Result<Vec<u8>> {
        self.pay_base(base)?;
        self.pay_for_writing_bytes(self.context.random_seed.len())?;
        Ok(self.context.random_seed.clone())
    }

    fn sha256(&mut self, value: Vec<u8>) -> wasmtime::Result<runtime::U256> {
        self.pay_base(sha256_base)?;
        self.pay_for_reading_bytes(value.len())?;
        self.pay_per(sha256_byte, value.len() as u64)?;

        use sha2::Digest;

        let value_hash = sha2::Sha256::digest(&value);
        self.pay_for_writing_bytes(32)?;
        Ok(runtime::U256::from_le_bytes(value_hash.into()))
    }

    fn keccak256(&mut self, value: Vec<u8>) -> wasmtime::Result<runtime::U256> {
        self.pay_base(keccak256_base)?;
        self.pay_for_reading_bytes(value.len())?;
        self.pay_per(keccak256_byte, value.len() as u64)?;

        use sha3::Digest;

        let value_hash = sha3::Keccak256::digest(&value);
        self.pay_for_writing_bytes(32)?;
        Ok(runtime::U256::from_le_bytes(value_hash.into()))
    }

    fn keccak512(&mut self, value: Vec<u8>) -> wasmtime::Result<runtime::U512> {
        self.pay_base(keccak512_base)?;
        self.pay_for_reading_bytes(value.len())?;
        self.pay_per(keccak512_byte, value.len() as u64)?;

        use sha3::Digest;

        let value_hash = sha3::Keccak512::digest(&value);
        self.pay_for_writing_bytes(64)?;
        Ok(runtime::U512::from_le_bytes(value_hash.into()))
    }

    fn ripemd160(&mut self, value: Vec<u8>) -> wasmtime::Result<runtime::U160> {
        self.pay_base(ripemd160_base)?;
        self.pay_for_reading_bytes(value.len())?;

        let message_blocks = value
            .len()
            .checked_add(8)
            .ok_or_else(|| ErrorContainer::new(HostError::IntegerOverflow))?
            / 64
            + 1;

        self.pay_per(ripemd160_block, message_blocks as u64)?;

        use ripemd::Digest;

        let value_hash = ripemd::Ripemd160::digest(&value);
        self.pay_for_writing_bytes(20)?;
        Ok(runtime::U160::from_le_bytes(value_hash.into()))
    }

    fn ecrecover(
        &mut self,
        hash: runtime::U256,
        signature: runtime::U512,
        v: u8,
        malleability: bool,
    ) -> wasmtime::Result<Option<runtime::U512>> {
        self.pay_base(ecrecover_base)?;
        self.pay_for_reading_bytes(64)?;
        let signature = {
            if v > 3 {
                return Err(ErrorContainer::new(HostError::ECRecoverError {
                    msg: format!("V recovery byte 0 through 3 are valid but was provided {}", v),
                })
                .into());
            }
            let signature = signature.to_le_bytes();
            let bytes = array::from_fn(|i| if i == 64 { v } else { signature[i] });
            Secp256K1Signature::from(bytes)
        };

        self.pay_for_reading_bytes(32)?;
        let hash = hash.to_le_bytes();

        if !signature.check_signature_values(malleability) {
            return Ok(None);
        }

        if let Ok(pk) = signature.recover(hash) {
            self.pay_for_writing_bytes(64)?;
            return Ok(Some(runtime::U512::from_le_bytes(pk.0)));
        };

        Ok(None)
    }

    fn ed25519_verify(
        &mut self,
        signature: runtime::U512,
        message: Vec<u8>,
        public_key: runtime::U256,
    ) -> wasmtime::Result<bool> {
        use ed25519_dalek::Verifier;

        self.pay_base(ed25519_verify_base)?;
        self.pay_for_reading_bytes(ed25519_dalek::SIGNATURE_LENGTH)?;
        let signature: ed25519_dalek::Signature = {
            let b = signature.to_le_bytes();
            // Sanity-check that was performed by ed25519-dalek in from_bytes before version 2,
            // but was removed with version 2. It is not actually any good a check, but we need
            // it to avoid costs changing.
            if b[ed25519_dalek::SIGNATURE_LENGTH - 1] & 0b1110_0000 != 0 {
                return Ok(false);
            }
            ed25519_dalek::Signature::from_bytes(&b)
        };

        self.pay_for_reading_bytes(message.len())?;
        self.pay_per(ed25519_verify_byte, message.len() as _)?;

        self.pay_for_reading_bytes(ed25519_dalek::PUBLIC_KEY_LENGTH)?;
        let public_key = match ed25519_dalek::VerifyingKey::from_bytes(&public_key.to_le_bytes()) {
            Ok(public_key) => public_key,
            Err(_) => return Ok(false),
        };

        match public_key.verify(&message, &signature) {
            Err(_) => Ok(false),
            Ok(()) => Ok(true),
        }
    }

    fn value_return(&mut self, value: Vec<u8>) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        self.pay_for_reading_bytes(value.len())?;
        let mut burn_gas: Gas = Gas::ZERO;
        let num_bytes = value.len() as u64;
        if num_bytes > self.config.limit_config.max_length_returned_data {
            return Err(ErrorContainer::new(HostError::ReturnedValueLengthExceeded {
                length: num_bytes,
                limit: self.config.limit_config.max_length_returned_data,
            })
            .into());
        }
        for data_receiver in &self.context.output_data_receivers {
            let sir = data_receiver == &self.context.current_account_id;
            // We deduct for execution here too, because if we later have an OR combinator
            // for promises then we might have some valid data receipts that arrive too late
            // to be picked up by the execution that waits on them (because it has started
            // after it receives the first data receipt) and then we need to issue a special
            // refund in this situation. Which we avoid by just paying for execution of
            // data receipt that might not be performed.
            // The gas here is considered burnt, cause we'll prepay for it upfront.
            burn_gas = burn_gas
                .checked_add(
                    self.fees_config
                        .fee(ActionCosts::new_data_receipt_byte)
                        .send_fee(sir)
                        .checked_add(
                            self.fees_config.fee(ActionCosts::new_data_receipt_byte).exec_fee(),
                        )
                        .ok_or(HostError::IntegerOverflow)
                        .map_err(ErrorContainer::new)?
                        .checked_mul(num_bytes)
                        .ok_or(HostError::IntegerOverflow)
                        .map_err(ErrorContainer::new)?,
                )
                .ok_or(HostError::IntegerOverflow)
                .map_err(ErrorContainer::new)?;
        }
        pay_action_accumulated(
            &mut self.result_state.gas_counter,
            burn_gas,
            burn_gas,
            ActionCosts::new_data_receipt_byte,
        )?;
        self.result_state.return_data = ReturnData::Value(value);
        Ok(())
    }

    fn panic(&mut self, s: Option<String>) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        let panic_msg = if let Some(s) = s {
            self.pay_for_reading_string(s.len())?;
            let max_len = self
                .result_state
                .config
                .limit_config
                .max_total_log_length
                .saturating_sub(self.result_state.total_log_length);
            if s.len() as u64 > max_len {
                return self
                    .result_state
                    .total_log_length_exceeded(s.len() as _)
                    .map_err(Into::into);
            }
            s
        } else {
            "explicit guest panic".to_string()
        };
        Err(ErrorContainer::new(HostError::GuestPanic { panic_msg }).into())
    }

    fn log(&mut self, s: String) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        self.result_state.check_can_add_a_log_message()?;
        self.pay_base(log_base)?;
        self.pay_per(log_byte, s.len() as u64)?;
        self.result_state.checked_push_log(s)?;
        Ok(())
    }

    fn current_contract_code(&mut self) -> wasmtime::Result<Option<runtime::ContractCodeKind>> {
        self.pay_base(base)?;
        match &self.context.account_contract {
            AccountContract::None => Ok(None),
            AccountContract::Local(crypto_hash) => {
                self.pay_for_writing_bytes(32)?;
                Ok(Some(runtime::ContractCodeKind::Local(runtime::U256::from_le_bytes(
                    crypto_hash.0,
                ))))
            }
            AccountContract::Global(crypto_hash) => {
                self.pay_for_writing_bytes(32)?;
                Ok(Some(runtime::ContractCodeKind::Global(runtime::U256::from_le_bytes(
                    crypto_hash.0,
                ))))
            }
            AccountContract::GlobalByAccount(account_id) => {
                let account_id = self.store_account_id(account_id.clone())?;
                Ok(Some(runtime::ContractCodeKind::GlobalByAccount(account_id)))
            }
        }
    }

    fn storage_write(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        register_id: u64,
    ) -> wasmtime::Result<bool> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "storage_write".to_string(),
            })
            .into());
        }
        self.pay_base(storage_write_base)?;
        self.pay_for_reading_bytes(key.len())?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(ErrorContainer::new(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            })
            .into());
        }
        self.pay_for_reading_bytes(value.len())?;
        if value.len() as u64 > self.config.limit_config.max_length_storage_value {
            return Err(ErrorContainer::new(HostError::ValueLengthExceeded {
                length: value.len() as u64,
                limit: self.config.limit_config.max_length_storage_value,
            })
            .into());
        }
        self.pay_per(storage_write_key_byte, key.len() as u64)?;
        self.pay_per(storage_write_value_byte, value.len() as u64)?;
        let evicted = self
            .ext
            .storage_set(&mut self.result_state.gas_counter, &key, &value)
            .map_err(ErrorContainer::new)?;
        let storage_config = &self.fees_config.storage_usage_config;
        self.recorded_storage_counter
            .observe_size(self.ext.get_recorded_storage_size())
            .map_err(ErrorContainer::new)?;
        match evicted {
            Some(old_value) => {
                // Inner value can't overflow, because the value length is limited.
                self.result_state.current_storage_usage = self
                    .result_state
                    .current_storage_usage
                    .checked_sub(old_value.len() as u64)
                    .ok_or(InconsistentStateError::IntegerOverflow)
                    .map_err(ErrorContainer::new)?;
                // Inner value can't overflow, because the value length is limited.
                self.result_state.current_storage_usage = self
                    .result_state
                    .current_storage_usage
                    .checked_add(value.len() as u64)
                    .ok_or(InconsistentStateError::IntegerOverflow)
                    .map_err(ErrorContainer::new)?;
                self.registers
                    .set(
                        &mut self.result_state.gas_counter,
                        &self.config.limit_config,
                        register_id,
                        old_value,
                    )
                    .map_err(ErrorContainer::new)?;
                Ok(true)
            }
            None => {
                // Inner value can't overflow, because the key/value length is limited.
                self.result_state.current_storage_usage = self
                    .result_state
                    .current_storage_usage
                    .checked_add(
                        value.len() as u64
                            + key.len() as u64
                            + storage_config.num_extra_bytes_record,
                    )
                    .ok_or(InconsistentStateError::IntegerOverflow)
                    .map_err(ErrorContainer::new)?;
                Ok(false)
            }
        }
    }

    fn storage_read(&mut self, key: Vec<u8>) -> wasmtime::Result<Option<Vec<u8>>> {
        self.pay_base(base)?;
        self.pay_base(storage_read_base)?;
        self.pay_for_reading_bytes(key.len())?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(ErrorContainer::new(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            })
            .into());
        }
        self.pay_per(storage_read_key_byte, key.len() as u64)?;
        let read = self.ext.storage_get(&mut self.result_state.gas_counter, &key);
        let read = match read.map_err(ErrorContainer::new)? {
            Some(read) => {
                // Here we'll do u32 -> usize -> u64, which is always infallible
                let read_len = read.len() as usize;
                pay_per(
                    &mut self.result_state.gas_counter,
                    storage_read_value_byte,
                    read_len as u64,
                )?;
                if read_len > INLINE_DISK_VALUE_THRESHOLD {
                    pay_base(&mut self.result_state.gas_counter, storage_large_read_overhead_base)?;
                    pay_per(
                        &mut self.result_state.gas_counter,
                        storage_large_read_overhead_byte,
                        read_len as u64,
                    )?;
                }
                Some(read.deref(&mut FreeGasCounter).map_err(ErrorContainer::new)?)
            }
            None => None,
        };

        self.recorded_storage_counter
            .observe_size(self.ext.get_recorded_storage_size())
            .map_err(ErrorContainer::new)?;
        match read {
            Some(value) => {
                self.pay_for_writing_bytes(value.len())?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn storage_remove(&mut self, key: Vec<u8>, register_id: u64) -> wasmtime::Result<bool> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "storage_remove".to_string(),
            })
            .into());
        }
        self.pay_base(storage_remove_base)?;
        self.pay_for_reading_bytes(key.len())?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(ErrorContainer::new(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            })
            .into());
        }
        self.pay_per(storage_remove_key_byte, key.len() as u64)?;
        let removed = self
            .ext
            .storage_remove(&mut self.result_state.gas_counter, &key)
            .map_err(ErrorContainer::new)?;
        let storage_config = &self.fees_config.storage_usage_config;
        self.recorded_storage_counter
            .observe_size(self.ext.get_recorded_storage_size())
            .map_err(ErrorContainer::new)?;
        match removed {
            Some(value) => {
                // Inner value can't overflow, because the key/value length is limited.
                self.result_state.current_storage_usage = self
                    .result_state
                    .current_storage_usage
                    .checked_sub(
                        value.len() as u64
                            + key.len() as u64
                            + storage_config.num_extra_bytes_record,
                    )
                    .ok_or_else(|| ErrorContainer::new(InconsistentStateError::IntegerOverflow))?;
                self.registers
                    .set(
                        &mut self.result_state.gas_counter,
                        &self.config.limit_config,
                        register_id,
                        value,
                    )
                    .map_err(ErrorContainer::new)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn storage_has_key(&mut self, key: Vec<u8>) -> wasmtime::Result<bool> {
        self.pay_base(base)?;
        self.pay_base(storage_has_key_base)?;
        self.pay_for_reading_bytes(key.len())?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(ErrorContainer::new(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            })
            .into());
        }
        self.pay_per(storage_has_key_byte, key.len() as u64)?;
        let res = self
            .ext
            .storage_has_key(&mut self.result_state.gas_counter, &key)
            .map_err(ErrorContainer::new);

        self.recorded_storage_counter
            .observe_size(self.ext.get_recorded_storage_size())
            .map_err(ErrorContainer::new)?;
        Ok(res?)
    }
}

impl runtime::HostPromiseAction for Ctx {
    fn drop(&mut self, action: Resource<ActionIndex>) -> wasmtime::Result<()> {
        self.table.delete(action)?;
        Ok(())
    }
}

impl runtime::HostPromise for Ctx {
    fn new(&mut self, account_id: Resource<AccountId>) -> wasmtime::Result<Resource<PromiseIndex>> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_create".to_string(),
            })
            .into());
        }
        let account_id = self.load_account_id(&account_id)?;
        let sir = *account_id == self.context.current_account_id;
        pay_gas_for_new_receipt(&mut self.result_state.gas_counter, &self.fees_config, sir, &[])?;
        let new_receipt_idx =
            self.ext.create_action_receipt(vec![], account_id).map_err(ErrorContainer::new)?;

        self.checked_push_promise(Promise::Receipt(new_receipt_idx))
    }

    fn to_index(&mut self, promise: Resource<PromiseIndex>) -> wasmtime::Result<PromiseIndex> {
        self.pay_base(base)?;
        self.load_promise_idx(&promise)
    }

    fn from_index(
        &mut self,
        promise_idx: PromiseIndex,
    ) -> wasmtime::Result<Resource<PromiseIndex>> {
        self.pay_base(base)?;
        self.store_promise_idx(promise_idx)
    }

    fn then(
        &mut self,
        promise: Resource<PromiseIndex>,
        account_id: Resource<AccountId>,
    ) -> wasmtime::Result<Resource<PromiseIndex>> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_then".to_string(),
            })
            .into());
        }
        let account_id = self.load_account_id(&account_id)?;
        let promise_idx = self.load_promise_idx(&promise)?;
        // Update the DAG and return new promise idx.
        let promise = self
            .promises
            .get(promise_idx as usize)
            .ok_or(HostError::InvalidPromiseIndex { promise_idx })
            .map_err(ErrorContainer::new)?;
        let receipt_dependencies = match &promise {
            Promise::Receipt(receipt_idx) => vec![*receipt_idx],
            Promise::NotReceipt(receipt_indices) => receipt_indices.clone(),
        };

        let sir = account_id == self.context.current_account_id;
        let deps: Vec<_> = receipt_dependencies
            .iter()
            .map(|&receipt_idx| self.ext.get_receipt_receiver(receipt_idx) == &account_id)
            .collect();
        pay_gas_for_new_receipt(&mut self.result_state.gas_counter, &self.fees_config, sir, &deps)?;

        let new_receipt_idx = self
            .ext
            .create_action_receipt(receipt_dependencies, account_id)
            .map_err(ErrorContainer::new)?;

        self.checked_push_promise(Promise::Receipt(new_receipt_idx))
    }

    fn and(
        &mut self,
        promises: Vec<Resource<PromiseIndex>>,
    ) -> wasmtime::Result<Resource<PromiseIndex>> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_and".to_string(),
            })
            .into());
        }
        self.pay_base(promise_and_base)?;
        let memory_len = promises
            .len()
            .checked_mul(size_of::<u64>())
            .ok_or(HostError::IntegerOverflow)
            .map_err(ErrorContainer::new)?;
        self.pay_for_reading_bytes(memory_len)?;
        self.pay_per(promise_and_per_promise, memory_len as _)?;

        let mut receipt_dependencies = vec![];
        for promise in promises {
            let promise_idx = self.load_promise_idx(&promise)?;
            let promise = self
                .promises
                .get(promise_idx as usize)
                .ok_or(HostError::InvalidPromiseIndex { promise_idx })
                .map_err(ErrorContainer::new)?;
            match &promise {
                Promise::Receipt(receipt_idx) => {
                    receipt_dependencies.push(*receipt_idx);
                }
                Promise::NotReceipt(receipt_indices) => {
                    receipt_dependencies.extend(receipt_indices.clone());
                }
            }
            // Checking this in the loop to prevent abuse of too many joined vectors.
            if receipt_dependencies.len() as u64
                > self.config.limit_config.max_number_input_data_dependencies
            {
                return Err(ErrorContainer::new(HostError::NumberInputDataDependenciesExceeded {
                    number_of_input_data_dependencies: receipt_dependencies.len() as u64,
                    limit: self.config.limit_config.max_number_input_data_dependencies,
                })
                .into());
            }
        }
        self.checked_push_promise(Promise::NotReceipt(receipt_dependencies))
    }

    fn set_refund_to(
        &mut self,
        promise: Resource<PromiseIndex>,
        account_id: Resource<AccountId>,
    ) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_set_refund_to".to_string(),
            })
            .into());
        }
        let refund_to = self.load_account_id(&account_id)?;
        let promise_idx = self.load_promise_idx(&promise)?;
        let promise = self
            .promises
            .get(promise_idx as usize)
            .ok_or(HostError::InvalidPromiseIndex { promise_idx })
            .map_err(ErrorContainer::new)?;

        let receipt_idx = match &promise {
            Promise::Receipt(receipt_idx) => Ok(*receipt_idx),
            Promise::NotReceipt(_) => {
                Err(ErrorContainer::new(HostError::CannotSetRefundToOnJointPromise))
            }
        }?;

        self.ext.set_refund_to(receipt_idx, refund_to);
        Ok(())
    }

    fn state_init(
        &mut self,
        promise: Resource<PromiseIndex>,
        code_hash: runtime::U256,
        amount: runtime::U128,
    ) -> wasmtime::Result<Resource<ActionIndex>> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_state_init".to_string(),
            })
            .into());
        }
        self.pay_for_reading_bytes(CryptoHash::LENGTH)?;
        let code_hash: [_; CryptoHash::LENGTH] = code_hash.to_le_bytes();
        let amount = amount.read(&mut self.result_state.gas_counter)?;
        let amount = Balance::from_yoctonear(amount);
        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::deterministic_state_init_base,
            sir,
        )?;
        self.deduct_balance(amount)?;
        let action = self
            .ext
            .append_action_deterministic_state_init(
                receipt_idx,
                GlobalContractIdentifier::CodeHash(CryptoHash(code_hash)),
                amount,
            )
            .map_err(ErrorContainer::new)?;
        self.store_action(action)
    }

    fn state_init_by_account_id(
        &mut self,
        promise: Resource<PromiseIndex>,
        account_id: Resource<AccountId>,
        amount: runtime::U128,
    ) -> wasmtime::Result<Resource<ActionIndex>> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_state_init_by_account_id".to_string(),
            })
            .into());
        }
        let account_id = self.load_account_id(&account_id)?;
        let amount = amount.read(&mut self.result_state.gas_counter)?;
        let amount = Balance::from_yoctonear(amount);
        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::deterministic_state_init_base,
            sir,
        )?;
        self.deduct_balance(amount)?;
        let action = self
            .ext
            .append_action_deterministic_state_init(
                receipt_idx,
                GlobalContractIdentifier::AccountId(account_id),
                amount,
            )
            .map_err(ErrorContainer::new)?;
        self.store_action(action)
    }

    fn set_state_init_data_entry(
        &mut self,
        promise: Resource<PromiseIndex>,
        action: Resource<ActionIndex>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "set_state_init_data_entry".to_string(),
            })
            .into());
        }

        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;
        self.pay_for_reading_bytes(key.len())?;
        self.pay_for_reading_bytes(value.len())?;

        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::deterministic_state_init_entry,
            sir,
        )?;
        let bytes = (key.len() as u64)
            .checked_add(value.len() as u64)
            .ok_or(HostError::IntegerOverflow)
            .map_err(ErrorContainer::new)?;
        pay_action_per_byte(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::deterministic_state_init_byte,
            bytes,
            sir,
        )?;

        let action_index = self.load_action(&action)?;
        self.ext
            .set_deterministic_state_init_data_entry(receipt_idx, action_index, key, value)
            .map_err(ErrorContainer::new)?;

        Ok(())
    }

    fn create_account(&mut self, promise: Resource<PromiseIndex>) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_create_account".to_string(),
            })
            .into());
        }
        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::create_account,
            sir,
        )?;

        self.ext.append_action_create_account(receipt_idx).map_err(ErrorContainer::new)?;
        Ok(())
    }

    fn deploy_contract(
        &mut self,
        promise: Resource<PromiseIndex>,
        code: Vec<u8>,
    ) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_deploy_contract".to_string(),
            })
            .into());
        }
        self.pay_for_reading_bytes(code.len())?;
        let code_len = code.len() as u64;
        let limit = self.config.limit_config.max_contract_size;
        if code_len > limit {
            return Err(ErrorContainer::new(HostError::ContractSizeExceeded {
                size: code_len,
                limit,
            })
            .into());
        }

        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::deploy_contract_base,
            sir,
        )?;
        pay_action_per_byte(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::deploy_contract_byte,
            code_len,
            sir,
        )?;

        self.ext.append_action_deploy_contract(receipt_idx, code).map_err(ErrorContainer::new)?;
        Ok(())
    }

    fn deploy_global_contract(
        &mut self,
        promise: Resource<PromiseIndex>,
        code: Vec<u8>,
    ) -> wasmtime::Result<()> {
        self.promise_batch_action_deploy_global_contract_impl(
            promise,
            code,
            GlobalContractDeployMode::CodeHash,
            "promise_batch_action_deploy_global_contract",
        )
    }

    fn deploy_global_contract_by_account_id(
        &mut self,
        promise: Resource<PromiseIndex>,
        code: Vec<u8>,
    ) -> wasmtime::Result<()> {
        self.promise_batch_action_deploy_global_contract_impl(
            promise,
            code,
            GlobalContractDeployMode::AccountId,
            "promise_batch_action_deploy_global_contract_by_account_id",
        )
    }

    fn use_global_contract(
        &mut self,
        promise: Resource<PromiseIndex>,
        code_hash: runtime::U256,
    ) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_use_global_contract".to_string(),
            })
            .into());
        }
        self.pay_for_reading_bytes(CryptoHash::LENGTH)?;
        let code_hash: [_; CryptoHash::LENGTH] = code_hash.to_le_bytes();
        let contract_id = GlobalContractIdentifier::CodeHash(CryptoHash(code_hash));

        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::use_global_contract_base,
            sir,
        )?;
        let len = contract_id.len() as u64;
        pay_action_per_byte(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::use_global_contract_byte,
            len,
            sir,
        )?;

        self.ext
            .append_action_use_global_contract(receipt_idx, contract_id)
            .map_err(ErrorContainer::new)?;
        Ok(())
    }

    fn use_global_contract_by_account_id(
        &mut self,
        promise: Resource<PromiseIndex>,
        account_id: Resource<AccountId>,
    ) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_use_global_contract_by_account_id".to_string(),
            })
            .into());
        }
        let account_id = self.load_account_id(&account_id)?;
        let contract_id = GlobalContractIdentifier::AccountId(account_id);

        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::use_global_contract_base,
            sir,
        )?;
        let len = contract_id.len() as u64;
        pay_action_per_byte(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::use_global_contract_byte,
            len,
            sir,
        )?;

        self.ext
            .append_action_use_global_contract(receipt_idx, contract_id)
            .map_err(ErrorContainer::new)?;
        Ok(())
    }

    fn function_call(
        &mut self,
        promise: Resource<PromiseIndex>,
        method: Vec<u8>,
        arguments: Vec<u8>,
        amount: runtime::U128,
        gas: u64,
        gas_weight: u64,
    ) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_function_call".to_string(),
            })
            .into());
        }
        let amount = amount.read(&mut self.result_state.gas_counter)?;
        let amount = Balance::from_yoctonear(amount);
        if method.is_empty() {
            return Err(ErrorContainer::new(HostError::EmptyMethodName).into());
        }
        self.pay_for_reading_bytes(method.len())?;
        self.pay_for_reading_bytes(arguments.len())?;

        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        // Input can't be large enough to overflow
        let num_bytes = method.len() as u64 + arguments.len() as u64;

        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::function_call_base,
            sir,
        )?;
        pay_action_per_byte(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::function_call_byte,
            num_bytes,
            sir,
        )?;
        // Prepaid gas
        self.result_state
            .gas_counter
            .prepay_gas(Gas::from_gas(gas))
            .map_err(ErrorContainer::new)?;
        self.deduct_balance(amount)?;
        self.ext
            .append_action_function_call_weight(
                receipt_idx,
                method,
                arguments,
                amount,
                Gas::from_gas(gas),
                GasWeight(gas_weight),
            )
            .map_err(ErrorContainer::new)?;
        Ok(())
    }

    fn transfer(
        &mut self,
        promise: Resource<PromiseIndex>,
        amount: runtime::U128,
    ) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_transfer".to_string(),
            })
            .into());
        }
        let amount = Balance::from_yoctonear(amount.read(&mut self.result_state.gas_counter)?);

        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;
        let receiver_id = self.ext.get_receipt_receiver(receipt_idx);
        let send_fee = transfer_send_fee(
            &self.fees_config,
            sir,
            self.config.implicit_account_creation,
            self.config.eth_implicit_accounts,
            receiver_id.get_account_type(),
        );
        let exec_fee = transfer_exec_fee(
            &self.fees_config,
            self.config.implicit_account_creation,
            self.config.eth_implicit_accounts,
            receiver_id.get_account_type(),
        );
        let burn_gas = send_fee;
        let use_gas = burn_gas
            .checked_add(exec_fee)
            .ok_or(HostError::IntegerOverflow)
            .map_err(ErrorContainer::new)?;

        pay_action_accumulated(
            &mut self.result_state.gas_counter,
            burn_gas,
            use_gas,
            ActionCosts::transfer,
        )?;
        self.deduct_balance(amount)?;
        self.ext.append_action_transfer(receipt_idx, amount).map_err(ErrorContainer::new)?;
        Ok(())
    }

    fn stake(
        &mut self,
        promise: Resource<PromiseIndex>,
        amount: runtime::U128,
        public_key: runtime::PublicKey,
    ) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_stake".to_string(),
            })
            .into());
        }
        let amount = Balance::from_yoctonear(amount.read(&mut self.result_state.gas_counter)?);
        let public_key = self.read_public_key(&public_key)?;
        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::stake,
            sir,
        )?;
        self.ext.append_action_stake(receipt_idx, amount, public_key);
        Ok(())
    }

    fn add_key_with_full_access(
        &mut self,
        promise: Resource<PromiseIndex>,
        public_key: runtime::PublicKey,
        nonce: u64,
    ) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_add_key_with_full_access".to_string(),
            })
            .into());
        }
        let public_key = self.read_public_key(&public_key)?;
        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;
        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::add_full_access_key,
            sir,
        )?;
        self.ext.append_action_add_key_with_full_access(receipt_idx, public_key, nonce);
        Ok(())
    }

    fn add_key_with_function_call(
        &mut self,
        promise: Resource<PromiseIndex>,
        public_key: runtime::PublicKey,
        nonce: u64,
        allowance: runtime::U128,
        receiver_id: Resource<AccountId>,
        methods: Vec<Vec<u8>>,
    ) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_add_key_with_function_call".to_string(),
            })
            .into());
        }
        let public_key = self.read_public_key(&public_key)?;
        let allowance =
            Balance::from_yoctonear(allowance.read(&mut self.result_state.gas_counter)?);
        let allowance = if allowance > Balance::ZERO { Some(allowance) } else { None };
        let receiver_id = self.load_account_id(&receiver_id)?;
        for method in &methods {
            self.pay_for_reading_bytes(method.len())?;
        }

        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        // +1 is to account for null-terminating characters.
        let num_bytes = methods.iter().map(|v| v.len() as u64 + 1).sum::<u64>();
        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::add_function_call_key_base,
            sir,
        )?;
        pay_action_per_byte(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::add_function_call_key_byte,
            num_bytes,
            sir,
        )?;

        self.ext
            .append_action_add_key_with_function_call(
                receipt_idx,
                public_key,
                nonce,
                allowance,
                receiver_id,
                methods,
            )
            .map_err(ErrorContainer::new)?;
        Ok(())
    }

    fn delete_key(
        &mut self,
        promise: Resource<PromiseIndex>,
        public_key: runtime::PublicKey,
    ) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_delete_key".to_string(),
            })
            .into());
        }
        let public_key = self.read_public_key(&public_key)?;
        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;
        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::delete_key,
            sir,
        )?;
        self.ext.append_action_delete_key(receipt_idx, public_key);
        Ok(())
    }

    fn delete_account(
        &mut self,
        promise: Resource<PromiseIndex>,
        beneficiary_id: Resource<AccountId>,
    ) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_delete_account".to_string(),
            })
            .into());
        }
        let beneficiary_id = self.load_account_id(&beneficiary_id)?;

        let promise_idx = self.load_promise_idx(&promise)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::delete_account,
            sir,
        )?;

        self.ext
            .append_action_delete_account(receipt_idx, beneficiary_id)
            .map_err(ErrorContainer::new)?;
        Ok(())
    }

    fn yield_create(
        &mut self,
        method: Vec<u8>,
        arguments: Vec<u8>,
        gas: u64,
        gas_weight: u64,
        register_id: u64,
    ) -> wasmtime::Result<Resource<PromiseIndex>> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_yield_create".to_string(),
            })
            .into());
        }
        self.pay_base(yield_create_base)?;

        if method.is_empty() {
            return Err(ErrorContainer::new(HostError::EmptyMethodName).into());
        }
        self.pay_for_reading_bytes(method.len())?;
        self.pay_for_reading_bytes(arguments.len())?;

        // Input can't be large enough to overflow, WebAssembly address space is 32-bits.
        let num_bytes = method.len() as u64 + arguments.len() as u64;
        self.pay_per(yield_create_byte, num_bytes)?;
        // Prepay gas for the callback so that it cannot be used for this execution any longer.
        self.result_state
            .gas_counter
            .prepay_gas(Gas::from_gas(gas))
            .map_err(ErrorContainer::new)?;

        // Here we are creating a receipt with a single data dependency which will then be
        // resolved by the resume call.
        pay_gas_for_new_receipt(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            true,
            &[true],
        )?;
        let (new_receipt_idx, data_id) = self
            .ext
            .create_promise_yield_receipt(self.context.current_account_id.clone())
            .map_err(ErrorContainer::new)?;

        let new_promise_idx = self.checked_push_promise(Promise::Receipt(new_receipt_idx))?;
        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::function_call_base,
            true,
        )?;
        pay_action_per_byte(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::function_call_byte,
            num_bytes,
            true,
        )?;
        self.ext
            .append_action_function_call_weight(
                new_receipt_idx,
                method,
                arguments,
                Balance::ZERO,
                Gas::from_gas(gas),
                GasWeight(gas_weight),
            )
            .map_err(ErrorContainer::new)?;

        self.registers
            .set(
                &mut self.result_state.gas_counter,
                &self.config.limit_config,
                register_id,
                *data_id.as_bytes(),
            )
            .map_err(ErrorContainer::new)?;
        Ok(new_promise_idx)
    }

    fn yield_resume(&mut self, data_id: runtime::U256, payload: Vec<u8>) -> wasmtime::Result<bool> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_submit_data".to_string(),
            })
            .into());
        }
        self.pay_base(yield_resume_base)?;
        self.pay_for_reading_bytes(payload.len())?;
        let payload_len = payload.len() as u64;
        pay_per(&mut self.result_state.gas_counter, yield_resume_byte, payload_len)?;
        self.pay_for_reading_bytes(CryptoHash::LENGTH)?;
        if payload_len > self.config.limit_config.max_yield_payload_size {
            return Err(ErrorContainer::new(HostError::YieldPayloadLength {
                length: payload_len,
                limit: self.config.limit_config.max_yield_payload_size,
            })
            .into());
        }

        let data_id: [_; CryptoHash::LENGTH] = data_id.to_le_bytes();
        let v = self
            .ext
            .submit_promise_resume_data(CryptoHash(data_id), payload)
            .map_err(ErrorContainer::new)?;
        Ok(v)
    }

    fn get_results_count(&mut self) -> wasmtime::Result<u64> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_results_count".to_string(),
            })
            .into());
        }
        Ok(self.context.promise_results.len() as _)
    }

    fn get_result(
        &mut self,
        result_idx: u64,
        register_id: u64,
    ) -> wasmtime::Result<Option<Result<(), ()>>> {
        self.pay_base(base)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_result".to_string(),
            })
            .into());
        }
        match self
            .context
            .promise_results
            .get(result_idx as usize)
            .ok_or(HostError::InvalidPromiseResultIndex { result_idx })
            .map_err(ErrorContainer::new)?
        {
            PromiseResult::NotReady => Ok(None),
            PromiseResult::Successful(data) => {
                let charge_bytes_gas = !self.config.deterministic_account_ids;
                self.registers
                    .set_rc_data(
                        &mut self.result_state.gas_counter,
                        &self.config.limit_config,
                        register_id,
                        Rc::clone(data),
                        charge_bytes_gas,
                    )
                    .map_err(ErrorContainer::new)?;
                Ok(Some(Ok(())))
            }
            PromiseResult::Failed => Ok(Some(Err(()))),
        }
    }

    fn return_(&mut self, promise: Resource<PromiseIndex>) -> wasmtime::Result<()> {
        self.pay_base(base)?;
        self.pay_base(promise_return)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_return".to_string(),
            })
            .into());
        }
        let promise_idx = self.load_promise_idx(&promise)?;
        match self
            .promises
            .get(promise_idx as usize)
            .ok_or(HostError::InvalidPromiseIndex { promise_idx })
            .map_err(ErrorContainer::new)?
        {
            Promise::Receipt(receipt_idx) => {
                self.result_state.return_data = ReturnData::ReceiptIndex(*receipt_idx);
                Ok(())
            }
            Promise::NotReceipt(_) => {
                Err(ErrorContainer::new(HostError::CannotReturnJointPromise).into())
            }
        }
    }

    fn drop(&mut self, promise: Resource<PromiseIndex>) -> wasmtime::Result<()> {
        self.table.delete(promise)?;
        Ok(())
    }
}

impl runtime::HostAccountId for Ctx {
    fn from_string(
        &mut self,
        account_id: String,
    ) -> wasmtime::Result<Result<Resource<AccountId>, ()>> {
        self.pay_for_reading_string(account_id.len())?;
        match account_id.parse() {
            Ok(account_id) => {
                let account_id = self.store_account_id(account_id)?;
                Ok(Ok(account_id))
            }
            Err(_err) => Ok(Err(())),
        }
    }

    fn to_string(&mut self, account_id: Resource<AccountId>) -> wasmtime::Result<String> {
        let account_id = self.load_account_id(&account_id)?;
        pay_for_writing_bytes(&mut self.result_state.gas_counter, account_id.len())?;
        Ok(account_id.to_string())
    }

    fn drop(&mut self, account_id: Resource<AccountId>) -> wasmtime::Result<()> {
        self.table.delete(account_id)?;
        Ok(())
    }
}
