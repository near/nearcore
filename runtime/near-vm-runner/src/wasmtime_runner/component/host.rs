use crate::logic::errors::InconsistentStateError;
use crate::logic::gas_counter::FreeGasCounter;
use crate::logic::logic::{Promise, PublicKeyBuffer};
use crate::logic::types::{
    ActionIndex, GlobalContractDeployMode, GlobalContractIdentifier, PromiseIndex, PromiseResult,
    ReceiptIndex,
};
use crate::logic::utils::split_method_names;
use crate::logic::vmstate::Registers;
use crate::logic::{GasCounter, HostError, ReturnData, alt_bn128, bls12381};
use crate::wasmtime_runner::ErrorContainer;
use crate::wasmtime_runner::component::Ctx;
use crate::wasmtime_runner::component::bindings::near::nearcore::{finite_wasm, runtime};
use near_crypto::{PublicKey, Secp256K1Signature};
use near_parameters::{
    ActionCosts, ExtCosts::*, RuntimeFeesConfig, transfer_exec_fee, transfer_send_fee,
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
        (u128::from(hi) << 64) | u128::from(lo)
    }
}

impl runtime::U128 {
    fn read(self, gas_counter: &mut GasCounter) -> wasmtime::Result<u128> {
        gas_counter.pay_base(read_memory_base).map_err(ErrorContainer::new)?;
        gas_counter.pay_per(read_memory_byte, 16).map_err(ErrorContainer::new)?;
        Ok(u128::from(self))
    }

    fn write(v: u128, gas_counter: &mut GasCounter) -> wasmtime::Result<Self> {
        gas_counter.pay_base(write_memory_base).map_err(ErrorContainer::new)?;
        gas_counter.pay_per(write_memory_byte, 16).map_err(ErrorContainer::new)?;
        Ok(Self::from(v))
    }
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
    gas_counter.pay_action_accumulated(burn_gas, use_gas, action).map_err(ErrorContainer::new)?;
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

    let use_gas = burn_gas
        .checked_add(
            per_byte_fee
                .exec_fee()
                .checked_mul(num_bytes)
                .ok_or(HostError::IntegerOverflow)
                .map_err(ErrorContainer::new)?,
        )
        .ok_or(HostError::IntegerOverflow)
        .map_err(ErrorContainer::new)?;
    gas_counter.pay_action_accumulated(burn_gas, use_gas, action).map_err(ErrorContainer::new)?;
    Ok(())
}

impl runtime::ValueOrRegister {
    fn as_bytes<'a>(
        &'a self,
        gas_counter: &mut GasCounter,
        registers: &'a Registers,
    ) -> wasmtime::Result<&'a [u8]> {
        match self {
            Self::Value(buf) => {
                gas_counter.pay_base(read_memory_base).map_err(ErrorContainer::new)?;
                gas_counter
                    .pay_per(read_memory_byte, buf.len() as _)
                    .map_err(ErrorContainer::new)?;
                Ok(buf)
            }
            Self::Register(register_id) => {
                let buf = registers.get(gas_counter, *register_id).map_err(ErrorContainer::new)?;
                Ok(buf)
            }
        }
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
    gas_counter
        .pay_action_accumulated(burn_gas, use_gas, ActionCosts::new_action_receipt)
        .map_err(ErrorContainer::new)?;
    Ok(())
}

impl Ctx {
    fn read_account_id(&mut self, account_id: &Resource<AccountId>) -> wasmtime::Result<AccountId> {
        self.result_state.gas_counter.pay_base(read_register_base).map_err(ErrorContainer::new)?;
        let account_id = self.table.get(account_id)?;
        self.result_state
            .gas_counter
            .pay_per(read_register_byte, account_id.len() as _)
            .map_err(ErrorContainer::new)?;
        Ok(account_id.clone())
    }

    fn read_public_key(&mut self, public_key: &Resource<PublicKey>) -> wasmtime::Result<PublicKey> {
        self.result_state.gas_counter.pay_base(read_register_base).map_err(ErrorContainer::new)?;
        let public_key = self.table.get(public_key)?;
        self.result_state
            .gas_counter
            .pay_per(read_register_byte, public_key.len() as _)
            .map_err(ErrorContainer::new)?;
        Ok(public_key.clone())
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
            let promise = self.table.push(new_promise_idx)?;
            Ok(promise)
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
        code: runtime::ValueOrRegister,
        mode: GlobalContractDeployMode,
        method_name: &str,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: method_name.to_owned(),
            })
            .into());
        }
        let code = code.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        let code_len = code.len() as u64;
        let limit = self.config.limit_config.max_contract_size;
        if code_len > limit {
            return Err(ErrorContainer::new(HostError::ContractSizeExceeded {
                size: code_len,
                limit,
            })
            .into());
        }
        let code = code.into();

        let promise_idx = self.table.get(&promise).copied()?;
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
        fn $fn_name(
            &mut self,
            value: runtime::ValueOrRegister,
            register_id: u64,
        ) -> wasmtime::Result<Result<(), ()>> {
            self.result_state.gas_counter.pay_base($bls12381_base)?;

            let data = value.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;

            let elements_count = data.len() / $ITEM_SIZE;
            self.result_state.gas_counter.pay_per($bls12381_element, elements_count as u64)?;

            let res_option = bls12381::$impl_fn_name(&data)?;

            if let Some(res) = res_option {
                self.registers.set(
                    &mut self.result_state.gas_counter,
                    &self.config.limit_config,
                    register_id,
                    res.as_slice(),
                )?;

                Ok(Ok(()))
            } else {
                Ok(Err(()))
            }
        }
    };
}

impl finite_wasm::Host for Ctx {
    fn gas_exhausted(&mut self) -> wasmtime::Result<()> {
        // Burn all remaining gas
        self.result_state.gas_counter.burn_gas(self.result_state.gas_counter.remaining_gas())?;
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
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        self.result_state.gas_counter.pay_base(read_memory_base)?;
        self.result_state.gas_counter.pay_per(read_memory_byte, data.len() as _)?;
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            data,
        )?;
        Ok(())
    }

    fn read_register(&mut self, register_id: u64) -> wasmtime::Result<Vec<u8>> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        let buf = self.registers.get(&mut self.result_state.gas_counter, register_id)?;
        self.result_state.gas_counter.pay_base(write_memory_base)?;
        self.result_state.gas_counter.pay_per(write_memory_byte, buf.len() as _)?;
        Ok(buf.into())
    }

    fn register_len(&mut self, register_id: u64) -> wasmtime::Result<Option<u64>> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        Ok(self.registers.get_len(register_id))
    }

    fn current_account_id(&mut self) -> wasmtime::Result<Resource<AccountId>> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        let account_id = self.context.current_account_id.clone();
        self.result_state.gas_counter.pay_base(write_register_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(write_register_byte, account_id.len() as _)
            .map_err(ErrorContainer::new)?;
        let account_id = self.table.push(account_id)?;
        Ok(account_id)
    }

    fn signer_account_id(&mut self) -> wasmtime::Result<Resource<AccountId>> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;

        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "signer_account_id".to_string(),
            })
            .into());
        }
        let account_id = self.context.signer_account_id.clone();
        self.result_state.gas_counter.pay_base(write_register_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(write_register_byte, account_id.len() as _)
            .map_err(ErrorContainer::new)?;
        let account_id = self.table.push(account_id)?;
        Ok(account_id)
    }

    fn signer_account_pk(&mut self) -> wasmtime::Result<Resource<PublicKey>> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;

        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "signer_account_pk".to_string(),
            })
            .into());
        }
        let public_key = PublicKeyBuffer::new(&self.context.signer_account_pk)
            .decode()
            .map_err(ErrorContainer::new)?;
        self.result_state.gas_counter.pay_base(write_register_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(write_register_byte, public_key.len() as _)
            .map_err(ErrorContainer::new)?;
        let public_key = self.table.push(public_key)?;
        Ok(public_key)
    }

    fn predecessor_account_id(&mut self) -> wasmtime::Result<Resource<AccountId>> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;

        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "predecessor_account_id".to_string(),
            })
            .into());
        }
        let account_id = self.context.predecessor_account_id.clone();
        self.result_state.gas_counter.pay_base(write_register_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(write_register_byte, account_id.len() as _)
            .map_err(ErrorContainer::new)?;
        let account_id = self.table.push(account_id)?;
        Ok(account_id)
    }

    fn refund_to_account_id(&mut self, register_id: u64) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;

        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "refund_to_account_id".to_string(),
            })
            .into());
        }
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            self.context.refund_to_account_id.as_bytes(),
        )?;
        Ok(())
    }

    fn input(&mut self) -> wasmtime::Result<Vec<u8>> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        self.result_state.gas_counter.pay_base(write_memory_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(write_memory_byte, self.context.input.len() as _)
            .map_err(ErrorContainer::new)?;
        Ok(self.context.input.to_vec())
    }

    fn block_height(&mut self) -> wasmtime::Result<u64> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        Ok(self.context.block_height)
    }

    fn block_timestamp(&mut self) -> wasmtime::Result<u64> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        Ok(self.context.block_timestamp)
    }

    fn epoch_height(&mut self) -> wasmtime::Result<u64> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        Ok(self.context.epoch_height)
    }

    fn validator_stake(
        &mut self,
        account_id: Resource<AccountId>,
    ) -> wasmtime::Result<runtime::U128> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_base(validator_stake_base)
            .map_err(ErrorContainer::new)?;
        self.result_state.gas_counter.pay_base(read_register_base).map_err(ErrorContainer::new)?;
        let account_id = self.table.get(&account_id)?;
        let balance =
            self.ext.validator_stake(account_id).map_err(ErrorContainer::new)?.unwrap_or_default();
        let v = runtime::U128::write(balance.as_yoctonear(), &mut self.result_state.gas_counter)?;
        Ok(v)
    }

    fn validator_total_stake(&mut self) -> wasmtime::Result<runtime::U128> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        self.result_state.gas_counter.pay_base(validator_total_stake_base)?;
        let total_stake = self.ext.validator_total_stake().map_err(ErrorContainer::new)?;
        let v =
            runtime::U128::write(total_stake.as_yoctonear(), &mut self.result_state.gas_counter)?;
        Ok(v)
    }

    fn storage_usage(&mut self) -> wasmtime::Result<u64> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        Ok(self.result_state.current_storage_usage)
    }

    fn account_balance(&mut self) -> wasmtime::Result<runtime::U128> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        let v = runtime::U128::write(
            self.result_state.current_account_balance.as_yoctonear(),
            &mut self.result_state.gas_counter,
        )?;
        Ok(v)
    }

    fn account_locked_balance(&mut self) -> wasmtime::Result<runtime::U128> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        let v = runtime::U128::write(
            self.current_account_locked_balance.as_yoctonear(),
            &mut self.result_state.gas_counter,
        )?;
        Ok(v)
    }

    fn attached_deposit(&mut self) -> wasmtime::Result<runtime::U128> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        let v = runtime::U128::write(
            self.context.attached_deposit.as_yoctonear(),
            &mut self.result_state.gas_counter,
        )?;
        Ok(v)
    }

    fn prepaid_gas(&mut self) -> wasmtime::Result<u64> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "prepaid_gas".to_string(),
            })
            .into());
        }
        Ok(self.context.prepaid_gas.as_gas())
    }

    fn used_gas(&mut self) -> wasmtime::Result<u64> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "used_gas".to_string(),
            })
            .into());
        }
        Ok(self.result_state.gas_counter.used_gas().as_gas())
    }

    fn alt_bn128_g1_multiexp(
        &mut self,
        value: runtime::ValueOrRegister,
        register_id: u64,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(alt_bn128_g1_multiexp_base)?;
        let data = value.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;

        let elements = alt_bn128::split_elements(&data).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(alt_bn128_g1_multiexp_element, elements.len() as u64)?;

        let res = alt_bn128::g1_multiexp(elements).map_err(ErrorContainer::new)?;

        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            res,
        )?;
        Ok(())
    }

    fn alt_bn128_g1_sum(
        &mut self,
        value: runtime::ValueOrRegister,
        register_id: u64,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(alt_bn128_g1_sum_base)?;
        let data = value.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;

        let elements = alt_bn128::split_elements(&data).map_err(ErrorContainer::new)?;
        self.result_state.gas_counter.pay_per(alt_bn128_g1_sum_element, elements.len() as u64)?;

        let res = alt_bn128::g1_sum(elements).map_err(ErrorContainer::new)?;

        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            res,
        )?;
        Ok(())
    }

    fn alt_bn128_pairing_check(
        &mut self,
        value: runtime::ValueOrRegister,
    ) -> wasmtime::Result<bool> {
        self.result_state.gas_counter.pay_base(alt_bn128_pairing_check_base)?;
        let data = value.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;

        let elements = alt_bn128::split_elements(&data).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(alt_bn128_pairing_check_element, elements.len() as u64)?;

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

    fn bls12381_pairing_check(
        &mut self,
        value: runtime::ValueOrRegister,
    ) -> wasmtime::Result<Result<bool, ()>> {
        self.result_state.gas_counter.pay_base(bls12381_pairing_base)?;

        const BLS_P1_SIZE: usize = 96;
        const BLS_P2_SIZE: usize = 192;
        const ITEM_SIZE: usize = BLS_P1_SIZE + BLS_P2_SIZE;

        let data = value.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        let elements_count = data.len() / ITEM_SIZE;

        self.result_state.gas_counter.pay_per(bls12381_pairing_element, elements_count as u64)?;

        match bls12381::pairing_check(&data)? {
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
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        self.result_state.gas_counter.pay_base(write_memory_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(write_memory_byte, self.context.random_seed.len() as _)
            .map_err(ErrorContainer::new)?;
        Ok(self.context.random_seed.clone())
    }

    fn sha256(
        &mut self,
        value: runtime::ValueOrRegister,
        register_id: u64,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(sha256_base)?;
        let value = value.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        self.result_state.gas_counter.pay_per(sha256_byte, value.len() as u64)?;

        use sha2::Digest;

        let value_hash = sha2::Sha256::digest(&value);
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            value_hash.as_slice(),
        )?;
        Ok(())
    }

    fn keccak256(
        &mut self,
        value: runtime::ValueOrRegister,
        register_id: u64,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(keccak256_base)?;
        let value = value.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        self.result_state.gas_counter.pay_per(keccak256_byte, value.len() as u64)?;

        use sha3::Digest;

        let value_hash = sha3::Keccak256::digest(&value);
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            value_hash.as_slice(),
        )?;
        Ok(())
    }

    fn keccak512(
        &mut self,
        value: runtime::ValueOrRegister,
        register_id: u64,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(keccak512_base)?;
        let value = value.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        self.result_state.gas_counter.pay_per(keccak512_byte, value.len() as u64)?;

        use sha3::Digest;

        let value_hash = sha3::Keccak512::digest(&value);
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            value_hash.as_slice(),
        )?;
        Ok(())
    }

    fn ripemd160(
        &mut self,
        value: runtime::ValueOrRegister,
        register_id: u64,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(ripemd160_base)?;
        let value = value.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;

        let message_blocks = value
            .len()
            .checked_add(8)
            .ok_or_else(|| ErrorContainer::new(HostError::IntegerOverflow))?
            / 64
            + 1;

        self.result_state.gas_counter.pay_per(ripemd160_block, message_blocks as u64)?;

        use ripemd::Digest;

        let value_hash = ripemd::Ripemd160::digest(&value);
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            value_hash.as_slice(),
        )?;
        Ok(())
    }

    fn ecrecover(
        &mut self,
        hash: runtime::ValueOrRegister,
        sig: runtime::ValueOrRegister,
        v: u8,
        malleability: runtime::EcrecoverMalleability,
        register_id: u64,
    ) -> wasmtime::Result<bool> {
        self.result_state.gas_counter.pay_base(ecrecover_base)?;

        let signature = {
            let vec = sig.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
            if vec.len() != 64 {
                return Err(ErrorContainer::new(HostError::ECRecoverError {
                    msg: format!(
                        "The length of the signature: {}, exceeds the limit of 64 bytes",
                        vec.len()
                    ),
                })
                .into());
            }

            let mut bytes = [0u8; 65];
            bytes[0..64].copy_from_slice(&vec);

            if v < 4 {
                bytes[64] = v as u8;
                Secp256K1Signature::from(bytes)
            } else {
                return Err(ErrorContainer::new(HostError::ECRecoverError {
                    msg: format!("V recovery byte 0 through 3 are valid but was provided {}", v),
                })
                .into());
            }
        };

        let hash = {
            let vec = hash.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
            if vec.len() != 32 {
                return Err(ErrorContainer::new(HostError::ECRecoverError {
                    msg: format!(
                        "The length of the hash: {}, exceeds the limit of 32 bytes",
                        vec.len()
                    ),
                })
                .into());
            }

            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&vec);
            bytes
        };

        if !signature.check_signature_values(match malleability {
            runtime::EcrecoverMalleability::NoExtraChecks => false,
            runtime::EcrecoverMalleability::RejectingUpperRange => true,
        }) {
            return Ok(false);
        }

        if let Ok(pk) = signature.recover(hash) {
            self.registers.set(
                &mut self.result_state.gas_counter,
                &self.config.limit_config,
                register_id,
                pk.as_ref(),
            )?;
            return Ok(true);
        };

        Ok(false)
    }

    fn ed25519_verify(
        &mut self,
        signature: runtime::ValueOrRegister,
        message: Vec<u8>,
        public_key: Resource<PublicKey>,
    ) -> wasmtime::Result<bool> {
        use ed25519_dalek::Verifier;

        self.result_state.gas_counter.pay_base(ed25519_verify_base)?;

        let signature: ed25519_dalek::Signature = {
            let vec = signature.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
            let b = <&[u8; ed25519_dalek::SIGNATURE_LENGTH]>::try_from(&vec[..]).map_err(|_| {
                ErrorContainer::new(HostError::Ed25519VerifyInvalidInput {
                    msg: "invalid signature length".to_string(),
                })
            })?;
            // Sanity-check that was performed by ed25519-dalek in from_bytes before version 2,
            // but was removed with version 2. It is not actually any good a check, but we need
            // it to avoid costs changing.
            if b[ed25519_dalek::SIGNATURE_LENGTH - 1] & 0b1110_0000 != 0 {
                return Ok(false);
            }
            ed25519_dalek::Signature::from_bytes(b)
        };

        self.result_state.gas_counter.pay_base(read_memory_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(read_memory_byte, message.len() as _)
            .map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(ed25519_verify_byte, message.len() as _)
            .map_err(ErrorContainer::new)?;

        let public_key: ed25519_dalek::VerifyingKey = {
            let public_key = self.read_public_key(&public_key)?;
            let PublicKey::ED25519(key) = public_key else {
                return Err(ErrorContainer::new(HostError::Ed25519VerifyInvalidInput {
                    msg: "invalid public key".to_string(),
                })
                .into());
            };
            match ed25519_dalek::VerifyingKey::from_bytes(&key.0) {
                Ok(public_key) => public_key,
                Err(_) => return Ok(false),
            }
        };

        match public_key.verify(&message, &signature) {
            Err(_) => Ok(false),
            Ok(()) => Ok(true),
        }
    }

    fn value_return(&mut self, value: runtime::ValueOrRegister) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        let return_val = value.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        let mut burn_gas: Gas = Gas::ZERO;
        let num_bytes = return_val.len() as u64;
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
        self.result_state.gas_counter.pay_action_accumulated(
            burn_gas,
            burn_gas,
            ActionCosts::new_data_receipt_byte,
        )?;
        self.result_state.return_data = ReturnData::Value(return_val.into());
        Ok(())
    }

    fn panic(&mut self, s: Option<String>) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        let panic_msg = if let Some(s) = s {
            self.result_state.gas_counter.pay_base(utf8_decoding_base)?;
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
            self.result_state.gas_counter.pay_base(read_memory_base)?;
            self.result_state.gas_counter.pay_per(read_memory_byte, s.len() as _)?;
            self.result_state.gas_counter.pay_per(utf8_decoding_byte, s.len() as _)?;
            s
        } else {
            "explicit guest panic".to_string()
        };
        Err(ErrorContainer::new(HostError::GuestPanic { panic_msg }).into())
    }

    fn log(&mut self, s: String) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        self.result_state.check_can_add_a_log_message()?;
        self.result_state.gas_counter.pay_base(log_base)?;
        self.result_state.gas_counter.pay_per(log_byte, s.len() as u64)?;
        self.result_state.checked_push_log(s)?;
        Ok(())
    }

    fn current_contract_code(
        &mut self,
        register_id: u64,
    ) -> wasmtime::Result<Option<runtime::ContractCodeKind>> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        match &self.context.account_contract {
            AccountContract::None => Ok(None),
            AccountContract::Local(crypto_hash) => {
                self.registers.set(
                    &mut self.result_state.gas_counter,
                    &self.config.limit_config,
                    register_id,
                    crypto_hash.0,
                )?;
                Ok(Some(runtime::ContractCodeKind::Local))
            }
            AccountContract::Global(crypto_hash) => {
                self.registers.set(
                    &mut self.result_state.gas_counter,
                    &self.config.limit_config,
                    register_id,
                    crypto_hash.0,
                )?;
                Ok(Some(runtime::ContractCodeKind::Global))
            }
            AccountContract::GlobalByAccount(account_id) => {
                self.registers.set(
                    &mut self.result_state.gas_counter,
                    &self.config.limit_config,
                    register_id,
                    account_id.as_bytes(),
                )?;
                Ok(Some(runtime::ContractCodeKind::GlobalByAccount))
            }
        }
    }

    fn storage_write(
        &mut self,
        key: runtime::ValueOrRegister,
        value: runtime::ValueOrRegister,
        register_id: u64,
    ) -> wasmtime::Result<bool> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "storage_write".to_string(),
            })
            .into());
        }
        self.result_state.gas_counter.pay_base(storage_write_base)?;
        let key = key.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(ErrorContainer::new(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            })
            .into());
        }
        let value = value.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        if value.len() as u64 > self.config.limit_config.max_length_storage_value {
            return Err(ErrorContainer::new(HostError::ValueLengthExceeded {
                length: value.len() as u64,
                limit: self.config.limit_config.max_length_storage_value,
            })
            .into());
        }
        self.result_state.gas_counter.pay_per(storage_write_key_byte, key.len() as u64)?;
        self.result_state.gas_counter.pay_per(storage_write_value_byte, value.len() as u64)?;
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
                self.registers.set(
                    &mut self.result_state.gas_counter,
                    &self.config.limit_config,
                    register_id,
                    old_value,
                )?;
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

    fn storage_read(
        &mut self,
        key: runtime::ValueOrRegister,
        register_id: u64,
    ) -> wasmtime::Result<bool> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        self.result_state.gas_counter.pay_base(storage_read_base)?;
        let key = key.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(ErrorContainer::new(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            })
            .into());
        }
        self.result_state.gas_counter.pay_per(storage_read_key_byte, key.len() as u64)?;
        let read = self.ext.storage_get(&mut self.result_state.gas_counter, &key);
        let read = match read.map_err(ErrorContainer::new)? {
            Some(read) => {
                // Here we'll do u32 -> usize -> u64, which is always infallible
                let read_len = read.len() as usize;
                self.result_state.gas_counter.pay_per(storage_read_value_byte, read_len as u64)?;
                if read_len > INLINE_DISK_VALUE_THRESHOLD {
                    self.result_state.gas_counter.pay_base(storage_large_read_overhead_base)?;
                    self.result_state
                        .gas_counter
                        .pay_per(storage_large_read_overhead_byte, read_len as u64)?;
                }
                Some(read.deref(&mut FreeGasCounter)?)
            }
            None => None,
        };

        self.recorded_storage_counter
            .observe_size(self.ext.get_recorded_storage_size())
            .map_err(ErrorContainer::new)?;
        match read {
            Some(value) => {
                self.registers.set(
                    &mut self.result_state.gas_counter,
                    &self.config.limit_config,
                    register_id,
                    value,
                )?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn storage_remove(
        &mut self,
        key: runtime::ValueOrRegister,
        register_id: u64,
    ) -> wasmtime::Result<bool> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "storage_remove".to_string(),
            })
            .into());
        }
        self.result_state.gas_counter.pay_base(storage_remove_base)?;
        let key = key.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(ErrorContainer::new(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            })
            .into());
        }
        self.result_state.gas_counter.pay_per(storage_remove_key_byte, key.len() as u64)?;
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
                self.registers.set(
                    &mut self.result_state.gas_counter,
                    &self.config.limit_config,
                    register_id,
                    value,
                )?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn storage_has_key(&mut self, key: runtime::ValueOrRegister) -> wasmtime::Result<bool> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        self.result_state.gas_counter.pay_base(storage_has_key_base)?;
        let key = key.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(ErrorContainer::new(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            })
            .into());
        }
        self.result_state.gas_counter.pay_per(storage_has_key_byte, key.len() as u64)?;
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
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_create".to_string(),
            })
            .into());
        }
        let account_id = self.read_account_id(&account_id)?;
        let sir = *account_id == self.context.current_account_id;
        pay_gas_for_new_receipt(&mut self.result_state.gas_counter, &self.fees_config, sir, &[])?;
        let new_receipt_idx =
            self.ext.create_action_receipt(vec![], account_id).map_err(ErrorContainer::new)?;

        self.checked_push_promise(Promise::Receipt(new_receipt_idx))
    }

    fn then(
        &mut self,
        promise: Resource<PromiseIndex>,
        account_id: Resource<AccountId>,
    ) -> wasmtime::Result<Resource<PromiseIndex>> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_then".to_string(),
            })
            .into());
        }
        let account_id = self.read_account_id(&account_id)?;
        let promise_idx = self.table.get(&promise).copied()?;
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
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_and".to_string(),
            })
            .into());
        }
        self.result_state.gas_counter.pay_base(promise_and_base)?;
        let memory_len = promises
            .len()
            .checked_mul(size_of::<u64>())
            .and_then(|n| u64::try_from(n).ok())
            .ok_or(HostError::IntegerOverflow)
            .map_err(ErrorContainer::new)?;
        self.result_state.gas_counter.pay_per(promise_and_per_promise, memory_len)?;
        self.result_state.gas_counter.pay_base(read_memory_base)?;
        self.result_state.gas_counter.pay_per(read_memory_byte, memory_len)?;

        let mut receipt_dependencies = vec![];
        for promise in promises {
            let promise_idx = self.table.get(&promise).copied()?;
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
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_set_refund_to".to_string(),
            })
            .into());
        }
        let refund_to = self.read_account_id(&account_id)?;
        let promise_idx = self.table.get(&promise).copied()?;
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
        code_hash: runtime::ValueOrRegister,
        amount: runtime::U128,
    ) -> wasmtime::Result<Resource<ActionIndex>> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_state_init".to_string(),
            })
            .into());
        }
        let code_hash_bytes =
            code_hash.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        let code_hash: [_; CryptoHash::LENGTH] = (&*code_hash_bytes)
            .try_into()
            .map_err(|_| ErrorContainer::new(HostError::ContractCodeHashMalformed))?;
        let amount = amount.read(&mut self.result_state.gas_counter)?;
        let amount = Balance::from_yoctonear(amount);
        let promise_idx = self.table.get(&promise).copied()?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::deterministic_state_init_base,
            sir,
        )?;
        self.result_state.deduct_balance(amount).map_err(ErrorContainer::new)?;
        let action = self
            .ext
            .append_action_deterministic_state_init(
                receipt_idx,
                GlobalContractIdentifier::CodeHash(CryptoHash(code_hash)),
                amount,
            )
            .map_err(ErrorContainer::new)?;
        let action = self.table.push(action)?;
        Ok(action)
    }

    fn state_init_by_account_id(
        &mut self,
        promise: Resource<PromiseIndex>,
        account_id: Resource<AccountId>,
        amount: runtime::U128,
    ) -> wasmtime::Result<Resource<ActionIndex>> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_state_init_by_account_id".to_string(),
            })
            .into());
        }
        let account_id = self.read_account_id(&account_id)?;
        let amount = amount.read(&mut self.result_state.gas_counter)?;
        let amount = Balance::from_yoctonear(amount);
        let promise_idx = self.table.get(&promise).copied()?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        pay_action_base(
            &mut self.result_state.gas_counter,
            &self.fees_config,
            ActionCosts::deterministic_state_init_base,
            sir,
        )?;
        self.result_state.deduct_balance(amount).map_err(ErrorContainer::new)?;
        let action = self
            .ext
            .append_action_deterministic_state_init(
                receipt_idx,
                GlobalContractIdentifier::AccountId(account_id),
                amount,
            )
            .map_err(ErrorContainer::new)?;
        let action = self.table.push(action)?;
        Ok(action)
    }

    fn set_state_init_data_entry(
        &mut self,
        promise: Resource<PromiseIndex>,
        action: Resource<ActionIndex>,
        key: runtime::ValueOrRegister,
        value: runtime::ValueOrRegister,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "set_state_init_data_entry".to_string(),
            })
            .into());
        }

        let promise_idx = self.table.get(&promise).copied()?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;
        let key = key.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        let key = key.to_vec();
        let value = value.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        let value = value.to_vec();

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

        let action_index = self.table.get(&action).copied()?;
        self.ext
            .set_deterministic_state_init_data_entry(receipt_idx, action_index, key, value)
            .map_err(ErrorContainer::new)?;

        Ok(())
    }

    fn create_account(&mut self, promise: Resource<PromiseIndex>) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_create_account".to_string(),
            })
            .into());
        }
        let promise_idx = self.table.get(&promise).copied()?;
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
        code: runtime::ValueOrRegister,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_deploy_contract".to_string(),
            })
            .into());
        }
        let code = code.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        let code_len = code.len() as u64;
        let limit = self.config.limit_config.max_contract_size;
        if code_len > limit {
            return Err(ErrorContainer::new(HostError::ContractSizeExceeded {
                size: code_len,
                limit,
            })
            .into());
        }
        let code = code.into();

        let promise_idx = self.table.get(&promise).copied()?;
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
        code: runtime::ValueOrRegister,
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
        code: runtime::ValueOrRegister,
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
        code_hash: runtime::ValueOrRegister,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_use_global_contract".to_string(),
            })
            .into());
        }
        let code_hash_bytes =
            code_hash.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        let code_hash: [_; CryptoHash::LENGTH] = (&*code_hash_bytes)
            .try_into()
            .map_err(|_| ErrorContainer::new(HostError::ContractCodeHashMalformed))?;
        let contract_id = GlobalContractIdentifier::CodeHash(CryptoHash(code_hash));

        let promise_idx = self.table.get(&promise).copied()?;
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
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_use_global_contract_by_account_id".to_string(),
            })
            .into());
        }
        let account_id = self.read_account_id(&account_id)?;
        let contract_id = GlobalContractIdentifier::AccountId(account_id);

        let promise_idx = self.table.get(&promise).copied()?;
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
        method: runtime::ValueOrRegister,
        arguments: runtime::ValueOrRegister,
        amount: runtime::U128,
        gas: u64,
        gas_weight: u64,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_function_call".to_string(),
            })
            .into());
        }
        let amount = amount.read(&mut self.result_state.gas_counter)?;
        let amount = Balance::from_yoctonear(amount);
        let method_name = method.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        if method_name.is_empty() {
            return Err(ErrorContainer::new(HostError::EmptyMethodName).into());
        }
        let arguments = arguments.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;

        let method_name = method_name.to_owned();
        let arguments = arguments.to_owned();

        let promise_idx = self.table.get(&promise).copied()?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        // Input can't be large enough to overflow
        let num_bytes = method_name.len() as u64 + arguments.len() as u64;

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
        self.result_state.gas_counter.prepay_gas(Gas::from_gas(gas))?;
        self.result_state.deduct_balance(amount).map_err(ErrorContainer::new)?;
        self.ext
            .append_action_function_call_weight(
                receipt_idx,
                method_name,
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
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_transfer".to_string(),
            })
            .into());
        }
        let amount = Balance::from_yoctonear(amount.read(&mut self.result_state.gas_counter)?);

        let promise_idx = self.table.get(&promise).copied()?;
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
        self.result_state.gas_counter.pay_action_accumulated(
            burn_gas,
            use_gas,
            ActionCosts::transfer,
        )?;
        self.result_state.deduct_balance(amount).map_err(ErrorContainer::new)?;
        self.ext.append_action_transfer(receipt_idx, amount).map_err(ErrorContainer::new)?;
        Ok(())
    }

    fn stake(
        &mut self,
        promise: Resource<PromiseIndex>,
        amount: runtime::U128,
        public_key: Resource<PublicKey>,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_stake".to_string(),
            })
            .into());
        }
        let amount = Balance::from_yoctonear(amount.read(&mut self.result_state.gas_counter)?);
        let public_key = self.read_public_key(&public_key)?;
        let promise_idx = self.table.get(&promise).copied()?;
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
        public_key: Resource<PublicKey>,
        nonce: u64,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_add_key_with_full_access".to_string(),
            })
            .into());
        }
        let public_key = self.read_public_key(&public_key)?;
        let promise_idx = self.table.get(&promise).copied()?;
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
        public_key: Resource<PublicKey>,
        nonce: u64,
        allowance: runtime::U128,
        receiver_id: Resource<AccountId>,
        method_names: runtime::ValueOrRegister,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
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
        let receiver_id = self.read_account_id(&receiver_id)?;
        let raw_method_names =
            method_names.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        let method_names = split_method_names(raw_method_names).map_err(ErrorContainer::new)?;

        let promise_idx = self.table.get(&promise).copied()?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        // +1 is to account for null-terminating characters.
        let num_bytes = method_names.iter().map(|v| v.len() as u64 + 1).sum::<u64>();
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
                method_names,
            )
            .map_err(ErrorContainer::new)?;
        Ok(())
    }

    fn delete_key(
        &mut self,
        promise: Resource<PromiseIndex>,
        public_key: Resource<PublicKey>,
    ) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_delete_key".to_string(),
            })
            .into());
        }
        let public_key = self.read_public_key(&public_key)?;
        let promise_idx = self.table.get(&promise).copied()?;
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
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_batch_action_delete_account".to_string(),
            })
            .into());
        }
        let beneficiary_id = self.read_account_id(&beneficiary_id)?;

        let promise_idx = self.table.get(&promise).copied()?;
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
        method_name: runtime::ValueOrRegister,
        arguments: runtime::ValueOrRegister,
        gas: u64,
        gas_weight: u64,
        register_id: u64,
    ) -> wasmtime::Result<Resource<PromiseIndex>> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_yield_create".to_string(),
            })
            .into());
        }
        self.result_state.gas_counter.pay_base(yield_create_base)?;

        let method_name =
            method_name.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        if method_name.is_empty() {
            return Err(ErrorContainer::new(HostError::EmptyMethodName).into());
        }
        let arguments = arguments.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        let method_name = method_name.to_owned();
        let arguments = arguments.to_owned();

        // Input can't be large enough to overflow, WebAssembly address space is 32-bits.
        let num_bytes = method_name.len() as u64 + arguments.len() as u64;
        self.result_state.gas_counter.pay_per(yield_create_byte, num_bytes)?;
        // Prepay gas for the callback so that it cannot be used for this execution any longer.
        self.result_state.gas_counter.prepay_gas(Gas::from_gas(gas))?;

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
                method_name,
                arguments,
                Balance::ZERO,
                Gas::from_gas(gas),
                GasWeight(gas_weight),
            )
            .map_err(ErrorContainer::new)?;

        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            *data_id.as_bytes(),
        )?;
        Ok(new_promise_idx)
    }

    fn yield_resume(
        &mut self,
        data_id: runtime::ValueOrRegister,
        payload: runtime::ValueOrRegister,
    ) -> wasmtime::Result<bool> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_submit_data".to_string(),
            })
            .into());
        }
        self.result_state.gas_counter.pay_base(yield_resume_base)?;
        let payload = payload.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        let payload_len = payload.len() as u64;
        self.result_state.gas_counter.pay_per(yield_resume_byte, payload_len)?;
        let data_id = data_id.as_bytes(&mut self.result_state.gas_counter, &self.registers)?;
        if payload_len > self.config.limit_config.max_yield_payload_size {
            return Err(ErrorContainer::new(HostError::YieldPayloadLength {
                length: payload_len,
                limit: self.config.limit_config.max_yield_payload_size,
            })
            .into());
        }

        let data_id: [_; CryptoHash::LENGTH] = (&*data_id)
            .try_into()
            .map_err(|_| HostError::DataIdMalformed)
            .map_err(ErrorContainer::new)?;
        let data_id = CryptoHash(data_id);
        let payload = payload.into();
        let v =
            self.ext.submit_promise_resume_data(data_id, payload).map_err(ErrorContainer::new)?;
        Ok(v)
    }

    fn get_results_count(&mut self) -> wasmtime::Result<u64> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
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
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
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
                self.registers.set_rc_data(
                    &mut self.result_state.gas_counter,
                    &self.config.limit_config,
                    register_id,
                    Rc::clone(data),
                    charge_bytes_gas,
                )?;
                Ok(Some(Ok(())))
            }
            PromiseResult::Failed => Ok(Some(Err(()))),
        }
    }

    fn return_(&mut self, promise: Resource<PromiseIndex>) -> wasmtime::Result<()> {
        self.result_state.gas_counter.pay_base(base).map_err(ErrorContainer::new)?;
        self.result_state.gas_counter.pay_base(promise_return)?;
        if self.context.is_view() {
            return Err(ErrorContainer::new(HostError::ProhibitedInView {
                method_name: "promise_return".to_string(),
            })
            .into());
        }
        let promise_idx = self.table.get(&promise).copied()?;
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
        self.result_state.gas_counter.pay_base(read_memory_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(read_memory_byte, account_id.len() as _)
            .map_err(ErrorContainer::new)?;
        self.result_state.gas_counter.pay_base(utf8_decoding_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(utf8_decoding_byte, account_id.len() as u64)
            .map_err(ErrorContainer::new)?;
        match account_id.parse() {
            Ok(account_id) => {
                let account_id = self.table.push(account_id)?;
                Ok(Ok(account_id))
            }
            Err(_err) => Ok(Err(())),
        }
    }

    fn to_string(&mut self, account_id: Resource<AccountId>) -> wasmtime::Result<String> {
        let account_id = self.table.get(&account_id)?;
        self.result_state.gas_counter.pay_base(write_memory_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(write_memory_byte, account_id.len() as _)
            .map_err(ErrorContainer::new)?;
        Ok(account_id.to_string())
    }

    fn drop(&mut self, account_id: Resource<AccountId>) -> wasmtime::Result<()> {
        self.table.delete(account_id)?;
        Ok(())
    }
}

impl runtime::HostPublicKey for Ctx {
    fn from_string(
        &mut self,
        public_key: String,
    ) -> wasmtime::Result<Result<Resource<PublicKey>, ()>> {
        self.result_state.gas_counter.pay_base(read_memory_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(read_memory_byte, public_key.len() as _)
            .map_err(ErrorContainer::new)?;
        self.result_state.gas_counter.pay_base(utf8_decoding_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(utf8_decoding_byte, public_key.len() as u64)
            .map_err(ErrorContainer::new)?;
        match public_key.parse() {
            Ok(public_key) => {
                let public_key = self.table.push(public_key)?;
                Ok(Ok(public_key))
            }
            Err(_err) => Ok(Err(())),
        }
    }

    fn to_string(&mut self, public_key: Resource<PublicKey>) -> wasmtime::Result<String> {
        let public_key = self.table.get(&public_key)?;
        self.result_state.gas_counter.pay_base(write_memory_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(write_memory_byte, public_key.len() as _)
            .map_err(ErrorContainer::new)?;
        Ok(public_key.to_string())
    }

    fn to_bytes(&mut self, public_key: Resource<PublicKey>) -> wasmtime::Result<Vec<u8>> {
        let public_key = self.table.get(&public_key)?;
        self.result_state.gas_counter.pay_base(write_memory_base).map_err(ErrorContainer::new)?;
        self.result_state
            .gas_counter
            .pay_per(write_memory_byte, public_key.len() as _)
            .map_err(ErrorContainer::new)?;
        match public_key {
            PublicKey::ED25519(public_key) => {
                let public_key = public_key.as_ref();
                let mut buf = Vec::with_capacity(1 + public_key.len());
                buf.push(0);
                buf.extend_from_slice(public_key);
                Ok(buf)
            }
            PublicKey::SECP256K1(public_key) => {
                let public_key = public_key.as_ref();
                let mut buf = Vec::with_capacity(1 + public_key.len());
                buf.push(1);
                buf.extend_from_slice(public_key);
                Ok(buf)
            }
        }
    }

    fn drop(&mut self, public_key: Resource<PublicKey>) -> wasmtime::Result<()> {
        self.table.delete(public_key)?;
        Ok(())
    }
}
