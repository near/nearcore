use std::sync::Arc;

use ethereum_types::{Address, U256};
use evm::{CreateContractAddress, Factory};
use near_runtime_fees::EvmCostConfig;
use vm::{
    ActionParams, ActionValue, CallType, ContractCreateResult, ExecTrapResult, Ext, GasLeft,
    MessageCallResult, ParamsType, ReturnData, Schedule,
};

use near_vm_errors::{EvmError, VMLogicError};

use crate::evm_state::{EvmState, StateStore, SubState};
use crate::near_ext::NearExt;
use crate::types::{convert_vm_error, Result};
use crate::utils;

pub fn deploy_code<T: EvmState>(
    state: &mut T,
    origin: &Address,
    sender: &Address,
    value: U256,
    call_stack_depth: usize,
    address_type: CreateContractAddress,
    recreate: bool,
    code: &[u8],
    gas: &U256,
    evm_gas_config: &EvmCostConfig,
    chain_id: u128,
) -> Result<ContractCreateResult> {
    let mut nonce = U256::zero();
    if address_type == CreateContractAddress::FromSenderAndNonce {
        nonce = state.next_nonce(&sender)?;
    };
    let (address, _) = utils::evm_contract_address(address_type, &sender, &nonce, &code);

    if recreate {
        state.recreate(address);
    } else if state.code_at(&address)?.is_some() {
        return Err(VMLogicError::EvmError(EvmError::DuplicateContract(address.0.to_vec())));
    }

    let (result, state_updates) = _create(
        state,
        origin,
        sender,
        value,
        call_stack_depth,
        &address,
        code,
        gas,
        evm_gas_config,
        chain_id,
    )?;

    // Apply known gas amount changes (all reverts are NeedsReturn)
    // Apply NeedsReturn changes if apply_state
    // Return the result unmodified
    let (return_data, apply, gas_left) = match result {
        Ok(Ok(GasLeft::Known(left))) => (ReturnData::empty(), true, left),
        Ok(Ok(GasLeft::NeedsReturn { gas_left: left, data, apply_state })) => {
            (data, apply_state, left)
        }
        Ok(Err(err)) => return Err(convert_vm_error(err)),
        Err(_) => return Err(VMLogicError::EvmError(EvmError::Reverted)),
    };

    if apply {
        state.commit_changes(&state_updates.unwrap())?;
        state.set_code(&address, &return_data.to_vec())?;
        Ok(ContractCreateResult::Created(address, gas_left))
    } else {
        Ok(ContractCreateResult::Reverted(gas_left, return_data))
    }
}

pub fn _create<T: EvmState>(
    state: &mut T,
    origin: &Address,
    sender: &Address,
    value: U256,
    call_stack_depth: usize,
    address: &Address,
    code: &[u8],
    gas: &U256,
    evm_gas_config: &EvmCostConfig,
    chain_id: u128,
) -> Result<(ExecTrapResult<GasLeft>, Option<StateStore>)> {
    let mut store = StateStore::default();
    let mut sub_state = SubState::new(sender, &mut store, state);

    let params = ActionParams {
        code_address: *address,
        address: *address,
        sender: *sender,
        origin: *origin,
        gas: *gas,
        gas_price: 1.into(),
        value: ActionValue::Transfer(value),
        code: Some(Arc::new(code.to_vec())),
        code_hash: None,
        data: None,
        call_type: CallType::None,
        params_type: vm::ParamsType::Embedded,
    };

    sub_state.transfer_balance(sender, address, value)?;

    let mut ext = NearExt::new(
        *address,
        *origin,
        &mut sub_state,
        call_stack_depth + 1,
        false,
        evm_gas_config,
        chain_id,
    );
    ext.info.gas_limit = U256::from(gas);
    ext.schedule = Schedule::new_constantinople();

    let instance = Factory::default().create(params, ext.schedule(), ext.depth());

    // Run the code
    let result = instance.exec(&mut ext);
    Ok((result, Some(store)))
}

#[allow(clippy::too_many_arguments)]
pub fn call<T: EvmState>(
    state: &mut T,
    origin: &Address,
    sender: &Address,
    value: Option<U256>,
    call_stack_depth: usize,
    contract_address: &Address,
    input: &[u8],
    should_commit: bool,
    gas: &U256,
    evm_gas_config: &EvmCostConfig,
    chain_id: u128,
) -> Result<MessageCallResult> {
    run_and_commit_if_success(
        state,
        origin,
        sender,
        value,
        call_stack_depth,
        CallType::Call,
        contract_address,
        contract_address,
        input,
        false,
        should_commit,
        gas,
        evm_gas_config,
        chain_id,
    )
}

pub fn delegate_call<T: EvmState>(
    state: &mut T,
    origin: &Address,
    sender: &Address,
    call_stack_depth: usize,
    context: &Address,
    delegee: &Address,
    input: &[u8],
    gas: &U256,
    evm_gas_config: &EvmCostConfig,
    chain_id: u128,
) -> Result<MessageCallResult> {
    run_and_commit_if_success(
        state,
        origin,
        sender,
        None,
        call_stack_depth,
        CallType::DelegateCall,
        context,
        delegee,
        input,
        false,
        true,
        gas,
        evm_gas_config,
        chain_id,
    )
}

pub fn static_call<T: EvmState>(
    state: &mut T,
    origin: &Address,
    sender: &Address,
    call_stack_depth: usize,
    contract_address: &Address,
    input: &[u8],
    gas: &U256,
    evm_gas_config: &EvmCostConfig,
    chain_id: u128,
) -> Result<MessageCallResult> {
    run_and_commit_if_success(
        state,
        origin,
        sender,
        None,
        call_stack_depth,
        CallType::StaticCall,
        contract_address,
        contract_address,
        input,
        true,
        false,
        gas,
        evm_gas_config,
        chain_id,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_and_commit_if_success<T: EvmState>(
    state: &mut T,
    origin: &Address,
    sender: &Address,
    value: Option<U256>,
    call_stack_depth: usize,
    call_type: CallType,
    state_address: &Address,
    code_address: &Address,
    input: &[u8],
    is_static: bool,
    should_commit: bool,
    gas: &U256,
    evm_gas_config: &EvmCostConfig,
    chain_id: u128,
) -> Result<MessageCallResult> {
    // run the interpreter and
    let (result, state_updates) = run_against_state(
        state,
        origin,
        sender,
        value,
        call_stack_depth,
        call_type,
        state_address,
        code_address,
        input,
        is_static,
        gas,
        evm_gas_config,
        chain_id,
    )?;

    // Apply known gas amount changes (all reverts are NeedsReturn)
    // Apply NeedsReturn changes if apply_state
    // Return the result unmodified
    let mut should_apply_state = true;
    let return_data = match result {
        Ok(Ok(GasLeft::Known(gas_left))) => {
            Ok(MessageCallResult::Success(gas_left, ReturnData::empty()))
        }
        Ok(Ok(GasLeft::NeedsReturn { gas_left, data, apply_state: true })) => {
            Ok(MessageCallResult::Success(gas_left, data))
        }
        Ok(Ok(GasLeft::NeedsReturn { gas_left, data, apply_state: false })) => {
            should_apply_state = false;
            Ok(MessageCallResult::Reverted(gas_left, data))
        }
        Ok(Err(err)) => Err(convert_vm_error(err)),
        Err(_) => Err(VMLogicError::EvmError(EvmError::Reverted)),
    };

    // Don't apply changes from a static context (these _should_ error in the ext)
    if !is_static && return_data.is_ok() && should_apply_state && should_commit {
        state.commit_changes(&state_updates.unwrap())?;
    }
    return_data
}

/// Runs the interpreter. Produces state diffs
#[allow(clippy::too_many_arguments)]
fn run_against_state<T: EvmState>(
    state: &mut T,
    origin: &Address,
    sender: &Address,
    value: Option<U256>,
    call_stack_depth: usize,
    call_type: CallType,
    state_address: &Address,
    code_address: &Address,
    input: &[u8],
    is_static: bool,
    gas: &U256,
    evm_gas_config: &EvmCostConfig,
    chain_id: u128,
) -> Result<(ExecTrapResult<GasLeft>, Option<StateStore>)> {
    let code = state.code_at(code_address)?.unwrap_or_else(Vec::new);

    // Check that if there are arguments this is contract call.
    // Otherwise, this is just transfer call.
    if code.len() == 0 && input.len() > 0 {
        return Err(VMLogicError::EvmError(EvmError::ContractNotFound));
    }

    let mut store = StateStore::default();
    let mut sub_state = SubState::new(sender, &mut store, state);

    let mut params = ActionParams {
        code_address: *code_address,
        code_hash: None,
        address: *state_address,
        sender: *sender,
        origin: *origin,
        gas: *gas,
        gas_price: 1.into(),
        value: ActionValue::Apparent(0.into()),
        code: Some(Arc::new(code)),
        data: Some(input.to_vec()),
        call_type,
        params_type: ParamsType::Separate,
    };

    if let Some(val) = value {
        params.value = ActionValue::Transfer(val);
        // substate transfer will get reverted if the call fails
        sub_state.transfer_balance(sender, state_address, val)?;
    }

    let mut ext = NearExt::new(
        *state_address,
        *origin,
        &mut sub_state,
        call_stack_depth + 1,
        is_static,
        evm_gas_config,
        chain_id,
    );
    // Gas limit is evm block gas limit, should at least prepaid gas.
    ext.info.gas_limit = *gas;
    ext.schedule = Schedule::new_constantinople();

    let instance = Factory::default().create(params, ext.schedule(), ext.depth());

    // Run the code
    let result = instance.exec(&mut ext);
    Ok((result, Some(store)))
}
