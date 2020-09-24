#[macro_use]
extern crate enum_primitive_derive;

use borsh::{BorshDeserialize, BorshSerialize};
use ethereum_types::{Address, H160, U256};
use evm::CreateContractAddress;
use vm::{ContractCreateResult, MessageCallResult};

use near_runtime_fees::{EvmCostConfig, RuntimeFeesConfig};
use near_vm_errors::{EvmError, FunctionCallError, VMError};
use near_vm_logic::gas_counter::GasCounter;
use near_vm_logic::types::{AccountId, Balance, Gas, ReturnData, StorageUsage};
use near_vm_logic::{ActionCosts, External, VMConfig, VMLogicError, VMOutcome};

use crate::evm_state::{EvmAccount, EvmGasCounter, EvmState, StateStore};
use crate::types::{
    AddressArg, GetStorageAtArgs, Result, TransferArgs, ViewCallArgs, WithdrawArgs,
};
use crate::utils::{ecrecover_address, near_erc721_domain, prepare_meta_call_args};

mod builtins;
mod evm_state;
mod interpreter;
mod near_ext;
pub mod types;
pub mod utils;

pub struct EvmContext<'a> {
    ext: &'a mut dyn External,
    account_id: AccountId,
    signer_id: AccountId,
    predecessor_id: AccountId,
    current_amount: Balance,
    pub attached_deposit: Balance,
    storage_usage: StorageUsage,
    pub logs: Vec<String>,
    gas_counter: GasCounter,
    pub evm_gas_counter: EvmGasCounter,
    fees_config: &'a RuntimeFeesConfig,
    domain_separator: [u8; 32],
}

// Different kind of evm operations that result in different gas calculation
pub enum EvmOpForGas {
    Deploy,
    Funcall,
    Other,
}

enum KeyPrefix {
    Account = 0,
    Contract = 1,
}

pub const PREPAID_EVM_GAS: u128 = 1_000_000_000;

fn address_to_key(prefix: KeyPrefix, address: &H160) -> Vec<u8> {
    let mut result = Vec::with_capacity(21);
    result.push(prefix as u8);
    result.extend_from_slice(&address.0);
    result
}

impl<'a> EvmState for EvmContext<'a> {
    fn code_at(&self, address: &H160) -> Result<Option<Vec<u8>>> {
        self.ext
            .storage_get(&address_to_key(KeyPrefix::Contract, address))
            .map(|value| value.map(|x| x.deref().unwrap_or(vec![])))
    }

    fn set_code(&mut self, address: &H160, bytecode: &[u8]) -> Result<()> {
        self.ext.storage_set(&address_to_key(KeyPrefix::Contract, address), bytecode)
    }

    fn get_account(&self, address: &Address) -> Result<Option<EvmAccount>> {
        self.ext.storage_get(&address_to_key(KeyPrefix::Account, address)).map(|value| {
            value.map(|x| {
                EvmAccount::try_from_slice(&x.deref().expect("Failed to deref")).unwrap_or_default()
            })
        })
    }

    fn set_account(&mut self, address: &Address, account: &EvmAccount) -> Result<()> {
        self.ext.storage_set(
            &address_to_key(KeyPrefix::Account, address),
            &account.try_to_vec().expect("Failed to serialize"),
        )
    }

    fn _read_contract_storage(&self, key: [u8; 52]) -> Result<Option<[u8; 32]>> {
        self.ext
            .storage_get(&key)
            .map(|value| value.map(|x| utils::vec_to_arr_32(x.deref().expect("Failed to deref"))))
    }

    fn _set_contract_storage(&mut self, key: [u8; 52], value: [u8; 32]) -> Result<()> {
        self.ext.storage_set(&key, &value)
    }

    fn commit_changes(&mut self, other: &StateStore) -> Result<()> {
        for address in other.self_destructs.iter() {
            self.clear_contract_info(address)?;
        }
        for address in other.recreated.iter() {
            self.clear_contract_info(address)?;
        }
        for (address, code) in other.code.iter() {
            self.set_code(&H160(*address), code)?;
        }
        for (address, account) in other.accounts.iter() {
            self.set_account(&H160(*address), account)?;
        }
        for (key, value) in other.storages.iter() {
            let mut arr = [0; 52];
            arr.copy_from_slice(&key);
            self._set_contract_storage(arr, *value)?;
        }
        self.logs.extend_from_slice(&other.logs);
        Ok(())
    }

    fn recreate(&mut self, _address: [u8; 20]) {
        unreachable!()
    }
}

impl<'a> EvmContext<'a> {
    pub fn new(
        ext: &'a mut dyn External,
        config: &'a VMConfig,
        fees_config: &'a RuntimeFeesConfig,
        current_amount: Balance,
        account_id: AccountId,
        signer_id: AccountId,
        predecessor_id: AccountId,
        attached_deposit: Balance,
        storage_usage: StorageUsage,
        prepaid_gas: Gas,
        is_view: bool,
        evm_gas: U256,
    ) -> Self {
        let max_gas_burnt = if is_view {
            config.limit_config.max_gas_burnt_view
        } else {
            config.limit_config.max_gas_burnt
        };
        // TODO: pass chain id from ??? genesis / config.
        let domain_separator = near_erc721_domain(U256::from(0x4e454152));
        Self {
            ext,
            account_id,
            signer_id,
            predecessor_id,
            current_amount,
            attached_deposit,
            storage_usage,
            logs: Vec::default(),
            gas_counter: GasCounter::new(
                config.ext_costs.clone(),
                max_gas_burnt,
                prepaid_gas,
                is_view,
                None,
            ),
            evm_gas_counter: EvmGasCounter::new(0.into(), evm_gas),
            fees_config,
            domain_separator,
        }
    }

    fn clear_contract_info(&mut self, other: &[u8; 20]) -> Result<()> {
        self.ext.storage_remove_subtree(other)
    }

    pub fn deploy_code(&mut self, bytecode: Vec<u8>) -> Result<Address> {
        let sender = utils::near_account_id_to_evm_address(&self.predecessor_id);
        self.add_balance(&sender, U256::from(self.attached_deposit))?;
        let r = interpreter::deploy_code(
            self,
            &sender,
            &sender,
            U256::from(self.attached_deposit),
            0,
            CreateContractAddress::FromSenderAndNonce,
            false,
            &bytecode,
            &self.evm_gas_counter.gas_left(),
            &self.fees_config.evm_config,
        )?;
        match r {
            ContractCreateResult::Created(address, gas_left) => {
                self.evm_gas_counter.set_gas_left(gas_left);
                Ok(address)
            }
            ContractCreateResult::Reverted(gas_left, return_data) => {
                self.evm_gas_counter.set_gas_left(gas_left);
                Err(VMLogicError::EvmError(EvmError::DeployFail(hex::encode(return_data.to_vec()))))
            }
            _ => unreachable!(),
        }
    }

    /// Make an EVM transaction. Calls `contract_address` with RLP encoded `input`. Execution
    /// continues until all EVM messages have been processed. We expect this to behave identically
    /// to an Ethereum transaction, however there may be some edge cases.
    pub fn call_function(&mut self, args: Vec<u8>) -> Result<Vec<u8>> {
        if args.len() <= 20 {
            return Err(VMLogicError::EvmError(EvmError::ArgumentParseError));
        }
        let contract_address = Address::from_slice(&args[..20]);
        let input = &args[20..];
        let origin = utils::near_account_id_to_evm_address(&self.signer_id);
        let sender = utils::near_account_id_to_evm_address(&self.predecessor_id);
        self.add_balance(&sender, U256::from(self.attached_deposit))?;
        let value =
            if self.attached_deposit == 0 { None } else { Some(U256::from(self.attached_deposit)) };
        let rd = interpreter::call(
            self,
            &sender,
            &sender,
            value,
            0,
            &contract_address,
            &input,
            true,
            &self.evm_gas_counter.gas_left(),
            &self.fees_config.evm_config,
        )?;
        match rd {
            MessageCallResult::Success(gas_left, data) => {
                self.evm_gas_counter.set_gas_left(gas_left);
                Ok(data.to_vec())
            }
            MessageCallResult::Reverted(gas_left, data) => {
                self.evm_gas_counter.set_gas_left(gas_left);
                Err(VMLogicError::EvmError(EvmError::Revert(hex::encode(data.to_vec()))))
            }
            _ => unreachable!(),
        }
    }

    /// Make an EVM call via a meta transaction pattern.
    /// Specifically, providing signature and NEAREvm message that determines which contract and arguments to be called.
    /// Format
    /// 0..95: signature: v - 32 bytes, s - 32 bytes, r - 32 bytes
    /// 96..115: contract_id: address for contract to call
    /// 116..: RLP encoded arguments.
    pub fn meta_call_function(&mut self, args: Vec<u8>) -> Result<Vec<u8>> {
        if args.len() <= 116 {
            return Err(VMLogicError::EvmError(EvmError::ArgumentParseError));
        }
        let mut signature: [u8; 96] = [0; 96];
        signature.copy_from_slice(&args[..96]);
        let args = &args[96..];
        let sender = ecrecover_address(
            &prepare_meta_call_args(&self.domain_separator, &self.account_id, args),
            &signature,
        )?;
        if sender == Address::zero() {
            return Err(VMLogicError::EvmError(EvmError::InvalidEcRecoverSignature));
        }
        let contract_address = Address::from_slice(&args[..20]);
        let input = &args[20..];
        self.add_balance(&sender, U256::from(self.attached_deposit))?;
        let value =
            if self.attached_deposit == 0 { None } else { Some(U256::from(self.attached_deposit)) };
        let rd = interpreter::call(self, &sender, &sender, value, 0, &contract_address, &input, true, &self.evm_gas_counter.gas_left(), &self.fees_config.evm_config)?;
        match rd {
            MessageCallResult::Success(gas_left, data) => {
                self.evm_gas_counter.set_gas_left(gas_left);
                Ok(data.to_vec())
            }
            MessageCallResult::Reverted(gas_left, data) => {
                self.evm_gas_counter.set_gas_left(gas_left);
                Err(VMLogicError::EvmError(EvmError::Revert(hex::encode(data.to_vec()))))
            }
            _ => unreachable!(),
        }
    }

    /// Make an EVM transaction. Calls `contract_address` with `encoded_input`. Execution
    /// continues until all EVM messages have been processed. We expect this to behave identically
    /// to an Ethereum transaction, however there may be some edge cases.
    ///
    /// This function serves the eth_call functionality, and will NOT apply state changes.
    pub fn view_call_function(&mut self, args: Vec<u8>) -> Result<Vec<u8>> {
        let args = ViewCallArgs::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        let sender = Address::from(&args.sender);
        let rd = interpreter::call(
            self,
            &sender,
            &sender,
            Some(U256::from(args.amount)),
            0,
            &Address::from(&args.address),
            &args.args,
            false,
            &self.evm_gas_counter.gas_left(),
            &self.fees_config.evm_config,
        )?;
        match rd {
            MessageCallResult::Success(gas_left, data) => {
                self.evm_gas_counter.set_gas_left(gas_left);
                Ok(data.to_vec())
            }
            MessageCallResult::Reverted(gas_left, data) => {
                self.evm_gas_counter.set_gas_left(gas_left);
                Err(VMLogicError::EvmError(EvmError::Revert(hex::encode(data.to_vec()))))
            }
            _ => unreachable!(),
        }
    }

    pub fn get_code(&self, args: Vec<u8>) -> Result<Vec<u8>> {
        let args = AddressArg::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        Ok(self.code_at(&Address::from_slice(&args.address)).unwrap_or(None).unwrap_or(vec![]))
    }

    pub fn get_storage_at(&self, args: Vec<u8>) -> Result<Vec<u8>> {
        let args = GetStorageAtArgs::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        Ok(self
            .read_contract_storage(&Address::from_slice(&args.address), args.key)?
            .unwrap_or([0u8; 32])
            .to_vec())
    }

    pub fn get_balance(&self, args: Vec<u8>) -> Result<U256> {
        let args = AddressArg::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        self.balance_of(&Address::from_slice(&args.address))
    }

    pub fn get_nonce(&self, args: Vec<u8>) -> Result<U256> {
        let args = AddressArg::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        self.nonce_of(&Address::from_slice(&args.address))
    }

    pub fn deposit(&mut self, args: Vec<u8>) -> Result<U256> {
        let args = AddressArg::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        if self.attached_deposit == 0 {
            return Err(VMLogicError::EvmError(EvmError::MissingDeposit));
        }
        let address = Address::from_slice(&args.address);
        self.add_balance(&address, U256::from(self.attached_deposit))?;
        self.balance_of(&address)
    }

    pub fn withdraw(&mut self, args: Vec<u8>) -> Result<()> {
        let args = WithdrawArgs::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        let sender = utils::near_account_id_to_evm_address(&self.predecessor_id);
        let amount = U256::from(args.amount);
        if amount > self.balance_of(&sender)? {
            return Err(VMLogicError::EvmError(EvmError::InsufficientFunds));
        }
        self.sub_balance(&sender, amount)?;
        let receipt_index = self.ext.create_receipt(vec![], args.account_id)?;
        // We use low_u128, because NEAR native currency fits into u128.
        let amount = amount.low_u128();
        self.current_amount = self
            .current_amount
            .checked_sub(amount)
            .ok_or_else(|| VMLogicError::EvmError(EvmError::InsufficientFunds))?;
        self.pay_gas_for_new_receipt(false, &[])?;
        self.gas_counter.pay_action_base(
            &self.fees_config.action_creation_config.transfer_cost,
            // TOOD: Hm, what if they withdraw to itself? We should probably close circuit that here.
            false,
            ActionCosts::transfer,
        )?;
        self.ext.append_action_transfer(receipt_index, amount).map_err(|err| err.into())
    }

    /// Transfer tokens from sender to given EVM address.
    pub fn transfer(&mut self, args: Vec<u8>) -> Result<()> {
        let args = TransferArgs::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        let sender = utils::near_account_id_to_evm_address(&self.predecessor_id);
        let amount = U256::from(args.amount);
        if amount > self.balance_of(&sender)? {
            return Err(VMLogicError::EvmError(EvmError::InsufficientFunds));
        }
        self.transfer_balance(&sender, &Address::from(args.address), amount)
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
    /// `burnt_gas := dispatch cost of the receipt + base dispatch cost  cost of the data receipt`
    /// `used_gas := burnt_gas + exec cost of the receipt + base exec cost  cost of the data receipt`
    /// Notice that we prepay all base cost upon the creation of the data dependency, we are going to
    /// pay for the content transmitted through the dependency upon the actual creation of the
    /// DataReceipt.
    fn pay_gas_for_new_receipt(&mut self, sir: bool, data_dependencies: &[bool]) -> Result<()> {
        let fees_config_cfg = &self.fees_config;
        let mut burn_gas = fees_config_cfg.action_receipt_creation_config.send_fee(sir);
        let mut use_gas = fees_config_cfg.action_receipt_creation_config.exec_fee();
        for dep in data_dependencies {
            // Both creation and execution for data receipts are considered burnt gas.
            burn_gas = burn_gas
                .checked_add(fees_config_cfg.data_receipt_creation_config.base_cost.send_fee(*dep))
                .ok_or(VMLogicError::EvmError(EvmError::IntegerOverflow))?
                .checked_add(fees_config_cfg.data_receipt_creation_config.base_cost.exec_fee())
                .ok_or(VMLogicError::EvmError(EvmError::IntegerOverflow))?;
        }
        use_gas = use_gas
            .checked_add(burn_gas)
            .ok_or(VMLogicError::EvmError(EvmError::IntegerOverflow))?;
        self.gas_counter.pay_action_accumulated(burn_gas, use_gas, ActionCosts::new_receipt)
    }

    fn pay_gas_from_evm_gas(&mut self, op: EvmOpForGas) -> Result<()> {
        let fee_cfg = &self.fees_config.evm_config;
        let evm_gas = self.evm_gas_counter.used_gas.as_u64();
        println!("============== evm_gas {}", evm_gas);
        self.gas_counter.inc_evm_gas_counter(evm_gas);
        let gas = match op {
            EvmOpForGas::Deploy => {
                evm_gas * fee_cfg.deploy_cost_per_evm_gas + fee_cfg.bootstrap_cost
            }
            EvmOpForGas::Funcall => {
                evm_gas * fee_cfg.funcall_cost_per_evm_gas
                    + fee_cfg.funcall_cost_base
                    + fee_cfg.bootstrap_cost
            }
            EvmOpForGas::Other => fee_cfg.bootstrap_cost,
        };
        self.gas_counter.pay_evm_gas(gas)
    }
}

fn max_evm_gas_from_near_gas(
    near_gas: Gas,
    evm_gas_config: &EvmCostConfig,
    method_name: &str,
) -> Option<U256> {
    match method_name {
        "deploy_code" => {
            if near_gas < evm_gas_config.bootstrap_cost {
                return None;
            }
            Some(
                ((near_gas - evm_gas_config.bootstrap_cost)
                    / evm_gas_config.deploy_cost_per_evm_gas)
                    .into(),
            )
        }
        "call_function" => {
            if near_gas < evm_gas_config.bootstrap_cost + evm_gas_config.funcall_cost_base {
                return None;
            }
            Some(
                ((near_gas - evm_gas_config.bootstrap_cost - evm_gas_config.funcall_cost_base)
                    / evm_gas_config.funcall_cost_per_evm_gas)
                    .into(),
            )
        }
        _ => {
            if near_gas < evm_gas_config.bootstrap_cost {
                return None;
            }
            Some(evm_gas_config.bootstrap_cost.into())
        }
    }
}

pub fn run_evm(
    ext: &mut dyn External,
    config: &VMConfig,
    fees_config: &RuntimeFeesConfig,
    account_id: &AccountId,
    signer_id: &AccountId,
    predecessor_id: &AccountId,
    amount: Balance,
    attached_deposit: Balance,
    storage_usage: StorageUsage,
    method_name: String,
    args: Vec<u8>,
    prepaid_gas: Gas,
    is_view: bool,
) -> (Option<VMOutcome>, Option<VMError>) {
    let evm_gas_result =
        max_evm_gas_from_near_gas(prepaid_gas, &fees_config.evm_config, &method_name);
    // println!("evm_gas_result: {:?}", evm_gas_result);
    // let evm_gas_result = Some(1_000_000_000_000u64.into());
    if evm_gas_result.is_none() {
        return (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::EvmError(EvmError::Revert(
                "Not enough to run EVM".to_string(),
            )))),
        );
    }
    let evm_gas = evm_gas_result.unwrap();
    let mut context = EvmContext::new(
        ext,
        config,
        fees_config,
        // This is total amount of all $NEAR inside this EVM.
        // Should already validate that will not overflow external to this call.
        amount.checked_add(attached_deposit).unwrap_or(amount),
        account_id.clone(),
        signer_id.clone(),
        predecessor_id.clone(),
        attached_deposit,
        storage_usage,
        prepaid_gas,
        is_view,
        evm_gas,
    );
    let result = match method_name.as_str() {
        // Change the state methods.
        "deploy_code" => context.deploy_code(args).map(|address| utils::address_to_vec(&address)),
        "call_function" => context.call_function(args),
        "call" => context.call_function(args),
        "meta_call" => context.meta_call_function(args),
        "deposit" => context.deposit(args).map(|balance| utils::u256_to_arr(&balance).to_vec()),
        "withdraw" => context.withdraw(args).map(|_| vec![]),
        "transfer" => context.transfer(args).map(|_| vec![]),
        // View methods.
        "view_function_call" => context.view_call_function(args),
        "view" => context.view_call_function(args),
        "get_code" => context.get_code(args),
        "get_storage_at" => context.get_storage_at(args),
        "get_nonce" => context.get_nonce(args).map(|nonce| utils::u256_to_arr(&nonce).to_vec()),
        "get_balance" => {
            context.get_balance(args).map(|balance| utils::u256_to_arr(&balance).to_vec())
        }
        _ => Err(VMLogicError::EvmError(EvmError::MethodNotFound)),
    };
    context
        .pay_gas_from_evm_gas(match method_name.as_str() {
            "deploy_code" => EvmOpForGas::Deploy,
            "call_function" => EvmOpForGas::Funcall,
            _ => EvmOpForGas::Other,
        })
        // It's not possible deduct near gas underflow, because even use full evm gas it's less than prepaid near gas
        // If full evm gas isn't enough for evm operation, evm will revert result and all near gas is used to pay for evm gas
        .unwrap();

    match result {
        Ok(value) => {
            let outcome = VMOutcome {
                balance: context.current_amount,
                storage_usage: context.storage_usage,
                return_data: ReturnData::Value(value),
                burnt_gas: context.gas_counter.burnt_gas(),
                used_gas: context.gas_counter.used_gas(),
                logs: context.logs,
            };
            (Some(outcome), None)
        }
        Err(VMLogicError::EvmError(err)) => {
            (None, Some(VMError::FunctionCallError(FunctionCallError::EvmError(err))))
        }
        Err(_) => (None, Some(VMError::FunctionCallError(FunctionCallError::WasmUnknownError))),
    }
}

#[cfg(test)]
mod tests {
    use near_vm_logic::mocks::mock_external::MockedExternal;

    use crate::evm_state::SubState;

    use super::*;

    fn setup() -> (MockedExternal, VMConfig, RuntimeFeesConfig) {
        let vm_config = VMConfig::default();
        let fees_config = RuntimeFeesConfig::default();
        let fake_external = MockedExternal::new();
        (fake_external, vm_config, fees_config)
    }

    fn create_context<'a>(
        external: &'a mut MockedExternal,
        vm_config: &'a VMConfig,
        fees_config: &'a RuntimeFeesConfig,
        account_id: &str,
    ) -> EvmContext<'a> {
        EvmContext::new(
            external,
            vm_config,
            fees_config,
            0,
            "evm".to_string(),
            account_id.to_string(),
            account_id.to_string(),
            0,
            0,
            0,
            false,
        )
    }

    #[test]
    fn state_management() {
        let (mut fake_external, vm_config, fees_config) = setup();
        let mut context = create_context(&mut fake_external, &vm_config, &fees_config, "alice");
        let addr_0 = Address::repeat_byte(0);
        let addr_1 = Address::repeat_byte(1);
        let addr_2 = Address::repeat_byte(2);

        let zero = U256::zero();
        let code: [u8; 3] = [0, 1, 2];
        let nonce = U256::from_dec_str("103030303").unwrap();
        let balance = U256::from_dec_str("3838209").unwrap();
        let storage_key_0 = [4u8; 32];
        let storage_key_1 = [5u8; 32];
        let storage_value_0 = [6u8; 32];
        let storage_value_1 = [7u8; 32];

        context.set_code(&addr_0, &code).unwrap();
        assert_eq!(context.code_at(&addr_0).unwrap(), Some(code.to_vec()));
        assert_eq!(context.code_at(&addr_1).unwrap(), None);
        assert_eq!(context.code_at(&addr_2).unwrap(), None);

        context.set_nonce(&addr_0, nonce).unwrap();
        assert_eq!(context.nonce_of(&addr_0).unwrap(), nonce);
        assert_eq!(context.nonce_of(&addr_1).unwrap(), zero);
        assert_eq!(context.nonce_of(&addr_2).unwrap(), zero);

        context.set_balance(&addr_0, balance).unwrap();
        assert_eq!(context.balance_of(&addr_0).unwrap(), balance);
        assert_eq!(context.balance_of(&addr_1).unwrap(), zero);
        assert_eq!(context.balance_of(&addr_2).unwrap(), zero);

        context.set_contract_storage(&addr_0, storage_key_0, storage_value_0).unwrap();
        assert_eq!(
            context.read_contract_storage(&addr_0, storage_key_0).unwrap(),
            Some(storage_value_0)
        );
        assert_eq!(context.read_contract_storage(&addr_1, storage_key_0).unwrap(), None);
        assert_eq!(context.read_contract_storage(&addr_2, storage_key_0).unwrap(), None);

        let next = {
            // Open a new store
            let mut next = StateStore::default();
            let mut sub1 = SubState::new(&addr_0, &mut next, &context);

            sub1.set_code(&addr_1, &code).unwrap();
            assert_eq!(sub1.code_at(&addr_0).unwrap(), Some(code.to_vec()));
            assert_eq!(sub1.code_at(&addr_1).unwrap(), Some(code.to_vec()));
            assert_eq!(sub1.code_at(&addr_2).unwrap(), None);

            sub1.set_nonce(&addr_1, nonce).unwrap();
            assert_eq!(sub1.nonce_of(&addr_0).unwrap(), nonce);
            assert_eq!(sub1.nonce_of(&addr_1).unwrap(), nonce);
            assert_eq!(sub1.nonce_of(&addr_2).unwrap(), zero);

            sub1.set_balance(&addr_1, balance).unwrap();
            assert_eq!(sub1.balance_of(&addr_0).unwrap(), balance);
            assert_eq!(sub1.balance_of(&addr_1).unwrap(), balance);
            assert_eq!(sub1.balance_of(&addr_2).unwrap(), zero);

            sub1.set_contract_storage(&addr_1, storage_key_0, storage_value_0).unwrap();
            assert_eq!(
                sub1.read_contract_storage(&addr_0, storage_key_0).unwrap(),
                Some(storage_value_0)
            );
            assert_eq!(
                sub1.read_contract_storage(&addr_1, storage_key_0).unwrap(),
                Some(storage_value_0)
            );
            assert_eq!(sub1.read_contract_storage(&addr_2, storage_key_0).unwrap(), None);

            sub1.set_contract_storage(&addr_1, storage_key_0, storage_value_1).unwrap();
            assert_eq!(
                sub1.read_contract_storage(&addr_0, storage_key_0).unwrap(),
                Some(storage_value_0)
            );
            assert_eq!(
                sub1.read_contract_storage(&addr_1, storage_key_0).unwrap(),
                Some(storage_value_1)
            );
            assert_eq!(sub1.read_contract_storage(&addr_2, storage_key_0).unwrap(), None);

            sub1.set_contract_storage(&addr_1, storage_key_1, storage_value_1).unwrap();
            assert_eq!(
                sub1.read_contract_storage(&addr_1, storage_key_0).unwrap(),
                Some(storage_value_1)
            );
            assert_eq!(
                sub1.read_contract_storage(&addr_1, storage_key_1).unwrap(),
                Some(storage_value_1)
            );

            sub1.set_contract_storage(&addr_1, storage_key_0, storage_value_0).unwrap();
            assert_eq!(
                sub1.read_contract_storage(&addr_1, storage_key_0).unwrap(),
                Some(storage_value_0)
            );
            assert_eq!(
                sub1.read_contract_storage(&addr_1, storage_key_1).unwrap(),
                Some(storage_value_1)
            );

            next
        };

        context.commit_changes(&next).unwrap();
        assert_eq!(context.code_at(&addr_0).unwrap(), Some(code.to_vec()));
        assert_eq!(context.code_at(&addr_1).unwrap(), Some(code.to_vec()));
        assert_eq!(context.code_at(&addr_2).unwrap(), None);
        assert_eq!(context.nonce_of(&addr_0).unwrap(), nonce);
        assert_eq!(context.nonce_of(&addr_1).unwrap(), nonce);
        assert_eq!(context.nonce_of(&addr_2).unwrap(), zero);
        assert_eq!(context.balance_of(&addr_0).unwrap(), balance);
        assert_eq!(context.balance_of(&addr_1).unwrap(), balance);
        assert_eq!(context.balance_of(&addr_2).unwrap(), zero);
        assert_eq!(
            context.read_contract_storage(&addr_0, storage_key_0).unwrap(),
            Some(storage_value_0)
        );
        assert_eq!(
            context.read_contract_storage(&addr_1, storage_key_0).unwrap(),
            Some(storage_value_0)
        );
        assert_eq!(
            context.read_contract_storage(&addr_1, storage_key_1).unwrap(),
            Some(storage_value_1)
        );
        assert_eq!(context.read_contract_storage(&addr_2, storage_key_0).unwrap(), None);
    }
}
