use std::iter;

use itertools::Itertools;
use near_primitives::{
    runtime::fees::{Fee, RuntimeFeesConfig},
    types::Gas,
};
use near_vm_logic::ExtCosts;
use node_runtime::config::RuntimeConfig;
use strum::IntoEnumIterator;

#[derive(Clone, Copy)]
pub(crate) enum EstimatedParameter {
    /// Gas parameters charged for receipt processing, outside the contract
    /// runtime
    ReceiptParameter(ReceiptParameter, ReceiptCostKind),
    /// Gas parameters charged for executing smart contract code
    ContractRuntimeParameter(ExtCosts),
    /// Cost per WASM operation is a special case, logically it belongs to
    /// `ContractRuntimeParameter`
    WasmInstruction,
    /// Parameters we estimate to tune the estimator itself. These are used
    /// indirectly to determine gas costs.
    EstimatorInternal(EstimatorParameter),
}

/// Gas fee parameters that are charged in two parts, once when creating a
/// receipt and once when executing it. Three parameter values are required, to
/// differentiate creation cost for local receipts (sender = receiver) and
/// otherwise.
#[derive(Clone, Copy, strum::EnumIter)]
pub(crate) enum ReceiptParameter {
    ActionReceiptCreation,
    DataReceiptCreationBase,
    DataReceiptCreationByte,
    CreateAccount,
    DeployContractBase,
    DeployContractByte,
    FunctionCallBase,
    FunctionCallByte,
    Transfer,
    Stake,
    AddFullAccessKey,
    AddFunctionAccessKeyBase,
    AddFunctionAccessKeyByte,
    DeleteKey,
    DeleteAccount,
}

/// Receipt costs are always split. The estimations should ideally estimate each
/// split individually, but it is easier to estimate total cost. This
/// enumeration marks the choice regarding this estimation strategy.
#[derive(Clone, Copy, strum::EnumIter)]
pub(crate) enum ReceiptCostKind {
    #[allow(unused)]
    SendLocal,
    #[allow(unused)]
    SendRemote,
    Execution,
    TotalLocal,
    TotalRemote,
}

/// Parameter used internally in the estimator
#[derive(Clone, Copy, strum::EnumIter)]
pub(crate) enum EstimatorParameter {
    IoReadByte,
    IoWriteByte,
    GasInInstr,
}

impl EstimatedParameter {
    pub(crate) fn parameter_value(self, config: &RuntimeConfig) -> Gas {
        match self {
            EstimatedParameter::ReceiptParameter(receipt_param, kind) => {
                let fee = receipt_param.fee(&config.transaction_costs);
                match kind {
                    ReceiptCostKind::SendLocal => fee.send_sir,
                    ReceiptCostKind::SendRemote => fee.send_not_sir,
                    ReceiptCostKind::Execution => fee.execution,
                    ReceiptCostKind::TotalLocal => fee.send_sir + fee.execution,
                    ReceiptCostKind::TotalRemote => fee.send_not_sir + fee.execution,
                }
            }
            EstimatedParameter::ContractRuntimeParameter(ext_costs) => {
                ext_costs.value(&config.wasm_config.ext_costs)
            }
            EstimatedParameter::WasmInstruction => config.wasm_config.regular_op_cost as u64,
            EstimatedParameter::EstimatorInternal(EstimatorParameter::IoReadByte) => {
                crate::estimator_params::IO_READ_BYTE_COST.round().to_integer()
            }
            EstimatedParameter::EstimatorInternal(EstimatorParameter::IoWriteByte) => {
                crate::estimator_params::IO_WRITE_BYTE_COST.round().to_integer()
            }
            EstimatedParameter::EstimatorInternal(EstimatorParameter::GasInInstr) => {
                crate::estimator_params::GAS_IN_INSTR.round().to_integer()
            }
        }
    }

    /// Estimations are not perfect. Therefore, we have a safety factor by
    /// default. It can be lifted when we are confident in a specific
    /// estimation.
    pub fn safety_multiplier(self) -> u64 {
        match self {
            // Receipts are estimated without safety factor
            EstimatedParameter::ReceiptParameter(_, _) => 1,
            // Contract runtime parameters historically all used an average cost
            // and a safety factor of 3. As we move to statistically more sound
            // approaches, the safety factors can be reduced or removed.
            EstimatedParameter::ContractRuntimeParameter(ExtCosts::read_cached_trie_node) => 1,
            EstimatedParameter::ContractRuntimeParameter(_) => 3,
            EstimatedParameter::WasmInstruction => 3,
            EstimatedParameter::EstimatorInternal(_) => 1,
        }
    }

    pub(crate) fn fully_qualified_name(self) -> String {
        match self {
            EstimatedParameter::ReceiptParameter(param, kind) => {
                format!("receipt_processing::{}::{}", kind.as_str(), param.as_str())
            }
            EstimatedParameter::ContractRuntimeParameter(param) => {
                format!("contract_runtime::{param}")
            }
            EstimatedParameter::WasmInstruction => "regular_op_cost".to_owned(),
            EstimatedParameter::EstimatorInternal(param) => {
                format!("estimator::{}", param.as_str())
            }
        }
    }

    pub fn all() -> impl Iterator<Item = EstimatedParameter> {
        let runtime_params = ExtCosts::iter().map(EstimatedParameter::ContractRuntimeParameter);
        let internals = EstimatorParameter::iter().map(EstimatedParameter::EstimatorInternal);
        let receipt_params = ReceiptParameter::iter()
            .cartesian_product(ReceiptCostKind::iter())
            .map(|(param, kind)| EstimatedParameter::ReceiptParameter(param, kind));

        iter::once(EstimatedParameter::WasmInstruction)
            .chain(runtime_params)
            .chain(internals)
            .chain(receipt_params)
    }
}

impl ReceiptParameter {
    fn fee(self, config: &RuntimeFeesConfig) -> &Fee {
        match self {
            ReceiptParameter::ActionReceiptCreation => &config.action_receipt_creation_config,
            ReceiptParameter::DataReceiptCreationBase => {
                &config.data_receipt_creation_config.base_cost
            }
            ReceiptParameter::DataReceiptCreationByte => {
                &config.data_receipt_creation_config.cost_per_byte
            }
            ReceiptParameter::CreateAccount => &config.action_creation_config.create_account_cost,
            ReceiptParameter::DeployContractBase => {
                &config.action_creation_config.deploy_contract_cost
            }
            ReceiptParameter::DeployContractByte => {
                &config.action_creation_config.deploy_contract_cost_per_byte
            }
            ReceiptParameter::FunctionCallBase => &config.action_creation_config.function_call_cost,
            ReceiptParameter::FunctionCallByte => {
                &config.action_creation_config.function_call_cost_per_byte
            }
            ReceiptParameter::Transfer => &config.action_creation_config.transfer_cost,
            ReceiptParameter::Stake => &config.action_creation_config.stake_cost,
            ReceiptParameter::AddFullAccessKey => {
                &config.action_creation_config.add_key_cost.full_access_cost
            }
            ReceiptParameter::AddFunctionAccessKeyBase => {
                &config.action_creation_config.add_key_cost.function_call_cost
            }
            ReceiptParameter::AddFunctionAccessKeyByte => {
                &config.action_creation_config.add_key_cost.function_call_cost_per_byte
            }
            ReceiptParameter::DeleteKey => &config.action_creation_config.delete_key_cost,
            ReceiptParameter::DeleteAccount => &config.action_creation_config.delete_account_cost,
        }
    }
}

impl ReceiptParameter {
    fn as_str(self) -> &'static str {
        match self {
            ReceiptParameter::ActionReceiptCreation => "action_receipt_creation",
            ReceiptParameter::DataReceiptCreationBase => "data_receipt_creation_base",
            ReceiptParameter::DataReceiptCreationByte => "data_receipt_creation_byte",
            ReceiptParameter::CreateAccount => "create_account",
            ReceiptParameter::DeployContractBase => "deploy_contract_base",
            ReceiptParameter::DeployContractByte => "deploy_contract_byte",
            ReceiptParameter::FunctionCallBase => "function_call_base",
            ReceiptParameter::FunctionCallByte => "function_call_byte",
            ReceiptParameter::Transfer => "transfer",
            ReceiptParameter::Stake => "stake",
            ReceiptParameter::AddFullAccessKey => "add_full_access_key",
            ReceiptParameter::AddFunctionAccessKeyBase => "add_function_access_key_base",
            ReceiptParameter::AddFunctionAccessKeyByte => "add_function_access_key_byte",
            ReceiptParameter::DeleteKey => "delete_key",
            ReceiptParameter::DeleteAccount => "delete_account",
        }
    }
}

impl ReceiptCostKind {
    fn as_str(self) -> &'static str {
        match self {
            ReceiptCostKind::SendLocal => "send_sir",
            ReceiptCostKind::SendRemote => "send_not_sir",
            ReceiptCostKind::Execution => "exec",
            ReceiptCostKind::TotalLocal => "total_sir",
            ReceiptCostKind::TotalRemote => "total_not_sir",
        }
    }
}

impl EstimatorParameter {
    fn as_str(self) -> &'static str {
        match self {
            EstimatorParameter::IoReadByte => "io_read_byte",
            EstimatorParameter::IoWriteByte => "io_write_byte",
            EstimatorParameter::GasInInstr => "gas_in_instr",
        }
    }
}
