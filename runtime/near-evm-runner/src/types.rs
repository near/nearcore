use std::convert::TryInto;

use borsh::{BorshDeserialize, BorshSerialize};
use ethereum_types::{Address, U256};

use near_vm_errors::{EvmError, InconsistentStateError, VMLogicError};
use near_vm_logic::types::AccountId;

pub type RawAddress = [u8; 20];
pub type RawHash = [u8; 32];
pub type RawU256 = [u8; 32];
pub type DataKey = [u8; 52];

pub type Result<T> = std::result::Result<T, VMLogicError>;

#[derive(Debug, Eq, PartialEq)]
pub enum Method {
    DeployCode,
    Call,
    MetaCall,
    Deposit,
    Withdraw,
    Transfer,
    // View methods.
    ViewCall,
    GetCode,
    GetStorageAt,
    GetNonce,
    GetBalance,
}

impl Method {
    pub fn parse(method_name: &str) -> Option<Self> {
        Some(match method_name {
            // Change the state methods.
            "deploy_code" => Self::DeployCode,
            "call_function" | "call" => Self::Call,
            // TODO: Meta calls are temporary disabled.
            // "meta_call" => Self::MetaCall,
            "deposit" => Self::Deposit,
            "withdraw" => Self::Withdraw,
            "transfer" => Self::Transfer,
            // View methods.
            "view_function_call" | "view" => Self::ViewCall,
            "get_code" => Self::GetCode,
            "get_storage_at" => Self::GetStorageAt,
            "get_nonce" => Self::GetNonce,
            "get_balance" => Self::GetBalance,
            _ => return None,
        })
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct AddressArg {
    pub address: RawAddress,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct GetStorageAtArgs {
    pub address: RawAddress,
    pub key: RawHash,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct WithdrawArgs {
    pub account_id: AccountId,
    pub amount: RawU256,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct TransferArgs {
    pub address: RawAddress,
    pub amount: RawU256,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct FunctionCallArgs {
    pub contract: RawAddress,
    pub input: Vec<u8>,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Eq, PartialEq)]
pub struct ViewCallArgs {
    pub sender: RawAddress,
    pub address: RawAddress,
    pub amount: RawU256,
    pub input: Vec<u8>,
}

pub struct MetaCallArgs {
    pub sender: Address,
    pub nonce: U256,
    pub fee_amount: U256,
    pub fee_address: Address,
    pub contract_address: Address,
    pub input: Vec<u8>,
}

pub fn convert_vm_error(err: vm::Error) -> VMLogicError {
    match err {
        vm::Error::OutOfGas => VMLogicError::EvmError(EvmError::OutOfGas),
        vm::Error::BadJumpDestination { destination } => {
            VMLogicError::EvmError(EvmError::BadJumpDestination {
                destination: destination.try_into().unwrap_or(0),
            })
        }
        vm::Error::BadInstruction { instruction } => {
            VMLogicError::EvmError(EvmError::BadInstruction { instruction })
        }
        vm::Error::StackUnderflow { instruction, wanted, on_stack } => {
            VMLogicError::EvmError(EvmError::StackUnderflow {
                instruction: instruction.to_string(),
                wanted: wanted.try_into().unwrap_or(0),
                on_stack: on_stack.try_into().unwrap_or(0),
            })
        }
        vm::Error::OutOfStack { instruction, wanted, limit } => {
            VMLogicError::EvmError(EvmError::OutOfStack {
                instruction: instruction.to_string(),
                wanted: wanted.try_into().unwrap_or(0),
                limit: limit.try_into().unwrap_or(0),
            })
        }
        vm::Error::BuiltIn(msg) => VMLogicError::EvmError(EvmError::BuiltIn(msg.to_string())),
        vm::Error::MutableCallInStaticContext => VMLogicError::EvmError(EvmError::OutOfBounds),
        vm::Error::Internal(err) => {
            VMLogicError::InconsistentStateError(InconsistentStateError::StorageError(err))
        }
        // This should not happen ever, because NEAR EVM is not using WASM.
        vm::Error::Wasm(_) => unreachable!(),
        vm::Error::OutOfBounds => VMLogicError::EvmError(EvmError::OutOfBounds),
        vm::Error::Reverted => VMLogicError::EvmError(EvmError::Reverted),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_view_call() {
        let x = ViewCallArgs {
            sender: [1; 20],
            address: [2; 20],
            amount: [3; 32],
            input: vec![1, 2, 3],
        };
        let bytes = x.try_to_vec().unwrap();
        let res = ViewCallArgs::try_from_slice(&bytes).unwrap();
        assert_eq!(x, res);
    }

    #[test]
    fn test_view_call_fail() {
        let bytes = [0; 71];
        let _ = ViewCallArgs::try_from_slice(&bytes).unwrap_err();
    }
}
