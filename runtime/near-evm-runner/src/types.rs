use std::convert::TryInto;

use borsh::{BorshDeserialize, BorshSerialize};
use ethereum_types::{Address, U256};
use keccak_hash::keccak;
use rlp::{Decodable, DecoderError, Encodable, Rlp, RlpStream};

use near_vm_errors::{EvmError, InconsistentStateError, VMLogicError};
use near_vm_logic::types::AccountId;

use crate::utils::ecrecover_address;

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
    RawCall,
}

impl Method {
    pub fn parse(method_name: &str) -> Option<Self> {
        Some(match method_name {
            // Change the state methods.
            "deploy_code" => Self::DeployCode,
            "call_function" | "call" => Self::Call,
            "meta_call" => Self::MetaCall,
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

#[derive(Debug, Eq, PartialEq)]
pub struct EthTransaction {
    pub nonce: U256,
    pub gas_price: U256,
    pub gas: U256,
    pub to: Option<Address>,
    pub value: U256,
    pub data: Vec<u8>,
}

impl EthTransaction {
    pub fn rlp_append_unsigned(&self, s: &mut RlpStream, chain_id: Option<u64>) {
        s.begin_list(if chain_id.is_none() { 6 } else { 9 });
        s.append(&self.nonce);
        s.append(&self.gas_price);
        s.append(&self.gas);
        match self.to.as_ref() {
            None => s.append(&""),
            Some(address) => s.append(address),
        };
        s.append(&self.value);
        s.append(&self.data);
        if let Some(chain_id) = chain_id {
            s.append(&chain_id);
            s.append(&0u8);
            s.append(&0u8);
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct EthSignedTransaction {
    pub transaction: EthTransaction,
    pub v: u64,
    pub r: U256,
    pub s: U256,
}

fn vrs_to_arr(v: u8, r: U256, s: U256) -> [u8; 65] {
    let mut result = [0u8; 65];
    result[0] = v;
    r.to_big_endian(&mut result[1..33]);
    s.to_big_endian(&mut result[33..65]);
    result
}

impl EthSignedTransaction {
    /// Returns sender of given signed transaction by doing ecrecover on the signature.
    pub fn sender(&self) -> Address {
        let mut rlp_stream = RlpStream::new();
        // See details of CHAIN_ID computation here - https://github.com/ethereum/EIPs/blob/master/EIPS/eip-155.md#specification
        let chain_id =
            if [27u64, 28u64].contains(&self.v) { None } else { Some((self.v - 35) / 2) };
        let rec_id = ((self.v - 35) % 2) as u8;
        self.transaction.rlp_append_unsigned(&mut rlp_stream, chain_id);
        let message_hash = keccak(rlp_stream.as_raw());
        ecrecover_address(&message_hash.into(), &vrs_to_arr(rec_id, self.r, self.s))
    }
}

impl Encodable for EthSignedTransaction {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list(9);
        s.append(&self.transaction.nonce);
        s.append(&self.transaction.gas_price);
        s.append(&self.transaction.gas);
        match self.transaction.to.as_ref() {
            None => s.append(&""),
            Some(address) => s.append(address),
        };
        s.append(&self.transaction.value);
        s.append(&self.transaction.data);
        s.append(&self.v);
        s.append(&self.r);
        s.append(&self.s);
    }
}

impl Decodable for EthSignedTransaction {
    fn decode(rlp: &Rlp<'_>) -> std::result::Result<Self, DecoderError> {
        if rlp.item_count() != Ok(9) {
            return Err(rlp::DecoderError::RlpIncorrectListLen);
        }
        let nonce = rlp.val_at(0)?;
        let gas_price = rlp.val_at(1)?;
        let gas = rlp.val_at(2)?;
        let to = {
            let value = rlp.at(3)?;
            if value.is_empty() {
                if value.is_data() {
                    None
                } else {
                    return Err(rlp::DecoderError::RlpExpectedToBeData);
                }
            } else {
                Some(value.as_val()?)
            }
        };
        let value = rlp.val_at(4)?;
        let data = rlp.val_at(5)?;
        let v = rlp.val_at(6)?;
        let r = rlp.val_at(7)?;
        let s = rlp.val_at(8)?;
        Ok(Self { transaction: EthTransaction { nonce, gas, gas_price, to, value, data }, v, r, s })
    }
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
    use crate::utils::address_from_arr;

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

    #[test]
    fn test_decode_eth_signed_transaction() {
        let encoded_tx = hex::decode("f86a8086d55698372431831e848094f0109fc8df283027b6285cc889f5aa624eac1f55843b9aca008025a009ebb6ca057a0535d6186462bc0b465b561c94a295bdb0621fc19208ab149a9ca0440ffd775ce91a833ab410777204d5341a6f9fa91216a6f3ee2c051fea6a0428").unwrap();
        let tx = EthSignedTransaction::decode(&Rlp::new(&encoded_tx)).unwrap();
        assert_eq!(tx.v, 37);
        assert_eq!(
            tx.transaction,
            EthTransaction {
                nonce: U256::zero(),
                gas_price: U256::from(234567897654321u128),
                gas: U256::from(2000000u128),
                to: Some(address_from_arr(
                    &hex::decode("F0109fC8DF283027b6285cc889F5aA624EaC1F55").unwrap()
                )),
                value: U256::from(1000000000),
                data: vec![],
            }
        );
        assert_eq!(
            tx.sender(),
            address_from_arr(&hex::decode("2c7536e3605d9c16a7a3d7b1898e529396a65c23").unwrap())
        );
    }
}
