use crate::{
    error::{Error, UserError},
    near_action::{
        self, AccessKey, AccessKeyPermission, AddKeyAction, DeleteKeyAction, FunctionCallAction,
        FunctionCallPermission, TransferAction,
    },
};
use ethabi::{Address, ParamType};
use near_sdk::{AccountId, Gas, NearToken, PublicKey};
use std::num::NonZeroU128;
use std::sync::LazyLock;

pub const FUNCTION_CALL_SELECTOR: &[u8] = &[0x61, 0x79, 0xb7, 0x07];
pub const FUNCTION_CALL_SIGNATURE: [ParamType; 5] = [
    ParamType::String,   // receiver_id
    ParamType::String,   // method_name
    ParamType::Bytes,    // args
    ParamType::Uint(64), // gas
    ParamType::Uint(32), // yocto_near
];

pub const TRANSFER_SELECTOR: &[u8] = &[0x3e, 0xd6, 0x41, 0x24];
pub const TRANSFER_SIGNATURE: [ParamType; 2] = [
    ParamType::String,   // receiver_id
    ParamType::Uint(32), // yocto_near
];

pub const ADD_KEY_SELECTOR: &[u8] = &[0x75, 0x3c, 0xe5, 0xab];
// This one needs to be `LazyLock` because it requires `Box` (non-const) in the `Array`.
pub static ADD_KEY_SIGNATURE: LazyLock<[ParamType; 8]> = LazyLock::new(|| {
    [
        ParamType::Uint(8),                            // public_key_kind
        ParamType::Bytes,                              // public_key
        ParamType::Uint(64),                           // nonce
        ParamType::Bool,                               // is_full_access
        ParamType::Bool,                               // is_limited_allowance
        ParamType::Uint(128),                          // allowance
        ParamType::String,                             // receiver_id
        ParamType::Array(Box::new(ParamType::String)), // method_names
    ]
});

pub const DELETE_KEY_SELECTOR: &[u8] = &[0x3f, 0xc6, 0xd4, 0x04];
pub const DELETE_KEY_SIGNATURE: [ParamType; 2] = [
    ParamType::Uint(8), // public_key_kind
    ParamType::Bytes,   // public_key
];

/// Response given from the `rlp_execute` entry point to the contract.
/// The error information is needed because that method is not meant to panic,
/// therefore success/failure must be communicated via the return value.
/// The reason that method should never panic is to ensure the contract's state
/// can be changed even in error cases. For example, banning a dishonest relayer.
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecuteResponse {
    pub success: bool,
    pub success_value: Option<Vec<u8>>,
    pub error: Option<String>,
}

impl From<Error> for ExecuteResponse {
    fn from(value: Error) -> Self {
        Self { success: false, success_value: None, error: Some(format!("{value}")) }
    }
}

/// Struct holding environment parameters that are needed to validate transactions
/// before executing them. This struct is used in the `internal` module so that it
/// can be unit tested without mocking up the whole Near runtime. In the Wasm contract,
/// the struct is constructed via functions in `near_sdk::env`.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ExecutionContext {
    pub current_address: Address,
    pub attached_deposit: NearToken,
    pub predecessor_account_id: AccountId,
    pub current_account_id: AccountId,
}

impl ExecutionContext {
    pub fn new(
        current_account_id: AccountId,
        predecessor_account_id: AccountId,
        attached_deposit: NearToken,
    ) -> Result<Self, Error> {
        let current_address = crate::internal::extract_address(&current_account_id)?;
        Ok(Self { current_address, attached_deposit, predecessor_account_id, current_account_id })
    }

    /// In production eth-implicit accounts are top-level, so this suffix will
    /// always be empty. The purpose of finding a suffix is that it allows for
    /// testing environments where the wallet contract is deployed to an address
    /// that is a sub-account. For example, this allows testing on Near testnet
    /// before the eth-implicit accounts feature is stabilized.
    /// The suffix is only needed in testing.
    pub fn current_account_suffix(&self) -> &str {
        self.current_account_id
            .as_str()
            .find('.')
            .map(|index| &self.current_account_id.as_str()[index..])
            .unwrap_or("")
    }
}

/// The `target` of the transaction (set by the relayer)
/// is one of the following: the current account, another eth-implicit account
/// (i.e. another wallet contract) or some other Near account. This distinction
/// is important because the only kind of transaction that can be sent to another
/// eth-implicit account is a base token transfer (EOAs are not contracts on Ethereum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum TargetKind<'a> {
    CurrentAccount,
    EthImplicit(Address),
    OtherNearAccount(&'a AccountId),
}

/// A transaction can either contain an ABI-encoded Near action
/// or it can be a normal Ethereum transaction who's behaviour
/// we are trying to emulate.
#[must_use]
pub enum TransactionKind {
    NearNativeAction,
    EthEmulation(EthEmulationKind),
}

#[must_use]
pub enum EthEmulationKind {
    EOABaseTokenTransfer { address_check: Option<Address>, fee: NearToken },
    SelfBaseTokenTransfer,
    ERC20Balance,
    ERC20Transfer { receiver_id: AccountId, fee: NearToken },
    ERC20TotalSupply,
}

/// Describes a kind of transaction that is directly parsable
/// from an Ethereum-formatted transaction's calldata. Notably
/// `EthEmulationKind::EOABaseTokenTransfer` is missing because
/// on Ethereum base token transfers are inferred from the target
/// of the transaction, not its data.
#[must_use]
pub enum ParsableTransactionKind {
    /// Near native actions with an explicit receiver
    /// (i.e. `FunctionCall` and `Transfer`).
    NearNativeAction,
    /// Near native actions where the receiver should be equal
    /// to the current account (i.e. `AddKey` and `DeleteKey`).
    SelfNearNativeAction,
    /// Emulated Ethereum standards
    EthEmulation(ParsableEthEmulationKind),
}

/// See docs for `ParsableTransactionKind`.
#[must_use]
pub enum ParsableEthEmulationKind {
    ERC20Balance,
    ERC20Transfer { receiver_id: AccountId, fee: NearToken },
    ERC20TotalSupply,
}

impl From<ParsableEthEmulationKind> for EthEmulationKind {
    fn from(value: ParsableEthEmulationKind) -> Self {
        match value {
            ParsableEthEmulationKind::ERC20Balance => Self::ERC20Balance,
            ParsableEthEmulationKind::ERC20Transfer { receiver_id, fee } => {
                Self::ERC20Transfer { receiver_id, fee }
            }
            ParsableEthEmulationKind::ERC20TotalSupply => Self::ERC20TotalSupply,
        }
    }
}

/// A data type to keep track of the deposit given by an external caller.
/// This allows us to refund the caller's deposit if the cross-contract call fails.
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub struct CallerDeposit {
    pub account_id: AccountId,
    pub yocto_near: NonZeroU128,
}

impl CallerDeposit {
    pub fn new(context: &ExecutionContext) -> Option<Self> {
        // Only track for external (non-self) callers
        if context.current_account_id == context.predecessor_account_id {
            return None;
        }

        NonZeroU128::new(context.attached_deposit.as_yoctonear()).map(|yocto_near| Self {
            account_id: context.predecessor_account_id.clone(),
            yocto_near,
        })
    }
}

/// The Near protocol actions represented in a form that is suitable for the
/// Solidity ABI. This allows them to be encoded into the `data` field of an
/// Ethereum transaction in a way that can be parsed by Ethereum tooling.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Action {
    FunctionCall {
        receiver_id: String,
        method_name: String,
        args: Vec<u8>,
        gas: u64,
        yocto_near: u32,
    },
    Transfer {
        receiver_id: String,
        yocto_near: u32,
    },
    AddKey {
        public_key_kind: u8,
        public_key: Vec<u8>,
        nonce: u64,
        is_full_access: bool,
        is_limited_allowance: bool,
        allowance: u128,
        receiver_id: String,
        method_names: Vec<String>,
    },
    DeleteKey {
        public_key_kind: u8,
        public_key: Vec<u8>,
    },
}

impl Action {
    pub fn value(&self) -> NearToken {
        match self {
            Action::FunctionCall { yocto_near, .. } => {
                NearToken::from_yoctonear((*yocto_near).into())
            }
            Action::Transfer { yocto_near, .. } => NearToken::from_yoctonear((*yocto_near).into()),
            Action::AddKey { .. } => NearToken::from_yoctonear(0),
            Action::DeleteKey { .. } => NearToken::from_yoctonear(0),
        }
    }

    pub fn try_into_near_action(
        self,
        additional_value: u128,
    ) -> Result<near_action::Action, Error> {
        let action = match self {
            Action::FunctionCall { receiver_id: _, method_name, args, gas, yocto_near } => {
                let action = FunctionCallAction {
                    method_name,
                    args,
                    gas: Gas::from_gas(gas),
                    deposit: NearToken::from_yoctonear(
                        additional_value.saturating_add(yocto_near.into()),
                    ),
                };
                near_action::Action::FunctionCall(action)
            }
            Action::Transfer { receiver_id: _, yocto_near } => {
                let action = TransferAction {
                    deposit: NearToken::from_yoctonear(
                        additional_value.saturating_add(yocto_near.into()),
                    ),
                };
                near_action::Action::Transfer(action)
            }
            Action::AddKey {
                public_key_kind,
                public_key,
                nonce,
                is_full_access,
                is_limited_allowance,
                allowance,
                receiver_id,
                method_names,
            } => {
                let public_key = construct_public_key(public_key_kind, &public_key)?;
                let access_key = if is_full_access {
                    AccessKey { nonce, permission: AccessKeyPermission::FullAccess }
                } else {
                    let allowance = if is_limited_allowance { Some(allowance) } else { None };
                    AccessKey {
                        nonce,
                        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                            allowance: allowance.map(NearToken::from_yoctonear),
                            receiver_id: receiver_id
                                .parse()
                                .map_err(|_| Error::User(UserError::InvalidAccessKeyAccountId))?,
                            method_names,
                        }),
                    }
                };
                let action = AddKeyAction { public_key, access_key };
                near_action::Action::AddKey(action)
            }
            Action::DeleteKey { public_key_kind, public_key } => {
                let action = DeleteKeyAction {
                    public_key: construct_public_key(public_key_kind, &public_key)?,
                };
                near_action::Action::DeleteKey(action)
            }
        };
        Ok(action)
    }
}

fn construct_public_key(public_key_kind: u8, public_key: &[u8]) -> Result<PublicKey, Error> {
    if public_key_kind > 1 {
        return Err(Error::User(UserError::UnknownPublicKeyKind));
    }
    let mut bytes = Vec::with_capacity(public_key.len() + 1);
    bytes.push(public_key_kind);
    bytes.extend_from_slice(public_key);
    bytes.try_into().map_err(|_| {
        if public_key_kind == 0 {
            Error::User(UserError::InvalidEd25519Key)
        } else {
            Error::User(UserError::InvalidSecp256k1Key)
        }
    })
}

#[test]
fn test_function_selectors() {
    let function_call_signature = ethabi::short_signature("functionCall", &FUNCTION_CALL_SIGNATURE);

    let transfer_signature = ethabi::short_signature("transfer", &TRANSFER_SIGNATURE);

    let add_key_signature = ethabi::short_signature("addKey", ADD_KEY_SIGNATURE.as_ref());

    let delete_key = ethabi::short_signature("deleteKey", &DELETE_KEY_SIGNATURE);

    assert_eq!(function_call_signature, FUNCTION_CALL_SELECTOR); // 0x6179b707
    assert_eq!(transfer_signature, TRANSFER_SELECTOR); // 0x3ed64124
    assert_eq!(add_key_signature, ADD_KEY_SELECTOR); // 0x753ce5ab
    assert_eq!(delete_key, DELETE_KEY_SELECTOR); // 0x3fc6d404
}
