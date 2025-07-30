pub mod delegate;

use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
use near_primitives_core::{
    account::AccessKey,
    hash::CryptoHash,
    serialize::dec_format,
    types::{AccountId, Balance, Gas},
};
use near_schema_checker_lib::ProtocolSchema;
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::fmt;
use std::sync::Arc;

use crate::trie_key::GlobalContractCodeIdentifier;

pub fn base64(s: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(s)
}

/// An action that adds key with public key associated
#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct AddKeyAction {
    /// A public key which will be associated with an access_key
    pub public_key: PublicKey,
    /// An access key with the permission
    pub access_key: AccessKey,
}

/// Create account action
#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct CreateAccountAction {}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DeleteAccountAction {
    pub beneficiary_id: AccountId,
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DeleteKeyAction {
    /// A public key associated with the access_key to be deleted.
    pub public_key: PublicKey,
}

/// Deploy contract action
#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    Clone,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DeployContractAction {
    /// WebAssembly binary
    #[serde_as(as = "Base64")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub code: Vec<u8>,
}

impl fmt::Debug for DeployContractAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeployContractAction")
            .field("code", &format_args!("{}", base64(&self.code)))
            .finish()
    }
}

#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    Clone,
    ProtocolSchema,
    Debug,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[repr(u8)]
pub enum GlobalContractDeployMode {
    /// Contract is deployed under its code hash.
    /// Users will be able reference it by that hash.
    /// This effectively makes the contract immutable.
    CodeHash,
    /// Contract is deployed under the owner account id.
    /// Users will be able reference it by that account id.
    /// This allows the owner to update the contract for all its users.
    AccountId,
}

/// Deploy global contract action
#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    Clone,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DeployGlobalContractAction {
    /// WebAssembly binary
    #[serde_as(as = "Base64")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub code: Arc<[u8]>,

    pub deploy_mode: GlobalContractDeployMode,
}

impl fmt::Debug for DeployGlobalContractAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeployGlobalContractAction")
            .field("code", &format_args!("{}", base64(&self.code)))
            .field("deploy_mode", &format_args!("{:?}", &self.deploy_mode))
            .finish()
    }
}

#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Hash,
    PartialEq,
    Eq,
    Clone,
    ProtocolSchema,
    Debug,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum GlobalContractIdentifier {
    CodeHash(CryptoHash) = 0,
    AccountId(AccountId) = 1,
}

impl From<GlobalContractCodeIdentifier> for GlobalContractIdentifier {
    fn from(identifier: GlobalContractCodeIdentifier) -> Self {
        match identifier {
            GlobalContractCodeIdentifier::CodeHash(hash) => {
                GlobalContractIdentifier::CodeHash(hash)
            }
            GlobalContractCodeIdentifier::AccountId(account_id) => {
                GlobalContractIdentifier::AccountId(account_id)
            }
        }
    }
}

impl GlobalContractIdentifier {
    pub fn len(&self) -> usize {
        match self {
            GlobalContractIdentifier::CodeHash(_) => 32,
            GlobalContractIdentifier::AccountId(account_id) => account_id.len(),
        }
    }
}

/// Use global contract action
#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    Clone,
    ProtocolSchema,
    Debug,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct UseGlobalContractAction {
    pub contract_identifier: GlobalContractIdentifier,
}

#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    Clone,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FunctionCallAction {
    pub method_name: String,
    #[serde_as(as = "Base64")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub args: Vec<u8>,
    pub gas: Gas,
    #[serde(with = "dec_format")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub deposit: Balance,
}

impl fmt::Debug for FunctionCallAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FunctionCallAction")
            .field("method_name", &format_args!("{}", &self.method_name))
            .field("args", &format_args!("{}", base64(&self.args)))
            .field("gas", &format_args!("{}", &self.gas))
            .field("deposit", &format_args!("{}", &self.deposit))
            .finish()
    }
}

/// An action which stakes signer_id tokens and setup's validator public key
#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct StakeAction {
    /// Amount of tokens to stake.
    #[serde(with = "dec_format")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub stake: Balance,
    /// Validator key which will be used to sign transactions on behalf of signer_id
    pub public_key: PublicKey,
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct TransferAction {
    #[serde(with = "dec_format")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub deposit: Balance,
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    strum::AsRefStr,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum Action {
    /// Create an (sub)account using a transaction `receiver_id` as an ID for
    /// a new account ID must pass validation rules described here
    /// <http://nomicon.io/Primitives/Account.html>.
    CreateAccount(CreateAccountAction) = 0,
    /// Sets a Wasm code to a receiver_id
    DeployContract(DeployContractAction) = 1,
    FunctionCall(Box<FunctionCallAction>) = 2,
    Transfer(TransferAction) = 3,
    Stake(Box<StakeAction>) = 4,
    AddKey(Box<AddKeyAction>) = 5,
    DeleteKey(Box<DeleteKeyAction>) = 6,
    DeleteAccount(DeleteAccountAction) = 7,
    Delegate(Box<delegate::SignedDelegateAction>) = 8,
    DeployGlobalContract(DeployGlobalContractAction) = 9,
    UseGlobalContract(Box<UseGlobalContractAction>) = 10,
}

const _: () = assert!(
    // 1 word for tag plus the largest variant `DeployContractAction` which is a 3-word `Vec`.
    // The `<=` check covers platforms that have pointers smaller than 8 bytes as well as random
    // freak night lies that somehow find a way to pack everything into one less word.
    std::mem::size_of::<Action>() <= 32,
    "Action <= 32 bytes for performance reasons, see #9451"
);

impl Action {
    pub fn get_prepaid_gas(&self) -> Gas {
        match self {
            Action::FunctionCall(a) => a.gas,
            _ => 0,
        }
    }
    pub fn get_deposit_balance(&self) -> Balance {
        match self {
            Action::FunctionCall(a) => a.deposit,
            Action::Transfer(a) => a.deposit,
            _ => 0,
        }
    }
}

impl From<CreateAccountAction> for Action {
    fn from(create_account_action: CreateAccountAction) -> Self {
        Self::CreateAccount(create_account_action)
    }
}

impl From<DeployContractAction> for Action {
    fn from(deploy_contract_action: DeployContractAction) -> Self {
        Self::DeployContract(deploy_contract_action)
    }
}

impl From<DeployGlobalContractAction> for Action {
    fn from(deploy_global_contract_action: DeployGlobalContractAction) -> Self {
        Self::DeployGlobalContract(deploy_global_contract_action)
    }
}

impl From<FunctionCallAction> for Action {
    fn from(function_call_action: FunctionCallAction) -> Self {
        Self::FunctionCall(Box::new(function_call_action))
    }
}

impl From<TransferAction> for Action {
    fn from(transfer_action: TransferAction) -> Self {
        Self::Transfer(transfer_action)
    }
}

impl From<StakeAction> for Action {
    fn from(stake_action: StakeAction) -> Self {
        Self::Stake(Box::new(stake_action))
    }
}

impl From<AddKeyAction> for Action {
    fn from(add_key_action: AddKeyAction) -> Self {
        Self::AddKey(Box::new(add_key_action))
    }
}

impl From<DeleteKeyAction> for Action {
    fn from(delete_key_action: DeleteKeyAction) -> Self {
        Self::DeleteKey(Box::new(delete_key_action))
    }
}

impl From<DeleteAccountAction> for Action {
    fn from(delete_account_action: DeleteAccountAction) -> Self {
        Self::DeleteAccount(delete_account_action)
    }
}
