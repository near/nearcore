use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;

use protobuf::{RepeatedField, SingularPtrField};

use near_protos::signed_transaction as transaction_proto;
use near_protos::Message as ProtoMessage;

use crate::account::AccessKey;
use crate::crypto::signature::{verify, PublicKey, Signature};
use crate::crypto::signer::EDSigner;
use crate::hash::{hash, CryptoHash};
use crate::logging;
use crate::serialize::{base_bytes_format, u128_dec_format};
use crate::types::{AccountId, Balance, Gas, Nonce, ShardId, StructSignature};
use crate::utils::{proto_to_result, proto_to_type, proto_to_vec};
use std::sync::Arc;

pub type LogEntry = String;

#[derive(Hash, PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub signer_id: AccountId,
    pub public_key: PublicKey,
    pub nonce: Nonce,
    pub receiver_id: AccountId,

    pub actions: Vec<Action>,
}

impl Transaction {
    pub fn get_hash(&self) -> CryptoHash {
        let proto: transaction_proto::Transaction = self.clone().into();
        let bytes = proto.write_to_bytes().unwrap();
        hash(&bytes)
    }
}

impl TryFrom<transaction_proto::Transaction> for Transaction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(t: transaction_proto::Transaction) -> Result<Self, Self::Error> {
        Ok(Transaction {
            signer_id: t.signer_id,
            public_key: proto_to_type(t.public_key)?,
            nonce: t.nonce,
            receiver_id: t.receiver_id,
            actions: proto_to_vec(t.actions)?,
        })
    }
}

impl From<Transaction> for transaction_proto::Transaction {
    fn from(t: Transaction) -> transaction_proto::Transaction {
        transaction_proto::Transaction {
            signer_id: t.signer_id,
            public_key: SingularPtrField::some(t.public_key.into()),
            nonce: t.nonce,
            receiver_id: t.receiver_id,
            actions: RepeatedField::from_iter(t.actions.into_iter().map(std::convert::Into::into)),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

#[derive(Hash, PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    CreateAccount(CreateAccountAction),
    DeployContract(DeployContractAction),
    FunctionCall(FunctionCallAction),
    Transfer(TransferAction),
    Stake(StakeAction),
    AddKey(AddKeyAction),
    DeleteKey(DeleteKeyAction),
    DeleteAccount(DeleteAccountAction),
}

impl TryFrom<transaction_proto::Action> for Action {
    type Error = Box<dyn std::error::Error>;

    fn try_from(a: transaction_proto::Action) -> Result<Self, Self::Error> {
        match a.action {
            Some(transaction_proto::Action_oneof_action::create_account(t)) => {
                Ok(Action::CreateAccount(CreateAccountAction::try_from(t)?))
            }
            Some(transaction_proto::Action_oneof_action::deploy_contract(t)) => {
                Ok(Action::DeployContract(DeployContractAction::try_from(t)?))
            }
            Some(transaction_proto::Action_oneof_action::function_call(t)) => {
                Ok(Action::FunctionCall(FunctionCallAction::try_from(t)?))
            }
            Some(transaction_proto::Action_oneof_action::transfer(t)) => {
                Ok(Action::Transfer(TransferAction::try_from(t)?))
            }
            Some(transaction_proto::Action_oneof_action::stake(t)) => {
                Ok(Action::Stake(StakeAction::try_from(t)?))
            }
            Some(transaction_proto::Action_oneof_action::add_key(t)) => {
                Ok(Action::AddKey(AddKeyAction::try_from(t)?))
            }
            Some(transaction_proto::Action_oneof_action::delete_key(t)) => {
                Ok(Action::DeleteKey(DeleteKeyAction::try_from(t)?))
            }
            Some(transaction_proto::Action_oneof_action::delete_account(t)) => {
                Ok(Action::DeleteAccount(DeleteAccountAction::try_from(t)?))
            }
            None => Err("No such action".into()),
        }
    }
}

impl From<Action> for transaction_proto::Action {
    fn from(a: Action) -> transaction_proto::Action {
        let action = match a {
            Action::CreateAccount(a) => {
                transaction_proto::Action_oneof_action::create_account(a.into())
            }
            Action::DeployContract(a) => {
                transaction_proto::Action_oneof_action::deploy_contract(a.into())
            }
            Action::FunctionCall(a) => {
                transaction_proto::Action_oneof_action::function_call(a.into())
            }
            Action::Transfer(a) => transaction_proto::Action_oneof_action::transfer(a.into()),
            Action::Stake(a) => transaction_proto::Action_oneof_action::stake(a.into()),
            Action::AddKey(a) => transaction_proto::Action_oneof_action::add_key(a.into()),
            Action::DeleteKey(a) => transaction_proto::Action_oneof_action::delete_key(a.into()),
            Action::DeleteAccount(a) => {
                transaction_proto::Action_oneof_action::delete_account(a.into())
            }
        };

        transaction_proto::Action {
            action: Some(action),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

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

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct CreateAccountAction {}

impl TryFrom<transaction_proto::Action_CreateAccount> for CreateAccountAction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(_: transaction_proto::Action_CreateAccount) -> Result<Self, Self::Error> {
        Ok(CreateAccountAction {})
    }
}

impl From<CreateAccountAction> for transaction_proto::Action_CreateAccount {
    fn from(_: CreateAccountAction) -> transaction_proto::Action_CreateAccount {
        transaction_proto::Action_CreateAccount {
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DeployContractAction {
    #[serde(with = "base_bytes_format")]
    pub code: Vec<u8>,
}

impl TryFrom<transaction_proto::Action_DeployContract> for DeployContractAction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(a: transaction_proto::Action_DeployContract) -> Result<Self, Self::Error> {
        Ok(DeployContractAction { code: a.code })
    }
}

impl From<DeployContractAction> for transaction_proto::Action_DeployContract {
    fn from(a: DeployContractAction) -> transaction_proto::Action_DeployContract {
        transaction_proto::Action_DeployContract {
            code: a.code,

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

impl fmt::Debug for DeployContractAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DeployContractAction")
            .field("code", &format_args!("{}", logging::pretty_utf8(&self.code)))
            .finish()
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FunctionCallAction {
    pub method_name: String,
    #[serde(with = "base_bytes_format")]
    pub args: Vec<u8>,
    pub gas: Gas,
    #[serde(with = "u128_dec_format")]
    pub deposit: Balance,
}

impl TryFrom<transaction_proto::Action_FunctionCall> for FunctionCallAction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(a: transaction_proto::Action_FunctionCall) -> Result<Self, Self::Error> {
        Ok(FunctionCallAction {
            method_name: a.method_name,
            args: a.args,
            gas: a.gas,
            deposit: proto_to_type(a.deposit)?,
        })
    }
}

impl From<FunctionCallAction> for transaction_proto::Action_FunctionCall {
    fn from(a: FunctionCallAction) -> transaction_proto::Action_FunctionCall {
        transaction_proto::Action_FunctionCall {
            method_name: a.method_name,
            args: a.args,
            gas: a.gas,
            deposit: SingularPtrField::some(a.deposit.into()),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

impl fmt::Debug for FunctionCallAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FunctionCallAction")
            .field("method_name", &format_args!("{}", &self.method_name))
            .field("args", &format_args!("{}", logging::pretty_utf8(&self.args)))
            .field("gas", &format_args!("{}", &self.gas))
            .field("deposit", &format_args!("{}", &self.deposit))
            .finish()
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct TransferAction {
    #[serde(with = "u128_dec_format")]
    pub deposit: Balance,
}

impl TryFrom<transaction_proto::Action_Transfer> for TransferAction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(a: transaction_proto::Action_Transfer) -> Result<Self, Self::Error> {
        Ok(TransferAction { deposit: proto_to_type(a.deposit)? })
    }
}

impl From<TransferAction> for transaction_proto::Action_Transfer {
    fn from(a: TransferAction) -> transaction_proto::Action_Transfer {
        transaction_proto::Action_Transfer {
            deposit: SingularPtrField::some(a.deposit.into()),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct StakeAction {
    #[serde(with = "u128_dec_format")]
    pub stake: Balance,
    pub public_key: PublicKey,
}

impl TryFrom<transaction_proto::Action_Stake> for StakeAction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(a: transaction_proto::Action_Stake) -> Result<Self, Self::Error> {
        Ok(StakeAction { stake: proto_to_type(a.stake)?, public_key: proto_to_type(a.public_key)? })
    }
}

impl From<StakeAction> for transaction_proto::Action_Stake {
    fn from(a: StakeAction) -> transaction_proto::Action_Stake {
        transaction_proto::Action_Stake {
            stake: SingularPtrField::some(a.stake.into()),
            public_key: SingularPtrField::some(a.public_key.into()),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct AddKeyAction {
    pub public_key: PublicKey,
    pub access_key: AccessKey,
}

impl TryFrom<transaction_proto::Action_AddKey> for AddKeyAction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(a: transaction_proto::Action_AddKey) -> Result<Self, Self::Error> {
        Ok(AddKeyAction {
            public_key: proto_to_type(a.public_key)?,
            access_key: proto_to_type(a.access_key)?,
        })
    }
}

impl From<AddKeyAction> for transaction_proto::Action_AddKey {
    fn from(a: AddKeyAction) -> transaction_proto::Action_AddKey {
        transaction_proto::Action_AddKey {
            public_key: SingularPtrField::some(a.public_key.into()),
            access_key: SingularPtrField::some(a.access_key.into()),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct DeleteKeyAction {
    pub public_key: PublicKey,
}

impl TryFrom<transaction_proto::Action_DeleteKey> for DeleteKeyAction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(a: transaction_proto::Action_DeleteKey) -> Result<Self, Self::Error> {
        Ok(DeleteKeyAction { public_key: proto_to_type(a.public_key)? })
    }
}

impl From<DeleteKeyAction> for transaction_proto::Action_DeleteKey {
    fn from(a: DeleteKeyAction) -> transaction_proto::Action_DeleteKey {
        transaction_proto::Action_DeleteKey {
            public_key: SingularPtrField::some(a.public_key.into()),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct DeleteAccountAction {
    pub beneficiary_id: AccountId,
}

impl TryFrom<transaction_proto::Action_DeleteAccount> for DeleteAccountAction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(a: transaction_proto::Action_DeleteAccount) -> Result<Self, Self::Error> {
        Ok(DeleteAccountAction { beneficiary_id: a.beneficiary_id })
    }
}

impl From<DeleteAccountAction> for transaction_proto::Action_DeleteAccount {
    fn from(a: DeleteAccountAction) -> transaction_proto::Action_DeleteAccount {
        transaction_proto::Action_DeleteAccount {
            beneficiary_id: a.beneficiary_id,

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

#[derive(Eq, Debug, Clone, Serialize, Deserialize)]
pub struct SignedTransaction {
    pub signature: StructSignature,
    pub transaction: Transaction,
    hash: CryptoHash,
}

impl SignedTransaction {
    pub fn new(signature: StructSignature, transaction: Transaction) -> Self {
        let hash = transaction.get_hash();
        Self { signature, transaction, hash }
    }

    pub fn get_hash(&self) -> CryptoHash {
        self.hash
    }

    pub fn from_actions(
        nonce: Nonce,
        signer_id: AccountId,
        receiver_id: AccountId,
        signer: Arc<dyn EDSigner>,
        actions: Vec<Action>,
    ) -> Self {
        Transaction { nonce, signer_id, public_key: signer.public_key(), receiver_id, actions }
            .sign(&*signer)
    }

    pub fn send_money(
        nonce: Nonce,
        signer_id: AccountId,
        receiver_id: AccountId,
        signer: Arc<dyn EDSigner>,
        deposit: Balance,
    ) -> SignedTransaction {
        Self::from_actions(
            nonce,
            signer_id,
            receiver_id,
            signer,
            vec![Action::Transfer(TransferAction { deposit })],
        )
    }
}

impl Hash for SignedTransaction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state)
    }
}

impl PartialEq for SignedTransaction {
    fn eq(&self, other: &SignedTransaction) -> bool {
        self.hash == other.hash && self.signature == other.signature
    }
}

impl TryFrom<transaction_proto::SignedTransaction> for SignedTransaction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(t: transaction_proto::SignedTransaction) -> Result<Self, Self::Error> {
        let signature: Signature =
            Signature::try_from(&t.signature as &[u8]).map_err(|e| format!("{}", e))?;
        let transaction_message = proto_to_result(t.transaction)?;
        let hash = hash(&transaction_message.write_to_bytes()?);
        let transaction = transaction_message.try_into()?;
        Ok(SignedTransaction { signature, transaction, hash })
    }
}

impl From<SignedTransaction> for transaction_proto::SignedTransaction {
    fn from(t: SignedTransaction) -> transaction_proto::SignedTransaction {
        transaction_proto::SignedTransaction {
            signature: t.signature.as_ref().to_vec(),
            transaction: SingularPtrField::some(t.transaction.into()),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

#[derive(Hash, Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum TransactionStatus {
    Unknown,
    Completed,
    Failed,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum FinalTransactionStatus {
    Unknown,
    Started,
    Failed,
    Completed,
}

impl Default for FinalTransactionStatus {
    fn default() -> Self {
        FinalTransactionStatus::Unknown
    }
}

impl FinalTransactionStatus {
    pub fn to_code(&self) -> u64 {
        match self {
            FinalTransactionStatus::Completed => 0,
            FinalTransactionStatus::Failed => 1,
            FinalTransactionStatus::Started => 2,
            FinalTransactionStatus::Unknown => std::u64::MAX,
        }
    }
}

impl Default for TransactionStatus {
    fn default() -> Self {
        TransactionStatus::Unknown
    }
}

#[derive(PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct TransactionResult {
    /// Transaction status.
    pub status: TransactionStatus,
    /// Logs from this transaction.
    pub logs: Vec<LogEntry>,
    /// Receipt ids generated by this transaction.
    pub receipts: Vec<CryptoHash>,
    /// Execution Result
    pub result: Option<Vec<u8>>,
}

impl fmt::Debug for TransactionResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TransactionResult")
            .field("status", &self.status)
            .field("logs", &format_args!("{}", logging::pretty_vec(&self.logs)))
            .field("receipts", &format_args!("{}", logging::pretty_vec(&self.receipts)))
            .field("result", &format_args!("{}", logging::pretty_result(&self.result)))
            .finish()
    }
}

#[derive(PartialEq, Clone, Serialize, Deserialize, Default, Debug)]
pub struct TransactionLog {
    /// Hash of a transaction or a receipt that generated this result.
    pub hash: CryptoHash,
    pub result: TransactionResult,
}

/// Result of transaction and all of subsequent the receipts.
#[derive(PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct FinalTransactionResult {
    /// Status of the whole transaction and it's receipts.
    pub status: FinalTransactionStatus,
    /// Transaction results.
    pub transactions: Vec<TransactionLog>,
}

impl fmt::Debug for FinalTransactionResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FinalTransactionResult")
            .field("status", &self.status)
            .field("transactions", &format_args!("{}", logging::pretty_vec(&self.transactions)))
            .finish()
    }
}

impl FinalTransactionResult {
    pub fn final_log(&self) -> String {
        let mut logs = vec![];
        for transaction in &self.transactions {
            for line in &transaction.result.logs {
                logs.push(line.clone());
            }
        }
        logs.join("\n")
    }

    pub fn last_result(&self) -> Vec<u8> {
        for transaction in self.transactions.iter().rev() {
            if let Some(r) = &transaction.result.result {
                return r.clone();
            }
        }
        vec![]
    }
}

/// Represents address of certain transaction within block
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TransactionAddress {
    /// Block hash
    pub block_hash: CryptoHash,
    /// Transaction index within the block. If it is a receipt,
    /// index is the index in the receipt block.
    pub index: usize,
    /// Only for receipts. The shard that the receipt
    /// block is supposed to go
    pub shard_id: Option<ShardId>,
}

pub fn verify_transaction_signature(
    transaction: &SignedTransaction,
    public_keys: &[PublicKey],
) -> bool {
    let hash = transaction.get_hash();
    let hash = hash.as_ref();
    public_keys.iter().any(|key| verify(&hash, &transaction.signature, &key))
}

#[cfg(test)]
mod tests {
    use crate::crypto::signature::{get_key_pair, sign, DEFAULT_SIGNATURE};

    use super::*;

    #[test]
    fn test_verify_transaction() {
        let (public_key, private_key) = get_key_pair();
        let mut transaction = SignedTransaction::new(
            DEFAULT_SIGNATURE,
            Transaction {
                signer_id: "".to_string(),
                public_key,
                nonce: 0,
                receiver_id: "".to_string(),
                actions: vec![],
            },
        );
        transaction.signature = sign(&transaction.hash.as_ref(), &private_key);
        let (wrong_public_key, _) = get_key_pair();
        let valid_keys = vec![public_key, wrong_public_key];
        assert!(verify_transaction_signature(&transaction, &valid_keys));

        let invalid_keys = vec![wrong_public_key];
        assert!(!verify_transaction_signature(&transaction, &invalid_keys));
    }
}
