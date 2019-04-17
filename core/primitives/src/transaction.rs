use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;

use protobuf::well_known_types::BytesValue;
use protobuf::SingularPtrField;

use near_protos::receipt as receipt_proto;
use near_protos::signed_transaction as transaction_proto;
use near_protos::Message as ProtoMessage;

use crate::crypto::signature::{verify, PublicKey, Signature, DEFAULT_SIGNATURE};
use crate::hash::{hash, CryptoHash};
use crate::logging;
use crate::types::{
    AccountId, AccountingInfo, Balance, CallbackId, Mana, ManaAccounting, Nonce, ShardId,
    StructSignature,
};
use crate::utils::{account_to_shard_id, proto_to_result};
use crate::traits::ToBytes;

pub type LogEntry = String;

#[derive(Hash, PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub enum TransactionBody {
    CreateAccount(CreateAccountTransaction),
    DeployContract(DeployContractTransaction),
    FunctionCall(FunctionCallTransaction),
    SendMoney(SendMoneyTransaction),
    Stake(StakeTransaction),
    SwapKey(SwapKeyTransaction),
    AddKey(AddKeyTransaction),
    DeleteKey(DeleteKeyTransaction),
}

impl TransactionBody {
    pub fn send_money(nonce: Nonce, originator: &str, receiver: &str, amount: u64) -> Self {
        TransactionBody::SendMoney(SendMoneyTransaction {
            nonce,
            originator: originator.to_string(),
            receiver: receiver.to_string(),
            amount,
        })
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct CreateAccountTransaction {
    pub nonce: Nonce,
    pub originator: AccountId,
    pub new_account_id: AccountId,
    pub amount: u64,
    pub public_key: Vec<u8>,
}

impl From<transaction_proto::CreateAccountTransaction> for CreateAccountTransaction {
    fn from(t: transaction_proto::CreateAccountTransaction) -> Self {
        CreateAccountTransaction {
            nonce: t.nonce,
            originator: t.originator,
            new_account_id: t.new_account_id,
            amount: t.amount,
            public_key: t.public_key,
        }
    }
}

impl From<CreateAccountTransaction> for transaction_proto::CreateAccountTransaction {
    fn from(t: CreateAccountTransaction) -> transaction_proto::CreateAccountTransaction {
        transaction_proto::CreateAccountTransaction {
            nonce: t.nonce,
            originator: t.originator,
            new_account_id: t.new_account_id,
            amount: t.amount,
            public_key: t.public_key,
            ..Default::default()
        }
    }
}

impl fmt::Debug for CreateAccountTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CreateAccountTransaction")
            .field("nonce", &format_args!("{}", &self.nonce))
            .field("originator", &format_args!("{}", &self.originator))
            .field("new_account_id", &format_args!("{}", &self.new_account_id))
            .field("amount", &format_args!("{}", &self.amount))
            .field("public_key", &format_args!("{}", logging::pretty_utf8(&self.public_key)))
            .finish()
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DeployContractTransaction {
    pub nonce: Nonce,
    pub contract_id: AccountId,
    pub wasm_byte_array: Vec<u8>,
}

impl fmt::Debug for DeployContractTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DeployContractTransaction")
            .field("nonce", &format_args!("{}", &self.nonce))
            .field("contract_id", &format_args!("{}", &self.contract_id))
            .field(
                "wasm_byte_array",
                &format_args!("{}", logging::pretty_utf8(&self.wasm_byte_array)),
            )
            .finish()
    }
}

impl From<transaction_proto::DeployContractTransaction> for DeployContractTransaction {
    fn from(t: transaction_proto::DeployContractTransaction) -> Self {
        DeployContractTransaction {
            nonce: t.nonce,
            contract_id: t.contract_id,
            wasm_byte_array: t.wasm_byte_array,
        }
    }
}

impl From<DeployContractTransaction> for transaction_proto::DeployContractTransaction {
    fn from(t: DeployContractTransaction) -> transaction_proto::DeployContractTransaction {
        transaction_proto::DeployContractTransaction {
            nonce: t.nonce,
            contract_id: t.contract_id,
            wasm_byte_array: t.wasm_byte_array,
            ..Default::default()
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FunctionCallTransaction {
    pub nonce: Nonce,
    pub originator: AccountId,
    pub contract_id: AccountId,
    pub method_name: Vec<u8>,
    pub args: Vec<u8>,
    pub amount: Balance,
}

impl From<transaction_proto::FunctionCallTransaction> for FunctionCallTransaction {
    fn from(t: transaction_proto::FunctionCallTransaction) -> Self {
        FunctionCallTransaction {
            nonce: t.nonce,
            originator: t.originator,
            contract_id: t.contract_id,
            method_name: t.method_name,
            args: t.args,
            amount: t.amount,
        }
    }
}

impl From<FunctionCallTransaction> for transaction_proto::FunctionCallTransaction {
    fn from(t: FunctionCallTransaction) -> transaction_proto::FunctionCallTransaction {
        transaction_proto::FunctionCallTransaction {
            nonce: t.nonce,
            originator: t.originator,
            contract_id: t.contract_id,
            method_name: t.method_name,
            args: t.args,
            amount: t.amount,
            ..Default::default()
        }
    }
}

impl fmt::Debug for FunctionCallTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FunctionCallTransaction")
            .field("nonce", &format_args!("{}", &self.nonce))
            .field("originator", &format_args!("{}", &self.originator))
            .field("contract_id", &format_args!("{}", &self.contract_id))
            .field("method_name", &format_args!("{}", logging::pretty_utf8(&self.method_name)))
            .field("args", &format_args!("{}", logging::pretty_utf8(&self.args)))
            .field("amount", &format_args!("{}", &self.amount))
            .finish()
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct SendMoneyTransaction {
    pub nonce: Nonce,
    pub originator: AccountId,
    pub receiver: AccountId,
    pub amount: Balance,
}

impl From<transaction_proto::SendMoneyTransaction> for SendMoneyTransaction {
    fn from(t: transaction_proto::SendMoneyTransaction) -> Self {
        SendMoneyTransaction {
            nonce: t.nonce,
            originator: t.originator,
            receiver: t.receiver,
            amount: t.amount,
        }
    }
}

impl From<SendMoneyTransaction> for transaction_proto::SendMoneyTransaction {
    fn from(t: SendMoneyTransaction) -> transaction_proto::SendMoneyTransaction {
        transaction_proto::SendMoneyTransaction {
            nonce: t.nonce,
            originator: t.originator,
            receiver: t.receiver,
            amount: t.amount,
            ..Default::default()
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct StakeTransaction {
    pub nonce: Nonce,
    pub originator: AccountId,
    pub amount: Balance,
    pub public_key: String,
    pub bls_public_key: String,
}

impl From<transaction_proto::StakeTransaction> for StakeTransaction {
    fn from(t: transaction_proto::StakeTransaction) -> Self {
        StakeTransaction {
            nonce: t.nonce,
            originator: t.originator,
            amount: t.amount,
            public_key: t.public_key,
            bls_public_key: t.bls_public_key,
        }
    }
}

impl From<StakeTransaction> for transaction_proto::StakeTransaction {
    fn from(t: StakeTransaction) -> transaction_proto::StakeTransaction {
        transaction_proto::StakeTransaction {
            nonce: t.nonce,
            originator: t.originator,
            amount: t.amount,
            public_key: t.public_key,
            bls_public_key: t.bls_public_key,
            ..Default::default()
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct SwapKeyTransaction {
    pub nonce: Nonce,
    pub originator: AccountId,
    // one of the current keys to the account that will be swapped out
    pub cur_key: Vec<u8>,
    pub new_key: Vec<u8>,
}

impl From<transaction_proto::SwapKeyTransaction> for SwapKeyTransaction {
    fn from(t: transaction_proto::SwapKeyTransaction) -> Self {
        SwapKeyTransaction {
            nonce: t.nonce,
            originator: t.originator,
            cur_key: t.cur_key,
            new_key: t.new_key,
        }
    }
}

impl From<SwapKeyTransaction> for transaction_proto::SwapKeyTransaction {
    fn from(t: SwapKeyTransaction) -> transaction_proto::SwapKeyTransaction {
        transaction_proto::SwapKeyTransaction {
            nonce: t.nonce,
            originator: t.originator,
            cur_key: t.cur_key,
            new_key: t.new_key,
            ..Default::default()
        }
    }
}

impl fmt::Debug for SwapKeyTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SwapKeyTransaction")
            .field("nonce", &format_args!("{}", &self.nonce))
            .field("originator", &format_args!("{}", &self.originator))
            .field("cur_key", &format_args!("{}", logging::pretty_utf8(&self.cur_key)))
            .field("new_key", &format_args!("{}", logging::pretty_utf8(&self.new_key)))
            .finish()
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct AddKeyTransaction {
    pub nonce: Nonce,
    pub originator: AccountId,
    pub new_key: Vec<u8>,
}

impl From<transaction_proto::AddKeyTransaction> for AddKeyTransaction {
    fn from(t: transaction_proto::AddKeyTransaction) -> Self {
        AddKeyTransaction { nonce: t.nonce, originator: t.originator, new_key: t.new_key }
    }
}

impl From<AddKeyTransaction> for transaction_proto::AddKeyTransaction {
    fn from(t: AddKeyTransaction) -> transaction_proto::AddKeyTransaction {
        transaction_proto::AddKeyTransaction {
            nonce: t.nonce,
            originator: t.originator,
            new_key: t.new_key,
            ..Default::default()
        }
    }
}

impl fmt::Debug for AddKeyTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AddKeyTransaction")
            .field("nonce", &format_args!("{}", &self.nonce))
            .field("originator", &format_args!("{}", &self.originator))
            .field("new_key", &format_args!("{}", logging::pretty_utf8(&self.new_key)))
            .finish()
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DeleteKeyTransaction {
    pub nonce: Nonce,
    pub originator: AccountId,
    pub cur_key: Vec<u8>,
}

impl From<transaction_proto::DeleteKeyTransaction> for DeleteKeyTransaction {
    fn from(t: transaction_proto::DeleteKeyTransaction) -> Self {
        DeleteKeyTransaction { nonce: t.nonce, originator: t.originator, cur_key: t.cur_key }
    }
}

impl From<DeleteKeyTransaction> for transaction_proto::DeleteKeyTransaction {
    fn from(t: DeleteKeyTransaction) -> transaction_proto::DeleteKeyTransaction {
        transaction_proto::DeleteKeyTransaction {
            nonce: t.nonce,
            originator: t.originator,
            cur_key: t.cur_key,
            ..Default::default()
        }
    }
}

impl fmt::Debug for DeleteKeyTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DeleteKeyTransaction")
            .field("nonce", &format_args!("{}", &self.nonce))
            .field("originator", &format_args!("{}", &self.originator))
            .field("cur_key", &format_args!("{}", logging::pretty_utf8(&self.cur_key)))
            .finish()
    }
}

impl TransactionBody {
    pub fn get_nonce(&self) -> u64 {
        match self {
            TransactionBody::Stake(t) => t.nonce,
            TransactionBody::SendMoney(t) => t.nonce,
            TransactionBody::DeployContract(t) => t.nonce,
            TransactionBody::FunctionCall(t) => t.nonce,
            TransactionBody::CreateAccount(t) => t.nonce,
            TransactionBody::SwapKey(t) => t.nonce,
            TransactionBody::AddKey(t) => t.nonce,
            TransactionBody::DeleteKey(t) => t.nonce,
        }
    }

    pub fn get_originator(&self) -> AccountId {
        match self {
            TransactionBody::Stake(t) => t.originator.clone(),
            TransactionBody::SendMoney(t) => t.originator.clone(),
            TransactionBody::DeployContract(t) => t.contract_id.clone(),
            TransactionBody::FunctionCall(t) => t.originator.clone(),
            TransactionBody::CreateAccount(t) => t.originator.clone(),
            TransactionBody::SwapKey(t) => t.originator.clone(),
            TransactionBody::AddKey(t) => t.originator.clone(),
            TransactionBody::DeleteKey(t) => t.originator.clone(),
        }
    }

    /// Returns option contract_id for Mana and Gas accounting
    pub fn get_contract_id(&self) -> Option<AccountId> {
        match self {
            TransactionBody::CreateAccount(_) => None,
            TransactionBody::DeployContract(t) => Some(t.contract_id.clone()),
            TransactionBody::FunctionCall(t) => Some(t.contract_id.clone()),
            TransactionBody::SendMoney(t) => Some(t.receiver.clone()),
            TransactionBody::Stake(_) => None,
            TransactionBody::SwapKey(_) => None,
            TransactionBody::AddKey(_) => None,
            TransactionBody::DeleteKey(_) => None,
        }
    }

    /// Returns mana required to execute this transaction.
    pub fn get_mana(&self) -> Mana {
        match self {
            TransactionBody::CreateAccount(_) => 1,
            TransactionBody::DeployContract(_) => 1,
            // TODO(#344): DEFAULT_MANA_LIMIT is 20. Need to check that the value is at least 1 mana.
            TransactionBody::FunctionCall(_t) => 20,
            TransactionBody::SendMoney(_) => 1,
            TransactionBody::Stake(_) => 1,
            TransactionBody::SwapKey(_) => 1,
            TransactionBody::AddKey(_) => 1,
            TransactionBody::DeleteKey(_) => 1,
        }
    }

    pub fn get_hash(&self) -> CryptoHash {
        let bytes = match self.clone() {
            TransactionBody::CreateAccount(t) => {
                let proto: transaction_proto::CreateAccountTransaction = t.into();
                proto.write_to_bytes()
            }
            TransactionBody::DeployContract(t) => {
                let proto: transaction_proto::DeployContractTransaction = t.into();
                proto.write_to_bytes()
            }
            TransactionBody::FunctionCall(t) => {
                let proto: transaction_proto::FunctionCallTransaction = t.into();
                proto.write_to_bytes()
            }
            TransactionBody::SendMoney(t) => {
                let proto: transaction_proto::SendMoneyTransaction = t.into();
                proto.write_to_bytes()
            }
            TransactionBody::Stake(t) => {
                let proto: transaction_proto::StakeTransaction = t.into();
                proto.write_to_bytes()
            }
            TransactionBody::SwapKey(t) => {
                let proto: transaction_proto::SwapKeyTransaction = t.into();
                proto.write_to_bytes()
            }
            TransactionBody::AddKey(t) => {
                let proto: transaction_proto::AddKeyTransaction = t.into();
                proto.write_to_bytes()
            }
            TransactionBody::DeleteKey(t) => {
                let proto: transaction_proto::DeleteKeyTransaction = t.into();
                proto.write_to_bytes()
            }
        };
        let bytes = bytes.unwrap();
        hash(&bytes)
    }
}

#[derive(Eq, Debug, Clone, Serialize, Deserialize)]
pub struct SignedTransaction {
    pub body: TransactionBody,
    pub signature: StructSignature,
    // In case this TX uses AccessKey, it needs to provide the public_key
    pub public_key: Option<PublicKey>,
    hash: CryptoHash,
}

impl SignedTransaction {
    pub fn new(
        signature: StructSignature,
        body: TransactionBody,
        public_key: Option<PublicKey>,
    ) -> Self {
        let hash = body.get_hash();
        Self { signature, body, public_key, hash }
    }

    pub fn get_hash(&self) -> CryptoHash {
        self.hash
    }

    // this is for tests
    pub fn empty() -> SignedTransaction {
        let body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 0,
            originator: AccountId::default(),
            receiver: AccountId::default(),
            amount: 0,
        });
        SignedTransaction {
            signature: DEFAULT_SIGNATURE,
            body,
            public_key: None,
            hash: CryptoHash::default(),
        }
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
    type Error = String;

    fn try_from(t: transaction_proto::SignedTransaction) -> Result<Self, Self::Error> {
        let mut bytes;
        let body = match t.body {
            Some(transaction_proto::SignedTransaction_oneof_body::create_account(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::CreateAccount(CreateAccountTransaction::from(t))
            }
            Some(transaction_proto::SignedTransaction_oneof_body::deploy_contract(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::DeployContract(DeployContractTransaction::from(t))
            }
            Some(transaction_proto::SignedTransaction_oneof_body::function_call(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::FunctionCall(FunctionCallTransaction::from(t))
            }
            Some(transaction_proto::SignedTransaction_oneof_body::send_money(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::SendMoney(SendMoneyTransaction::from(t))
            }
            Some(transaction_proto::SignedTransaction_oneof_body::stake(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::Stake(StakeTransaction::from(t))
            }
            Some(transaction_proto::SignedTransaction_oneof_body::swap_key(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::SwapKey(SwapKeyTransaction::from(t))
            }
            Some(transaction_proto::SignedTransaction_oneof_body::add_key(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::AddKey(AddKeyTransaction::from(t))
            }
            Some(transaction_proto::SignedTransaction_oneof_body::delete_key(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::DeleteKey(DeleteKeyTransaction::from(t))
            }
            None => return Err("No such transaction body type".to_string()),
        };
        let bytes = bytes.map_err(|e| format!("{}", e))?;
        let hash = hash(&bytes);
        let public_key: Option<PublicKey> = t.public_key.into_option()
            .map(|v| PublicKey::try_from(&v.value as &[u8]))
            .transpose()
            .map_err(|e| format!("{}", e))?;
        let signature: Signature =
            Signature::try_from(&t.signature as &[u8]).map_err(|e| format!("{}", e))?;
        Ok(SignedTransaction { body, signature, public_key, hash })
    }
}

impl From<SignedTransaction> for transaction_proto::SignedTransaction {
    fn from(tx: SignedTransaction) -> transaction_proto::SignedTransaction {
        let body = match tx.body {
            TransactionBody::CreateAccount(t) => {
                transaction_proto::SignedTransaction_oneof_body::create_account(t.into())
            }
            TransactionBody::DeployContract(t) => {
                transaction_proto::SignedTransaction_oneof_body::deploy_contract(t.into())
            }
            TransactionBody::FunctionCall(t) => {
                transaction_proto::SignedTransaction_oneof_body::function_call(t.into())
            }
            TransactionBody::SendMoney(t) => {
                transaction_proto::SignedTransaction_oneof_body::send_money(t.into())
            }
            TransactionBody::Stake(t) => {
                transaction_proto::SignedTransaction_oneof_body::stake(t.into())
            }
            TransactionBody::SwapKey(t) => {
                transaction_proto::SignedTransaction_oneof_body::swap_key(t.into())
            }
            TransactionBody::AddKey(t) => {
                transaction_proto::SignedTransaction_oneof_body::add_key(t.into())
            }
            TransactionBody::DeleteKey(t) => {
                transaction_proto::SignedTransaction_oneof_body::delete_key(t.into())
            }
        };
        transaction_proto::SignedTransaction {
            body: Some(body),
            signature: tx.signature.as_ref().to_vec(),
            public_key: SingularPtrField::from_option(tx.public_key.map(|v| {
                let mut res = BytesValue::new();
                res.set_value(v.to_bytes());
                res
            })),
            ..Default::default()
        }
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ReceiptBody {
    NewCall(AsyncCall),
    Callback(CallbackResult),
    Refund(u64),
    ManaAccounting(ManaAccounting),
}

#[derive(Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AsyncCall {
    pub amount: Balance,
    pub mana: Mana,
    pub method_name: Vec<u8>,
    pub args: Vec<u8>,
    pub callback: Option<CallbackInfo>,
    pub accounting_info: AccountingInfo,
}

impl TryFrom<receipt_proto::AsyncCall> for AsyncCall {
    type Error = String;

    fn try_from(proto: receipt_proto::AsyncCall) -> Result<Self, Self::Error> {
        match proto_to_result(proto.accounting_info) {
            Ok(accounting_info) => Ok(AsyncCall {
                amount: proto.amount,
                mana: proto.mana,
                method_name: proto.method_name,
                args: proto.args,
                callback: proto.callback.into_option().map(std::convert::Into::into),
                accounting_info: accounting_info.into(),
            }),
            Err(e) => Err(e),
        }
    }
}

impl From<AsyncCall> for receipt_proto::AsyncCall {
    fn from(call: AsyncCall) -> Self {
        receipt_proto::AsyncCall {
            amount: call.amount,
            mana: call.mana,
            method_name: call.method_name,
            args: call.args,
            callback: SingularPtrField::from_option(call.callback.map(std::convert::Into::into)),
            accounting_info: SingularPtrField::some(call.accounting_info.into()),
            ..Default::default()
        }
    }
}

impl AsyncCall {
    pub fn new(
        method_name: Vec<u8>,
        args: Vec<u8>,
        amount: Balance,
        mana: Mana,
        accounting_info: AccountingInfo,
    ) -> Self {
        AsyncCall { amount, mana, method_name, args, callback: None, accounting_info }
    }
}

impl fmt::Debug for AsyncCall {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AsyncCall")
            .field("amount", &format_args!("{}", &self.amount))
            .field("mana", &format_args!("{}", &self.mana))
            .field("method_name", &format_args!("{}", logging::pretty_utf8(&self.method_name)))
            .field("args", &format_args!("{}", logging::pretty_utf8(&self.args)))
            .field("callback", &self.callback)
            .field("accounting_info", &self.accounting_info)
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Callback {
    pub method_name: Vec<u8>,
    pub args: Vec<u8>,
    pub results: Vec<Option<Vec<u8>>>,
    pub mana: Mana,
    pub callback: Option<CallbackInfo>,
    pub result_counter: usize,
    pub accounting_info: AccountingInfo,
}

impl Callback {
    pub fn new(
        method_name: Vec<u8>,
        args: Vec<u8>,
        mana: Mana,
        accounting_info: AccountingInfo,
    ) -> Self {
        Callback {
            method_name,
            args,
            results: vec![],
            mana,
            callback: None,
            result_counter: 0,
            accounting_info,
        }
    }
}

impl fmt::Debug for Callback {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Callback")
            .field("method_name", &format_args!("{}", logging::pretty_utf8(&self.method_name)))
            .field("args", &format_args!("{}", logging::pretty_utf8(&self.args)))
            .field("results", &format_args!("{}", logging::pretty_results(&self.results)))
            .field("mana", &format_args!("{}", &self.mana))
            .field("callback", &self.callback)
            .field("result_counter", &format_args!("{}", &self.result_counter))
            .field("accounting_info", &self.accounting_info)
            .finish()
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CallbackInfo {
    // callback id
    pub id: CallbackId,
    // index to write to
    pub result_index: usize,
    // receiver
    pub receiver: AccountId,
}

impl From<receipt_proto::CallbackInfo> for CallbackInfo {
    fn from(proto: receipt_proto::CallbackInfo) -> Self {
        CallbackInfo {
            id: proto.id,
            result_index: proto.result_index as usize,
            receiver: proto.receiver,
        }
    }
}

impl From<CallbackInfo> for receipt_proto::CallbackInfo {
    fn from(info: CallbackInfo) -> Self {
        receipt_proto::CallbackInfo {
            id: info.id,
            result_index: info.result_index as u64,
            receiver: info.receiver,
            ..Default::default()
        }
    }
}

impl CallbackInfo {
    pub fn new(id: CallbackId, result_index: usize, receiver: AccountId) -> Self {
        CallbackInfo { id, result_index, receiver }
    }
}

impl fmt::Debug for CallbackInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CallbackInfo")
            .field("id", &format_args!("{}", logging::pretty_utf8(&self.id)))
            .field("result_index", &format_args!("{}", self.result_index))
            .field("receiver", &format_args!("{}", self.receiver))
            .finish()
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CallbackResult {
    // callback id
    pub info: CallbackInfo,
    // callback result
    pub result: Option<Vec<u8>>,
}

impl TryFrom<receipt_proto::CallbackResult> for CallbackResult {
    type Error = String;

    fn try_from(proto: receipt_proto::CallbackResult) -> Result<Self, Self::Error> {
        match proto_to_result(proto.info) {
            Ok(info) => Ok(CallbackResult {
                info: info.into(),
                result: proto.result.into_option().map(|v| v.value),
            }),
            Err(e) => Err(e),
        }
    }
}

impl From<CallbackResult> for receipt_proto::CallbackResult {
    fn from(result: CallbackResult) -> Self {
        receipt_proto::CallbackResult {
            info: SingularPtrField::some(result.info.into()),
            result: SingularPtrField::from_option(result.result.map(|v| {
                let mut res = BytesValue::new();
                res.set_value(v);
                res
            })),
            ..Default::default()
        }
    }
}

impl CallbackResult {
    pub fn new(info: CallbackInfo, result: Option<Vec<u8>>) -> Self {
        CallbackResult { info, result }
    }
}

impl fmt::Debug for CallbackResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CallbackResult")
            .field("info", &self.info)
            .field("result", &format_args!("{}", logging::pretty_result(&self.result)))
            .finish()
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ReceiptTransaction {
    // sender is the immediate predecessor
    pub originator: AccountId,
    pub receiver: AccountId,
    // nonce will be a hash
    pub nonce: CryptoHash,
    pub body: ReceiptBody,
}

impl TryFrom<receipt_proto::ReceiptTransaction> for ReceiptTransaction {
    type Error = String;

    fn try_from(proto: receipt_proto::ReceiptTransaction) -> Result<Self, Self::Error> {
        let body = match proto.body {
            Some(receipt_proto::ReceiptTransaction_oneof_body::new_call(new_call)) => {
                new_call.try_into().map(ReceiptBody::NewCall)
            }
            Some(receipt_proto::ReceiptTransaction_oneof_body::callback(callback)) => {
                callback.try_into().map(ReceiptBody::Callback)
            }
            Some(receipt_proto::ReceiptTransaction_oneof_body::refund(refund)) => {
                Ok(ReceiptBody::Refund(refund))
            }
            Some(receipt_proto::ReceiptTransaction_oneof_body::mana_accounting(accounting)) => {
                accounting.try_into().map(ReceiptBody::ManaAccounting)
            }
            None => Err("No such receipt body type".to_string()),
        };
        match body {
            Ok(body) => Ok(ReceiptTransaction {
                originator: proto.originator,
                receiver: proto.receiver,
                nonce: proto.nonce.try_into()?,
                body,
            }),
            Err(e) => Err(e),
        }
    }
}

impl From<ReceiptTransaction> for receipt_proto::ReceiptTransaction {
    fn from(t: ReceiptTransaction) -> Self {
        let body = match t.body {
            ReceiptBody::NewCall(new_call) => {
                receipt_proto::ReceiptTransaction_oneof_body::new_call(new_call.into())
            }
            ReceiptBody::Callback(callback) => {
                receipt_proto::ReceiptTransaction_oneof_body::callback(callback.into())
            }
            ReceiptBody::Refund(refund) => {
                receipt_proto::ReceiptTransaction_oneof_body::refund(refund)
            }
            ReceiptBody::ManaAccounting(accounting) => {
                receipt_proto::ReceiptTransaction_oneof_body::mana_accounting(accounting.into())
            }
        };
        receipt_proto::ReceiptTransaction {
            originator: t.originator,
            receiver: t.receiver,
            nonce: t.nonce.into(),
            body: Some(body),
            ..Default::default()
        }
    }
}

impl Borrow<CryptoHash> for ReceiptTransaction {
    fn borrow(&self) -> &CryptoHash {
        &self.nonce
    }
}

impl ReceiptTransaction {
    pub fn new(
        originator: AccountId,
        receiver: AccountId,
        nonce: CryptoHash,
        body: ReceiptBody,
    ) -> Self {
        ReceiptTransaction { originator, receiver, nonce, body }
    }

    pub fn shard_id(&self) -> ShardId {
        account_to_shard_id(&self.receiver)
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

/// Logs for transaction or receipt with given hash.
#[derive(PartialEq, Clone, Serialize, Deserialize)]
pub struct TransactionLogs {
    pub hash: CryptoHash,
    pub lines: Vec<LogEntry>,
    pub receipts: Vec<CryptoHash>,
    pub result: Option<Vec<u8>>,
}

impl fmt::Debug for TransactionLogs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TransactionLogs")
            .field("hash", &self.hash)
            .field("lines", &format_args!("{}", logging::pretty_vec(&self.lines)))
            .field("receipts", &format_args!("{}", logging::pretty_vec(&self.receipts)))
            .field("result", &format_args!("{}", logging::pretty_result(&self.result)))
            .finish()
    }
}

/// Result of transaction and all of subsequent the receipts.
#[derive(PartialEq, Clone, Serialize, Deserialize)]
pub struct FinalTransactionResult {
    /// Status of the whole transaction and it's receipts.
    pub status: FinalTransactionStatus,
    /// Logs per transaction / receipt ids ordered in DFS manner.
    pub logs: Vec<TransactionLogs>,
}

impl fmt::Debug for FinalTransactionResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FinalTransactionResult")
            .field("status", &self.status)
            .field("logs", &format_args!("{}", logging::pretty_vec(&self.logs)))
            .finish()
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
    use crate::crypto::signature::{get_key_pair, sign};

    use super::*;

    #[test]
    fn test_verify_transaction() {
        let (public_key, private_key) = get_key_pair();
        let mut transaction = SignedTransaction::empty();
        transaction.signature = sign(&transaction.hash.as_ref(), &private_key);
        let (wrong_public_key, _) = get_key_pair();
        let valid_keys = vec![public_key, wrong_public_key];
        assert!(verify_transaction_signature(&transaction, &valid_keys));

        let invalid_keys = vec![wrong_public_key];
        assert!(!verify_transaction_signature(&transaction, &invalid_keys));
    }
}
