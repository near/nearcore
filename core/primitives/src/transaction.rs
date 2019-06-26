use std::borrow::Borrow;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::hash::{Hash, Hasher};

use protobuf::well_known_types::BytesValue;
use protobuf::SingularPtrField;

use near_protos::receipt as receipt_proto;
use near_protos::signed_transaction as transaction_proto;
use near_protos::Message as ProtoMessage;

use crate::account::AccessKey;
use crate::crypto::signature::{verify, PublicKey, Signature, DEFAULT_SIGNATURE};
use crate::hash::{hash, CryptoHash};
use crate::logging;
use crate::serialize::base_format;
use crate::types::{AccountId, Balance, CallbackId, Nonce, ShardId, StructSignature};
use crate::utils::{account_to_shard_id, proto_to_result};

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
    pub fn send_money(nonce: Nonce, originator: &str, receiver: &str, amount: Balance) -> Self {
        TransactionBody::SendMoney(SendMoneyTransaction {
            nonce,
            originator_id: originator.to_string(),
            receiver_id: receiver.to_string(),
            amount,
        })
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct CreateAccountTransaction {
    pub nonce: Nonce,
    pub originator_id: AccountId,
    pub new_account_id: AccountId,
    pub amount: Balance,
    pub public_key: Vec<u8>,
}

impl TryFrom<transaction_proto::CreateAccountTransaction> for CreateAccountTransaction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(t: transaction_proto::CreateAccountTransaction) -> Result<Self, Self::Error> {
        Ok(CreateAccountTransaction {
            nonce: t.nonce,
            originator_id: t.originator_id,
            new_account_id: t.new_account_id,
            amount: t.amount.unwrap_or_default().try_into()?,
            public_key: t.public_key,
        })
    }
}

impl From<CreateAccountTransaction> for transaction_proto::CreateAccountTransaction {
    fn from(t: CreateAccountTransaction) -> transaction_proto::CreateAccountTransaction {
        transaction_proto::CreateAccountTransaction {
            nonce: t.nonce,
            originator_id: t.originator_id,
            new_account_id: t.new_account_id,
            amount: SingularPtrField::some(t.amount.into()),
            public_key: t.public_key,
            ..Default::default()
        }
    }
}

impl fmt::Debug for CreateAccountTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CreateAccountTransaction")
            .field("nonce", &format_args!("{}", &self.nonce))
            .field("originator_id", &format_args!("{}", &self.originator_id))
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
    pub originator_id: AccountId,
    pub contract_id: AccountId,
    pub method_name: Vec<u8>,
    pub args: Vec<u8>,
    pub amount: Balance,
}

impl TryFrom<transaction_proto::FunctionCallTransaction> for FunctionCallTransaction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(t: transaction_proto::FunctionCallTransaction) -> Result<Self, Self::Error> {
        Ok(FunctionCallTransaction {
            nonce: t.nonce,
            originator_id: t.originator_id,
            contract_id: t.contract_id,
            method_name: t.method_name,
            args: t.args,
            amount: t.amount.unwrap_or_default().try_into()?,
        })
    }
}

impl From<FunctionCallTransaction> for transaction_proto::FunctionCallTransaction {
    fn from(t: FunctionCallTransaction) -> Self {
        transaction_proto::FunctionCallTransaction {
            nonce: t.nonce,
            originator_id: t.originator_id,
            contract_id: t.contract_id,
            method_name: t.method_name,
            args: t.args,
            amount: SingularPtrField::some(t.amount.into()),
            ..Default::default()
        }
    }
}

impl fmt::Debug for FunctionCallTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FunctionCallTransaction")
            .field("nonce", &format_args!("{}", &self.nonce))
            .field("originator_id", &format_args!("{}", &self.originator_id))
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
    pub originator_id: AccountId,
    pub receiver_id: AccountId,
    pub amount: Balance,
}

impl TryFrom<transaction_proto::SendMoneyTransaction> for SendMoneyTransaction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(t: transaction_proto::SendMoneyTransaction) -> Result<Self, Self::Error> {
        Ok(SendMoneyTransaction {
            nonce: t.nonce,
            originator_id: t.originator_id,
            receiver_id: t.receiver_id,
            amount: t.amount.unwrap_or_default().try_into()?,
        })
    }
}

impl From<SendMoneyTransaction> for transaction_proto::SendMoneyTransaction {
    fn from(t: SendMoneyTransaction) -> Self {
        transaction_proto::SendMoneyTransaction {
            nonce: t.nonce,
            originator_id: t.originator_id,
            receiver_id: t.receiver_id,
            amount: SingularPtrField::some(t.amount.into()),
            ..Default::default()
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct StakeTransaction {
    pub nonce: Nonce,
    pub originator_id: AccountId,
    pub amount: Balance,
    pub public_key: String,
}

impl TryFrom<transaction_proto::StakeTransaction> for StakeTransaction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(t: transaction_proto::StakeTransaction) -> Result<Self, Self::Error> {
        Ok(StakeTransaction {
            nonce: t.nonce,
            originator_id: t.originator_id,
            amount: t.amount.unwrap_or_default().try_into()?,
            public_key: t.public_key,
        })
    }
}

impl From<StakeTransaction> for transaction_proto::StakeTransaction {
    fn from(t: StakeTransaction) -> transaction_proto::StakeTransaction {
        transaction_proto::StakeTransaction {
            nonce: t.nonce,
            originator_id: t.originator_id,
            amount: SingularPtrField::some(t.amount.into()),
            public_key: t.public_key,
            ..Default::default()
        }
    }
}

impl fmt::Debug for StakeTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StakeTransaction")
            .field("nonce", &format_args!("{}", &self.nonce))
            .field("originator_id", &format_args!("{}", &self.originator_id))
            .field("amount", &format_args!("{}", &self.amount))
            .field("public_key", &format_args!("{}", logging::pretty_hash(&self.public_key)))
            .finish()
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct SwapKeyTransaction {
    pub nonce: Nonce,
    pub originator_id: AccountId,
    // one of the current keys to the account that will be swapped out
    pub cur_key: Vec<u8>,
    pub new_key: Vec<u8>,
}

impl From<transaction_proto::SwapKeyTransaction> for SwapKeyTransaction {
    fn from(t: transaction_proto::SwapKeyTransaction) -> Self {
        SwapKeyTransaction {
            nonce: t.nonce,
            originator_id: t.originator_id,
            cur_key: t.cur_key,
            new_key: t.new_key,
        }
    }
}

impl From<SwapKeyTransaction> for transaction_proto::SwapKeyTransaction {
    fn from(t: SwapKeyTransaction) -> transaction_proto::SwapKeyTransaction {
        transaction_proto::SwapKeyTransaction {
            nonce: t.nonce,
            originator_id: t.originator_id,
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
            .field("originator_id", &format_args!("{}", &self.originator_id))
            .field("cur_key", &format_args!("{}", logging::pretty_utf8(&self.cur_key)))
            .field("new_key", &format_args!("{}", logging::pretty_utf8(&self.new_key)))
            .finish()
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct AddKeyTransaction {
    pub nonce: Nonce,
    pub originator_id: AccountId,
    pub new_key: Vec<u8>,
    pub access_key: Option<AccessKey>,
}

impl TryFrom<transaction_proto::AddKeyTransaction> for AddKeyTransaction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(t: transaction_proto::AddKeyTransaction) -> Result<Self, Self::Error> {
        Ok(AddKeyTransaction {
            nonce: t.nonce,
            originator_id: t.originator_id,
            new_key: t.new_key,
            access_key: t
                .access_key
                .into_option()
                .map_or(Ok(None), |x| AccessKey::try_from(x).map(Some))?,
        })
    }
}

impl From<AddKeyTransaction> for transaction_proto::AddKeyTransaction {
    fn from(t: AddKeyTransaction) -> transaction_proto::AddKeyTransaction {
        transaction_proto::AddKeyTransaction {
            nonce: t.nonce,
            originator_id: t.originator_id,
            new_key: t.new_key,
            access_key: SingularPtrField::from_option(t.access_key.map(std::convert::Into::into)),
            ..Default::default()
        }
    }
}

impl fmt::Debug for AddKeyTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AddKeyTransaction")
            .field("nonce", &format_args!("{}", &self.nonce))
            .field("originator_id", &format_args!("{}", &self.originator_id))
            .field("new_key", &format_args!("{}", logging::pretty_utf8(&self.new_key)))
            .finish()
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DeleteKeyTransaction {
    pub nonce: Nonce,
    pub originator_id: AccountId,
    pub cur_key: Vec<u8>,
}

impl From<transaction_proto::DeleteKeyTransaction> for DeleteKeyTransaction {
    fn from(t: transaction_proto::DeleteKeyTransaction) -> Self {
        DeleteKeyTransaction { nonce: t.nonce, originator_id: t.originator_id, cur_key: t.cur_key }
    }
}

impl From<DeleteKeyTransaction> for transaction_proto::DeleteKeyTransaction {
    fn from(t: DeleteKeyTransaction) -> transaction_proto::DeleteKeyTransaction {
        transaction_proto::DeleteKeyTransaction {
            nonce: t.nonce,
            originator_id: t.originator_id,
            cur_key: t.cur_key,
            ..Default::default()
        }
    }
}

impl fmt::Debug for DeleteKeyTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DeleteKeyTransaction")
            .field("nonce", &format_args!("{}", &self.nonce))
            .field("originator_id", &format_args!("{}", &self.originator_id))
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
            TransactionBody::Stake(t) => t.originator_id.clone(),
            TransactionBody::SendMoney(t) => t.originator_id.clone(),
            TransactionBody::DeployContract(t) => t.contract_id.clone(),
            TransactionBody::FunctionCall(t) => t.originator_id.clone(),
            TransactionBody::CreateAccount(t) => t.originator_id.clone(),
            TransactionBody::SwapKey(t) => t.originator_id.clone(),
            TransactionBody::AddKey(t) => t.originator_id.clone(),
            TransactionBody::DeleteKey(t) => t.originator_id.clone(),
        }
    }

    /// Returns option contract_id for Mana and Gas accounting
    pub fn get_contract_id(&self) -> Option<AccountId> {
        match self {
            TransactionBody::CreateAccount(_) => None,
            TransactionBody::DeployContract(t) => Some(t.contract_id.clone()),
            TransactionBody::FunctionCall(t) => Some(t.contract_id.clone()),
            TransactionBody::SendMoney(t) => Some(t.receiver_id.clone()),
            TransactionBody::Stake(_) => None,
            TransactionBody::SwapKey(_) => None,
            TransactionBody::AddKey(_) => None,
            TransactionBody::DeleteKey(_) => None,
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
            originator_id: AccountId::default(),
            receiver_id: AccountId::default(),
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
        self.hash == other.hash
            && self.signature == other.signature
            && self.public_key == other.public_key
    }
}

impl TryFrom<transaction_proto::SignedTransaction> for SignedTransaction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(t: transaction_proto::SignedTransaction) -> Result<Self, Self::Error> {
        let bytes;
        let body = match t.body {
            Some(transaction_proto::SignedTransaction_oneof_body::create_account(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::CreateAccount(CreateAccountTransaction::try_from(t)?)
            }
            Some(transaction_proto::SignedTransaction_oneof_body::deploy_contract(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::DeployContract(DeployContractTransaction::from(t))
            }
            Some(transaction_proto::SignedTransaction_oneof_body::function_call(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::FunctionCall(FunctionCallTransaction::try_from(t)?)
            }
            Some(transaction_proto::SignedTransaction_oneof_body::send_money(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::SendMoney(SendMoneyTransaction::try_from(t)?)
            }
            Some(transaction_proto::SignedTransaction_oneof_body::stake(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::Stake(StakeTransaction::try_from(t)?)
            }
            Some(transaction_proto::SignedTransaction_oneof_body::swap_key(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::SwapKey(SwapKeyTransaction::from(t))
            }
            Some(transaction_proto::SignedTransaction_oneof_body::add_key(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::AddKey(AddKeyTransaction::try_from(t)?)
            }
            Some(transaction_proto::SignedTransaction_oneof_body::delete_key(t)) => {
                bytes = t.write_to_bytes();
                TransactionBody::DeleteKey(DeleteKeyTransaction::try_from(t)?)
            }
            None => return Err("No such transaction body type".into()),
        };
        let bytes = bytes.map_err(|e| format!("{}", e))?;
        let hash = hash(&bytes);
        let public_key: Option<PublicKey> = t
            .public_key
            .into_option()
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
                res.set_value((&v).into());
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
    Refund(Balance),
}

#[derive(Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AsyncCall {
    pub amount: Balance,
    pub method_name: Vec<u8>,
    pub args: Vec<u8>,
    pub callback: Option<CallbackInfo>,
    pub refund_account_id: AccountId,
    /// Account ID of the account who signed the initial transaction.
    pub originator_id: AccountId,
    /// The public key used to sign the initial transaction.
    pub public_key: PublicKey,
}

impl TryFrom<receipt_proto::AsyncCall> for AsyncCall {
    type Error = Box<dyn std::error::Error>;

    fn try_from(proto: receipt_proto::AsyncCall) -> Result<Self, Self::Error> {
        Ok(AsyncCall {
            amount: proto.amount.unwrap_or_default().try_into()?,
            method_name: proto.method_name,
            args: proto.args,
            callback: proto.callback.into_option().map(std::convert::Into::into),
            refund_account_id: proto.refund_account_id,
            originator_id: proto.originator_id,
            public_key: PublicKey::try_from(&proto.public_key as &[u8])?,
        })
    }
}

impl From<AsyncCall> for receipt_proto::AsyncCall {
    fn from(call: AsyncCall) -> Self {
        receipt_proto::AsyncCall {
            amount: SingularPtrField::some(call.amount.into()),
            method_name: call.method_name,
            args: call.args,
            callback: SingularPtrField::from_option(call.callback.map(std::convert::Into::into)),
            refund_account_id: call.refund_account_id,
            originator_id: call.originator_id,
            public_key: call.public_key.as_ref().to_vec(),
            ..Default::default()
        }
    }
}

impl AsyncCall {
    pub fn new(
        method_name: Vec<u8>,
        args: Vec<u8>,
        amount: Balance,
        refund_account_id: AccountId,
        originator_id: AccountId,
        public_key: PublicKey,
    ) -> Self {
        AsyncCall {
            amount,
            method_name,
            args,
            callback: None,
            refund_account_id,
            originator_id,
            public_key,
        }
    }
}

impl fmt::Debug for AsyncCall {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AsyncCall")
            .field("amount", &format_args!("{}", &self.amount))
            .field("method_name", &format_args!("{}", logging::pretty_utf8(&self.method_name)))
            .field("args", &format_args!("{}", logging::pretty_utf8(&self.args)))
            .field("callback", &self.callback)
            .field("refund_account_id", &self.refund_account_id)
            .field("originator_id", &self.originator_id)
            .field("public_key", &self.public_key)
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Callback {
    pub method_name: Vec<u8>,
    pub args: Vec<u8>,
    pub results: Vec<Option<Vec<u8>>>,
    pub amount: Balance,
    pub callback: Option<CallbackInfo>,
    pub result_counter: usize,
    pub refund_account_id: AccountId,
    /// Account ID of the account who signed the initial transaction.
    pub originator_id: AccountId,
    /// The public key used to sign the initial transaction.
    pub public_key: PublicKey,
}

impl Callback {
    pub fn new(
        method_name: Vec<u8>,
        args: Vec<u8>,
        amount: Balance,
        refund_account_id: AccountId,
        originator_id: AccountId,
        public_key: PublicKey,
    ) -> Self {
        Callback {
            method_name,
            args,
            results: vec![],
            amount,
            callback: None,
            result_counter: 0,
            refund_account_id,
            originator_id,
            public_key,
        }
    }
}

impl fmt::Debug for Callback {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Callback")
            .field("method_name", &format_args!("{}", logging::pretty_utf8(&self.method_name)))
            .field("args", &format_args!("{}", logging::pretty_utf8(&self.args)))
            .field("results", &format_args!("{}", logging::pretty_results(&self.results)))
            .field("amount", &format_args!("{}", &self.amount))
            .field("callback", &self.callback)
            .field("result_counter", &format_args!("{}", &self.result_counter))
            .field("refund_account_id", &self.refund_account_id)
            .field("originator_id", &self.originator_id)
            .field("public_key", &self.public_key)
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
    pub receiver_id: AccountId,
}

impl From<receipt_proto::CallbackInfo> for CallbackInfo {
    fn from(proto: receipt_proto::CallbackInfo) -> Self {
        CallbackInfo {
            id: proto.id,
            result_index: proto.result_index as usize,
            receiver_id: proto.receiver_id,
        }
    }
}

impl From<CallbackInfo> for receipt_proto::CallbackInfo {
    fn from(info: CallbackInfo) -> Self {
        receipt_proto::CallbackInfo {
            id: info.id,
            result_index: info.result_index as u64,
            receiver_id: info.receiver_id,
            ..Default::default()
        }
    }
}

impl CallbackInfo {
    pub fn new(id: CallbackId, result_index: usize, receiver_id: AccountId) -> Self {
        CallbackInfo { id, result_index, receiver_id: receiver_id }
    }
}

impl fmt::Debug for CallbackInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CallbackInfo")
            .field("id", &format_args!("{}", logging::pretty_utf8(&self.id)))
            .field("result_index", &format_args!("{}", self.result_index))
            .field("receiver_id", &format_args!("{}", self.receiver_id))
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
    type Error = Box<dyn std::error::Error>;

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
    // sender_id is the immediate predecessor
    pub sender_id: AccountId,
    pub receiver_id: AccountId,
    // nonce will be a hash
    pub nonce: CryptoHash,
    pub body: ReceiptBody,
}

impl TryFrom<receipt_proto::ReceiptTransaction> for ReceiptTransaction {
    type Error = Box<dyn std::error::Error>;

    fn try_from(proto: receipt_proto::ReceiptTransaction) -> Result<Self, Self::Error> {
        let body = match proto.body {
            Some(receipt_proto::ReceiptTransaction_oneof_body::new_call(new_call)) => {
                new_call.try_into().map(ReceiptBody::NewCall)
            }
            Some(receipt_proto::ReceiptTransaction_oneof_body::callback(callback)) => {
                callback.try_into().map(ReceiptBody::Callback)
            }
            Some(receipt_proto::ReceiptTransaction_oneof_body::refund(refund)) => {
                Ok(ReceiptBody::Refund(refund.try_into()?))
            }
            None => Err("No such receipt body type".into()),
        };
        match body {
            Ok(body) => Ok(ReceiptTransaction {
                sender_id: proto.sender_id,
                receiver_id: proto.receiver_id,
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
                receipt_proto::ReceiptTransaction_oneof_body::refund(refund.into())
            }
        };
        receipt_proto::ReceiptTransaction {
            sender_id: t.sender_id,
            receiver_id: t.receiver_id,
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
        sender_id: AccountId,
        receiver_id: AccountId,
        nonce: CryptoHash,
        body: ReceiptBody,
    ) -> Self {
        ReceiptTransaction { sender_id, receiver_id, nonce, body }
    }

    pub fn shard_id(&self) -> ShardId {
        account_to_shard_id(&self.receiver_id)
    }

    pub fn get_hash(&self) -> CryptoHash {
        self.nonce
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

/// Logs for transaction or receipt with given hash.
#[derive(PartialEq, Clone, Serialize, Deserialize)]
pub struct TransactionLogs {
    #[serde(with = "base_format")]
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
#[derive(PartialEq, Clone, Serialize, Deserialize, Default)]
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

impl FinalTransactionResult {
    pub fn final_log(&self) -> String {
        let mut logs = vec![];
        for log in &self.logs {
            for line in &log.lines {
                logs.push(line.clone());
            }
        }
        logs.join("\n")
    }

    pub fn last_result(&self) -> Vec<u8> {
        for log in self.logs.iter().rev() {
            if let Some(r) = &log.result {
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
