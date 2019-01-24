#[macro_use]
extern crate serde_derive;

use std::{fmt, io};
use std::hash::{Hash, Hasher};

use near_protos::transaction as transaction_proto;
use primitives::hash::{CryptoHash, hash_struct};
use primitives::serialize::{Decode, DecodeResult, Encode, EncodeResult};
use primitives::signature::{DEFAULT_SIGNATURE, PublicKey, Signature, verify};
use primitives::traits::Payload;
use primitives::types::{
    AccountId, AccountingInfo, Balance, CallbackId, Mana,
    ManaAccounting, StructSignature,
};

#[derive(Hash, PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub enum TransactionBody {
    CreateAccount(CreateAccountTransaction),
    DeployContract(DeployContractTransaction),
    FunctionCall(FunctionCallTransaction),
    SendMoney(SendMoneyTransaction),
    Stake(StakeTransaction),
    SwapKey(SwapKeyTransaction),
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct CreateAccountTransaction {
    pub nonce: u64,
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

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DeployContractTransaction {
    pub nonce: u64,
    pub originator: AccountId,
    pub contract_id: AccountId,
    pub wasm_byte_array: Vec<u8>,
    pub public_key: Vec<u8>,
}

impl From<transaction_proto::DeployContractTransaction> for DeployContractTransaction {
    fn from(t: transaction_proto::DeployContractTransaction) -> Self {
        DeployContractTransaction {
            nonce: t.nonce,
            originator: t.originator,
            contract_id: t.contract_id,
            wasm_byte_array: t.wasm_byte_array,
            public_key: t.public_key,
        }
    }
}

impl fmt::Debug for DeployContractTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeployContractTransaction {{ nonce: {}, originator: {}, contract_id: {}, wasm_byte_array: ... }}", self.nonce, self.originator, self.contract_id)
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FunctionCallTransaction {
    pub nonce: u64,
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

impl fmt::Debug for FunctionCallTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FunctionCallTransaction {{ nonce: {}, originator: {}, contract_id: {}, method_name: {:?}, args: ..., amount: {} }}", self.nonce, self.originator, self.contract_id, String::from_utf8(self.method_name.clone()), self.amount)
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct SendMoneyTransaction {
    pub nonce: u64,
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
            amount: 0
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct StakeTransaction {
    pub nonce: u64,
    pub originator: AccountId,
    pub amount: Balance,
}

impl From<transaction_proto::StakeTransaction> for StakeTransaction {
    fn from(t: transaction_proto::StakeTransaction) -> Self {
        StakeTransaction {
            nonce: t.nonce,
            originator: t.originator,
            amount: t.amount,
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct SwapKeyTransaction {
    pub nonce: u64,
    pub originator: AccountId,
    // current key to the account.
    // originator must sign the transaction with this key
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

impl TransactionBody {
    pub fn get_nonce(&self) -> u64 {
        match self {
            TransactionBody::Stake(t) => t.nonce,
            TransactionBody::SendMoney(t) => t.nonce,
            TransactionBody::DeployContract(t) => t.nonce,
            TransactionBody::FunctionCall(t) => t.nonce,
            TransactionBody::CreateAccount(t) => t.nonce,
            TransactionBody::SwapKey(t) => t.nonce,
        }
    }

    pub fn get_originator(&self) -> AccountId {
        match self {
            TransactionBody::Stake(t) => t.originator.clone(),
            TransactionBody::SendMoney(t) => t.originator.clone(),
            TransactionBody::DeployContract(t) => t.originator.clone(),
            TransactionBody::FunctionCall(t) => t.originator.clone(),
            TransactionBody::CreateAccount(t) => t.originator.clone(),
            TransactionBody::SwapKey(t) => t.originator.clone(),
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
        }
    }
}

#[derive(Eq, Debug, Clone, Serialize, Deserialize)]
pub struct SignedTransaction {
    pub body: TransactionBody,
    pub signature: StructSignature,
}

impl From<transaction_proto::SignedTransaction> for SignedTransaction {
    fn from(t: transaction_proto::SignedTransaction) -> Self {
        let body = match t.body {
            Some(transaction_proto::SignedTransaction_oneof_body::create_account(t)) => {
                TransactionBody::CreateAccount(CreateAccountTransaction::from(t))
            },
            Some(transaction_proto::SignedTransaction_oneof_body::deploy_contract(t)) => {
                TransactionBody::DeployContract(DeployContractTransaction::from(t))
            },
            Some(transaction_proto::SignedTransaction_oneof_body::function_call(t)) => {
                TransactionBody::FunctionCall(FunctionCallTransaction::from(t))
            },
            Some(transaction_proto::SignedTransaction_oneof_body::send_money(t)) => {
                TransactionBody::SendMoney(SendMoneyTransaction::from(t))
            },
            Some(transaction_proto::SignedTransaction_oneof_body::stake(t)) => {
                TransactionBody::Stake(StakeTransaction::from(t))
            },
            Some(transaction_proto::SignedTransaction_oneof_body::swap_key(t)) => {
                TransactionBody::SwapKey(SwapKeyTransaction::from(t))
            },
            _ => unreachable!(),
        };
        SignedTransaction {
            body,
            signature: Signature::new(&t.signature),
        }
    }
}

impl SignedTransaction {
    pub fn new(
        signature: StructSignature,
        body: TransactionBody,
    ) -> SignedTransaction {
        SignedTransaction {
            signature,
            body,
        }
    }

    // this is for tests
    pub fn empty() -> SignedTransaction {
        let body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 0,
            originator: AccountId::default(),
            receiver: AccountId::default(),
            amount: 0,
        });
        SignedTransaction { signature: DEFAULT_SIGNATURE, body }
    }

    pub fn transaction_hash(&self) -> CryptoHash {
        // TODO(#227): Fill in hash when deserializing.
        hash_struct(&self.body)
    }
}

impl Hash for SignedTransaction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(hash_struct(&self.body).as_ref());
    }
}

impl PartialEq for SignedTransaction {
    fn eq(&self, other: &SignedTransaction) -> bool {
        self.body == other.body && self.signature == other.signature
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

impl AsyncCall {
    pub fn new(
        method_name: Vec<u8>,
        args: Vec<u8>,
        amount: Balance,
        mana: Mana,
        accounting_info: AccountingInfo
    ) -> Self {
        AsyncCall {
            amount,
            mana,
            method_name,
            args,
            callback: None,
            accounting_info,
        }
    }
}

impl fmt::Debug for AsyncCall {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncCall {{ amount: {}, mana: {}, method_name: {:?}, args: ..., callback: {:?}, accounting_info: {:?} }}",
               self.amount,
               self.mana,
               String::from_utf8(self.method_name.clone()),
               self.callback,
               self.accounting_info,
        )
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
    pub fn new(method_name: Vec<u8>, args: Vec<u8>, mana: Mana, accounting_info: AccountingInfo) -> Self {
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
        write!(f, "Callback {{ method_name: {:?}, args: ..., results: ..., mana: {}, callback: {:?}, result_counter: {}, accounting_info: {:?} }}",
               String::from_utf8(self.method_name.clone()),
               self.mana,
               self.callback,
               self.result_counter,
               self.accounting_info,
        )
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct CallbackInfo {
    // callback id
    pub id: CallbackId,
    // index to write to
    pub result_index: usize,
    // receiver
    pub receiver: AccountId,
}

impl CallbackInfo {
    pub fn new(id: CallbackId, result_index: usize, receiver: AccountId) -> Self {
        CallbackInfo { id, result_index, receiver }
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct CallbackResult {
    // callback id
    pub info: CallbackInfo,
    // callback result
    pub result: Option<Vec<u8>>,
}

impl CallbackResult {
    pub fn new(info: CallbackInfo, result: Option<Vec<u8>>) -> Self {
        CallbackResult { info, result }
    }
}

#[derive(Hash, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ReceiptTransaction {
    // sender is the immediate predecessor
    pub originator: AccountId,
    pub receiver: AccountId,
    // nonce will be a hash
    pub nonce: Vec<u8>,
    pub body: ReceiptBody,
}

impl ReceiptTransaction {
    pub fn new(
        originator: AccountId,
        receiver: AccountId,
        nonce: Vec<u8>,
        body: ReceiptBody,
    ) -> Self {
        ReceiptTransaction {
            originator,
            receiver,
            nonce,
            body,
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub enum Transaction {
    SignedTransaction(SignedTransaction),
    Receipt(ReceiptTransaction),
}

//impl Encode for TransactionBody {
//    fn encode(&self) -> EncodeResult {
//        let mut m = transaction_proto::TransactionBody::new();
//        m.set_originator(self.get_originator());
//        m.set_nonce(self.get_nonce());
//        match &self {
//            TransactionBody::CreateAccount(t) => {
//                m.set_destination(system_account());
//                m.set_args(vec![]);
//            },
//            _ => {}
//        }
//        encode_proto(&m)
//    }
//}

//impl Decode for TransactionBody {
//    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
//        let m: transaction_proto::TransactionBody = decode_proto(bytes)?;
//        Err(io::Error::new(io::ErrorKind::Other, "Failed to serialize"))
//        // Ok(Transaction::SignedTransaction(SignedTransaction { }))
//    }
//}
//
//impl Encode for SignedTransaction {
//    fn encode(&self) -> EncodeResult {
//        let mut m = transaction_proto::SignedTransaction::new();
//        encode_proto(&m)
//    }
//}
//
//impl Decode for SignedTransaction {
//    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
//        let m: transaction_proto::SignedTransaction = decode_proto(bytes)?;
//        Err(io::Error::new(io::ErrorKind::Other, "Failed to serialize"))
//        // Ok(Transaction::SignedTransaction(SignedTransaction { }))
//    }
//}
//
//impl Encode for Transaction {
//    fn encode(&self) -> EncodeResult {
//        let mut m = transaction_proto::TransactionBody::new();
//        encode_proto(&m)
//    }
//}
//
//impl Decode for Transaction {
//    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
//        let m: transaction_proto::TransactionBody = decode_proto(bytes)?;
//        Err(io::Error::new(io::ErrorKind::Other, "Failed to serialize"))
//        // Ok(Transaction::SignedTransaction(SignedTransaction { }))
//    }
//}
//

#[derive(Hash, Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ChainPayload {
    pub body: Vec<Transaction>,
}

impl Payload for ChainPayload {
    fn verify(&self) -> Result<(), &'static str> {
        Ok(())
    }

    fn union_update(&mut self, mut other: Self) {
        self.body.extend(other.body.drain(..));
    }

    fn is_empty(&self) -> bool {
        self.body.is_empty()
    }

    fn new() -> Self {
        Self {
            body: vec![]
        }
    }
}

//impl Encode for ChainPayload {
//    fn encode(&self) -> EncodeResult {
//        let m = transaction_proto::ChainPayload::new();
//        encode_proto(&m)
//    }
//}
//
//impl Decode for ChainPayload {
//    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
//        let m: transaction_proto::ChainPayload = decode_proto(&bytes)?;
//        let mut body = vec![];
//        for t in m.get_transactions().iter() {
////            body.push(Transaction::decode(&t)?);
//        }
//        Ok(ChainPayload {
//            body
//        })
//    }
//}

pub fn verify_transaction_signature(
    transaction: &SignedTransaction,
    public_keys: &Vec<PublicKey>,
) -> bool {
    let hash = transaction.transaction_hash();
    let hash = hash.as_ref();
    public_keys.iter().any(|key| {
        verify(&hash, &transaction.signature, &key)
    })
}

#[cfg(test)]
mod tests {
    use primitives::signature::{get_key_pair, sign};

    use super::*;

    #[test]
    fn test_verify_transaction() {
        let (public_key, private_key) = get_key_pair();
        let mut transaction = SignedTransaction::empty();
        transaction.signature = sign(
            &transaction.transaction_hash().as_ref(),
            &private_key,
        );
        let (wrong_public_key, _) = get_key_pair();
        let valid_keys = vec![public_key, wrong_public_key];
        assert!(verify_transaction_signature(&transaction, &valid_keys));

        let invalid_keys = vec![wrong_public_key];
        assert!(!verify_transaction_signature(&transaction, &invalid_keys));
    }

    #[test]
    fn test_protobuf_to_transaction() {
        let mut t = transaction_proto::SignedTransaction::new();

    }
}
