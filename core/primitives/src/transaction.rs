use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;

use crate::account::AccessKey;
use crate::crypto::signature::{verify, PublicKey, Signature};
use crate::crypto::signer::EDSigner;
use crate::hash::{hash, CryptoHash};
use crate::logging;
use crate::serialize::{
    base_bytes_format, u128_dec_format, Decode, DecodeResult, Encode, Writable, WritableResult,
};
use crate::types::{AccountId, Balance, Gas, Nonce, ShardId, StructSignature};
use serde::{Deserialize, Deserializer};
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
        let bytes = self.write().expect("Failed to deserialize");
        hash(&bytes)
    }
}

impl Writable for Transaction {
    fn write_into(&self, out: &mut Vec<u8>) -> WritableResult {
        self.signer_id.write_into(out)?;
        self.public_key.write_into(out)?;
        self.nonce.write_into(out)?;
        self.receiver_id.write_into(out)?;
        self.actions.write_into(out)?;
        Ok(())
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

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DeployContractAction {
    #[serde(with = "base_bytes_format")]
    pub code: Vec<u8>,
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

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct StakeAction {
    #[serde(with = "u128_dec_format")]
    pub stake: Balance,
    pub public_key: PublicKey,
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct AddKeyAction {
    pub public_key: PublicKey,
    pub access_key: AccessKey,
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct DeleteKeyAction {
    pub public_key: PublicKey,
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct DeleteAccountAction {
    pub beneficiary_id: AccountId,
}

#[derive(Eq, Debug, Clone, Serialize, Deserialize)]
pub struct SignedTransaction {
    pub signature: StructSignature,
    pub transaction: Transaction,
    #[serde(skip)]
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
    use crate::serialize::{to_base, Decode, Encode};

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

        let bytes = transaction.encode().unwrap();
        let new_tx = SignedTransaction::decode(&bytes).unwrap().into();
        assert!(verify_transaction_signature(&new_tx, &valid_keys));
    }

    /// This test is change checker for a reason - we don't expect transaction format to change.
    /// If it does - you MUST update all of the dependencies: like nearlib.
    #[test]
    fn test_serialize_transaction() {
        let transaction = Transaction {
            signer_id: "test.near".to_string(),
            public_key: PublicKey::try_from("22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV")
                .unwrap(),
            nonce: 1,
            receiver_id: "123".to_string(),
            actions: vec![],
        };
        let hash = to_base(&transaction.get_hash());
        assert_eq!(hash, "EsTRpLernDsH2hzznZ6wKMu1XYdyT4ynKK2H13hbMyzb");
    }
}
