use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize, Serializable};

use crate::account::AccessKey;
use crate::crypto::signature::{verify, PublicKey};
use crate::crypto::signer::EDSigner;
use crate::hash::{hash, CryptoHash};
use crate::logging;
use crate::types::{AccountId, Balance, Gas, Nonce, StructSignature};

pub type LogEntry = String;

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Debug, Clone)]
pub struct Transaction {
    pub signer_id: AccountId,
    pub public_key: PublicKey,
    pub nonce: Nonce,
    pub receiver_id: AccountId,

    pub actions: Vec<Action>,
}

impl Transaction {
    pub fn get_hash(&self) -> CryptoHash {
        let bytes = self.try_to_vec().expect("Failed to deserialize");
        hash(&bytes)
    }
}

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Debug, Clone)]
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

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Clone, Debug)]
pub struct CreateAccountAction {}

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Clone)]
pub struct DeployContractAction {
    pub code: Vec<u8>,
}

impl fmt::Debug for DeployContractAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DeployContractAction")
            .field("code", &format_args!("{}", logging::pretty_utf8(&self.code)))
            .finish()
    }
}

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Clone)]
pub struct FunctionCallAction {
    pub method_name: String,
    pub args: Vec<u8>,
    pub gas: Gas,
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

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Clone, Debug)]
pub struct TransferAction {
    pub deposit: Balance,
}

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Clone, Debug)]
pub struct StakeAction {
    pub stake: Balance,
    pub public_key: PublicKey,
}

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Clone, Debug)]
pub struct AddKeyAction {
    pub public_key: PublicKey,
    pub access_key: AccessKey,
}

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Clone, Debug)]
pub struct DeleteKeyAction {
    pub public_key: PublicKey,
}

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Clone, Debug)]
pub struct DeleteAccountAction {
    pub beneficiary_id: AccountId,
}

#[derive(BorshSerialize, BorshDeserialize, Eq, Debug, Clone)]
#[borsh_init(init)]
pub struct SignedTransaction {
    pub transaction: Transaction,
    pub signature: StructSignature,
    #[borsh_skip]
    hash: CryptoHash,
}

impl SignedTransaction {
    pub fn new(signature: StructSignature, transaction: Transaction) -> Self {
        let mut signed_tx = Self { signature, transaction, hash: CryptoHash::default() };
        signed_tx.init();
        signed_tx
    }

    pub fn init(&mut self) {
        self.hash = self.transaction.get_hash();
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

#[derive(
    BorshSerialize, BorshDeserialize, Hash, Debug, PartialEq, Eq, Clone, Serialize, Deserialize,
)]
pub enum TransactionStatus {
    Unknown,
    Completed,
    Failed,
}

impl Default for TransactionStatus {
    fn default() -> Self {
        TransactionStatus::Unknown
    }
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Clone, Default)]
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

#[derive(PartialEq, Clone, Default, Debug)]
pub struct TransactionLog {
    /// Hash of a transaction or a receipt that generated this result.
    pub hash: CryptoHash,
    pub result: TransactionResult,
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
    use std::convert::TryFrom;

    use borsh::Deserializable;

    use crate::crypto::signature::{get_key_pair, sign, DEFAULT_SIGNATURE};
    use crate::serialize::to_base;

    use super::*;
    use crate::account::{AccessKeyPermission, FunctionCallPermission};

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

        //let bytes = transaction.encode().unwrap();
        // let new_tx = SignedTransaction::decode(&bytes).unwrap().into();
        //assert!(verify_transaction_signature(&new_tx, &valid_keys));
    }

    /// This test is change checker for a reason - we don't expect transaction format to change.
    /// If it does - you MUST update all of the dependencies: like nearlib and other clients.
    #[test]
    fn test_serialize_transaction() {
        let public_key =
            PublicKey::try_from("22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV").unwrap();
        let transaction = Transaction {
            signer_id: "test.near".to_string(),
            public_key,
            nonce: 1,
            receiver_id: "123".to_string(),
            actions: vec![
                Action::CreateAccount(CreateAccountAction {}),
                Action::DeployContract(DeployContractAction { code: vec![1, 2, 3] }),
                Action::FunctionCall(FunctionCallAction {
                    method_name: "qqq".to_string(),
                    args: vec![1, 2, 3],
                    gas: 1_000,
                    deposit: 1_000_000,
                }),
                Action::Transfer(TransferAction { deposit: 123 }),
                Action::Stake(StakeAction { public_key, stake: 1_000_000 }),
                Action::AddKey(AddKeyAction {
                    public_key,
                    access_key: AccessKey {
                        nonce: 0,
                        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                            allowance: None,
                            receiver_id: "zzz".to_string(),
                            method_names: vec!["www".to_string()],
                        }),
                    },
                }),
                Action::DeleteKey(DeleteKeyAction { public_key }),
                Action::DeleteAccount(DeleteAccountAction { beneficiary_id: "123".to_string() }),
            ],
        };
        let signed_tx = SignedTransaction::new(DEFAULT_SIGNATURE, transaction);
        let new_signed_tx =
            SignedTransaction::try_from_slice(&signed_tx.try_to_vec().unwrap()).unwrap();

        assert_eq!(
            to_base(&new_signed_tx.get_hash()),
            "244ZQ9cgj3CQ6bWBdytfrJMuMQ1jdXLFGnr4HhvtCTnM"
        );
    }
}
