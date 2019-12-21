use std::fmt;
use std::hash::{Hash, Hasher};

use borsh::{BorshDeserialize, BorshSerialize};

use near_crypto::{PublicKey, Signature, Signer};

use crate::account::AccessKey;
use crate::errors::ExecutionError;
use crate::hash::{hash, CryptoHash};
use crate::logging;
use crate::merkle::MerklePath;
use crate::types::{AccountId, Balance, Gas, Nonce};
use std::borrow::Borrow;

pub type LogEntry = String;

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Debug, Clone)]
pub struct Transaction {
    /// An account on which behalf transaction is signed
    pub signer_id: AccountId,
    /// A public key of the access key which was used to sign an account.
    /// Access key holds permissions for calling certain kinds of actions.
    pub public_key: PublicKey,
    /// Nonce is used to determine order of transaction in the pool.
    /// It increments for a combination of `signer_id` and `public_key`
    pub nonce: Nonce,
    /// Receiver account for this transaction
    pub receiver_id: AccountId,
    /// The hash of the block in the blockchain on top of which the given transaction is valid
    pub block_hash: CryptoHash,
    /// A list of actions to be applied
    pub actions: Vec<Action>,
}

impl Transaction {
    /// Computes a hash of the transaction for signing
    pub fn get_hash(&self) -> CryptoHash {
        let bytes = self.try_to_vec().expect("Failed to deserialize");
        hash(&bytes)
    }
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Debug, Clone)]
pub enum Action {
    /// Create an (sub)account using a transaction `receiver_id` as an ID for a new account
    /// ID must pass validation rules described here http://nomicon.io/Primitives/Account.html
    CreateAccount(CreateAccountAction),
    /// Sets a Wasm code to a receiver_id
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

/// Create account action
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct CreateAccountAction {}

/// Deploy contract action
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone)]
pub struct DeployContractAction {
    /// WebAssembly binary
    pub code: Vec<u8>,
}

impl fmt::Debug for DeployContractAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DeployContractAction")
            .field("code", &format_args!("{}", logging::pretty_utf8(&self.code)))
            .finish()
    }
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone)]
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

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct TransferAction {
    pub deposit: Balance,
}

/// An action which stakes singer_id tokens and setup's validator public key
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct StakeAction {
    /// Amount of tokens to stake.
    pub stake: Balance,
    /// Validator key which will be used to sign transactions on behalf of singer_id
    pub public_key: PublicKey,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct AddKeyAction {
    /// A public key which will be associated with an access_key
    pub public_key: PublicKey,
    /// An access key with the permission
    pub access_key: AccessKey,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct DeleteKeyAction {
    ///
    pub public_key: PublicKey,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct DeleteAccountAction {
    pub beneficiary_id: AccountId,
}

#[derive(BorshSerialize, BorshDeserialize, Eq, Debug, Clone)]
#[borsh_init(init)]
pub struct SignedTransaction {
    pub transaction: Transaction,
    pub signature: Signature,
    #[borsh_skip]
    hash: CryptoHash,
}

impl SignedTransaction {
    pub fn new(signature: Signature, transaction: Transaction) -> Self {
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
        signer: &dyn Signer,
        actions: Vec<Action>,
        block_hash: CryptoHash,
    ) -> Self {
        Transaction {
            nonce,
            signer_id,
            public_key: signer.public_key(),
            receiver_id,
            block_hash,
            actions,
        }
        .sign(signer)
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

impl Borrow<CryptoHash> for SignedTransaction {
    fn borrow(&self) -> &CryptoHash {
        &self.hash
    }
}

/// The status of execution for a transaction or a receipt.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone)]
pub enum ExecutionStatus {
    /// The execution is pending or unknown.
    Unknown,
    /// The execution has failed with the given execution error.
    Failure(ExecutionError),
    /// The final action succeeded and returned some value or an empty vec.
    SuccessValue(Vec<u8>),
    /// The final action of the receipt returned a promise or the signed transaction was converted
    /// to a receipt. Contains the receipt_id of the generated receipt.
    SuccessReceiptId(CryptoHash),
}

impl fmt::Debug for ExecutionStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExecutionStatus::Unknown => f.write_str("Unknown"),
            ExecutionStatus::Failure(e) => f.write_fmt(format_args!("Failure({})", e)),
            ExecutionStatus::SuccessValue(v) => {
                f.write_fmt(format_args!("SuccessValue({})", logging::pretty_utf8(&v)))
            }
            ExecutionStatus::SuccessReceiptId(receipt_id) => {
                f.write_fmt(format_args!("SuccessReceiptId({})", receipt_id))
            }
        }
    }
}

impl Default for ExecutionStatus {
    fn default() -> Self {
        ExecutionStatus::Unknown
    }
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Clone, Default)]
struct PartialExecutionOutcome {
    pub status: ExecutionStatus,
    pub receipt_ids: Vec<CryptoHash>,
    pub gas_burnt: Gas,
}

/// Execution outcome for one signed transaction or one receipt.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Clone, Default, Eq)]
pub struct ExecutionOutcome {
    /// Execution status. Contains the result in case of successful execution.
    pub status: ExecutionStatus,
    /// Logs from this transaction or receipt.
    pub logs: Vec<LogEntry>,
    /// Receipt IDs generated by this transaction or receipt.
    pub receipt_ids: Vec<CryptoHash>,
    /// The amount of the gas burnt by the given transaction or receipt.
    pub gas_burnt: Gas,
}

impl ExecutionOutcome {
    pub fn to_hashes(&self) -> Vec<CryptoHash> {
        let mut result = vec![hash(
            &PartialExecutionOutcome {
                status: self.status.clone(),
                receipt_ids: self.receipt_ids.clone(),
                gas_burnt: self.gas_burnt,
            }
            .try_to_vec()
            .expect("Failed to serialize"),
        )];
        for log in self.logs.iter() {
            result.push(hash(log.as_bytes()));
        }
        result
    }
}

impl fmt::Debug for ExecutionOutcome {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ExecutionOutcome")
            .field("status", &self.status)
            .field("logs", &format_args!("{}", logging::pretty_vec(&self.logs)))
            .field("receipt_ids", &format_args!("{}", logging::pretty_vec(&self.receipt_ids)))
            .field("burnt_gas", &self.gas_burnt)
            .finish()
    }
}

/// Execution outcome with the identifier.
/// For a signed transaction, the ID is the hash of the transaction.
/// For a receipt, the ID is the receipt ID.
#[derive(PartialEq, Clone, Default, Debug, BorshSerialize, BorshDeserialize, Eq)]
pub struct ExecutionOutcomeWithId {
    /// The transaction hash or the receipt ID.
    pub id: CryptoHash,
    pub outcome: ExecutionOutcome,
}

impl ExecutionOutcomeWithId {
    pub fn to_hashes(&self) -> Vec<CryptoHash> {
        let mut result = vec![self.id];
        result.extend(self.outcome.to_hashes());
        result
    }
}

/// Execution outcome with path from it to the outcome root and ID.
#[derive(PartialEq, Clone, Default, Debug, BorshSerialize, BorshDeserialize, Eq)]
pub struct ExecutionOutcomeWithIdAndProof {
    pub outcome_with_id: ExecutionOutcomeWithId,
    pub proof: MerklePath,
    pub block_hash: CryptoHash,
}

impl ExecutionOutcomeWithIdAndProof {
    pub fn id(&self) -> &CryptoHash {
        &self.outcome_with_id.id
    }
}

pub fn verify_transaction_signature(
    transaction: &SignedTransaction,
    public_keys: &[PublicKey],
) -> bool {
    let hash = transaction.get_hash();
    let hash = hash.as_ref();
    public_keys.iter().any(|key| transaction.signature.verify(&hash, &key))
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use borsh::BorshDeserialize;

    use near_crypto::{InMemorySigner, KeyType, Signature};

    use crate::account::{AccessKeyPermission, FunctionCallPermission};
    use crate::serialize::to_base;

    use super::*;

    #[test]
    fn test_verify_transaction() {
        let signer = InMemorySigner::from_random("test".to_string(), KeyType::ED25519);
        let transaction = Transaction {
            signer_id: "".to_string(),
            public_key: signer.public_key(),
            nonce: 0,
            receiver_id: "".to_string(),
            block_hash: Default::default(),
            actions: vec![],
        }
        .sign(&signer);
        let wrong_public_key = PublicKey::from_seed(KeyType::ED25519, "wrong");
        let valid_keys = vec![signer.public_key(), wrong_public_key.clone()];
        assert!(verify_transaction_signature(&transaction, &valid_keys));

        let invalid_keys = vec![wrong_public_key];
        assert!(!verify_transaction_signature(&transaction, &invalid_keys));

        let bytes = transaction.try_to_vec().unwrap();
        let decoded_tx = SignedTransaction::try_from_slice(&bytes).unwrap();
        assert!(verify_transaction_signature(&decoded_tx, &valid_keys));
    }

    /// This test is change checker for a reason - we don't expect transaction format to change.
    /// If it does - you MUST update all of the dependencies: like nearlib and other clients.
    #[test]
    fn test_serialize_transaction() {
        let public_key: PublicKey =
            "22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV".to_string().try_into().unwrap();
        let transaction = Transaction {
            signer_id: "test.near".to_string(),
            public_key: public_key.clone(),
            nonce: 1,
            receiver_id: "123".to_string(),
            block_hash: Default::default(),
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
                Action::Stake(StakeAction { public_key: public_key.clone(), stake: 1_000_000 }),
                Action::AddKey(AddKeyAction {
                    public_key: public_key.clone(),
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
        let signed_tx = SignedTransaction::new(Signature::empty(KeyType::ED25519), transaction);
        let new_signed_tx =
            SignedTransaction::try_from_slice(&signed_tx.try_to_vec().unwrap()).unwrap();

        assert_eq!(
            to_base(&new_signed_tx.get_hash()),
            "4GXvjMFN6wSxnU9jEVT8HbXP5Yk6yELX9faRSKp6n9fX"
        );
    }

    #[test]
    fn test_outcome_to_hashes() {
        let outcome = ExecutionOutcome {
            status: ExecutionStatus::SuccessValue(vec![123]),
            logs: vec!["123".to_string(), "321".to_string()],
            receipt_ids: vec![],
            gas_burnt: 123,
        };
        let hashes = outcome.to_hashes();
        assert_eq!(hashes.len(), 3);
    }
}
