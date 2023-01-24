use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::{Error, ErrorKind};

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::BlockHeight;
use serde::{Deserialize, Serialize};

use near_crypto::{PublicKey, Signature};
use near_o11y::pretty;
use near_primitives_core::profile::{ProfileDataV2, ProfileDataV3};

use crate::account::AccessKey;
use crate::errors::TxExecutionError;
use crate::hash::{hash, CryptoHash};
use crate::merkle::MerklePath;
use crate::serialize::{base64_format, dec_format};
use crate::types::{AccountId, Balance, Gas, Nonce};

pub type LogEntry = String;

// This is an index number of Action::Delegate in Action enumeration
const ACTION_DELEGATE_NUMBER: u8 = 8;

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
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
    /// Computes a hash of the transaction for signing and size of serialized transaction
    pub fn get_hash_and_size(&self) -> (CryptoHash, u64) {
        let bytes = self.try_to_vec().expect("Failed to deserialize");
        (hash(&bytes), bytes.len() as u64)
    }
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Debug,
    Clone,
    strum::AsRefStr,
)]
pub enum Action {
    /// Create an (sub)account using a transaction `receiver_id` as an ID for
    /// a new account ID must pass validation rules described here
    /// <http://nomicon.io/Primitives/Account.html>.
    CreateAccount(CreateAccountAction),
    /// Sets a Wasm code to a receiver_id
    DeployContract(DeployContractAction),
    FunctionCall(FunctionCallAction),
    Transfer(TransferAction),
    Stake(StakeAction),
    AddKey(AddKeyAction),
    DeleteKey(DeleteKeyAction),
    DeleteAccount(DeleteAccountAction),
    #[cfg(feature = "protocol_feature_nep366_delegate_action")]
    Delegate(SignedDelegateAction),
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
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct CreateAccountAction {}

impl From<CreateAccountAction> for Action {
    fn from(create_account_action: CreateAccountAction) -> Self {
        Self::CreateAccount(create_account_action)
    }
}

/// Deploy contract action
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DeployContractAction {
    /// WebAssembly binary
    #[serde(with = "base64_format")]
    pub code: Vec<u8>,
}

impl From<DeployContractAction> for Action {
    fn from(deploy_contract_action: DeployContractAction) -> Self {
        Self::DeployContract(deploy_contract_action)
    }
}

impl fmt::Debug for DeployContractAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeployContractAction")
            .field("code", &format_args!("{}", pretty::AbbrBytes(&self.code)))
            .finish()
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FunctionCallAction {
    pub method_name: String,
    #[serde(with = "base64_format")]
    pub args: Vec<u8>,
    pub gas: Gas,
    #[serde(with = "dec_format")]
    pub deposit: Balance,
}

impl From<FunctionCallAction> for Action {
    fn from(function_call_action: FunctionCallAction) -> Self {
        Self::FunctionCall(function_call_action)
    }
}

impl fmt::Debug for FunctionCallAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FunctionCallAction")
            .field("method_name", &format_args!("{}", &self.method_name))
            .field("args", &format_args!("{}", pretty::AbbrBytes(&self.args)))
            .field("gas", &format_args!("{}", &self.gas))
            .field("deposit", &format_args!("{}", &self.deposit))
            .finish()
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct TransferAction {
    #[serde(with = "dec_format")]
    pub deposit: Balance,
}

impl From<TransferAction> for Action {
    fn from(transfer_action: TransferAction) -> Self {
        Self::Transfer(transfer_action)
    }
}

/// An action which stakes signer_id tokens and setup's validator public key
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct StakeAction {
    /// Amount of tokens to stake.
    #[serde(with = "dec_format")]
    pub stake: Balance,
    /// Validator key which will be used to sign transactions on behalf of signer_id
    pub public_key: PublicKey,
}

impl From<StakeAction> for Action {
    fn from(stake_action: StakeAction) -> Self {
        Self::Stake(stake_action)
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct AddKeyAction {
    /// A public key which will be associated with an access_key
    pub public_key: PublicKey,
    /// An access key with the permission
    pub access_key: AccessKey,
}

impl From<AddKeyAction> for Action {
    fn from(add_key_action: AddKeyAction) -> Self {
        Self::AddKey(add_key_action)
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct DeleteKeyAction {
    /// A public key associated with the access_key to be deleted.
    pub public_key: PublicKey,
}

impl From<DeleteKeyAction> for Action {
    fn from(delete_key_action: DeleteKeyAction) -> Self {
        Self::DeleteKey(delete_key_action)
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct DeleteAccountAction {
    pub beneficiary_id: AccountId,
}

impl From<DeleteAccountAction> for Action {
    fn from(delete_account_action: DeleteAccountAction) -> Self {
        Self::DeleteAccount(delete_account_action)
    }
}

/// This is Action which mustn't contain DelegateAction.
// This struct is needed to avoid the recursion when Action/DelegateAction is deserialized.
#[derive(Serialize, BorshSerialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct NonDelegateAction(pub Action);

impl From<NonDelegateAction> for Action {
    fn from(action: NonDelegateAction) -> Self {
        action.0
    }
}

impl borsh::de::BorshDeserialize for NonDelegateAction {
    fn deserialize(buf: &mut &[u8]) -> ::core::result::Result<Self, borsh::maybestd::io::Error> {
        if buf.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Failed to deserialize DelegateAction",
            ));
        }
        match buf[0] {
            ACTION_DELEGATE_NUMBER => Err(Error::new(
                ErrorKind::InvalidInput,
                "DelegateAction mustn't contain a nested one",
            )),
            _ => Ok(Self(borsh::BorshDeserialize::deserialize(buf)?)),
        }
    }
}

/// This action allows to execute the inner actions behalf of the defined sender.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct DelegateAction {
    /// Signer of the delegated actions
    pub sender_id: AccountId,
    /// Receiver of the delegated actions.
    pub receiver_id: AccountId,
    /// List of actions to be executed.
    pub actions: Vec<NonDelegateAction>,
    /// Nonce to ensure that the same delegate action is not sent twice by a relayer and should match for given account's `public_key`.
    /// After this action is processed it will increment.
    pub nonce: Nonce,
    /// The maximal height of the block in the blockchain below which the given DelegateAction is valid.
    pub max_block_height: BlockHeight,
    /// Public key that is used to sign this delegated action.
    pub public_key: PublicKey,
}

#[cfg_attr(feature = "protocol_feature_nep366_delegate_action", derive(BorshDeserialize))]
#[derive(BorshSerialize, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct SignedDelegateAction {
    pub delegate_action: DelegateAction,
    pub signature: Signature,
}

#[cfg(not(feature = "protocol_feature_nep366_delegate_action"))]
impl borsh::de::BorshDeserialize for SignedDelegateAction {
    fn deserialize(_buf: &mut &[u8]) -> ::core::result::Result<Self, borsh::maybestd::io::Error> {
        return Err(Error::new(ErrorKind::InvalidInput, "Delegate action isn't supported"));
    }
}

impl SignedDelegateAction {
    pub fn verify(&self) -> bool {
        let delegate_action = &self.delegate_action;
        let hash = delegate_action.get_hash();
        let public_key = &delegate_action.public_key;

        self.signature.verify(hash.as_ref(), public_key)
    }
}

#[cfg(feature = "protocol_feature_nep366_delegate_action")]
impl From<SignedDelegateAction> for Action {
    fn from(delegate_action: SignedDelegateAction) -> Self {
        Self::Delegate(delegate_action)
    }
}

impl DelegateAction {
    pub fn get_actions(&self) -> Vec<Action> {
        self.actions.iter().map(|a| a.clone().into()).collect()
    }

    pub fn get_hash(&self) -> CryptoHash {
        let bytes = self.try_to_vec().expect("Failed to deserialize");
        hash(&bytes)
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Eq, Debug, Clone)]
#[borsh_init(init)]
pub struct SignedTransaction {
    pub transaction: Transaction,
    pub signature: Signature,
    #[borsh_skip]
    hash: CryptoHash,
    #[borsh_skip]
    size: u64,
}

impl SignedTransaction {
    pub fn new(signature: Signature, transaction: Transaction) -> Self {
        let mut signed_tx =
            Self { signature, transaction, hash: CryptoHash::default(), size: u64::default() };
        signed_tx.init();
        signed_tx
    }

    pub fn init(&mut self) {
        let (hash, size) = self.transaction.get_hash_and_size();
        self.hash = hash;
        self.size = size;
    }

    pub fn get_hash(&self) -> CryptoHash {
        self.hash
    }

    pub fn get_size(&self) -> u64 {
        self.size
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
    Failure(TxExecutionError),
    /// The final action succeeded and returned some value or an empty vec.
    SuccessValue(Vec<u8>),
    /// The final action of the receipt returned a promise or the signed transaction was converted
    /// to a receipt. Contains the receipt_id of the generated receipt.
    SuccessReceiptId(CryptoHash),
}

impl fmt::Debug for ExecutionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionStatus::Unknown => f.write_str("Unknown"),
            ExecutionStatus::Failure(e) => f.write_fmt(format_args!("Failure({})", e)),
            ExecutionStatus::SuccessValue(v) => {
                f.write_fmt(format_args!("SuccessValue({})", pretty::AbbrBytes(v)))
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

/// ExecutionOutcome for proof. Excludes logs and metadata
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Clone)]
pub struct PartialExecutionOutcome {
    pub receipt_ids: Vec<CryptoHash>,
    pub gas_burnt: Gas,
    pub tokens_burnt: Balance,
    pub executor_id: AccountId,
    pub status: PartialExecutionStatus,
}

impl From<&ExecutionOutcome> for PartialExecutionOutcome {
    fn from(outcome: &ExecutionOutcome) -> Self {
        Self {
            receipt_ids: outcome.receipt_ids.clone(),
            gas_burnt: outcome.gas_burnt,
            tokens_burnt: outcome.tokens_burnt,
            executor_id: outcome.executor_id.clone(),
            status: outcome.status.clone().into(),
        }
    }
}

/// ExecutionStatus for proof. Excludes failure debug info.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Clone)]
pub enum PartialExecutionStatus {
    Unknown,
    Failure,
    SuccessValue(Vec<u8>),
    SuccessReceiptId(CryptoHash),
}

impl From<ExecutionStatus> for PartialExecutionStatus {
    fn from(status: ExecutionStatus) -> PartialExecutionStatus {
        match status {
            ExecutionStatus::Unknown => PartialExecutionStatus::Unknown,
            ExecutionStatus::Failure(_) => PartialExecutionStatus::Failure,
            ExecutionStatus::SuccessValue(value) => PartialExecutionStatus::SuccessValue(value),
            ExecutionStatus::SuccessReceiptId(id) => PartialExecutionStatus::SuccessReceiptId(id),
        }
    }
}

/// Execution outcome for one signed transaction or one receipt.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Clone, smart_default::SmartDefault, Eq)]
pub struct ExecutionOutcome {
    /// Logs from this transaction or receipt.
    pub logs: Vec<LogEntry>,
    /// Receipt IDs generated by this transaction or receipt.
    pub receipt_ids: Vec<CryptoHash>,
    /// The amount of the gas burnt by the given transaction or receipt.
    pub gas_burnt: Gas,
    /// The amount of tokens burnt corresponding to the burnt gas amount.
    /// This value doesn't always equal to the `gas_burnt` multiplied by the gas price, because
    /// the prepaid gas price might be lower than the actual gas price and it creates a deficit.
    pub tokens_burnt: Balance,
    /// The id of the account on which the execution happens. For transaction this is signer_id,
    /// for receipt this is receiver_id.
    #[default("test".parse().unwrap())]
    pub executor_id: AccountId,
    /// Execution status. Contains the result in case of successful execution.
    /// NOTE: Should be the latest field since it contains unparsable by light client
    /// ExecutionStatus::Failure
    pub status: ExecutionStatus,
    /// Execution metadata, versioned
    pub metadata: ExecutionMetadata,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Clone, Eq, Debug)]
pub enum ExecutionMetadata {
    /// V1: Empty Metadata
    V1,
    /// V2: With ProfileData by legacy `Cost` enum
    V2(ProfileDataV2),
    // V3: With ProfileData by gas parameters
    V3(ProfileDataV3),
}

impl Default for ExecutionMetadata {
    fn default() -> Self {
        ExecutionMetadata::V1
    }
}

impl fmt::Debug for ExecutionOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionOutcome")
            .field("logs", &pretty::Slice(&self.logs))
            .field("receipt_ids", &pretty::Slice(&self.receipt_ids))
            .field("burnt_gas", &self.gas_burnt)
            .field("tokens_burnt", &self.tokens_burnt)
            .field("status", &self.status)
            .field("metadata", &self.metadata)
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
    /// Should be the latest field since contains unparsable by light client ExecutionStatus::Failure
    pub outcome: ExecutionOutcome,
}

impl ExecutionOutcomeWithId {
    pub fn to_hashes(&self) -> Vec<CryptoHash> {
        let mut result = Vec::with_capacity(2 + self.outcome.logs.len());
        result.push(self.id);
        result.push(CryptoHash::hash_borsh(PartialExecutionOutcome::from(&self.outcome)));
        result.extend(self.outcome.logs.iter().map(|log| hash(log.as_bytes())));
        result
    }
}

/// Execution outcome with path from it to the outcome root and ID.
#[derive(PartialEq, Clone, Default, Debug, BorshSerialize, BorshDeserialize, Eq)]
pub struct ExecutionOutcomeWithIdAndProof {
    pub proof: MerklePath,
    pub block_hash: CryptoHash,
    /// Should be the latest field since contains unparsable by light client ExecutionStatus::Failure
    pub outcome_with_id: ExecutionOutcomeWithId,
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
    public_keys.iter().any(|key| transaction.signature.verify(hash, key))
}

/// A more compact struct, just for storage.
#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub struct ExecutionOutcomeWithProof {
    pub proof: MerklePath,
    pub outcome: ExecutionOutcome,
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::{AccessKeyPermission, FunctionCallPermission};
    use borsh::BorshDeserialize;
    use near_crypto::{InMemorySigner, KeyType, Signature, Signer};

    /// A serialized `Action::Delegate(SignedDelegateAction)` for testing.
    ///
    /// We want this to be parseable and accepted by protocol versions with meta
    /// transactions enabled. But it should fail either in parsing or in
    /// validation when this is included in a receipt for a block of an earlier
    /// version. For now, it just fails to parse, as a test below checks.
    const DELEGATE_ACTION_HEX: &str = concat!(
        "0803000000616161030000006262620100000000010000000000000002000000000000",
        "0000000000000000000000000000000000000000000000000000000000000000000000",
        "0000000000000000000000000000000000000000000000000000000000000000000000",
        "0000000000000000000000000000000000000000000000000000000000"
    );

    #[test]
    fn test_verify_transaction() {
        let signer = InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519);
        let transaction = Transaction {
            signer_id: "test".parse().unwrap(),
            public_key: signer.public_key(),
            nonce: 0,
            receiver_id: "test".parse().unwrap(),
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
        let public_key: PublicKey = "22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV".parse().unwrap();
        let transaction = Transaction {
            signer_id: "test.near".parse().unwrap(),
            public_key: public_key.clone(),
            nonce: 1,
            receiver_id: "123".parse().unwrap(),
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
                            receiver_id: "zzz".parse().unwrap(),
                            method_names: vec!["www".to_string()],
                        }),
                    },
                }),
                Action::DeleteKey(DeleteKeyAction { public_key }),
                Action::DeleteAccount(DeleteAccountAction {
                    beneficiary_id: "123".parse().unwrap(),
                }),
            ],
        };
        let signed_tx = SignedTransaction::new(Signature::empty(KeyType::ED25519), transaction);
        let new_signed_tx =
            SignedTransaction::try_from_slice(&signed_tx.try_to_vec().unwrap()).unwrap();

        assert_eq!(
            new_signed_tx.get_hash().to_string(),
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
            tokens_burnt: 1234000,
            executor_id: "alice".parse().unwrap(),
            metadata: ExecutionMetadata::V1,
        };
        let id = CryptoHash([42u8; 32]);
        let outcome = ExecutionOutcomeWithId { id, outcome };
        assert_eq!(
            vec![
                id,
                "5JQs5ekQqKudMmYejuccbtEu1bzhQPXa92Zm4HdV64dQ".parse().unwrap(),
                hash("123".as_bytes()),
                hash("321".as_bytes()),
            ],
            outcome.to_hashes()
        );
    }

    #[cfg(feature = "protocol_feature_nep366_delegate_action")]
    fn create_delegate_action(actions: Vec<Action>) -> Action {
        Action::Delegate(SignedDelegateAction {
            delegate_action: DelegateAction {
                sender_id: "aaa".parse().unwrap(),
                receiver_id: "bbb".parse().unwrap(),
                actions: actions.iter().map(|a| NonDelegateAction(a.clone())).collect(),
                nonce: 1,
                max_block_height: 2,
                public_key: PublicKey::empty(KeyType::ED25519),
            },
            signature: Signature::empty(KeyType::ED25519),
        })
    }

    #[test]
    #[cfg(feature = "protocol_feature_nep366_delegate_action")]
    fn test_delegate_action_deserialization() {
        // Expected an error. Buffer is empty
        assert_eq!(
            NonDelegateAction::try_from_slice(Vec::new().as_ref()).map_err(|e| e.kind()),
            Err(ErrorKind::InvalidInput)
        );

        let delegate_action = create_delegate_action(Vec::<Action>::new());
        let serialized_non_delegate_action =
            create_delegate_action(vec![delegate_action]).try_to_vec().expect("Expect ok");

        // Expected Action::Delegate has not been moved in enum Action
        assert_eq!(serialized_non_delegate_action[0], ACTION_DELEGATE_NUMBER);

        // Expected a nested DelegateAction error
        assert_eq!(
            NonDelegateAction::try_from_slice(&serialized_non_delegate_action)
                .map_err(|e| e.kind()),
            Err(ErrorKind::InvalidInput)
        );

        let delegate_action =
            create_delegate_action(vec![Action::CreateAccount(CreateAccountAction {})]);
        let serialized_delegate_action = delegate_action.try_to_vec().expect("Expect ok");

        // Valid action
        assert_eq!(
            Action::try_from_slice(&serialized_delegate_action).expect("Expect ok"),
            delegate_action
        );
    }

    /// Check that we will not accept delegate actions with the feature
    /// disabled.
    ///
    /// This test is to ensure that while working on meta transactions, we don't
    /// accientally start accepting delegate actions in receipts. Otherwise, a
    /// malicious validator could create receipts that include delegate actions
    /// and other nodes will accept such a receipt.
    ///
    /// TODO: Before stabilizing "protocol_feature_nep366_delegate_action" we
    /// have to replace this rest with a test that checks that we discard
    /// delegate actions for earlier versions somewhere in validation.
    #[test]
    #[cfg(not(feature = "protocol_feature_nep366_delegate_action"))]
    fn test_delegate_action_deserialization() {
        let serialized_delegate_action = hex::decode(DELEGATE_ACTION_HEX).expect("invalid hex");

        // DelegateAction isn't supported
        assert_eq!(
            Action::try_from_slice(&serialized_delegate_action).map_err(|e| e.kind()),
            Err(ErrorKind::InvalidInput)
        );
    }

    /// Check that the hard-coded delegate action is valid.
    #[test]
    #[cfg(feature = "protocol_feature_nep366_delegate_action")]
    fn test_delegate_action_deserialization_hard_coded() {
        let serialized_delegate_action = hex::decode(DELEGATE_ACTION_HEX).expect("invalid hex");
        // The hex data is the same as the one we create below.
        let delegate_action =
            create_delegate_action(vec![Action::CreateAccount(CreateAccountAction {})]);

        // Valid action
        assert_eq!(
            Action::try_from_slice(&serialized_delegate_action).expect("Expect ok"),
            delegate_action
        );
    }
}
