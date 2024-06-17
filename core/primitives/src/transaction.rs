use crate::errors::TxExecutionError;
use crate::hash::{hash, CryptoHash};
use crate::merkle::MerklePath;
use crate::profile_data_v3::ProfileDataV3;
use crate::types::{AccountId, Balance, Gas, Nonce};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_fmt::{AbbrBytes, Slice};
use near_primitives_core::serialize::{from_base64, to_base64};
use near_primitives_core::types::Compute;
use serde::de::Error as DecodeError;
use serde::ser::Error as EncodeError;
use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::{Error, ErrorKind, Read, Write};

#[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
pub use crate::action::NonrefundableStorageTransferAction;
pub use crate::action::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, FunctionCallAction, StakeAction, TransferAction,
};

pub type LogEntry = String;

#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, PartialEq, Eq, Debug, Clone)]
pub struct TransactionV0 {
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

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Debug, Clone)]
pub struct TransactionV1 {
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
    /// Priority fee. Unit is 10^12 yotcoNEAR
    pub priority_fee: u64,
}

impl Transaction {
    /// Computes a hash of the transaction for signing and size of serialized transaction
    pub fn get_hash_and_size(&self) -> (CryptoHash, u64) {
        let bytes = borsh::to_vec(&self).expect("Failed to deserialize");
        (hash(&bytes), bytes.len() as u64)
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum Transaction {
    V0(TransactionV0),
    V1(TransactionV1),
}

impl Transaction {
    pub fn signer_id(&self) -> &AccountId {
        match self {
            Transaction::V0(tx) => &tx.signer_id,
            Transaction::V1(tx) => &tx.signer_id,
        }
    }

    pub fn receiver_id(&self) -> &AccountId {
        match self {
            Transaction::V0(tx) => &tx.receiver_id,
            Transaction::V1(tx) => &tx.receiver_id,
        }
    }

    pub fn public_key(&self) -> &PublicKey {
        match self {
            Transaction::V0(tx) => &tx.public_key,
            Transaction::V1(tx) => &tx.public_key,
        }
    }

    pub fn nonce(&self) -> Nonce {
        match self {
            Transaction::V0(tx) => tx.nonce,
            Transaction::V1(tx) => tx.nonce,
        }
    }

    pub fn actions(&self) -> &[Action] {
        match self {
            Transaction::V0(tx) => &tx.actions,
            Transaction::V1(tx) => &tx.actions,
        }
    }

    pub fn take_actions(self) -> Vec<Action> {
        match self {
            Transaction::V0(tx) => tx.actions,
            Transaction::V1(tx) => tx.actions,
        }
    }

    pub fn block_hash(&self) -> &CryptoHash {
        match self {
            Transaction::V0(tx) => &tx.block_hash,
            Transaction::V1(tx) => &tx.block_hash,
        }
    }

    pub fn priority_fee(&self) -> Option<u64> {
        match self {
            Transaction::V0(_) => None,
            Transaction::V1(tx) => Some(tx.priority_fee),
        }
    }
}

impl BorshSerialize for Transaction {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        match self {
            Transaction::V0(tx) => tx.serialize(writer)?,
            Transaction::V1(tx) => {
                BorshSerialize::serialize(&1_u8, writer)?;
                tx.serialize(writer)?;
            }
        }
        Ok(())
    }
}

impl BorshDeserialize for Transaction {
    /// Deserialize based on the first and second bytes of the stream. For V0, we do backward compatible deserialization by deserializing
    /// the entire stream into V0. For V1, we consume the first byte and then deserialize the rest.
    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let u1 = u8::deserialize_reader(reader)?;
        let u2 = u8::deserialize_reader(reader)?;
        let u3 = u8::deserialize_reader(reader)?;
        let u4 = u8::deserialize_reader(reader)?;
        // This is a ridiculous hackery: because the first field in `TransactionV0` is an `AccountId`
        // and an account id is at most 64 bytes, for all valid `TransactionV0` the second byte must be 0
        // because of the littel endian encoding of the length of the account id.
        // On the other hand, for `TransactionV1`, since the first byte is 1 and an account id must have nonzero
        // length, so the second byte must not be zero. Therefore, we can distinguish between the two versions
        // by looking at the second byte.

        let read_signer_id = |buf: [u8; 4], reader: &mut R| -> std::io::Result<AccountId> {
            let str_len = u32::from_le_bytes(buf);
            let mut str_vec = Vec::with_capacity(str_len as usize);
            for _ in 0..str_len {
                str_vec.push(u8::deserialize_reader(reader)?);
            }
            AccountId::try_from(String::from_utf8(str_vec).map_err(|_| {
                Error::new(ErrorKind::InvalidData, "Failed to parse AccountId from bytes")
            })?)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))
        };

        if u2 == 0 {
            let signer_id = read_signer_id([u1, u2, u3, u4], reader)?;
            let public_key = PublicKey::deserialize_reader(reader)?;
            let nonce = Nonce::deserialize_reader(reader)?;
            let receiver_id = AccountId::deserialize_reader(reader)?;
            let block_hash = CryptoHash::deserialize_reader(reader)?;
            let actions = Vec::<Action>::deserialize_reader(reader)?;
            Ok(Transaction::V0(TransactionV0 {
                signer_id,
                public_key,
                nonce,
                receiver_id,
                block_hash,
                actions,
            }))
        } else {
            let u5 = u8::deserialize_reader(reader)?;
            let signer_id = read_signer_id([u2, u3, u4, u5], reader)?;
            let public_key = PublicKey::deserialize_reader(reader)?;
            let nonce = Nonce::deserialize_reader(reader)?;
            let receiver_id = AccountId::deserialize_reader(reader)?;
            let block_hash = CryptoHash::deserialize_reader(reader)?;
            let actions = Vec::<Action>::deserialize_reader(reader)?;
            let priority_fee = u64::deserialize_reader(reader)?;
            Ok(Transaction::V1(TransactionV1 {
                signer_id,
                public_key,
                nonce,
                receiver_id,
                block_hash,
                actions,
                priority_fee,
            }))
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Eq, Debug, Clone)]
#[borsh(init=init)]
pub struct SignedTransaction {
    pub transaction: Transaction,
    pub signature: Signature,
    #[borsh(skip)]
    hash: CryptoHash,
    #[borsh(skip)]
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

impl serde::Serialize for SignedTransaction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let signed_tx_borsh = borsh::to_vec(self).map_err(|err| {
            S::Error::custom(&format!("the value could not be borsh encoded due to: {}", err))
        })?;
        let signed_tx_base64 = to_base64(&signed_tx_borsh);
        serializer.serialize_str(&signed_tx_base64)
    }
}

impl<'de> serde::Deserialize<'de> for SignedTransaction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let signed_tx_base64 = <String as serde::Deserialize>::deserialize(deserializer)?;
        let signed_tx_borsh = from_base64(&signed_tx_base64).map_err(|err| {
            D::Error::custom(&format!("the value could not decoded from base64 due to: {}", err))
        })?;
        borsh::from_slice::<Self>(&signed_tx_borsh).map_err(|err| {
            D::Error::custom(&format!("the value could not decoded from borsh due to: {}", err))
        })
    }
}

/// The status of execution for a transaction or a receipt.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Default)]
pub enum ExecutionStatus {
    /// The execution is pending or unknown.
    #[default]
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
                f.write_fmt(format_args!("SuccessValue({})", AbbrBytes(v)))
            }
            ExecutionStatus::SuccessReceiptId(receipt_id) => {
                f.write_fmt(format_args!("SuccessReceiptId({})", receipt_id))
            }
        }
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
    /// The amount of compute time spent by the given transaction or receipt.
    // TODO(#8859): Treat this field in the same way as `gas_burnt`.
    // At the moment this field is only set at runtime and is not persisted in the database.
    // This means that when execution outcomes are read from the database, this value will not be
    // set and any code that attempts to use it will crash.
    #[borsh(skip)]
    pub compute_usage: Option<Compute>,
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

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Clone, Eq, Debug, Default)]
pub enum ExecutionMetadata {
    /// V1: Empty Metadata
    #[default]
    V1,
    /// V2: With ProfileData by legacy `Cost` enum
    V2(crate::profile_data_v2::ProfileDataV2),
    /// V3: With ProfileData by gas parameters
    V3(Box<ProfileDataV3>),
}

impl fmt::Debug for ExecutionOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionOutcome")
            .field("logs", &Slice(&self.logs))
            .field("receipt_ids", &Slice(&self.receipt_ids))
            .field("burnt_gas", &self.gas_burnt)
            .field("compute_usage", &self.compute_usage.unwrap_or_default())
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
#[derive(Clone, BorshSerialize, BorshDeserialize, Debug)]
pub struct ExecutionOutcomeWithProof {
    pub proof: MerklePath,
    pub outcome: ExecutionOutcome,
}
#[cfg(all(test, feature = "rand"))]

mod tests {
    use super::*;
    use crate::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
    use borsh::BorshDeserialize;
    use near_crypto::{InMemorySigner, KeyType, Signature, Signer};

    #[test]
    fn test_verify_transaction() {
        let signer: Signer =
            InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519).into();
        let transaction = Transaction::V0(TransactionV0 {
            signer_id: "test".parse().unwrap(),
            public_key: signer.public_key(),
            nonce: 0,
            receiver_id: "test".parse().unwrap(),
            block_hash: Default::default(),
            actions: vec![],
        })
        .sign(&signer);
        let wrong_public_key = PublicKey::from_seed(KeyType::ED25519, "wrong");
        let valid_keys = vec![signer.public_key(), wrong_public_key.clone()];
        assert!(verify_transaction_signature(&transaction, &valid_keys));

        let invalid_keys = vec![wrong_public_key];
        assert!(!verify_transaction_signature(&transaction, &invalid_keys));

        let bytes = borsh::to_vec(&transaction).unwrap();
        let decoded_tx = SignedTransaction::try_from_slice(&bytes).unwrap();
        assert!(verify_transaction_signature(&decoded_tx, &valid_keys));
    }

    fn create_transaction_v0() -> TransactionV0 {
        let public_key: PublicKey = "22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV".parse().unwrap();
        TransactionV0 {
            signer_id: "test.near".parse().unwrap(),
            public_key: public_key.clone(),
            nonce: 1,
            receiver_id: "123".parse().unwrap(),
            block_hash: Default::default(),
            actions: vec![
                Action::CreateAccount(CreateAccountAction {}),
                Action::DeployContract(DeployContractAction { code: vec![1, 2, 3] }),
                Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "qqq".to_string(),
                    args: vec![1, 2, 3],
                    gas: 1_000,
                    deposit: 1_000_000,
                })),
                Action::Transfer(TransferAction { deposit: 123 }),
                Action::Stake(Box::new(StakeAction {
                    public_key: public_key.clone(),
                    stake: 1_000_000,
                })),
                Action::AddKey(Box::new(AddKeyAction {
                    public_key: public_key.clone(),
                    access_key: AccessKey {
                        nonce: 0,
                        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                            allowance: None,
                            receiver_id: "zzz".parse().unwrap(),
                            method_names: vec!["www".to_string()],
                        }),
                    },
                })),
                Action::DeleteKey(Box::new(DeleteKeyAction { public_key })),
                Action::DeleteAccount(DeleteAccountAction {
                    beneficiary_id: "123".parse().unwrap(),
                }),
            ],
        }
    }

    fn create_transaction_v1() -> TransactionV1 {
        let public_key: PublicKey = "22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV".parse().unwrap();
        TransactionV1 {
            signer_id: "test.near".parse().unwrap(),
            public_key: public_key.clone(),
            nonce: 1,
            receiver_id: "123".parse().unwrap(),
            block_hash: Default::default(),
            actions: vec![
                Action::CreateAccount(CreateAccountAction {}),
                Action::DeployContract(DeployContractAction { code: vec![1, 2, 3] }),
                Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "qqq".to_string(),
                    args: vec![1, 2, 3],
                    gas: 1_000,
                    deposit: 1_000_000,
                })),
                Action::Transfer(TransferAction { deposit: 123 }),
                Action::Stake(Box::new(StakeAction {
                    public_key: public_key.clone(),
                    stake: 1_000_000,
                })),
                Action::AddKey(Box::new(AddKeyAction {
                    public_key: public_key.clone(),
                    access_key: AccessKey {
                        nonce: 0,
                        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                            allowance: None,
                            receiver_id: "zzz".parse().unwrap(),
                            method_names: vec!["www".to_string()],
                        }),
                    },
                })),
                Action::DeleteKey(Box::new(DeleteKeyAction { public_key })),
                Action::DeleteAccount(DeleteAccountAction {
                    beneficiary_id: "123".parse().unwrap(),
                }),
            ],
            priority_fee: 1,
        }
    }

    /// This test is change checker for a reason - we don't expect transaction format to change.
    /// If it does - you MUST update all of the dependencies: like nearlib and other clients.
    #[test]
    fn test_serialize_transaction() {
        let transaction = Transaction::V0(create_transaction_v0());
        let signed_tx = SignedTransaction::new(Signature::empty(KeyType::ED25519), transaction);
        let new_signed_tx =
            SignedTransaction::try_from_slice(&borsh::to_vec(&signed_tx).unwrap()).unwrap();

        assert_eq!(
            new_signed_tx.get_hash().to_string(),
            "4GXvjMFN6wSxnU9jEVT8HbXP5Yk6yELX9faRSKp6n9fX"
        );
    }

    #[test]
    fn test_serialize_transaction_versions() {
        let transaction_v0 = Transaction::V0(create_transaction_v0());
        let serialized_tx_v0 = borsh::to_vec(&transaction_v0).unwrap();
        let deserialized_tx_v0 = Transaction::try_from_slice(&serialized_tx_v0).unwrap();
        assert_eq!(transaction_v0, deserialized_tx_v0);

        let transaction_v1 = Transaction::V1(create_transaction_v1());
        let serialized_tx_v1 = borsh::to_vec(&transaction_v1).unwrap();
        let deserialized_tx_v1 = Transaction::try_from_slice(&serialized_tx_v1).unwrap();
        assert_eq!(transaction_v1, deserialized_tx_v1);
    }

    #[test]
    fn test_outcome_to_hashes() {
        let outcome = ExecutionOutcome {
            status: ExecutionStatus::SuccessValue(vec![123]),
            logs: vec!["123".to_string(), "321".to_string()],
            receipt_ids: vec![],
            gas_burnt: 123,
            compute_usage: Some(456),
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
}
