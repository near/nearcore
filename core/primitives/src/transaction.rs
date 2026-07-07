pub use crate::action::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, FunctionCallAction, StakeAction, TransferAction,
};
use crate::errors::{InvalidTxError, TxExecutionError};
use crate::hash::{CryptoHash, hash};
use crate::merkle::MerklePath;
use crate::profile_data_v3::ProfileDataV3;
use crate::types::{AccountId, Balance, Gas, Nonce};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_fmt::{AbbrBytes, Slice};
use near_parameters::RuntimeConfig;
use near_primitives_core::account::AccountContract;
use near_primitives_core::serialize::{from_base64, to_base64};
use near_primitives_core::types::{Compute, NonceIndex, ProtocolVersion};
use near_primitives_core::version::ProtocolFeature;
use near_schema_checker_lib::ProtocolSchema;
#[cfg(feature = "schemars")]
use schemars::json_schema;
use serde::de::Error as DecodeError;
use serde::ser::Error as EncodeError;
use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::{Error, ErrorKind, Read, Write};

pub type LogEntry = String;

#[derive(
    BorshSerialize, BorshDeserialize, serde::Serialize, PartialEq, Eq, Debug, Clone, ProtocolSchema,
)]
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

#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    Debug,
    Clone,
    Copy,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum TransactionNonce {
    /// Simple nonce without index, used by ordinary access keys
    Nonce { nonce: Nonce },
    /// Nonce with index, used by gas keys
    GasKeyNonce { nonce: Nonce, nonce_index: NonceIndex },
}

impl TransactionNonce {
    pub fn from_nonce(nonce: Nonce) -> Self {
        TransactionNonce::Nonce { nonce }
    }

    pub fn from_nonce_and_index(nonce: Nonce, nonce_index: NonceIndex) -> Self {
        TransactionNonce::GasKeyNonce { nonce, nonce_index }
    }

    pub fn nonce(&self) -> Nonce {
        match self {
            TransactionNonce::Nonce { nonce } => *nonce,
            TransactionNonce::GasKeyNonce { nonce, .. } => *nonce,
        }
    }

    pub fn nonce_index(&self) -> Option<NonceIndex> {
        match self {
            TransactionNonce::Nonce { .. } => None,
            TransactionNonce::GasKeyNonce { nonce_index, .. } => Some(*nonce_index),
        }
    }
}

/// Controls how the transaction nonce is validated against the access key nonce.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Default,
    ProtocolSchema,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum NonceMode {
    /// Any nonce strictly greater than the current access key nonce (default behavior).
    #[default]
    Monotonic,
    /// Nonce must be exactly `ak_nonce + 1` (sequential ordering).
    Strict,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Debug, Clone, ProtocolSchema)]
pub struct TransactionV1 {
    /// An account on which behalf transaction is signed
    pub signer_id: AccountId,
    /// A public key of the access key which was used to sign an account.
    /// Access key holds permissions for calling certain kinds of actions.
    pub public_key: PublicKey,
    /// Nonce is used to determine order of transaction in the pool.
    /// It increments for a combination of `signer_id` and `public_key`,
    /// and for gas key it also includes a `nonce_index`.
    pub nonce: TransactionNonce,
    /// Receiver account for this transaction
    pub receiver_id: AccountId,
    /// The hash of the block in the blockchain on top of which the given transaction is valid
    pub block_hash: CryptoHash,
    /// A list of actions to be applied
    pub actions: Vec<Action>,
    /// Controls nonce validation mode (monotonic or strict sequential).
    pub nonce_mode: NonceMode,
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

    pub fn nonce(&self) -> TransactionNonce {
        match self {
            Transaction::V0(tx) => TransactionNonce::from_nonce(tx.nonce),
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

    /// Check if this transaction version requires the GasKeys protocol feature to be enabled.
    pub fn gas_keys_required(&self) -> bool {
        match self {
            Transaction::V0(_) => false,
            Transaction::V1(_) => true,
        }
    }

    pub fn nonce_mode(&self) -> NonceMode {
        match self {
            Transaction::V0(_) => NonceMode::Monotonic,
            Transaction::V1(tx) => tx.nonce_mode,
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
    /// Deserialize based on the first and second bytes of the stream. For V0, we do backward
    /// compatible deserialization by deserializing the entire stream into V0. For V1, we consume
    /// the first byte and then deserialize the rest.
    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        // Read the first two bytes in order to discriminate between V0 and V1.
        //
        // The first field in `TransactionV0` is an `AccountId` whose borsh encoding starts with
        // a 4-byte little-endian length. Since an AccountId is at most 64 bytes, the second byte
        // of the length is always 0.
        //
        // `TransactionV1` is prefixed with a 1_u8 tag byte followed by the borsh-encoded
        // `TransactionV1` struct, whose first field is also an `AccountId` with nonzero length,
        // making the second byte nonzero.
        //
        // Therefore u2 == 0 implies V0 and u1 == 1 with u2 != 0 implies V1.
        let u1 = u8::deserialize_reader(reader)?;
        let u2 = u8::deserialize_reader(reader)?;

        if u2 == 0 {
            // V0: put both bytes back and deserialize TransactionV0.
            let prefix = [u1, u2];
            let mut reader = prefix.chain(reader);
            let tx = TransactionV0::deserialize_reader(&mut reader)?;
            return Ok(Transaction::V0(tx));
        }

        if u1 == 1 {
            // V1: u1 is the version tag, u2 is the first byte of TransactionV1.
            // Put u2 back and deserialize TransactionV1.
            let prefix = [u2];
            let mut reader = prefix.chain(reader);
            let tx = TransactionV1::deserialize_reader(&mut reader)?;
            return Ok(Transaction::V1(tx));
        }

        Err(Error::new(ErrorKind::InvalidData, format!("invalid transaction version tag: {}", u1)))
    }
}

/// Using the new type pattern to construct a type of signed transaction that is
/// guaranteed to have various checks performed on it.  In particular, ensure
/// that the signature is verified and the max transaction size checks have been
/// conducted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedTransaction(SignedTransaction);

impl ValidatedTransaction {
    #[allow(clippy::result_large_err)]
    pub fn new(
        config: &RuntimeConfig,
        signed_tx: SignedTransaction,
        protocol_version: ProtocolVersion,
    ) -> Result<Self, (InvalidTxError, SignedTransaction)> {
        match Self::check_valid_for_config(config, &signed_tx, protocol_version) {
            Ok(()) => {}
            Err(err) => return Err((err, signed_tx)),
        }

        if !signed_tx
            .signature
            .verify(signed_tx.get_hash().as_ref(), signed_tx.transaction.public_key())
        {
            return Err((InvalidTxError::InvalidSignature, signed_tx));
        }
        Ok(Self(signed_tx))
    }

    /// Performs validity checks that depend on the runtime config, but does not
    /// check the signature.
    pub fn check_valid_for_config(
        config: &RuntimeConfig,
        signed_tx: &SignedTransaction,
        protocol_version: ProtocolVersion,
    ) -> Result<(), InvalidTxError> {
        if !ProtocolFeature::GasKeys.enabled(protocol_version)
            && signed_tx.transaction.gas_keys_required()
        {
            return Err(InvalidTxError::InvalidTransactionVersion);
        }
        if signed_tx.transaction.nonce_mode() == NonceMode::Strict
            && !ProtocolFeature::StrictNonce.enabled(protocol_version)
        {
            return Err(InvalidTxError::InvalidTransactionVersion);
        }
        // Reject ML-DSA-65 transactions on pre-PostQuantumSignatures protocol
        // versions. The signature/pubkey types parse via Borsh unconditionally
        // (so pre-existing state remains readable), but the gate at this layer
        // ensures no PQ key is ever accepted into state on an old protocol.
        //
        // Centralized here (rather than scattered across per-action validators)
        // so the exhaustive `Action` match in
        // `Action::post_quantum_signatures_required` forces every future
        // action variant to make an explicit decision at compile time.
        if !ProtocolFeature::PostQuantumSignatures.enabled(protocol_version)
            && signed_tx.post_quantum_signatures_required()
        {
            return Err(InvalidTxError::InvalidTransactionVersion);
        }
        let tx_size = signed_tx.size_for_limits(protocol_version);
        let max_tx_size = config.wasm_config.limit_config.max_transaction_size;
        if tx_size > max_tx_size {
            return Err(InvalidTxError::TransactionSizeExceeded {
                size: tx_size,
                limit: max_tx_size,
            });
        }

        Ok(())
    }

    /// This method should only be used for test purposes.
    pub fn new_for_test(signed_tx: SignedTransaction) -> Self {
        Self(signed_tx)
    }

    pub fn to_signed_tx(&self) -> &SignedTransaction {
        &self.0
    }

    pub fn into_signed_tx(self) -> SignedTransaction {
        self.0
    }

    pub fn to_tx(&self) -> &Transaction {
        &self.0.transaction
    }

    /// This function should probably be deprecated in favour of `to_hash()`
    /// below as that offers stronger type safety.
    pub fn get_hash(&self) -> CryptoHash {
        self.0.get_hash()
    }

    pub fn get_size(&self) -> u64 {
        self.0.get_size()
    }

    /// See [`SignedTransaction::wire_size`].
    pub fn wire_size(&self) -> u64 {
        self.0.wire_size()
    }

    /// See [`SignedTransaction::size_for_limits`].
    pub fn size_for_limits(&self, protocol_version: ProtocolVersion) -> u64 {
        self.0.size_for_limits(protocol_version)
    }

    pub fn signer_id(&self) -> &AccountId {
        self.to_tx().signer_id()
    }

    pub fn receiver_id(&self) -> &AccountId {
        self.to_tx().receiver_id()
    }

    pub fn nonce(&self) -> TransactionNonce {
        self.to_tx().nonce()
    }

    pub fn public_key(&self) -> &PublicKey {
        self.to_tx().public_key()
    }

    pub fn actions(&self) -> &[Action] {
        self.to_tx().actions()
    }

    pub fn nonce_mode(&self) -> NonceMode {
        self.to_tx().nonce_mode()
    }
}

#[derive(BorshSerialize, BorshDeserialize, Eq, Debug, Clone, ProtocolSchema)]
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

    pub fn hash(&self) -> &CryptoHash {
        &self.hash
    }

    pub fn get_size(&self) -> u64 {
        self.size
    }

    /// Full borsh-serialized size of the signed transaction, including the
    /// signature.
    ///
    /// [`Self::get_size`] counts only the inner `Transaction` body, omitting
    /// the signature - ~65 B for ed25519 but up to ~3.3 KiB for ML-DSA-65.
    /// Use this where the true on-wire / in-witness size matters.
    pub fn wire_size(&self) -> u64 {
        let signature_size =
            borsh::object_length(&self.signature).expect("borsh signature length") as u64;
        self.size + signature_size
    }

    /// Transaction size charged against the `max_transaction_size` and
    /// `combined_transactions_size_limit` gates: [`Self::wire_size`] from
    /// [`ProtocolFeature::PostQuantumSignatures`] onward, else the legacy
    /// [`Self::get_size`]. Version-gated to keep chunk production deterministic.
    pub fn size_for_limits(&self, protocol_version: ProtocolVersion) -> u64 {
        if ProtocolFeature::PostQuantumSignatures.enabled(protocol_version) {
            self.wire_size()
        } else {
            self.get_size()
        }
    }

    /// Returns `true` if this transaction carries any post-quantum key
    /// material - its signer pubkey, its signature, or anywhere inside
    /// one of its actions (including a nested `Delegate` action).
    ///
    /// Used to decide whether the `PostQuantumSignatures` protocol
    /// feature must be enabled before admitting the transaction.
    pub fn post_quantum_signatures_required(&self) -> bool {
        self.transaction.public_key().key_type().is_post_quantum()
            || self.signature.key_type().is_post_quantum()
            || self.transaction.actions().iter().any(Action::post_quantum_signatures_required)
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

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for SignedTransaction {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SignedTransaction".to_string().into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        json_schema!({
            "type": "string",
            "format": "byte"
        })
    }
}

/// The status of execution for a transaction or a receipt.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Default, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ExecutionStatus {
    /// The execution is pending or unknown.
    #[default]
    Unknown = 0,
    /// The execution has failed with the given execution error.
    Failure(TxExecutionError) = 1,
    /// The final action succeeded and returned some value or an empty vec.
    SuccessValue(Vec<u8>) = 2,
    /// The final action of the receipt returned a promise or the signed transaction was converted
    /// to a receipt. Contains the receipt_id of the generated receipt.
    SuccessReceiptId(CryptoHash) = 3,
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
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum PartialExecutionStatus {
    Unknown = 0,
    Failure = 1,
    SuccessValue(Vec<u8>) = 2,
    SuccessReceiptId(CryptoHash) = 3,
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
#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Clone,
    smart_default::SmartDefault,
    Eq,
    ProtocolSchema,
)]
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
    /// Sum of tokens burnt for:
    /// - Gas: This value doesn't always equal to the `gas_burnt` multiplied by the gas price,
    ///   because the prepaid gas price might be lower than the actual gas price and it creates
    ///   a deficit.
    /// - Deleted gas keys: When a gas key or an account with gas keys is deleted, the remaining
    ///   balance on the gas key(s) is burnt.
    /// - Deployed global contracts: Tokens are burnt when deploying a global contract to
    ///   compensate for permanently storing contract code in the state.
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

#[derive(
    BorshSerialize, BorshDeserialize, PartialEq, Clone, Eq, Debug, Default, ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ExecutionMetadata {
    /// V1: Empty Metadata
    #[default]
    V1 = 0,
    /// V2: With ProfileData by legacy `Cost` enum
    V2(crate::profile_data_v2::ProfileDataV2) = 1,
    /// V3: With ProfileData by gas parameters
    V3(Box<ProfileDataV3>) = 2,
    /// V4: With ProfileData by gas parameters and the contract attached to
    /// the receiver account at the time each action ran. Lets consumers
    /// distinguish receiver from contract source (e.g. global contracts) and
    /// see what code an account had even on receipts that did not invoke a
    /// `FunctionCall`.
    V4(Box<ExecutionMetadataV4>) = 3,
}

#[derive(
    BorshSerialize, BorshDeserialize, PartialEq, Clone, Eq, Debug, Default, ProtocolSchema,
)]
pub struct ExecutionMetadataV4 {
    pub profile: ProfileDataV3,
    /// One entry per action in the receipt: the contract attached to the
    /// receiver account immediately before that action ran. Captured for
    /// every action kind, including ones that don't execute a contract;
    /// `AccountContract::None` when the account did not yet exist (e.g. the
    /// `CreateAccount` action that materialized it). Order matches the
    /// receipt's `actions` vector.
    pub contracts: Vec<AccountContract>,
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
#[derive(
    PartialEq, Clone, Default, Debug, BorshSerialize, BorshDeserialize, Eq, ProtocolSchema,
)]
pub struct ExecutionOutcomeWithId {
    /// The transaction hash or the receipt ID.
    pub id: CryptoHash,
    /// Should be the latest field since contains unparsable by light client ExecutionStatus::Failure
    pub outcome: ExecutionOutcome,
}

impl ExecutionOutcomeWithId {
    pub fn failed(transaction: &SignedTransaction, error: InvalidTxError) -> Self {
        Self::failed_with_gas_burnt(transaction, error, Gas::ZERO, Balance::ZERO)
    }

    pub fn failed_with_gas_burnt(
        transaction: &SignedTransaction,
        error: InvalidTxError,
        gas_burnt: Gas,
        tokens_burnt: Balance,
    ) -> Self {
        Self {
            id: transaction.get_hash(),
            outcome: ExecutionOutcome {
                executor_id: transaction.transaction.signer_id().clone(),
                status: ExecutionStatus::Failure(TxExecutionError::InvalidTxError(error)),
                gas_burnt,
                compute_usage: Some(gas_burnt.as_gas()),
                tokens_burnt,
                ..Default::default()
            },
        }
    }

    pub fn to_hashes(&self) -> Vec<CryptoHash> {
        let mut result = Vec::with_capacity(2 + self.outcome.logs.len());
        result.push(self.id);
        result.push(CryptoHash::hash_borsh(PartialExecutionOutcome::from(&self.outcome)));
        result.extend(self.outcome.logs.iter().map(|log| hash(log.as_bytes())));
        result
    }
}

/// Execution outcome with path from it to the outcome root and ID.
#[derive(
    PartialEq, Clone, Default, Debug, BorshSerialize, BorshDeserialize, Eq, ProtocolSchema,
)]
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
#[derive(Clone, BorshSerialize, BorshDeserialize, Debug, ProtocolSchema)]
pub struct ExecutionOutcomeWithProof {
    pub proof: MerklePath,
    pub outcome: ExecutionOutcome,
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
    use borsh::BorshDeserialize;
    use near_crypto::{InMemorySigner, KeyType, Signature, Signer};
    use near_primitives::types::Gas;

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
                    gas: Gas::from_gas(1_000),
                    deposit: Balance::from_yoctonear(1_000_000),
                })),
                Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(123) }),
                Action::Stake(Box::new(StakeAction {
                    public_key: public_key.clone(),
                    stake: Balance::from_yoctonear(1_000_000),
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
            nonce: TransactionNonce::from_nonce(1),
            receiver_id: "123".parse().unwrap(),
            block_hash: Default::default(),
            actions: vec![
                Action::CreateAccount(CreateAccountAction {}),
                Action::DeployContract(DeployContractAction { code: vec![1, 2, 3] }),
                Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "qqq".to_string(),
                    args: vec![1, 2, 3],
                    gas: Gas::from_gas(1_000),
                    deposit: Balance::from_yoctonear(1_000_000),
                })),
                Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(123) }),
                Action::Stake(Box::new(StakeAction {
                    public_key: public_key.clone(),
                    stake: Balance::from_yoctonear(1_000_000),
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
            nonce_mode: NonceMode::Monotonic,
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
    fn test_deserialize_invalid_account_id() {
        // Create a serialized V0 transaction with an invalid account ID (all null bytes).
        // The first 4 bytes are the little-endian length of the account ID string.
        let mut serialized_tx = vec![];
        serialized_tx.extend_from_slice(&10u32.to_le_bytes());
        serialized_tx.extend_from_slice(&[0u8; 10]); // Placeholder bytes

        let result = Transaction::try_from_slice(&serialized_tx);
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("the Account ID contains an invalid character"));
    }

    #[test]
    fn test_deserialize_invalid_account_id_length() {
        // Create a serialized V0 transaction with an account ID length exceeding the maximum.
        // The first 4 bytes are the little-endian length of the account ID string.
        let mut serialized_tx = vec![];
        serialized_tx.extend_from_slice(&100u32.to_le_bytes()); // 100 > AccountId::MAX_LEN (64)
        serialized_tx.extend_from_slice(&[b'a'; 100]); // Placeholder bytes

        let result = Transaction::try_from_slice(&serialized_tx);
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("the Account ID is too long"));
    }

    #[test]
    fn test_deserialize_invalid_version_tag() {
        // First byte is 2 (invalid tag), second byte is nonzero (so not V0).
        let serialized_tx = vec![2, 5, 0, 0, 0, 0, 0, 0];

        let result = Transaction::try_from_slice(&serialized_tx);
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("invalid transaction version tag: 2"));
    }

    #[test]
    fn test_outcome_to_hashes() {
        let outcome = ExecutionOutcome {
            status: ExecutionStatus::SuccessValue(vec![123]),
            logs: vec!["123".to_string(), "321".to_string()],
            receipt_ids: vec![],
            gas_burnt: Gas::from_gas(123),
            compute_usage: Some(456),
            tokens_burnt: Balance::from_yoctonear(1234000),
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

    /// Build a `SignedTransaction` carrying a single action and signed with
    /// `signer_key_type`. Used by the post-quantum gate tests below.
    fn signed_tx_with_action(signer_key_type: KeyType, action: Action) -> SignedTransaction {
        let signer_id: AccountId = "alice.near".parse().unwrap();
        let signer: Signer = InMemorySigner::from_random(signer_id.clone(), signer_key_type).into();
        let transaction = Transaction::V0(TransactionV0 {
            signer_id,
            public_key: signer.public_key(),
            nonce: 1,
            receiver_id: "bob.near".parse().unwrap(),
            block_hash: CryptoHash::default(),
            actions: vec![action],
        });
        let hash = transaction.get_hash_and_size().0;
        SignedTransaction::new(signer.sign(hash.as_ref()), transaction)
    }

    /// `Action::post_quantum_signatures_required` is `false` for actions
    /// that carry no key material and `false` for ed25519/secp256k1 actions.
    #[test]
    fn test_action_pq_required_classical_actions() {
        let ed = near_crypto::SecretKey::from_seed(KeyType::ED25519, "k").public_key();
        let cases: Vec<Action> = vec![
            Action::CreateAccount(CreateAccountAction {}),
            Action::DeployContract(DeployContractAction { code: vec![1, 2, 3] }),
            Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) }),
            Action::DeleteAccount(DeleteAccountAction {
                beneficiary_id: "bob.near".parse().unwrap(),
            }),
            Action::AddKey(Box::new(AddKeyAction {
                public_key: ed.clone(),
                access_key: AccessKey::full_access(),
            })),
            Action::DeleteKey(Box::new(DeleteKeyAction { public_key: ed.clone() })),
            Action::Stake(Box::new(StakeAction {
                public_key: ed,
                stake: Balance::from_yoctonear(1),
            })),
        ];
        for action in cases {
            assert!(
                !action.post_quantum_signatures_required(),
                "classical action wrongly flagged as PQ: {action:?}"
            );
        }
    }

    /// `Action::post_quantum_signatures_required` is `true` for every
    /// pubkey-carrying action variant when the pubkey is ML-DSA-65.
    #[test]
    fn test_action_pq_required_ml_dsa_actions() {
        let pq = near_crypto::SecretKey::from_seed(KeyType::MLDSA65, "k").public_key();
        let cases: Vec<Action> = vec![
            Action::AddKey(Box::new(AddKeyAction {
                public_key: pq.clone(),
                access_key: AccessKey::full_access(),
            })),
            Action::DeleteKey(Box::new(DeleteKeyAction { public_key: pq.clone() })),
            Action::Stake(Box::new(StakeAction {
                public_key: pq,
                stake: Balance::from_yoctonear(1),
            })),
        ];
        for action in cases {
            assert!(
                action.post_quantum_signatures_required(),
                "ML-DSA-65 action missed the PQ check: {action:?}"
            );
        }
    }

    /// Centralized gate in `check_valid_for_config`: a transaction signed
    /// with an ML-DSA-65 key is rejected pre-feature, accepted post-feature.
    #[test]
    fn test_check_valid_for_config_ml_dsa_signer_gated() {
        let config = RuntimeConfig::test();
        let signed_tx =
            signed_tx_with_action(KeyType::MLDSA65, Action::CreateAccount(CreateAccountAction {}));

        let pre = ProtocolFeature::PostQuantumSignatures.protocol_version() - 1;
        let post = ProtocolFeature::PostQuantumSignatures.protocol_version();

        assert!(matches!(
            ValidatedTransaction::check_valid_for_config(&config, &signed_tx, pre),
            Err(InvalidTxError::InvalidTransactionVersion)
        ));
        ValidatedTransaction::check_valid_for_config(&config, &signed_tx, post)
            .expect("ML-DSA-65 signer accepted post-feature");
    }

    /// Centralized gate: an ed25519-signed transaction carrying an
    /// ML-DSA-65 `AddKey` action is rejected pre-feature.
    #[test]
    fn test_check_valid_for_config_ml_dsa_in_add_key_gated() {
        let config = RuntimeConfig::test();
        let pq_pubkey = near_crypto::SecretKey::from_seed(KeyType::MLDSA65, "victim").public_key();
        let signed_tx = signed_tx_with_action(
            KeyType::ED25519,
            Action::AddKey(Box::new(AddKeyAction {
                public_key: pq_pubkey,
                access_key: AccessKey::full_access(),
            })),
        );
        let pre = ProtocolFeature::PostQuantumSignatures.protocol_version() - 1;
        assert!(matches!(
            ValidatedTransaction::check_valid_for_config(&config, &signed_tx, pre),
            Err(InvalidTxError::InvalidTransactionVersion)
        ));
    }

    /// Centralized gate: a pure-ed25519 transaction with no ML-DSA-65
    /// material is unaffected by the PostQuantumSignatures gate.
    #[test]
    fn test_check_valid_for_config_ed25519_pre_feature_still_ok() {
        let config = RuntimeConfig::test();
        let signed_tx = signed_tx_with_action(
            KeyType::ED25519,
            Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) }),
        );
        let pre = ProtocolFeature::PostQuantumSignatures.protocol_version() - 1;
        ValidatedTransaction::check_valid_for_config(&config, &signed_tx, pre)
            .expect("ed25519 tx unaffected by PQ gate pre-feature");
    }

    /// An outer ed25519 transaction whose only action is a `Delegate`
    /// wrapping an inner ML-DSA-65 signer + an inner `AddKey` of a third
    /// ML-DSA-65 pubkey must be rejected pre-feature: the centralized
    /// `post_quantum_signatures_required` walks into `SignedDelegateAction`
    /// and the nested `actions` vec recursively. This is the case the
    /// per-action gates would have had to remember to handle for both
    /// `validate_delegate_action` and `validate_add_key_action`.
    #[test]
    fn test_check_valid_for_config_ml_dsa_in_delegate_gated() {
        use crate::action::delegate::{DelegateAction, NonDelegateAction, SignedDelegateAction};

        let config = RuntimeConfig::test();

        // Inner signer (the meta-tx sender) uses ML-DSA-65.
        let inner_signer: Signer =
            InMemorySigner::from_random("carol.near".parse().unwrap(), KeyType::MLDSA65).into();
        // A *separate* ML-DSA-65 pubkey, target of an inner `AddKey`.
        let inner_target_pq =
            near_crypto::SecretKey::from_seed(KeyType::MLDSA65, "delegated-add-key").public_key();
        let inner_add_key = Action::AddKey(Box::new(AddKeyAction {
            public_key: inner_target_pq,
            access_key: AccessKey::full_access(),
        }));

        let delegate_action = DelegateAction {
            sender_id: "carol.near".parse().unwrap(),
            receiver_id: "bob.near".parse().unwrap(),
            actions: vec![NonDelegateAction::try_from(inner_add_key).unwrap()],
            nonce: 1,
            max_block_height: 1,
            public_key: inner_signer.public_key(),
        };
        let signed_delegate = SignedDelegateAction::sign(&inner_signer, delegate_action);

        // Outer transaction is signed with a classical ed25519 key, so the
        // gate must reach into the delegate to find the PQ material.
        let signed_tx =
            signed_tx_with_action(KeyType::ED25519, Action::Delegate(Box::new(signed_delegate)));

        let pre = ProtocolFeature::PostQuantumSignatures.protocol_version() - 1;
        let post = ProtocolFeature::PostQuantumSignatures.protocol_version();

        assert!(
            matches!(
                ValidatedTransaction::check_valid_for_config(&config, &signed_tx, pre),
                Err(InvalidTxError::InvalidTransactionVersion)
            ),
            "ML-DSA-65 buried inside a Delegate action must be rejected pre-feature",
        );
        ValidatedTransaction::check_valid_for_config(&config, &signed_tx, post)
            .expect("same transaction must be accepted post-feature");
    }

    /// `wire_size` equals the full borsh length of the signed transaction
    /// (body + signature) and exceeds the body-only `get_size` by exactly the
    /// serialized signature length. For ed25519 that delta is tiny.
    #[test]
    fn test_wire_size_counts_signature() {
        let signed_tx = signed_tx_with_action(
            KeyType::ED25519,
            Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) }),
        );
        let full_len = borsh::to_vec(&signed_tx).unwrap().len() as u64;
        assert_eq!(signed_tx.wire_size(), full_len, "wire_size must equal full borsh length");
        let signature_len = borsh::object_length(&signed_tx.signature).unwrap() as u64;
        assert_eq!(signed_tx.wire_size(), signed_tx.get_size() + signature_len);
        assert!(signature_len < 100, "ed25519 signature unexpectedly large: {signature_len}");
    }

    /// The motivation for the change: an ML-DSA-65 signature is invisible to
    /// `get_size` but adds ~3.3 KiB that `wire_size` accounts for.
    #[test]
    fn test_wire_size_ml_dsa_signature_is_large() {
        let signed_tx = signed_tx_with_action(
            KeyType::MLDSA65,
            Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) }),
        );
        assert_eq!(signed_tx.wire_size(), borsh::to_vec(&signed_tx).unwrap().len() as u64);
        let hidden_by_get_size = signed_tx.wire_size() - signed_tx.get_size();
        assert!(
            hidden_by_get_size > 3000,
            "ML-DSA-65 signature should add >3 KiB beyond get_size, got {hidden_by_get_size}"
        );
    }

    /// `size_for_limits` is the body-only size before `PostQuantumSignatures`
    /// and the full wire size from that version onward.
    #[test]
    fn test_size_for_limits_version_gated() {
        let signed_tx = signed_tx_with_action(
            KeyType::MLDSA65,
            Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) }),
        );
        let pre = ProtocolFeature::PostQuantumSignatures.protocol_version() - 1;
        let post = ProtocolFeature::PostQuantumSignatures.protocol_version();
        assert_eq!(signed_tx.size_for_limits(pre), signed_tx.get_size());
        assert_eq!(signed_tx.size_for_limits(post), signed_tx.wire_size());
        assert!(signed_tx.size_for_limits(post) > signed_tx.size_for_limits(pre));
    }
}
