use paperclip::actix::{api_v2_errors, Apiv2Schema};

use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, Nonce};

use crate::utils::{BlobInHexString, BorshInHexString};

/// An AccountBalanceRequest is utilized to make a balance request on the
/// /account/balance endpoint. If the block_identifier is populated, a
/// historical balance query should be performed.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct AccountBalanceRequest {
    pub network_identifier: NetworkIdentifier,

    pub account_identifier: AccountIdentifier,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_identifier: Option<PartialBlockIdentifier>,
}

/// An AccountBalanceResponse is returned on the /account/balance endpoint. If
/// an account has a balance for each AccountIdentifier describing it (ex: an
/// ERC-20 token balance on a few smart contracts), an account balance request
/// must be made with each AccountIdentifier.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub(crate) struct AccountBalanceResponse {
    pub block_identifier: BlockIdentifier,

    /// A single account may have a balance in multiple currencies.
    pub balances: Vec<Amount>,
    /// Rosetta Spec also optionally provides:
    /// Account-based blockchains that utilize a nonce or sequence number should
    /// include that number in the metadata. This number could be unique to the
    /// identifier or global across the account address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<AccountBalanceResponseMetadata>,
}
// Account-based blockchains that utilize a nonce or sequence number should
// include that number in the metadata.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct AccountBalanceResponseMetadata {
    pub nonces: Vec<Nonce>,
}
/// The account_identifier uniquely identifies an account within a network. All
/// fields in the account_identifier are utilized to determine this uniqueness
/// (including the metadata field, if populated).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct AccountIdentifier {
    /// The address may be a cryptographic public key (or some encoding of it)
    /// or a provided username.
    pub address: super::types::AccountId,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_account: Option<SubAccountIdentifier>,
    /// Rosetta Spec also optionally provides:
    /// Blockchains that utilize a username model (where the address is not a
    /// derivative of a cryptographic public key) should specify the public
    /// key(s) owned by the address in metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<AccountIdentifierMetadata>,
}

impl From<near_primitives::types::AccountId> for AccountIdentifier {
    fn from(account_id: near_primitives::types::AccountId) -> Self {
        Self { address: account_id.into(), sub_account: None, metadata: None }
    }
}

impl std::str::FromStr for AccountIdentifier {
    type Err = near_primitives::account::id::ParseAccountError;

    fn from_str(account_id: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(account_id.parse::<near_primitives::types::AccountId>()?))
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize, Apiv2Schema,
)]
pub(crate) struct AccountIdentifierMetadata {
    pub public_keys: Vec<PublicKey>,
}

/// Allow specifies supported Operation status, Operation types, and all
/// possible error statuses. This Allow object is used by clients to validate
/// the correctness of a Rosetta Server implementation. It is expected that
/// these clients will error if they receive some response that contains any of
/// the above information that is not specified here.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Allow {
    /// All Operation.Status this implementation supports. Any status that is
    /// returned during parsing that is not listed here will cause client
    /// validation to error.
    pub operation_statuses: Vec<OperationStatus>,

    /// All Operation.Type this implementation supports. Any type that is
    /// returned during parsing that is not listed here will cause client
    /// validation to error.
    pub operation_types: Vec<OperationType>,

    /// All Errors that this implementation could return. Any error that is
    /// returned during parsing that is not listed here will cause client
    /// validation to error.
    pub errors: Vec<Error>,

    /// Any Rosetta implementation that supports querying the balance of an
    /// account at any height in the past should set this to true.
    pub historical_balance_lookup: bool,
}

/// Amount is some Value of a Currency. It is considered invalid to specify a
/// Value without a Currency.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Amount {
    /// Value of the transaction in atomic units represented as an
    /// arbitrary-sized signed integer.  For example, 1 BTC would be represented
    /// by a value of 100000000.
    pub value: crate::utils::SignedDiff<near_primitives::types::Balance>,

    pub currency: Currency,
    /* Rosetta Spec also optionally provides:
     *
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

impl std::ops::Neg for Amount {
    type Output = Self;

    fn neg(mut self) -> Self::Output {
        self.value = -self.value;
        self
    }
}

impl Amount {
    pub(crate) fn from_yoctonear(amount: near_primitives::types::Balance) -> Self {
        Self { value: amount.into(), currency: Currency::near() }
    }

    pub(crate) fn from_yoctonear_diff(
        amount: crate::utils::SignedDiff<near_primitives::types::Balance>,
    ) -> Self {
        Self { value: amount, currency: Currency::near() }
    }
}

/// Blocks contain an array of Transactions that occurred at a particular
/// BlockIdentifier. A hard requirement for blocks returned by Rosetta
/// implementations is that they MUST be _inalterable_: once a client has
/// requested and received a block identified by a specific BlockIndentifier,
/// all future calls for that same BlockIdentifier must return the same block
/// contents.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Block {
    pub block_identifier: BlockIdentifier,

    pub parent_block_identifier: BlockIdentifier,

    /// The timestamp of the block in milliseconds since the Unix Epoch. The
    /// timestamp is stored in milliseconds because some blockchains produce
    /// blocks more often than once a second.
    pub timestamp: i64,

    pub transactions: Vec<Transaction>,
    /* Rosetta Spec also optionally provides:
     *
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

/// The block_identifier uniquely identifies a block in a particular network.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct BlockIdentifier {
    /// This is also known as the block height.
    pub index: i64,
    pub hash: String,
}

impl BlockIdentifier {
    pub fn new(height: BlockHeight, hash: &CryptoHash) -> Self {
        Self {
            index: height.try_into().expect("Rosetta only supports block indecies up to i64::MAX"),
            hash: hash.to_string(),
        }
    }
}

impl From<&near_primitives::views::BlockView> for BlockIdentifier {
    fn from(block: &near_primitives::views::BlockView) -> Self {
        Self::new(block.header.height, &block.header.hash)
    }
}

/// A BlockRequest is utilized to make a block request on the /block endpoint.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct BlockRequest {
    pub network_identifier: NetworkIdentifier,

    pub block_identifier: PartialBlockIdentifier,
}

/// A BlockResponse includes a fully-populated block or a partially-populated
/// block with a list of other transactions to fetch (other_transactions).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct BlockResponse {
    pub block: Option<Block>,

    /// Some blockchains may require additional transactions to be fetched that
    /// weren't returned in the block response (ex: block only returns
    /// transaction hashes). For blockchains with a lot of transactions in each
    /// block, this can be very useful as consumers can concurrently fetch all
    /// transactions returned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub other_transactions: Option<Vec<TransactionIdentifier>>,
}

/// A BlockTransactionRequest is used to fetch a Transaction included in a block
/// that is not returned in a BlockResponse.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct BlockTransactionRequest {
    pub network_identifier: NetworkIdentifier,

    pub block_identifier: PartialBlockIdentifier,

    pub transaction_identifier: TransactionIdentifier,
}

/// A BlockTransactionResponse contains information about a block transaction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct BlockTransactionResponse {
    pub transaction: Transaction,
}

/// ConstructionDeriveRequest is passed to the `/construction/derive`
/// endpoint. Network is provided in the request because some blockchains
/// have different address formats for different networks.
/// Metadata is provided in the request because some blockchains
/// allow for multiple address types (i.e. different address
/// for validators vs normal accounts).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionDeriveRequest {
    pub network_identifier: NetworkIdentifier,
    pub public_key: PublicKey,
    /* Rosetta Spec also optionally provides:
     *
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

/// ConstructionDeriveResponse is returned by the `/construction/derive`
/// endpoint.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionDeriveResponse {
    pub account_identifier: AccountIdentifier,
    /* Rosetta Spec also optionally provides:
     *
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

/// ConstructionPreprocessRequest is passed to the `/construction/preprocess`
/// endpoint so that a Rosetta implementation can determine which
/// metadata it needs to request for construction.
///
/// Metadata provided in this object should NEVER be a product
/// of live data (i.e. the caller must follow some network-specific
/// data fetching strategy outside of the Construction API to populate
/// required Metadata). If live data is required for construction, it MUST
/// be fetched in the call to `/construction/metadata`.
///
/// The caller can provide a max fee they are willing
/// to pay for a transaction. This is an array in the case fees
/// must be paid in multiple currencies.
///
/// The caller can also provide a suggested fee multiplier
/// to indicate that the suggested fee should be scaled.
/// This may be used to set higher fees for urgent transactions
/// or to pay lower fees when there is less urgency. It is assumed
/// that providing a very low multiplier (like 0.0001) will
/// never lead to a transaction being created with a fee
/// less than the minimum network fee (if applicable).
///
/// In the case that the caller provides both a max fee
/// and a suggested fee multiplier, the max fee will set an
/// upper bound on the suggested fee (regardless of the
/// multiplier provided).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionPreprocessRequest {
    pub network_identifier: NetworkIdentifier,
    pub operations: Vec<Operation>,
    /* Rosetta Spec also optionally provides:
     *
     * pub max_fee: Vec<Amount>,
     * pub suggested_fee_multiplier: f64,
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionMetadataOptions {
    pub signer_account_id: super::types::AccountId,
}

/// ConstructionPreprocessResponse contains `options` that will
/// be sent unmodified to `/construction/metadata`. If it is
/// not necessary to make a request to `/construction/metadata`,
/// `options` should be omitted.
/// Some blockchains require the PublicKey of particular AccountIdentifiers
/// to construct a valid transaction. To fetch these PublicKeys, populate
/// `required_public_keys` with the AccountIdentifiers associated with the
/// desired PublicKeys. If it is not necessary to retrieve any PublicKeys
/// for construction, `required_public_keys` should be omitted.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionPreprocessResponse {
    pub options: ConstructionMetadataOptions,
    pub required_public_keys: Vec<AccountIdentifier>,
}

/// A ConstructionMetadataRequest is utilized to get information required to
/// construct a transaction. The Options object used to specify which metadata
/// to return is left purposely unstructured to allow flexibility for
/// implementers.
///
/// Optionally, the request can also include an array
/// of PublicKeys associated with the AccountIdentifiers
/// returned in ConstructionPreprocessResponse.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionMetadataRequest {
    pub network_identifier: NetworkIdentifier,

    /// Some blockchains require different metadata for different types of
    /// transaction construction (ex: delegation versus a transfer). Instead of
    /// requiring a blockchain node to return all possible types of metadata for
    /// construction (which may require multiple node fetches), the client can
    /// populate an options object to limit the metadata returned to only the
    /// subset required.
    pub options: ConstructionMetadataOptions,

    pub public_keys: Vec<PublicKey>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionMetadata {
    pub signer_public_access_key_nonce: u64,
    pub recent_block_hash: String,
}

/// The ConstructionMetadataResponse returns network-specific metadata used for
/// transaction construction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionMetadataResponse {
    pub metadata: ConstructionMetadata,
}

/// ConstructionPayloadsRequest is the request to
/// `/construction/payloads`. It contains the network,
/// a slice of operations, and arbitrary metadata
/// that was returned by the call to `/construction/metadata`.
/// Optionally, the request can also include an array
/// of PublicKeys associated with the AccountIdentifiers
/// returned in ConstructionPreprocessResponse.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionPayloadsRequest {
    pub network_identifier: NetworkIdentifier,
    pub operations: Vec<Operation>,
    pub public_keys: Vec<PublicKey>,
    pub metadata: ConstructionMetadata,
}

/// ConstructionTransactionResponse is returned by `/construction/payloads`. It
/// contains an unsigned transaction blob (that is usually needed to construct
/// the a network transaction from a collection of signatures) and an
/// array of payloads that must be signed by the caller.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionPayloadsResponse {
    pub unsigned_transaction: BorshInHexString<near_primitives::transaction::Transaction>,
    pub payloads: Vec<SigningPayload>,
}

/// ConstructionCombineRequest is the input to the `/construction/combine`
/// endpoint. It contains the unsigned transaction blob returned by
/// `/construction/payloads` and all required signatures to create
/// a network transaction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionCombineRequest {
    pub network_identifier: NetworkIdentifier,
    pub unsigned_transaction: BorshInHexString<near_primitives::transaction::Transaction>,
    pub signatures: Vec<Signature>,
}

/// ConstructionCombineResponse is returned by `/construction/combine`.
/// The network payload will be sent directly to the
/// `construction/submit` endpoint.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionCombineResponse {
    pub signed_transaction: BorshInHexString<near_primitives::transaction::SignedTransaction>,
}

/// ConstructionParseRequest is the input to the `/construction/parse`
/// endpoint. It allows the caller to parse either an unsigned or
/// signed transaction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionParseRequest {
    pub network_identifier: NetworkIdentifier,
    pub signed: bool,
    /// This must be either the unsigned transaction blob returned by
    /// `/construction/payloads` or the signed transaction blob
    /// returned by `/construction/combine`.
    pub transaction: BlobInHexString<Vec<u8>>,
}

/// ConstructionParseResponse contains an array of operations that occur in
/// a transaction blob. This should match the array of operations provided
/// to `/construction/preprocess` and `/construction/payloads`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionParseResponse {
    pub operations: Vec<Operation>,

    /// All account identifiers of signers of a particular transaction. If the
    /// transaction is unsigned, it should be empty.
    pub account_identifier_signers: Vec<AccountIdentifier>,
    /* Rosetta Spec also optionally provides:
     *
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

/// The transaction submission request includes a signed transaction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionSubmitRequest {
    pub network_identifier: NetworkIdentifier,

    pub signed_transaction: BorshInHexString<near_primitives::transaction::SignedTransaction>,
}

/// TransactionIdentifierResponse contains the transaction_identifier of a
/// transaction that was submitted to either `/construction/hash` or
/// `/construction/submit`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct TransactionIdentifierResponse {
    pub transaction_identifier: TransactionIdentifier,
    /* Rosetta Spec also optionally provides:
     *
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

/// ConstructionHashRequest is the input to the /construction/hash endpoint.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionHashRequest {
    pub network_identifier: NetworkIdentifier,

    pub signed_transaction: BorshInHexString<near_primitives::transaction::SignedTransaction>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) enum CurrencySymbol {
    NEAR,
}

/// Currency is composed of a canonical Symbol and Decimals. This Decimals value
/// is used to convert an Amount.Value from atomic units (Satoshis) to standard
/// units (Bitcoins).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Currency {
    /// Canonical symbol associated with a currency.
    pub symbol: CurrencySymbol,

    /// Number of decimal places in the standard unit representation of the
    /// amount.  For example, BTC has 8 decimals. Note that it is not possible
    /// to represent the value of some currency in atomic units that is not base
    /// 10.
    pub decimals: u32,
    /* Rosetta Spec also optionally provides:
     *
     * /// Any additional information related to the currency itself.  For example,
     * /// it would be useful to populate this object with the contract address of
     * /// an ERC-20 token.
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

impl Currency {
    fn near() -> Self {
        Self { symbol: CurrencySymbol::NEAR, decimals: 24 }
    }
}

/// Instead of utilizing HTTP status codes to describe node errors (which often
/// do not have a good analog), rich errors are returned using this object.
#[api_v2_errors(code = 500, description = "See the inner `code` value to get more details")]
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Error {
    /// Code is a network-specific error code. If desired, this code can be
    /// equivalent to an HTTP status code.
    pub code: u32,

    /// Message is a network-specific error message.
    pub message: String,

    /// An error is retriable if the same request may succeed if submitted
    /// again.
    pub retriable: bool,
    /* Rosetta Spec also optionally provides:
     *
     * /// Often times it is useful to return context specific to the request that
     * /// caused the error (i.e. a sample of the stack trace or impacted account)
     * /// in addition to the standard error message.
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub details: Option<serde_json::Value>, */
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let retriable = if self.retriable { " (retriable)" } else { "" };
        write!(f, "Error #{}{}: {}", self.code, retriable, self.message)
    }
}

impl Error {
    pub(crate) fn from_error_kind(err: crate::errors::ErrorKind) -> Self {
        match err {
            crate::errors::ErrorKind::InvalidInput(message) => {
                Self { code: 400, message: format!("Invalid Input: {}", message), retriable: false }
            }
            crate::errors::ErrorKind::NotFound(message) => {
                Self { code: 404, message: format!("Not Found: {}", message), retriable: false }
            }
            crate::errors::ErrorKind::WrongNetwork(message) => {
                Self { code: 403, message: format!("Wrong Network: {}", message), retriable: false }
            }
            crate::errors::ErrorKind::Timeout(message) => {
                Self { code: 504, message: format!("Timeout: {}", message), retriable: true }
            }
            crate::errors::ErrorKind::InternalInvariantError(message) => Self {
                code: 501,
                message: format!("Internal Invariant Error (please, report it): {}", message),
                retriable: true,
            },
            crate::errors::ErrorKind::InternalError(message) => {
                Self { code: 500, message: format!("Internal Error: {}", message), retriable: true }
            }
        }
    }
}

impl<T> From<T> for Error
where
    T: Into<crate::errors::ErrorKind>,
{
    fn from(err: T) -> Self {
        Self::from_error_kind(err.into())
    }
}

impl actix_web::ResponseError for Error {
    fn error_response(&self) -> actix_web::HttpResponse {
        let data = paperclip::actix::web::Json(self);
        actix_web::HttpResponse::InternalServerError().json(data)
    }
}

/// A MempoolResponse contains all transaction identifiers in the mempool for a
/// particular network_identifier.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct MempoolResponse {
    pub transaction_identifiers: Vec<TransactionIdentifier>,
}

/// A MempoolTransactionRequest is utilized to retrieve a transaction from the
/// mempool.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct MempoolTransactionRequest {
    pub network_identifier: NetworkIdentifier,

    pub transaction_identifier: TransactionIdentifier,
}

/// A MempoolTransactionResponse contains an estimate of a mempool transaction.
/// It may not be possible to know the full impact of a transaction in the
/// mempool (ex: fee paid).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct MempoolTransactionResponse {
    pub transaction: Transaction,
    /* Rosetta Spec also optionally provides:
     *
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

/// A MetadataRequest is utilized in any request where the only argument is
/// optional metadata.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct MetadataRequest {
    // Rosetta Spec optionally provides, but we don't have any use for it:
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub metadata: Option<serde_json::Value>,
}

/// The network_identifier specifies which network a particular object is
/// associated with.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct NetworkIdentifier {
    pub blockchain: String,

    /// If a blockchain has a specific chain-id or network identifier, it should
    /// go in this field. It is up to the client to determine which
    /// network-specific identifier is mainnet or testnet.
    pub network: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_network_identifier: Option<SubNetworkIdentifier>,
}

#[derive(Debug, Copy, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum SyncStage {
    AwaitingPeers,
    NoSync,
    HeaderSync,
    StateSync,
    StateSyncDone,
    BodySync,
}

/// SyncStatus is used to provide additional context about an implementation's
/// sync status. It is often used to indicate that an implementation is healthy
/// when it cannot be queried  until some sync phase occurs. If an
/// implementation is immediately queryable, this model is often not populated.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct SyncStatus {
    pub current_index: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_index: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<SyncStage>,
}

/// A NetworkListResponse contains all NetworkIdentifiers that the node can
/// serve information for.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct NetworkListResponse {
    pub network_identifiers: Vec<NetworkIdentifier>,
}

/// NetworkOptionsResponse contains information about the versioning of the node
/// and the allowed operation statuses, operation types, and errors.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct NetworkOptionsResponse {
    pub version: Version,

    pub allow: Allow,
}

/// A NetworkRequest is utilized to retrieve some data specific exclusively to a
/// NetworkIdentifier.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct NetworkRequest {
    pub network_identifier: NetworkIdentifier,
    /* Rosetta Spec also optionally provides:
     *
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

/// NetworkStatusResponse contains basic information about the node's view of a
/// blockchain network. If a Rosetta implementation prunes historical state, it
/// should populate the optional `oldest_block_identifier` field with the oldest
/// block available to query. If this is not populated, it is assumed that the
/// `genesis_block_identifier` is the oldest queryable block.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct NetworkStatusResponse {
    pub current_block_identifier: BlockIdentifier,

    /// The timestamp of the block in milliseconds since the Unix Epoch. The
    /// timestamp is stored in milliseconds because some blockchains produce
    /// blocks more often than once a second.
    pub current_block_timestamp: i64,

    pub genesis_block_identifier: BlockIdentifier,

    pub oldest_block_identifier: BlockIdentifier,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_status: Option<SyncStatus>,

    pub peers: Vec<Peer>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Apiv2Schema,
    serde::Serialize,
    serde::Deserialize,
    strum::EnumIter,
    strum::IntoStaticStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum OperationType {
    InitiateCreateAccount,
    CreateAccount,
    InitiateDeleteAccount,
    DeleteAccount,
    DelegateAction,
    RefundDeleteAccount,
    InitiateAddKey,
    SignedDelegateAction,
    AddKey,
    InitiateDeleteKey,
    DeleteKey,
    Transfer,
    Stake,
    InitiateDeployContract,
    DeployContract,
    InitiateFunctionCall,
    InitiateSignedDelegateAction,
    InitiateDelegateAction,
    FunctionCall,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Apiv2Schema,
    serde::Serialize,
    serde::Deserialize,
    strum::EnumIter,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum OperationStatusKind {
    Success,
    //This OperationStatusKind was specifically requested by Coinbase Integration
    //team in order to continue their tests. It is NOT fully specified in the Rosetta
    //specs.
    #[serde(rename = "")]
    Empty,
}

impl OperationStatusKind {
    pub(crate) fn is_successful(&self) -> bool {
        match self {
            Self::Success => true,
            Self::Empty => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum OperationMetadataTransferFeeType {
    GasPrepayment,
    GasRefund,
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct OperationMetadata {
    /// Has to be specified for TRANSFER operations which represent gas prepayments or gas refunds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer_fee_type: Option<OperationMetadataTransferFeeType>,
    /// Has to be specified for ADD_KEY, REMOVE_KEY, DELEGATE_ACTION and STAKE operations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_key: Option<PublicKey>,
    // /// Has to be specified for ADD_KEY
    // TODO: Allow specifying the access key permissions and nonce. We go with full-access keys for
    // now
    //#[serde(skip_serializing_if = "Option::is_none")]
    // pub access_key: Option<TODO>,
    /// Has to be specified for DEPLOY_CONTRACT operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<BlobInHexString<Vec<u8>>>,
    /// Has to be specified for FUNCTION_CALL operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method_name: Option<String>,
    /// Has to be specified for FUNCTION_CALL operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<BlobInHexString<Vec<u8>>>,
    /// Has to be specified for FUNCTION_CALL operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attached_gas: Option<crate::utils::SignedDiff<near_primitives::types::Gas>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predecessor_id: Option<AccountIdentifier>,
    /// Has to be specified for DELEGATE_ACTION operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_block_height: Option<near_primitives::types::BlockHeight>,
    /// Has to be specified for DELEGATE_ACTION operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<Nonce>,
    /// Has to be specified for SIGNED_DELEGATE_ACTION operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

impl OperationMetadata {
    pub(crate) fn from_predecessor(
        predecessor_id: Option<AccountIdentifier>,
    ) -> Option<OperationMetadata> {
        predecessor_id.map(|predecessor_id| crate::models::OperationMetadata {
            predecessor_id: Some(predecessor_id),
            ..Default::default()
        })
    }

    pub(crate) fn with_transfer_fee_type(
        mut self,
        transfer_fee_type: OperationMetadataTransferFeeType,
    ) -> Self {
        self.transfer_fee_type = Some(transfer_fee_type);
        self
    }
}

/// Operations contain all balance-changing information within a transaction.
/// They are always one-sided (only affect 1 AccountIdentifier) and can
/// succeed or fail independently from a Transaction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Operation {
    pub operation_identifier: OperationIdentifier,

    /// Restrict referenced related_operations to identifier indexes < the
    /// current operation_identifier.index. This ensures there exists a clear
    /// DAG-structure of relations. Since operations are one-sided, one could
    /// imagine relating operations in a single transfer or linking operations
    /// in a call tree.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_operations: Option<Vec<OperationIdentifier>>,

    /// The network-specific type of the operation. Ensure that any type that
    /// can be returned here is also specified in the NetworkStatus. This can
    /// be very useful to downstream consumers that parse all block data.
    #[serde(rename = "type")]
    pub type_: OperationType,

    /// The network-specific status of the operation. Status is not defined on
    /// the transaction object because blockchains with smart contracts may have
    /// transactions that partially apply. Blockchains with atomic transactions
    /// (all operations succeed or all operations fail) will have the same
    /// status for each operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<OperationStatusKind>,

    pub account: AccountIdentifier,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<Amount>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<OperationMetadata>,
}

/// This is Operation which mustn't contain DelegateActionOperation.
///
/// This struct is needed to avoid the recursion when Action/DelegateAction is deserialized.
///
/// Important: Don't make the inner Action public, this must only be constructed
/// through the correct interface that ensures the inner Action is actually not
/// a delegate action. That would break an assumption of this type, which we use
/// in several places. For example, borsh de-/serialization relies on it. If the
/// invariant is broken, we may end up with a `Transaction` or `Receipt` that we
/// can serialize but deserializing it back causes a parsing error.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct NonDelegateActionOperation(crate::models::Operation);

impl From<NonDelegateActionOperation> for crate::models::Operation {
    fn from(action: NonDelegateActionOperation) -> Self {
        action.0
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug, thiserror::Error)]
#[error("Delegate operation cannot contain a delegate operation")]
pub struct IsDelegateOperation;

impl TryFrom<crate::models::Operation> for NonDelegateActionOperation {
    type Error = IsDelegateOperation;

    fn try_from(operation: crate::models::Operation) -> Result<Self, IsDelegateOperation> {
        if matches!(operation.type_, crate::models::OperationType::DelegateAction) {
            Err(IsDelegateOperation)
        } else {
            Ok(Self(operation))
        }
    }
}

impl From<IsDelegateOperation> for crate::errors::ErrorKind {
    fn from(value: IsDelegateOperation) -> Self {
        crate::errors::ErrorKind::InvalidInput(value.to_string())
    }
}

/// The operation_identifier uniquely identifies an operation within a
/// transaction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct OperationIdentifier {
    /// The operation index is used to ensure each operation has a unique
    /// identifier within a transaction. This index is only relative to the
    /// transaction and NOT GLOBAL. The operations in each transaction should
    /// start from index 0. To clarify, there may not be any notion of an
    /// operation index in the blockchain being described.
    pub index: i64,

    /// Some blockchains specify an operation index that is essential for
    /// client use. For example, Bitcoin uses a network_index to identify
    /// which UTXO was used in a transaction.  network_index should not be
    /// populated if there is no notion of an operation index in a blockchain
    /// (typically most account-based blockchains).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_index: Option<i64>,
}

impl OperationIdentifier {
    pub(crate) fn new(operations: &[Operation]) -> Self {
        Self {
            index: operations
                .len()
                .try_into()
                .expect("there cannot be more than i64::MAX operations in a single transaction"),
            network_index: None,
        }
    }
}

/// OperationStatus is utilized to indicate which Operation status are
/// considered successful.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct OperationStatus {
    /// The status is the network-specific status of the operation.
    pub status: OperationStatusKind,

    /// An Operation is considered successful if the Operation.Amount should
    /// affect the Operation.Account. Some blockchains (like Bitcoin) only
    /// include successful operations in blocks but other blockchains (like
    /// Ethereum) include unsuccessful operations that incur a fee.  To
    /// reconcile the computed balance from the stream of Operations, it is
    /// critical to understand which Operation.Status indicate an Operation is
    /// successful and should affect an Account.
    pub successful: bool,
}

/// When fetching data by BlockIdentifier, it may be possible to only specify
/// the index or hash. If neither property is specified, it is assumed that the
/// client is making a request at the current block.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct PartialBlockIdentifier {
    #[serde(skip_serializing_if = "Option::is_none")]
    index: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    hash: Option<String>,
}

impl TryFrom<PartialBlockIdentifier> for near_primitives::types::BlockReference {
    type Error = crate::errors::ErrorKind;

    fn try_from(block_identifier: PartialBlockIdentifier) -> Result<Self, Self::Error> {
        Ok(match (block_identifier.index, block_identifier.hash) {
            (Some(index), None) => {
                near_primitives::types::BlockId::Height(index.try_into().map_err(|err| {
                    Self::Error::InvalidInput(format!("Failed to parse Block Height: {}", err))
                })?)
                .into()
            }
            (_, Some(hash)) => {
                near_primitives::types::BlockId::Hash(hash.parse().map_err(|err| {
                    Self::Error::InvalidInput(format!("Failed to parse Block Hash: {}", err))
                })?)
                .into()
            }
            (None, None) => near_primitives::types::BlockReference::Finality(
                near_primitives::types::Finality::Final,
            ),
        })
    }
}

/// A Peer is a representation of a node's peer.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Peer {
    pub peer_id: String,
    /* Rosetta Spec also optionally provides:
     *
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum SubAccount {
    LiquidBalanceForStorage,
    Locked,
}

impl From<SubAccount> for crate::models::SubAccountIdentifier {
    fn from(sub_account: SubAccount) -> Self {
        crate::models::SubAccountIdentifier { address: sub_account }
    }
}

/// An account may have state specific to a contract address (ERC-20 token)
/// and/or a stake (delegated balance). The sub_account_identifier should
/// specify which state (if applicable) an account instantiation refers to.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct SubAccountIdentifier {
    /// The SubAccount address may be a cryptographic value or some other
    /// identifier (ex: bonded) that uniquely specifies a SubAccount.
    pub address: SubAccount,
    /* Rosetta Spec also optionally provides:
     *
     * /// If the SubAccount address is not sufficient to uniquely specify a
     * /// SubAccount, any other identifying information can be stored here.  It is
     * /// important to note that two SubAccounts with identical addresses but
     * /// differing metadata will not be considered equal by clients.
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

/// In blockchains with sharded state, the SubNetworkIdentifier is required to
/// query some object on a specific shard. This identifier is optional for all
/// non-sharded blockchains.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct SubNetworkIdentifier {
    pub network: String,
    /* Rosetta Spec also optionally provides:
     *
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

/// The timestamp of the block in milliseconds since the Unix Epoch. The
/// timestamp is stored in milliseconds because some blockchains produce blocks
/// more often than once a second.
#[derive(
    Debug, Clone, PartialEq, PartialOrd, Apiv2Schema, serde::Serialize, serde::Deserialize,
)]
pub(crate) struct Timestamp(i64);

/// Transactions contain an array of Operations that are attributable to the
/// same TransactionIdentifier.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Transaction {
    pub transaction_identifier: TransactionIdentifier,

    pub operations: Vec<Operation>,

    /// The related_transactions allow implementations to link together multiple
    /// transactions.  An unpopulated network identifier indicates that the
    /// related transaction is on the same network.
    ///
    /// We’re using it to link NEAR transactions and receipts with receipts they
    /// generated.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub related_transactions: Vec<RelatedTransaction>,

    /// Transactions that are related to other transactions (like a cross-shard
    /// transaction) should include the transaction_identifier of these
    /// transactions in the metadata.
    pub metadata: TransactionMetadata,
}

/// Transaction related to another [`Transaction`] such as matching transfer or
/// a cross-shard transaction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct RelatedTransaction {
    // Rosetta API defines an optional network_identifier field as well but
    // since all our related transactions are always on the same network we can
    // leave out this field.
    // pub network_identifier: NetworkIdentifier,
    //
    pub transaction_identifier: TransactionIdentifier,

    pub direction: RelatedTransactionDirection,
}

#[derive(Debug, Copy, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RelatedTransactionDirection {
    /// Direction indicating a transaction relation is from parent to child.
    Forward,
    // Rosetta also defines ‘backward’ direction (which indicates a transaction
    // relation is from child to parent) but we’re not implementing it at the
    // moment.
}

impl RelatedTransaction {
    pub fn forward(transaction_identifier: TransactionIdentifier) -> Self {
        Self { transaction_identifier, direction: RelatedTransactionDirection::Forward }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TransactionType {
    Block,
    Transaction,
    ActionReceipt,
    DataReceipt,
}

/// Extra data for Transaction
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct TransactionMetadata {
    #[serde(rename = "type")]
    pub(crate) type_: TransactionType,
}

/// The transaction_identifier uniquely identifies a transaction in a particular
/// network and block or in the mempool.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct TransactionIdentifier {
    /// Any transactions that are attributable only to a block (ex: a block
    /// event) should use the hash of the block as the identifier.
    pub hash: String,
}

impl TransactionIdentifier {
    /// Returns an identifier for a NEAR transaction with given hash.
    pub(crate) fn transaction(tx_hash: &near_primitives::hash::CryptoHash) -> Self {
        Self::from_prefix_and_hash("tx", tx_hash)
    }

    /// Returns an identifier for a NEAR receipt with given hash.
    pub(crate) fn receipt(receipt_hash: &near_primitives::hash::CryptoHash) -> Self {
        Self::from_prefix_and_hash("receipt", receipt_hash)
    }

    /// Returns an identifier for block events constructed as <prefix>:<hash>.
    ///
    /// Note: If constructing identifiers for transactions or receipts, use
    /// [`Self::transaction`] or [`Self::receipt`] methods instead.
    pub(crate) fn block_event(
        prefix: &'static str,
        block_hash: &near_primitives::hash::CryptoHash,
    ) -> Self {
        Self::from_prefix_and_hash(prefix, block_hash)
    }

    fn from_prefix_and_hash(
        prefix: &'static str,
        hash: &near_primitives::hash::CryptoHash,
    ) -> Self {
        Self { hash: format!("{}:{}", prefix, hash) }
    }
}

/// The Version object is utilized to inform the client of the versions of
/// different components of the Rosetta implementation.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Version {
    /// The rosetta_version is the version of the Rosetta interface the
    /// implementation adheres to. This can be useful for clients looking to
    /// reliably parse responses.
    pub rosetta_version: String,

    /// The node_version is the canonical version of the node runtime. This can
    /// help clients manage deployments.
    pub node_version: String,

    /// When a middleware server is used to adhere to the Rosetta interface, it
    /// should return its version here. This can help clients manage
    /// deployments.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub middleware_version: Option<String>,
    /* Rosetta Spec also optionally provides:
     *
     * /// Any other information that may be useful about versioning of dependent
     * /// services should be returned here.
     * #[serde(skip_serializing_if = "Option::is_none")]
     * pub metadata: Option<serde_json::Value>, */
}

/// PublicKey contains a public key byte array for a particular CurveType
/// encoded in hex. Note that there is no PrivateKey struct as this is NEVER the
/// concern of an implementation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct PublicKey {
    /// Hex-encoded public key bytes in the format specified by the CurveType.
    pub hex_bytes: BlobInHexString<Vec<u8>>,
    pub curve_type: CurveType,
}

impl From<&near_crypto::PublicKey> for PublicKey {
    fn from(public_key: &near_crypto::PublicKey) -> Self {
        let hex_bytes = public_key.key_data().to_owned().into();
        Self { hex_bytes, curve_type: public_key.key_type().into() }
    }
}

impl TryFrom<&PublicKey> for near_crypto::PublicKey {
    type Error = near_crypto::ParseKeyError;

    fn try_from(PublicKey { curve_type, hex_bytes }: &PublicKey) -> Result<Self, Self::Error> {
        Ok(match curve_type {
            CurveType::Edwards25519 => {
                near_crypto::PublicKey::ED25519((hex_bytes.as_ref() as &[u8]).try_into()?)
            }
            CurveType::Secp256k1 => {
                near_crypto::PublicKey::SECP256K1((hex_bytes.as_ref() as &[u8]).try_into()?)
            }
        })
    }
}

/// CurveType is the type of cryptographic curve associated with a PublicKey.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum CurveType {
    /// `y (255-bits) || x-sign-bit (1-bit)` - 32 bytes (<https://ed25519.cr.yp.to/ed25519-20110926.pdf>)
    Edwards25519,
    /// SEC compressed - 33 bytes (<https://secg.org/sec1-v2.pdf#subsubsection.2.3.3>)
    Secp256k1,
}

impl From<near_crypto::KeyType> for CurveType {
    fn from(key_type: near_crypto::KeyType) -> Self {
        match key_type {
            near_crypto::KeyType::ED25519 => Self::Edwards25519,
            near_crypto::KeyType::SECP256K1 => Self::Secp256k1,
        }
    }
}

/// SigningPayload is signed by the client with the keypair associated with an
/// address using the specified SignatureType. SignatureType can be optionally
/// populated if there is a restriction on the signature scheme that can be used
/// to sign the payload.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct SigningPayload {
    /// The account identifier that should sign the payload.
    pub account_identifier: AccountIdentifier,
    pub hex_bytes: BlobInHexString<Vec<u8>>,
    pub signature_type: Option<SignatureType>,
}

/// Signature contains the payload that was signed, the public keys of the
/// keypairs used to produce the signature, the signature (encoded in hex), and
/// the SignatureType. PublicKey is often times not known during construction of
/// the signing payloads but may be needed to combine signatures properly.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Signature {
    pub signing_payload: SigningPayload,
    pub public_key: PublicKey,
    pub signature_type: SignatureType,
    pub hex_bytes: BlobInHexString<Vec<u8>>,
}

impl TryFrom<&Signature> for near_crypto::Signature {
    type Error = near_crypto::ParseSignatureError;

    fn try_from(
        Signature { signature_type, hex_bytes, .. }: &Signature,
    ) -> Result<Self, Self::Error> {
        near_crypto::Signature::from_parts((*signature_type).into(), hex_bytes.as_ref())
    }
}

/// SignatureType is the type of a cryptographic signature.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SignatureType {
    /// `R (32-byte) || s (32-bytes)` - `64 bytes`
    Ed25519,
    /* Rosetta Spec also provides:
     *
     * /// `r (32-bytes) || s (32-bytes)` - `64 bytes`
     * ECDSA,
     * /// `r (32-bytes) || s (32-bytes) || v (1-byte)` - `65 bytes`
     * #[serde(rename = "ecdsa_recovery")]
     * ECDSARecovery,
     * #[serde(rename = "schnorr_1")]
     * /// `r (32-bytes) || s (32-bytes)` - `64 bytes` (schnorr signature
     * /// implemented by Zilliqa where both `r` and `s` are scalars encoded as
     * /// `32-bytes` values, most significant byte first.)
     * Schnorr1, */
}

impl From<near_crypto::KeyType> for SignatureType {
    fn from(key_type: near_crypto::KeyType) -> Self {
        match key_type {
            near_crypto::KeyType::ED25519 => Self::Ed25519,
            near_crypto::KeyType::SECP256K1 => {
                unimplemented!("SECP256K1 keys are not implemented in Rosetta yet")
            }
        }
    }
}

impl From<SignatureType> for near_crypto::KeyType {
    fn from(signature_type: SignatureType) -> Self {
        match signature_type {
            SignatureType::Ed25519 => Self::ED25519,
        }
    }
}
