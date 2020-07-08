use std::convert::{TryFrom, TryInto};

use paperclip::actix::{api_v2_errors, Apiv2Schema};

use near_primitives::borsh::{BorshDeserialize, BorshSerialize};

use crate::consts;

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

    /// Account-based blockchains that utilize a nonce or sequence number should
    /// include that number in the metadata. This number could be unique to the
    /// identifier or global across the account address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// The account_identifier uniquely identifies an account within a network. All
/// fields in the account_identifier are utilized to determine this uniqueness
/// (including the metadata field, if populated).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct AccountIdentifier {
    /// The address may be a cryptographic public key (or some encoding of it)
    /// or a provided username.
    pub address: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_account: Option<SubAccountIdentifier>,

    /// Blockchains that utilize a username model (where the address is not a
    /// derivative of a cryptographic public key) should specify the public
    /// key(s) owned by the address in metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
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
    pub value: String,

    pub currency: Currency,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Blocks contain an array of Transactions that occurred at a particular
/// BlockIdentifier.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Block {
    pub block_identifier: BlockIdentifier,

    pub parent_block_identifier: BlockIdentifier,

    /// The timestamp of the block in milliseconds since the Unix Epoch. The
    /// timestamp is stored in milliseconds because some blockchains produce
    /// blocks more often than once a second.
    pub timestamp: i64,

    pub transactions: Vec<Transaction>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// The block_identifier uniquely identifies a block in a particular network.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct BlockIdentifier {
    /// This is also known as the block height.
    pub index: i64,

    pub hash: String,
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
    pub block: Block,

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

/// A ConstructionMetadataRequest is utilized to get information required to
/// construct a transaction. The Options object used to specify which metadata
/// to return is left purposely unstructured to allow flexibility for
/// implementers.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionMetadataRequest {
    pub network_identifier: NetworkIdentifier,

    /// Some blockchains require different metadata for different types of
    /// transaction construction (ex: delegation versus a transfer). Instead of
    /// requiring a blockchain node to return all possible types of metadata for
    /// construction (which may require multiple node fetches), the client can
    /// populate an options object to limit the metadata returned to only the
    /// subset required.
    pub options: serde_json::Value,
}

/// The ConstructionMetadataResponse returns network-specific metadata used for
/// transaction construction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionMetadataResponse {
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JsonSignedTransaction(pub near_primitives::transaction::SignedTransaction);

impl paperclip::v2::schema::TypedData for JsonSignedTransaction {
    fn data_type() -> paperclip::v2::models::DataType {
        paperclip::v2::models::DataType::String
    }
}

impl serde::Serialize for JsonSignedTransaction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&near_primitives::serialize::to_base64(
            self.0.try_to_vec().expect("borsh serialization should never fail"),
        ))
    }
}

impl<'de> serde::Deserialize<'de> for JsonSignedTransaction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw_signed_transaction = near_primitives::serialize::from_base64(
            &<String as serde::Deserialize>::deserialize(deserializer)?,
        )
        .map_err(|err| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Other(&format!(
                    "signed transaction could not be decoded due to: {:?}",
                    err
                )),
                &"base64-encoded transaction was expected",
            )
        })?;
        Ok(Self(
            near_primitives::transaction::SignedTransaction::try_from_slice(
                &raw_signed_transaction,
            )
            .map_err(|err| {
                serde::de::Error::invalid_value(
                    serde::de::Unexpected::Other(&format!(
                        "signed transaction could not be deserialized due to: {:?}",
                        err
                    )),
                    &"a valid Borsh-serialized transaction was expected",
                )
            })?,
        ))
    }
}

/// The transaction submission request includes a signed transaction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionSubmitRequest {
    pub network_identifier: NetworkIdentifier,

    pub signed_transaction: JsonSignedTransaction,
}

/// A TransactionSubmitResponse contains the transaction_identifier of a
/// submitted transaction that was accepted into the mempool.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionSubmitResponse {
    pub transaction_identifier: TransactionIdentifier,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// ConstructionHashRequest is the input to the /construction/hash endpoint.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionHashRequest {
    pub network_identifier: NetworkIdentifier,

    pub signed_transaction: JsonSignedTransaction,
}

/// ConstructionHashResponse is the output of the /construction/hash endpoint.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct ConstructionHashResponse {
    pub transaction_hash: String,
}

/// Currency is composed of a canonical Symbol and Decimals. This Decimals value
/// is used to convert an Amount.Value from atomic units (Satoshis) to standard
/// units (Bitcoins).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Currency {
    /// Canonical symbol associated with a currency.
    pub symbol: String,

    /// Number of decimal places in the standard unit representation of the
    /// amount.  For example, BTC has 8 decimals. Note that it is not possible
    /// to represent the value of some currency in atomic units that is not base
    /// 10.
    pub decimals: u32,

    /// Any additional information related to the currency itself.  For example,
    /// it would be useful to populate this object with the contract address of
    /// an ERC-20 token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Instead of utilizing HTTP status codes to describe node errors (which often
/// do not have a good analog), rich errors are returned using this object.
#[api_v2_errors(
    code = 400,
    description = "See the inner `code` value to get more details",
    code = 500,
    description = "Unexpected internal server error, please, file an issue"
)]
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

    /// Often times it is useful to return context specific to the request that
    /// caused the error (i.e. a sample of the stack trace or impacted account)
    /// in addition to the standard error message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let retriable = if self.retriable { " (retriable)" } else { "" };
        write!(f, "Error #{}{}: {}", self.code, retriable, self.message)
    }
}

#[derive(Debug, strum::EnumIter)]
pub(crate) enum ErrorKind {
    InvalidInput(String),
    NotFound(String),
    WrongNetwork(String),
    Timeout(String),
    Other(String),
}

impl Error {
    pub(crate) fn from_error_kind(err: ErrorKind) -> Self {
        match err {
            ErrorKind::InvalidInput(message) => Self {
                code: 400,
                message: format!("InvalidInput: {}", message),
                retriable: true,
                details: None,
            },
            ErrorKind::NotFound(message) => Self {
                code: 404,
                message: format!("NotFound: {}", message),
                retriable: true,
                details: None,
            },
            ErrorKind::WrongNetwork(message) => Self {
                code: 404,
                message: format!("WrongNetwork: {}", message),
                retriable: true,
                details: None,
            },
            ErrorKind::Timeout(message) => Self {
                code: 504,
                message: format!("Timeout: {}", message),
                retriable: true,
                details: None,
            },
            ErrorKind::Other(message) => Self {
                code: 500,
                message: format!("Other: {}", message),
                retriable: true,
                details: None,
            },
        }
    }
}

impl<T> std::convert::From<T> for Error
where
    T: Into<ErrorKind>,
{
    fn from(err: T) -> Self {
        Self::from_error_kind(err.into())
    }
}

impl std::convert::From<actix::MailboxError> for ErrorKind {
    fn from(err: actix::MailboxError) -> Self {
        Self::Other(format!(
            "Server seems to be under a heavy load thus reaching a limit of Actix queue: {}",
            err
        ))
    }
}

impl std::convert::From<tokio::time::Elapsed> for ErrorKind {
    fn from(_: tokio::time::Elapsed) -> Self {
        Self::Timeout("The operation timed out.".to_string())
    }
}

impl std::convert::From<near_client::TxStatusError> for ErrorKind {
    fn from(err: near_client::TxStatusError) -> Self {
        match err {
            near_client::TxStatusError::ChainError(err) => Self::Other(format!(
                "Transaction could not be found due to an internal error: {:?}",
                err
            )),
            near_client::TxStatusError::MissingTransaction(err) => {
                Self::NotFound(format!("Transaction is missing: {:?}", err))
            }
            near_client::TxStatusError::InvalidTx(err) => Self::NotFound(format!(
                "Transaction is invalid, so it will never be included to the chain: {:?}",
                err
            )),
            near_client::TxStatusError::InternalError
            | near_client::TxStatusError::TimeoutError => {
                // TODO: remove the statuses from TxStatusError since they are
                // never constructed by the view client (it is a leak of
                // abstraction introduced in JSONRPC)
                unreachable!("You reached impossible {:?} status", err);
            }
        }
    }
}

impl actix_web::ResponseError for Error {
    fn error_response(&self) -> actix_web::HttpResponse {
        let status_code = if self.code == 500 {
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR
        } else {
            actix_web::http::StatusCode::BAD_REQUEST
        };
        let data = paperclip::actix::web::Json(self).clone();
        actix_web::HttpResponse::build(status_code).json(data)
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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// A MetadataRequest is utilized in any request where the only argument is
/// optional metadata.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct MetadataRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
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

    pub peers: Vec<Peer>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    Apiv2Schema,
    strum::EnumIter,
)]
pub(crate) enum OperationType {
    CreateAccount,
    DeployContract,
    FunctionCall,
    Transfer,
    Stake,
    AddKey,
    DeleteKey,
    DeleteAccount,
}

impl std::convert::From<&near_primitives::views::ActionView> for OperationType {
    fn from(action: &near_primitives::views::ActionView) -> Self {
        match action {
            near_primitives::views::ActionView::CreateAccount => Self::CreateAccount,
            near_primitives::views::ActionView::DeployContract { .. } => Self::DeployContract,
            near_primitives::views::ActionView::FunctionCall { .. } => Self::FunctionCall,
            near_primitives::views::ActionView::Transfer { .. } => Self::Transfer,
            near_primitives::views::ActionView::Stake { .. } => Self::Stake,
            near_primitives::views::ActionView::AddKey { .. } => Self::AddKey,
            near_primitives::views::ActionView::DeleteKey { .. } => Self::DeleteKey,
            near_primitives::views::ActionView::DeleteAccount { .. } => Self::DeleteAccount,
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    Apiv2Schema,
    strum::EnumIter,
)]
pub(crate) enum OperationStatusKind {
    Unknown,
    Failure,
    Success,
}

impl OperationStatusKind {
    pub(crate) fn is_successful(&self) -> bool {
        match self {
            Self::Unknown => false,
            Self::Failure => false,
            Self::Success => true,
        }
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
    pub status: OperationStatusKind,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<AccountIdentifier>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<Amount>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl From<&near_primitives::views::ActionView> for Operation {
    fn from(action: &near_primitives::views::ActionView) -> Self {
        let amount = match action {
            near_primitives::views::ActionView::Transfer { deposit } => Some(Amount {
                value: deposit.to_string(),
                currency: consts::YOCTO_NEAR_CURRENCY.clone(),
                metadata: None,
            }),
            _ => None,
        };
        Operation {
            operation_identifier: OperationIdentifier { index: 0, network_index: None },
            type_: OperationType::from(action),
            amount,
            account: None,
            related_operations: None,
            status: OperationStatusKind::Unknown,
            metadata: None,
        }
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

impl TryFrom<PartialBlockIdentifier> for near_primitives::types::BlockIdOrFinality {
    type Error = ();

    fn try_from(block_identifier: PartialBlockIdentifier) -> Result<Self, Self::Error> {
        Ok(match (block_identifier.index, block_identifier.hash) {
            (Some(index), None) => {
                near_primitives::types::BlockId::Height(index.try_into().map_err(|_| ())?).into()
            }
            (_, Some(hash)) => {
                near_primitives::types::BlockId::Hash(hash.try_into().map_err(|_| ())?).into()
            }
            (None, None) => near_primitives::types::BlockIdOrFinality::Finality(
                near_primitives::types::Finality::Final,
            ),
        })
    }
}

/// A Peer is a representation of a node's peer.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Peer {
    pub peer_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// An account may have state specific to a contract address (ERC-20 token)
/// and/or a stake (delegated balance). The sub_account_identifier should
/// specify which state (if applicable) an account instantiation refers to.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct SubAccountIdentifier {
    /// The SubAccount address may be a cryptographic value or some other
    /// identifier (ex: bonded) that uniquely specifies a SubAccount.
    pub address: String,

    /// If the SubAccount address is not sufficient to uniquely specify a
    /// SubAccount, any other identifying information can be stored here.  It is
    /// important to note that two SubAccounts with identical addresses but
    /// differing metadata will not be considered equal by clients.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// In blockchains with sharded state, the SubNetworkIdentifier is required to
/// query some object on a specific shard. This identifier is optional for all
/// non-sharded blockchains.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct SubNetworkIdentifier {
    pub network: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// The timestamp of the block in milliseconds since the Unix Epoch. The
/// timestamp is stored in milliseconds because some blockchains produce blocks
/// more often than once a second.
#[derive(Debug, Clone, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Timestamp(i64);

/// Transactions contain an array of Operations that are attributable to the
/// same TransactionIdentifier.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct Transaction {
    pub transaction_identifier: TransactionIdentifier,

    pub operations: Vec<Operation>,

    /// Transactions that are related to other transactions (like a cross-shard
    /// transaction) should include the transaction_identifier of these
    /// transactions in the metadata.
    pub metadata: TransactionMetadata,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) enum TransactionType {
    Transaction,
    Receipt,
}

/// Extra data for Transaction
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct TransactionMetadata {
    #[serde(rename = "type")]
    type_: TransactionType,
}

impl From<&near_primitives::views::SignedTransactionView> for Transaction {
    fn from(signed_transaction: &near_primitives::views::SignedTransactionView) -> Self {
        Self {
            transaction_identifier: TransactionIdentifier {
                hash: signed_transaction.hash.to_string(),
            },
            operations: signed_transaction
                .actions
                .iter()
                .enumerate()
                .map(|(index, action)| {
                    let mut operation = Operation::from(action);
                    operation.operation_identifier.index = index.try_into().unwrap();
                    operation.account = Some(AccountIdentifier {
                        address: signed_transaction.receiver_id.clone(),
                        sub_account: None,
                        metadata: None,
                    });
                    operation
                })
                .collect(),
            metadata: TransactionMetadata { type_: TransactionType::Transaction },
        }
    }
}

impl From<&near_primitives::views::ReceiptView> for Transaction {
    fn from(receipt: &near_primitives::views::ReceiptView) -> Self {
        let metadata = TransactionMetadata { type_: TransactionType::Receipt };
        let operations = match &receipt.receipt {
            near_primitives::views::ReceiptEnumView::Action { actions, .. } => actions
                .iter()
                .enumerate()
                .map(|(index, action)| {
                    let mut operation = Operation::from(action);
                    operation.operation_identifier.index = index.try_into().unwrap();
                    operation.account = Some(AccountIdentifier {
                        address: receipt.receiver_id.clone(),
                        sub_account: None,
                        metadata: None,
                    });
                    operation
                })
                .collect(),
            near_primitives::views::ReceiptEnumView::Data { .. } => vec![],
        };
        Self {
            transaction_identifier: TransactionIdentifier { hash: receipt.receipt_id.to_string() },
            operations,
            metadata,
        }
    }
}

/// The transaction_identifier uniquely identifies a transaction in a particular
/// network and block or in the mempool.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Apiv2Schema)]
pub(crate) struct TransactionIdentifier {
    /// Any transactions that are attributable only to a block (ex: a block
    /// event) should use the hash of the block as the identifier.
    pub hash: String,
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

    /// Any other information that may be useful about versioning of dependent
    /// services should be returned here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}
