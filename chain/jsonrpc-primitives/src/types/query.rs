#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcQueryRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    #[serde(flatten)]
    pub request: near_primitives::views::QueryRequest,
}

#[derive(thiserror::Error, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcQueryError {
    #[error("There are no fully synchronized blocks on the node yet")]
    NoSyncedBlocks,
    #[error("The node does not track the shard ID {requested_shard_id}")]
    UnavailableShard { requested_shard_id: near_primitives::types::ShardId },
    #[error(
        "The data for block #{block_height} is garbage collected on this node, use an archival node to fetch historical data"
    )]
    GarbageCollectedBlock {
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error(
        "Block either has never been observed on the node or has been garbage collected: {block_reference:?}"
    )]
    UnknownBlock { block_reference: near_primitives::types::BlockReference },
    #[error("Account ID {requested_account_id} is invalid")]
    InvalidAccount {
        requested_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("account {requested_account_id} does not exist while viewing")]
    UnknownAccount {
        requested_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error(
        "Contract code for contract ID #{contract_account_id} has never been observed on the node"
    )]
    NoContractCode {
        contract_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("State of contract {contract_account_id} is too large to be viewed")]
    TooLargeContractState {
        contract_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("Access key for public key {public_key} has never been observed on the node")]
    UnknownAccessKey {
        public_key: near_crypto::PublicKey,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("Gas key for public key {public_key} has never been observed on the node")]
    UnknownGasKey {
        public_key: near_crypto::PublicKey,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("Function call returned an error: {error:?}")]
    ContractExecutionError {
        error: near_primitives::errors::FunctionCallError,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error(
        "Global contract code with identifier {identifier:?} has never been observed on the node"
    )]
    NoGlobalContractCode {
        identifier: near_primitives::action::GlobalContractIdentifier,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcQueryResponse {
    #[serde(flatten)]
    pub kind: QueryResponseKind,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum QueryResponseKind {
    ViewAccount(near_primitives::views::AccountView),
    ViewCode(near_primitives::views::ContractCodeView),
    ViewState(near_primitives::views::ViewStateResult),
    CallResult(near_primitives::views::CallResult),
    AccessKey(near_primitives::views::AccessKeyView),
    AccessKeyList(near_primitives::views::AccessKeyList),
    GasKey(near_primitives::views::GasKeyView),
    GasKeyList(near_primitives::views::GasKeyList),
}

// ==================== view_account ====================

/// Request type for `view_account` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewAccountRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    pub account_id: near_primitives::types::AccountId,
}

/// Response type for `view_account` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewAccountResponse {
    #[serde(flatten)]
    pub account: near_primitives::views::AccountView,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

// ==================== view_code ====================

/// Request type for `view_code` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewCodeRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    pub account_id: near_primitives::types::AccountId,
}

/// Response type for `view_code` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewCodeResponse {
    #[serde(flatten)]
    pub code: near_primitives::views::ContractCodeView,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

// ==================== view_state ====================

/// Request type for `view_state` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewStateRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    pub account_id: near_primitives::types::AccountId,
    #[serde(rename = "prefix_base64")]
    pub prefix: near_primitives::types::StoreKey,
    #[serde(default)]
    pub include_proof: bool,
}

/// Response type for `view_state` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewStateResponse {
    #[serde(flatten)]
    pub state: near_primitives::views::ViewStateResult,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

// ==================== view_access_key ====================

/// Request type for `view_access_key` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewAccessKeyRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    pub account_id: near_primitives::types::AccountId,
    pub public_key: near_crypto::PublicKey,
}

/// Response type for `view_access_key` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewAccessKeyResponse {
    #[serde(flatten)]
    pub access_key: near_primitives::views::AccessKeyView,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

// ==================== view_access_key_list ====================

/// Request type for `view_access_key_list` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewAccessKeyListRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    pub account_id: near_primitives::types::AccountId,
}

/// Response type for `view_access_key_list` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewAccessKeyListResponse {
    #[serde(flatten)]
    pub access_key_list: near_primitives::views::AccessKeyList,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

// ==================== call_function ====================

/// Request type for `call_function` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcCallFunctionRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    pub account_id: near_primitives::types::AccountId,
    pub method_name: String,
    #[serde(rename = "args_base64")]
    pub args: near_primitives::types::FunctionArgs,
}

/// Response type for `call_function` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcCallFunctionResponse {
    #[serde(flatten)]
    pub result: near_primitives::views::CallResult,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

// ==================== view_gas_key ====================

/// Request type for `view_gas_key` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewGasKeyRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    pub account_id: near_primitives::types::AccountId,
    pub public_key: near_crypto::PublicKey,
}

/// Response type for `view_gas_key` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewGasKeyResponse {
    #[serde(flatten)]
    pub gas_key: near_primitives::views::GasKeyView,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

// ==================== view_gas_key_list ====================

/// Request type for `view_gas_key_list` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewGasKeyListRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    pub account_id: near_primitives::types::AccountId,
}

/// Response type for `view_gas_key_list` RPC method.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewGasKeyListResponse {
    #[serde(flatten)]
    pub gas_key_list: near_primitives::views::GasKeyList,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

impl From<RpcQueryError> for crate::errors::RpcError {
    fn from(error: RpcQueryError) -> Self {
        let error_data = Some(serde_json::Value::String(error.to_string()));
        let error_data_value = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcQueryError: {:?}", err),
                );
            }
        };
        Self::new_internal_or_handler_error(error_data, error_data_value)
    }
}
