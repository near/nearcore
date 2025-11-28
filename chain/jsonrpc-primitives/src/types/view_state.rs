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

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewStateResponse {
    #[serde(flatten)]
    pub state: near_primitives::views::ViewStateResult,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

#[derive(thiserror::Error, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcViewStateError {
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
    #[error("State of contract {contract_account_id} is too large to be viewed")]
    TooLargeContractState {
        contract_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

impl From<RpcViewStateError> for crate::errors::RpcError {
    fn from(error: RpcViewStateError) -> Self {
        let error_data = Some(serde_json::Value::String(error.to_string()));
        let error_data_value = match serde_json::to_value(&error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcViewStateError: {:?}", err),
                );
            }
        };
        Self::new_internal_or_handler_error(error_data, error_data_value)
    }
}

impl From<crate::types::query::RpcQueryError> for RpcViewStateError {
    fn from(error: crate::types::query::RpcQueryError) -> Self {
        match error {
            crate::types::query::RpcQueryError::NoSyncedBlocks => Self::InternalError {
                error_message: "There are no fully synchronized blocks on the node yet".to_string(),
            },
            crate::types::query::RpcQueryError::UnavailableShard { requested_shard_id } => {
                Self::InternalError {
                    error_message: format!(
                        "The node does not track the shard ID {requested_shard_id}"
                    ),
                }
            }
            crate::types::query::RpcQueryError::UnknownBlock { block_reference } => {
                Self::UnknownBlock { block_reference }
            }
            crate::types::query::RpcQueryError::InvalidAccount {
                requested_account_id,
                block_height,
                block_hash,
            } => Self::InvalidAccount { requested_account_id, block_height, block_hash },
            crate::types::query::RpcQueryError::UnknownAccount {
                requested_account_id,
                block_height,
                block_hash,
            } => Self::UnknownAccount { requested_account_id, block_height, block_hash },
            crate::types::query::RpcQueryError::GarbageCollectedBlock { block_height, .. } => {
                Self::InternalError {
                    error_message: format!(
                        "The data for block #{block_height} is garbage collected on this node, use an archival node to fetch historical data"
                    ),
                }
            }
            crate::types::query::RpcQueryError::TooLargeContractState {
                contract_account_id,
                block_height,
                block_hash,
            } => Self::TooLargeContractState { contract_account_id, block_height, block_hash },
            crate::types::query::RpcQueryError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            unexpected => Self::InternalError {
                error_message: format!("Unexpected query error: {unexpected:?}"),
            },
        }
    }
}
