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

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcCallFunctionResponse {
    #[serde(flatten)]
    pub result: near_primitives::views::CallResult,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

#[derive(thiserror::Error, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcCallFunctionError {
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
    #[error("Function call returned an error: {vm_error:?}")]
    ContractExecutionError {
        vm_error: RpcFunctionCallError,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

/// Mirror of `near_primitives::errors::FunctionCallError` for the RPC layer.
///
/// The `ExecutionError` variant holds a `serde_json::Value` instead of a `String`.
/// When the panic message starts with `"Smart contract panicked: "` and the
/// remainder is valid JSON, the parsed JSON is stored directly; otherwise the
/// original string is kept as a JSON string value.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[repr(u8)]
pub enum RpcFunctionCallError {
    CompilationError(near_primitives::errors::CompilationError) = 0,
    LinkError { msg: String } = 1,
    MethodResolveError(near_primitives::errors::MethodResolveError) = 2,
    WasmTrap(near_primitives::errors::WasmTrap) = 3,
    WasmUnknownError = 4,
    HostError(near_primitives::errors::HostError) = 5,
    ExecutionError(serde_json::Value) = 7,
}

impl From<near_primitives::errors::FunctionCallError> for RpcFunctionCallError {
    fn from(error: near_primitives::errors::FunctionCallError) -> Self {
        const PREFIX: &str = "Smart contract panicked: ";

        match error {
            near_primitives::errors::FunctionCallError::CompilationError(e) => {
                Self::CompilationError(e)
            }
            near_primitives::errors::FunctionCallError::LinkError { msg } => {
                Self::LinkError { msg }
            }
            near_primitives::errors::FunctionCallError::MethodResolveError(e) => {
                Self::MethodResolveError(e)
            }
            near_primitives::errors::FunctionCallError::WasmTrap(e) => Self::WasmTrap(e),
            near_primitives::errors::FunctionCallError::WasmUnknownError => Self::WasmUnknownError,
            near_primitives::errors::FunctionCallError::HostError(e) => Self::HostError(e),
            near_primitives::errors::FunctionCallError::ExecutionError(msg) => {
                let value = msg
                    .strip_prefix(PREFIX)
                    .and_then(|json_str| serde_json::from_str(json_str).ok())
                    .unwrap_or_else(|| serde_json::Value::String(msg));
                Self::ExecutionError(value)
            }
            // _EVMError is unused, but handle it gracefully
            _ => Self::ExecutionError(serde_json::Value::String(format!("{error:?}"))),
        }
    }
}

impl From<RpcCallFunctionError> for crate::errors::RpcError {
    fn from(error: RpcCallFunctionError) -> Self {
        let error_data = Some(serde_json::Value::String(error.to_string()));
        let error_data_value = match serde_json::to_value(&error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcCallFunctionError: {:?}", err),
                );
            }
        };
        Self::new_internal_or_handler_error(error_data, error_data_value)
    }
}

impl From<crate::types::query::RpcQueryError> for RpcCallFunctionError {
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
            crate::types::query::RpcQueryError::NoContractCode {
                contract_account_id,
                block_height,
                block_hash,
            } => Self::NoContractCode { contract_account_id, block_height, block_hash },
            crate::types::query::RpcQueryError::ContractExecutionError {
                vm_error: _,
                error,
                block_height,
                block_hash,
            } => Self::ContractExecutionError { vm_error: error.into(), block_height, block_hash },
            crate::types::query::RpcQueryError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            unexpected => Self::InternalError {
                error_message: format!("Unexpected query error: {unexpected:?}"),
            },
        }
    }
}
