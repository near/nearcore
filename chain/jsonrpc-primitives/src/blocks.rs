#[derive(thiserror::Error, Debug)]
pub enum RpcBlockError {
    #[error("Block `{0}` is missing")]
    BlockMissing(near_primitives::hash::CryptoHash),
    #[error("Block not found")]
    BlockNotFound(String),
    #[error("There are no fully synchronized blocks yet")]
    NotSyncedYet,
    #[error("Too many requests. Try again later: {0}")]
    TooManyRequests(actix::MailboxError),
    #[error("Unexpected error occurred: {0}")]
    Unexpected(String),
}

impl From<near_client_primitives::types::GetBlockError> for RpcBlockError {
    fn from(error: near_client_primitives::types::GetBlockError) -> RpcBlockError {
        match error {
            near_client_primitives::types::GetBlockError::BlockMissing(block_hash) => {
                RpcBlockError::BlockMissing(block_hash)
            }
            near_client_primitives::types::GetBlockError::BlockNotFound(s) => {
                RpcBlockError::BlockNotFound(s)
            }
            near_client_primitives::types::GetBlockError::NotSyncedYet => {
                RpcBlockError::NotSyncedYet
            }
            near_client_primitives::types::GetBlockError::IOError(s)
            | near_client_primitives::types::GetBlockError::Unexpected(s) => {
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNEXPECTED_ERROR_COUNT,
                    &["RpcBlockError", &s],
                );
                RpcBlockError::Unexpected(s)
            }
        }
    }
}

impl From<actix::MailboxError> for RpcBlockError {
    fn from(error: actix::MailboxError) -> RpcBlockError {
        RpcBlockError::TooManyRequests(error)
    }
}
