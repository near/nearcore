#[derive(Debug, strum::EnumIter)]
pub(crate) enum ErrorKind {
    InvalidInput(String),
    NotFound(String),
    WrongNetwork(String),
    Timeout(String),
    InternalInvariantError(String),
    InternalError(String),
}

impl std::convert::From<actix::MailboxError> for ErrorKind {
    fn from(err: actix::MailboxError) -> Self {
        Self::InternalError(format!(
            "Server seems to be under a heavy load thus reaching a limit of Actix queue: {}",
            err
        ))
    }
}

impl std::convert::From<tokio::time::error::Elapsed> for ErrorKind {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout("The operation timed out.".to_string())
    }
}

impl std::convert::From<near_client::TxStatusError> for ErrorKind {
    fn from(err: near_client::TxStatusError) -> Self {
        match err {
            near_client::TxStatusError::ChainError(err) => Self::InternalInvariantError(format!(
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
            near_client::TxStatusError::InternalError(_)
            | near_client::TxStatusError::TimeoutError => {
                // TODO: remove the statuses from TxStatusError since they are
                // never constructed by the view client (it is a leak of
                // abstraction introduced in JSONRPC)
                Self::InternalInvariantError(format!(
                    "TxStatusError reached unexpected state: {:?}",
                    err
                ))
            }
        }
    }
}

impl std::convert::From<near_client_primitives::types::GetStateChangesError> for ErrorKind {
    fn from(err: near_client_primitives::types::GetStateChangesError) -> Self {
        match err {
            near_client_primitives::types::GetStateChangesError::IOError { error_message } => {
                Self::InternalError(error_message)
            }
            near_client_primitives::types::GetStateChangesError::NotSyncedYet => {
                Self::NotFound(err.to_string())
            }
            near_client_primitives::types::GetStateChangesError::UnknownBlock { error_message } => {
                Self::NotFound(error_message)
            }
            near_client_primitives::types::GetStateChangesError::Unreachable { error_message } => {
                Self::InternalError(error_message)
            }
        }
    }
}
