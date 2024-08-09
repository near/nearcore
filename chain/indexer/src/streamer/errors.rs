use actix::MailboxError;

use crate::streamer::CryptoHash;

#[derive(thiserror::Error, Debug)]
pub enum IndexerError {
    #[error("Mailbox error: {0}")]
    MailboxError(#[from] MailboxError),
    #[error("Failed to fetch data: {0}")]
    FailedToFetchData(String),
    #[error("Failed to find local delayed receipt {receipt_id} in previous 1000 blocks")]
    LocalDelayedReceiptNotFound { receipt_id: CryptoHash },
    #[error("Internal error occured in indexer: {0}")]
    InternalError(String),
}
