use actix::MailboxError;
use near_async::messaging::AsyncSendError;

/// Error occurs in case of failed data fetch
#[derive(Debug)]
pub enum FailedToFetchData {
    MailboxError(MailboxError),
    String(String),
}

impl From<MailboxError> for FailedToFetchData {
    fn from(actix_error: MailboxError) -> Self {
        FailedToFetchData::MailboxError(actix_error)
    }
}

impl From<AsyncSendError> for FailedToFetchData {
    fn from(async_send_error: AsyncSendError) -> Self {
        FailedToFetchData::String(async_send_error.to_string())
    }
}
