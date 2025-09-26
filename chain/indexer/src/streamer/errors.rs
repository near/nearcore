use near_async::messaging::AsyncSendError;

/// Error occurs in case of failed data fetch
#[derive(Debug)]
pub enum FailedToFetchData {
    String(String),
}

impl From<AsyncSendError> for FailedToFetchData {
    fn from(async_send_error: AsyncSendError) -> Self {
        match async_send_error {
            AsyncSendError::Closed => FailedToFetchData::String("Actor is closed".to_string()),
            AsyncSendError::Timeout => {
                FailedToFetchData::String("Actor send timed out".to_string())
            }
            AsyncSendError::Dropped => {
                FailedToFetchData::String("Actor send was dropped".to_string())
            }
        }
    }
}
