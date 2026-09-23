use crate::INDEXER;
use near_async::messaging::{AsyncSender, CanSendAsync, IntoMultiSender};
use near_client::indexer::FailedToFetchData;
use near_client::{Status, StatusResponse};
use near_client_primitives::types::StatusError;
use near_o11y::span_wrapped_msg::{SpanWrapped, SpanWrappedMessageExt};

#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
struct IndexerClientSender {
    pub status_sender: AsyncSender<SpanWrapped<Status>, Result<StatusResponse, StatusError>>,
}

#[derive(Clone)]
pub struct IndexerClientFetcher {
    sender: IndexerClientSender,
}

impl IndexerClientFetcher {
    pub(crate) async fn fetch_status(&self) -> Result<StatusResponse, FailedToFetchData> {
        tracing::debug!(target: INDEXER, "fetch status");
        self.sender
            .send_async(Status { is_health_check: false, detailed: false }.span_wrap())
            .await?
            .map_err(|err| FailedToFetchData::String(err.to_string()))
    }
}

impl<T: IntoMultiSender<IndexerClientSender>> From<T> for IndexerClientFetcher {
    fn from(value: T) -> Self {
        Self { sender: value.into_multi_sender() }
    }
}
