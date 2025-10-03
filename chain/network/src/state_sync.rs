use near_async::Message;

/// State sync response from peers.
#[derive(Message, Debug)]
pub enum StateSyncResponse {
    HeaderResponse,
    PartResponse,
}
