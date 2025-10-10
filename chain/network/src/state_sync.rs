/// State sync response from peers.
#[derive(Debug)]
pub enum StateSyncResponse {
    HeaderResponse,
    PartResponse,
}
