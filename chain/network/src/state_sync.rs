/// State sync response from peers.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub enum StateSyncResponse {
    HeaderResponse,
    PartResponse,
}
