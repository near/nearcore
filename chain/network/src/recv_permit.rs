use tokio::sync::OwnedSemaphorePermit;

/// A permit obtained when receiving a message from the network.
/// Held alive for as long as the message exists, to limit total
/// memory consumed by incoming messages.
#[derive(Debug)]
#[must_use]
pub struct RecvMessagePermit {
    _permit: Option<OwnedSemaphorePermit>,
}

impl RecvMessagePermit {
    pub fn new(permit: OwnedSemaphorePermit) -> Self {
        Self { _permit: Some(permit) }
    }

    /// For locally-generated messages that don't come from the network.
    pub fn none() -> Self {
        Self { _permit: None }
    }
}
