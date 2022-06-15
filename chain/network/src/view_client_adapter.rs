use futures::future::BoxFuture;
use futures::Future;
use near_network_primitives::types::{NetworkViewClientMessages, NetworkViewClientResponses};
use std::fmt;
use std::sync::Arc;

/// [`ViewClientAdapter`] defines the subset of view client's interface which is
/// needed by the network.
///
/// It exists primarily to break inter-crate dependencies -- network should not
/// know the specific type of the client.
///
/// [`ViewClientAdapter`] is a [`Clone`] handle to some concurrent object
/// running in a different thread or task, so [`ViewClientAdapter::send`] is
/// `async`.
///
/// In theory, sending can fail if the other side has already shut down. At the
/// moment, we don't impose any lifetimes constraints here, and generally just
/// ignore this case.
///
/// Implementation wise, this is a struct which holds a `dyn Trait` inside. As
/// we only need to customize a single method (sending a message), a
/// [`ViewClientAdapter`] can be constructed from arbitrary closure.
#[derive(Clone)]
pub struct ViewClientAdapter {
    inner: Arc<
        dyn Fn(
                NetworkViewClientMessages,
            )
                -> BoxFuture<'static, Result<NetworkViewClientResponses, ViewClientIsDeadError>>
            + Send
            + Sync,
    >,
}

impl ViewClientAdapter {
    pub fn new<F, Fut>(send_msg: F) -> ViewClientAdapter
    where
        F: Fn(NetworkViewClientMessages) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<NetworkViewClientResponses, ViewClientIsDeadError>>
            + Send
            + 'static,
    {
        let inner = Arc::new(move |msg| Box::pin(send_msg(msg)) as BoxFuture<_>);
        ViewClientAdapter { inner }
    }

    pub(crate) fn send(
        &self,
        msg: NetworkViewClientMessages,
    ) -> impl Future<Output = Result<NetworkViewClientResponses, ViewClientIsDeadError>> {
        (self.inner)(msg)
    }
}

pub struct ViewClientIsDeadError;
impl fmt::Display for ViewClientIsDeadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ViewClient is dead")
    }
}

impl From<actix::Recipient<NetworkViewClientMessages>> for ViewClientAdapter {
    fn from(recipient: actix::Recipient<NetworkViewClientMessages>) -> Self {
        Self::new(move |msg| {
            let fut = recipient.send(msg);
            async move {
                let res = fut.await;
                match res {
                    Ok(it) => Ok(it),
                    Err(actix::MailboxError::Closed) => Err(ViewClientIsDeadError),
                    Err(actix::MailboxError::Timeout) => {
                        unreachable!("don't actually call `send().timeout()`")
                    }
                }
            }
        })
    }
}
