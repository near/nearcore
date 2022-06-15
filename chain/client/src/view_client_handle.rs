use actix::Addr;
use futures::FutureExt;
use near_client_primitives::types::{TxStatus, TxStatusError};
use near_network_primitives::types::{NetworkViewClientHandle, ViewClientIsDeadError};
use near_primitives::views::FinalExecutionOutcomeViewEnum;

use crate::ViewClientActor;

/// ViewClientHandle is the public API for read-only queries to chain.
///
/// It is an asynchronous API: the chain generally exists on a different thread
/// from `ViewClientHandle`, so its methods don't do computation directly, but
/// rather just send messages to that other thread. That's why this is a
/// "Handle", rather than just "View Client".
///
/// At the moment `ViewClientHandle` doesn't express any particular ownership
/// over the chain: even if you have `ViewClientHandle`, the chain might already
/// be dead. in such cases, methods here can return an error or panic.
///
/// Historically, we directly exposed underlying actix' actor here. Now we are
/// moving away from actix, and, as a first step, trying to make sure that actix
/// types do not leak outside. This isn't a one PR worth of work though, so you
/// are witnessing an intermediate state here!
#[derive(Clone)]
pub struct ViewClientHandle {
    addr: Addr<ViewClientActor>,
}

impl ViewClientHandle {
    /// NB: This is very intentionally `pub(crate)`, don't make this public.
    pub(crate) fn new(addr: Addr<ViewClientActor>) -> ViewClientHandle {
        ViewClientHandle { addr }
    }

    pub async fn tx_status(
        &self,
        params: TxStatus,
    ) -> Result<Option<FinalExecutionOutcomeViewEnum>, TxStatusError> {
        match self.send(params).await {
            Ok(it) => it,
            Err(mailbox_error) => {
                Result::Err(TxStatusError::InternalError(mailbox_error.to_string()))
            }
        }
    }

    pub fn send<M>(&self, msg: M) -> actix::prelude::Request<ViewClientActor, M>
    where
        M: actix::Message + Send + 'static,
        M::Result: Send,
        ViewClientActor: actix::Handler<M>,
    {
        self.addr.send(msg)
    }

    pub fn network_handle(self) -> NetworkViewClientHandle {
        NetworkViewClientHandle::new(move |msg| {
            self.send(msg).map(|it| {
                it.map_err(|err| match err {
                    actix::MailboxError::Closed | actix::MailboxError::Timeout => {
                        ViewClientIsDeadError
                    }
                })
            })
        })
    }
}
