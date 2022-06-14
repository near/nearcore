use actix::Addr;

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
pub type ViewClientHandle = Addr<ViewClientActor>;
