use std::sync::Arc;

/// Anything that allows sending a message of type M.
pub trait Sender<M>: Send + Sync + 'static {
    fn send(&self, message: M);
}

pub type ArcSender<M> = Arc<dyn Sender<M>>;

// TODO: Sender implementation for actix::Addr and other utils.
