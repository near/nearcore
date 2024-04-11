use crate::messaging::{CanSend, Sender};

/// Allows a Sender<M> to be used like a Sender<S> as long as S can be converted to M.
pub struct BreakApart<M: 'static> {
    pub(crate) sender: Sender<M>,
}

impl<S, M: From<S> + 'static> CanSend<S> for BreakApart<M> {
    fn send(&self, message: S) {
        self.sender.send(M::from(message))
    }
}
