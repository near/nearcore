use crate::peer::framed_read::{ThrottleController, ThrottleToken};
use actix::Message;

// Wrapper around Actix messages, used to track size of all messages sent to PeerManager.
// TODO(#5155) Finish implementation of this.

/// TODO - Once we start using this `ActixMessageWrapper` we will need to make following changes
/// to get this struct to work
/// - Add needed decorators. Probably `Debug`, `Message` from Actix, etc.
/// - Add two rate limiters (local per peer, global one)
/// - Any other metadata we need debugging, etc.
pub(crate) struct ActixMessageWrapper<T> {
    msg: T,
    throttle_token: ThrottleToken,
}

impl<T> ActixMessageWrapper<T> {
    pub fn new_without_size(msg: T, throttle_controller: Option<ThrottleController>) -> Self {
        Self { msg, throttle_token: ThrottleToken::new_without_size(throttle_controller) }
    }

    pub fn take(self) -> (T, ThrottleToken) {
        (self.msg, self.throttle_token)
    }
}

impl<T: Message> Message for ActixMessageWrapper<T> {
    type Result = ActixMessageResponse<T::Result>;
}

#[derive(actix::MessageResponse)]
pub(crate) struct ActixMessageResponse<T> {
    _msg: T,
    /// Ignore the warning, this code is used. We decrease counters `throttle_controller` when
    /// this attribute gets dropped.
    #[allow(unused)]
    throttle_token: ThrottleToken,
}

impl<T> ActixMessageResponse<T> {
    pub fn new(msg: T, throttle_token: ThrottleToken) -> Self {
        Self { _msg: msg, throttle_token }
    }
}
