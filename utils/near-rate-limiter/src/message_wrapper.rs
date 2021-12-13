use crate::{ThrottleController, ThrottleToken};
use actix::dev::MessageResponse;
use actix::Message;

// Wrapper around Actix messages, used to track size of all messages sent to PeerManager.
// TODO(#5155) Finish implementation of this.

#[allow(unused)]
/// TODO - Once we start using this `ActixMessageWrapper` we will need to make following changes
/// to get this struct to work
/// - Add needed decorators. Probably `Debug`, `Message` from Actix, etc.
/// - Add two rate limiters (local per peer, global one)
/// - Any other metadata we need debugging, etc.
pub struct ActixMessageWrapper<T> {
    msg: T,
    throttle_token: ThrottleToken,
}

impl<T> ActixMessageWrapper<T> {
    pub fn new_without_size(msg: T, throttle_controller: Option<ThrottleController>) -> Self {
        Self { msg, throttle_token: ThrottleToken::new_without_size(throttle_controller) }
    }

    #[allow(unused)]
    pub fn into_inner(mut self) -> T {
        self.msg
    }

    #[allow(unused)]
    pub fn take(mut self) -> (T, ThrottleToken) {
        (self.msg, self.throttle_token)
    }
}

impl<T: Message> Message for ActixMessageWrapper<T> {
    type Result = ActixMessageResponse<T::Result>;
}

#[derive(MessageResponse)]
pub struct ActixMessageResponse<T> {
    msg: T,
    /// Ignore the warning, this code is used. We decrease counters `throttle_controller` when
    /// this attribute gets dropped.
    #[allow(unused)]
    throttle_token: ThrottleToken,
}

impl<T> ActixMessageResponse<T> {
    #[allow(unused)]
    pub fn new(msg: T, throttle_token: ThrottleToken) -> Self {
        Self { msg, throttle_token }
    }

    #[allow(unused)]
    pub fn into_inner(mut self) -> T {
        self.msg
    }

    #[allow(unused)]
    pub fn take(mut self) -> (T, ThrottleToken) {
        (self.msg, self.throttle_token)
    }
}
