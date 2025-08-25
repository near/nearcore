/// TODO(#14005): Compatibility layer for actix messages.
impl<T: actix::Message> crate::messaging::Message for T {}

/// TODO(#14005): Inline all of these.
pub type ActixResult<T> = <T as actix::Message>::Result;
