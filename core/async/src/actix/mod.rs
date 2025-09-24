/// TODO(#14005): Compatibility layer for actix messages.
impl<T: actix::Message> crate::messaging::Message for T {}
