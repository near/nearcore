use near_async::messaging::Message;
use tracing::{Span, debug_span};

#[derive(Debug, Clone)]
pub struct SpanWrapped<T> {
    _span: Span,
    msg: T,
}

impl<T: Message> Message for SpanWrapped<T> {}

impl<T> SpanWrapped<T> {
    pub fn span_unwrap(self) -> T {
        self.msg
    }
}

impl<T> From<T> for SpanWrapped<T> {
    fn from(msg: T) -> Self {
        Self { _span: debug_span!("pending_message", message_type = pretty_type_name::<T>()), msg }
    }
}

impl<T: PartialEq> PartialEq for SpanWrapped<T> {
    fn eq(&self, other: &Self) -> bool {
        self.msg == other.msg
    }
}

impl<T: Eq> Eq for SpanWrapped<T> {}

pub trait SpanWrappedMessageExt: Sized {
    fn span_wrap(self) -> SpanWrapped<Self> {
        self.into()
    }
}

impl<T: Message> SpanWrappedMessageExt for T {}

// Quick and dirty way of getting the type name without the module path.
// Does not work for more complex types like std::sync::Arc<std::sync::atomic::AtomicBool<...>>
// example near_chunks::shards_manager_actor::ShardsManagerActor -> ShardsManagerActor
fn pretty_type_name<T>() -> &'static str {
    std::any::type_name::<T>().split("::").last().unwrap()
}
