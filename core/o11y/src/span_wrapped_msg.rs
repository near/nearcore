use crate::macros::type_name_of;
use tracing::{Span, debug_span};

#[derive(actix::Message, Debug, Clone)]
#[rtype(result = "<T as actix::Message>::Result")]
pub struct SpanWrapped<T: actix::Message> {
    _span: Span,
    msg: T,
}

impl<T: actix::Message> SpanWrapped<T> {
    pub fn span_unwrap(self) -> T {
        self.msg
    }
}

impl<T: actix::Message> From<T> for SpanWrapped<T> {
    fn from(msg: T) -> Self {
        Self { _span: debug_span!("pending_message", message_type = type_name_of(&msg)), msg }
    }
}

impl<T: actix::Message + PartialEq> PartialEq for SpanWrapped<T> {
    fn eq(&self, other: &Self) -> bool {
        self.msg == other.msg
    }
}

impl<T: actix::Message + Eq> Eq for SpanWrapped<T> {}

pub trait SpanWrappedMessageExt: actix::Message + Sized {
    fn span_wrap(self) -> SpanWrapped<Self> {
        self.into()
    }
}

impl<T: actix::Message> SpanWrappedMessageExt for T {}
