use borsh::{BorshDeserialize, BorshSerialize};
use opentelemetry::Context;
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};

/// Lowest available value.
/// 0x01 is reserved for `SAMPLED`: https://docs.rs/opentelemetry/latest/opentelemetry/struct.TraceFlags.html#associatedconstant.SAMPLED
const TRACE_FLAG_DEFERRED: TraceFlags = TraceFlags::new(0x02);

/// Inter-process tracing information attached to a borsh-encoded message header.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub(crate) struct TraceContext {
    /// 16 bytes representing `TraceId`: https://docs.rs/opentelemetry/latest/opentelemetry/trace/struct.TraceId.html
    pub trace_id: [u8; 16],
    /// 8 bytes representing `SpanId`: https://docs.rs/opentelemetry/latest/opentelemetry/trace/struct.SpanId.html
    pub span_id: [u8; 8],
    pub sampling_priority: SamplingPriority,
}

/// Sampling decision propagated with the trace context.
/// Discriminants match `proto::trace_context::SamplingPriority`.
#[derive(BorshSerialize, BorshDeserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub(crate) enum SamplingPriority {
    #[default]
    Unspecified = 0,
    AutoReject = 1,
    UserReject = 2,
    AutoKeep = 3,
    UserKeep = 4,
}

impl TraceContext {
    /// Returns the tracing context of the current span, or `None` if it has no valid context.
    pub(crate) fn from_context(cx: &Context) -> Option<Self> {
        let span = cx.span();
        let span_context = span.span_context();
        if !span_context.is_valid() {
            return None;
        }
        let sampling_priority =
            if span_context.trace_flags() & TRACE_FLAG_DEFERRED == TRACE_FLAG_DEFERRED {
                SamplingPriority::Unspecified
            } else if span_context.is_sampled() {
                SamplingPriority::AutoKeep
            } else {
                SamplingPriority::AutoReject
            };
        Some(Self {
            trace_id: span_context.trace_id().to_bytes(),
            span_id: span_context.span_id().to_bytes(),
            sampling_priority,
        })
    }

    /// Converts this context into a `SpanContext` representing a remote parent.
    pub(crate) fn span_context(&self) -> SpanContext {
        span_context(
            TraceId::from_bytes(self.trace_id),
            SpanId::from_bytes(self.span_id),
            self.sampling_priority,
        )
    }
}

fn span_context(
    trace_id: TraceId,
    span_id: SpanId,
    sampling_priority: SamplingPriority,
) -> SpanContext {
    let sampled = match sampling_priority {
        SamplingPriority::Unspecified => TRACE_FLAG_DEFERRED,
        SamplingPriority::AutoReject | SamplingPriority::UserReject => TraceFlags::default(),
        SamplingPriority::AutoKeep | SamplingPriority::UserKeep => TraceFlags::SAMPLED,
    };
    SpanContext::new(trace_id, span_id, sampled, true, TraceState::default())
}
