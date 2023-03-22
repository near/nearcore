use crate::network_protocol::proto::trace_context::SamplingPriority;
use crate::network_protocol::proto::TraceContext;
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use opentelemetry::Context;
use protobuf::{EnumOrUnknown, MessageField};

/// Lowest available value.
/// 0x01 is reserved for `SAMPLED`: https://docs.rs/opentelemetry/latest/opentelemetry/trace/struct.TraceFlags.html#associatedconstant.SAMPLED
const TRACE_FLAG_DEFERRED: TraceFlags = TraceFlags::new(0x02);

#[derive(Debug, thiserror::Error)]
pub(crate) enum ExtractError {
    #[error("Malformed or invalid TraceId")]
    TraceId,
    #[error("Malformed or invalid SpanId")]
    SpanId,
    #[error("Missing trace_id or span_id")]
    Empty,
}

/// Extracts a `SpanContext` from a potentially empty `TraceContext`.
pub(crate) fn extract_span_context(
    trace_context: &MessageField<TraceContext>,
) -> Result<SpanContext, ExtractError> {
    if trace_context.is_some() {
        let trace_id = extract_trace_id(&trace_context.trace_id)?;
        // If we have a trace_id but can't get the parent span, we default it to invalid instead of completely erroring
        // out so that the rest of the spans aren't completely lost.
        let span_id = extract_span_id(&trace_context.span_id).unwrap_or(SpanId::INVALID);
        let sampled = match trace_context.sampling_priority.enum_value() {
            Ok(SamplingPriority::UserReject) | Ok(SamplingPriority::AutoReject) => {
                TraceFlags::default()
            }
            Ok(SamplingPriority::UserKeep) | Ok(SamplingPriority::AutoKeep) => TraceFlags::SAMPLED,
            // Treat the sampling as DEFERRED instead of erring on extracting the span context
            Ok(SamplingPriority::UNKNOWN) | Err(_) => TRACE_FLAG_DEFERRED,
        };
        let trace_state = TraceState::default();
        Ok(SpanContext::new(trace_id, span_id, sampled, true, trace_state))
    } else {
        Err(ExtractError::Empty)
    }
}

/// Populates `TraceContext` representing the current span.
/// Returns `None` if no current span is available.
pub(crate) fn inject_trace_context(cx: &Context) -> MessageField<TraceContext> {
    let span = cx.span();
    let span_context = span.span_context();
    if span_context.is_valid() {
        let mut trace_context = TraceContext::new();

        // Uses `u128::to_be_bytes()` internally.
        trace_context.trace_id = span_context.trace_id().to_bytes().to_vec();
        // Uses `u64::to_be_bytes()` internally.
        trace_context.span_id = span_context.span_id().to_bytes().to_vec();

        if span_context.trace_flags() & TRACE_FLAG_DEFERRED != TRACE_FLAG_DEFERRED {
            let sampling_priority = if span_context.is_sampled() {
                SamplingPriority::AutoKeep
            } else {
                SamplingPriority::AutoReject
            };
            trace_context.sampling_priority = EnumOrUnknown::new(sampling_priority);
        }
        MessageField::some(trace_context)
    } else {
        MessageField::none()
    }
}
fn extract_trace_id(trace_id: &[u8]) -> Result<TraceId, ExtractError> {
    Ok(TraceId::from_bytes(trace_id.try_into().map_err(|_| ExtractError::TraceId)?))
}

fn extract_span_id(span_id: &[u8]) -> Result<SpanId, ExtractError> {
    Ok(SpanId::from_bytes(span_id.try_into().map_err(|_| ExtractError::SpanId)?))
}
