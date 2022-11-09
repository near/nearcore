use crate::network_protocol::proto::trace_context::SamplingPriority;
use crate::network_protocol::proto::TraceContext;
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use opentelemetry::Context;
use protobuf::{EnumOrUnknown, MessageField};

const TRACE_FLAG_DEFERRED: TraceFlags = TraceFlags::new(0x02);

#[derive(Debug)]
pub(crate) enum ExtractError {
    TraceId,
    SpanId,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct NodePropagator {}

impl NodePropagator {
    pub fn new() -> Self {
        NodePropagator::default()
    }

    fn extract_trace_id(&self, trace_id: &[u8]) -> Result<TraceId, ExtractError> {
        Ok(TraceId::from_bytes(trace_id.try_into().map_err(|_| ExtractError::TraceId)?))
    }

    fn extract_span_id(&self, span_id: &[u8]) -> Result<SpanId, ExtractError> {
        Ok(SpanId::from_bytes(span_id.try_into().map_err(|_| ExtractError::SpanId)?))
    }

    pub(crate) fn extract_span_context(
        &self,
        trace_context: &MessageField<TraceContext>,
    ) -> Result<SpanContext, ExtractError> {
        let trace_id = self.extract_trace_id(&trace_context.trace_id)?;
        // If we have a trace_id but can't get the parent span, we default it to invalid instead of completely erroring
        // out so that the rest of the spans aren't completely lost
        let span_id = self.extract_span_id(&trace_context.span_id).unwrap_or(SpanId::INVALID);
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
    }

    pub(crate) fn inject_trace_context(&self, cx: &Context) -> MessageField<TraceContext> {
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
}
