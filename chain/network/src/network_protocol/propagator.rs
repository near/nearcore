const TRACE_FLAG_DEFERRED: TraceFlags = TraceFlags::new(0x02);

use crate::network_protocol::proto::TraceContext;
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use opentelemetry::Context;
use protobuf::MessageField;

#[derive(Debug)]
enum SamplingPriority {
    UserReject = -1,
    AutoReject = 0,
    AutoKeep = 1,
    UserKeep = 2,
}

#[derive(Debug)]
pub(crate) enum ExtractError {
    TraceId,
    SpanId,
    SamplingPriority,
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

    fn extract_sampling_priority(
        &self,
        sampling_priority: i32,
    ) -> Result<SamplingPriority, ExtractError> {
        match sampling_priority {
            -1 => Ok(SamplingPriority::UserReject),
            0 => Ok(SamplingPriority::AutoReject),
            1 => Ok(SamplingPriority::AutoKeep),
            2 => Ok(SamplingPriority::UserKeep),
            _ => Err(ExtractError::SamplingPriority),
        }
    }

    pub(crate) fn extract_span_context(
        &self,
        trace_context: &MessageField<TraceContext>,
    ) -> Result<SpanContext, ExtractError> {
        let trace_id = self.extract_trace_id(&trace_context.trace_id)?;
        // If we have a trace_id but can't get the parent span, we default it to invalid instead of completely erroring
        // out so that the rest of the spans aren't completely lost
        let span_id = self.extract_span_id(&trace_context.span_id).unwrap_or(SpanId::INVALID);
        let sampling_priority = self.extract_sampling_priority(trace_context.sampling_priority);
        let sampled = match sampling_priority {
            Ok(SamplingPriority::UserReject) | Ok(SamplingPriority::AutoReject) => {
                TraceFlags::default()
            }
            Ok(SamplingPriority::UserKeep) | Ok(SamplingPriority::AutoKeep) => TraceFlags::SAMPLED,
            // Treat the sampling as DEFERRED instead of erring on extracting the span context
            Err(_) => TRACE_FLAG_DEFERRED,
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
                trace_context.sampling_priority = sampling_priority as i32;
            }
            MessageField::some(trace_context)
        } else {
            MessageField::none()
        }
    }
}
