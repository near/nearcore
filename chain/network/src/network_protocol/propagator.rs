const NODE_TRACE_ID_HEADER: &str = "x-node-trace-id";
const NODE_PARENT_ID_HEADER: &str = "x-node-parent-id";
const NODE_SAMPLING_PRIORITY_HEADER: &str = "x-node-sampling";
const TRACE_FLAG_DEFERRED: TraceFlags = TraceFlags::new(0x02);

use itertools::Itertools;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use opentelemetry::Context;
use std::collections::HashMap;
use std::str::Utf8Error;

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

#[derive(thiserror::Error, Debug)]
pub enum HeadersError {
    #[error("ToStrError")]
    ToStrError(#[source] Utf8Error),
}

#[derive(Debug, Default, Clone)]
pub(crate) struct NodePropagator {}

impl NodePropagator {
    pub fn new() -> Self {
        NodePropagator::default()
    }
    fn extract_trace_id(&self, trace_id: &str) -> Result<TraceId, ExtractError> {
        trace_id
            .parse::<u128>()
            .map(|id| TraceId::from((id as u128).to_be_bytes()))
            .map_err(|_| ExtractError::TraceId)
    }

    fn extract_span_id(&self, span_id: &str) -> Result<SpanId, ExtractError> {
        span_id
            .parse::<u64>()
            .map(|id| SpanId::from(id.to_be_bytes()))
            .map_err(|_| ExtractError::SpanId)
    }

    fn extract_sampling_priority(
        &self,
        sampling_priority: &str,
    ) -> Result<SamplingPriority, ExtractError> {
        let i = sampling_priority.parse::<i32>().map_err(|_| ExtractError::SamplingPriority)?;

        match i {
            -1 => Ok(SamplingPriority::UserReject),
            0 => Ok(SamplingPriority::AutoReject),
            1 => Ok(SamplingPriority::AutoKeep),
            2 => Ok(SamplingPriority::UserKeep),
            _ => Err(ExtractError::SamplingPriority),
        }
    }

    pub(crate) fn extract_span_context(
        &self,
        extractor: &dyn Extractor,
    ) -> Result<SpanContext, ExtractError> {
        let trace_id = self.extract_trace_id(extractor.get(NODE_TRACE_ID_HEADER).unwrap_or(""))?;
        // If we have a trace_id but can't get the parent span, we default it to invalid instead of completely erroring
        // out so that the rest of the spans aren't completely lost
        let span_id = self
            .extract_span_id(extractor.get(NODE_PARENT_ID_HEADER).unwrap_or(""))
            .unwrap_or(SpanId::INVALID);
        let sampling_priority = self
            .extract_sampling_priority(extractor.get(NODE_SAMPLING_PRIORITY_HEADER).unwrap_or(""));
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

    pub(crate) fn inject_context(&self, cx: &Context, injector: &mut dyn Injector) {
        let span = cx.span();
        let span_context = span.span_context();
        if span_context.is_valid() {
            injector.set(
                NODE_TRACE_ID_HEADER,
                (u128::from_be_bytes(span_context.trace_id().to_bytes()) as u128).to_string(),
            );
            injector.set(
                NODE_PARENT_ID_HEADER,
                u64::from_be_bytes(span_context.span_id().to_bytes()).to_string(),
            );
            if span_context.trace_flags() & TRACE_FLAG_DEFERRED != TRACE_FLAG_DEFERRED {
                let sampling_priority = if span_context.is_sampled() {
                    SamplingPriority::AutoKeep
                } else {
                    SamplingPriority::AutoReject
                };
                injector.set(NODE_SAMPLING_PRIORITY_HEADER, (sampling_priority as i32).to_string());
            }
        } else {
            tracing::warn!(
                trace_id=?span_context.trace_id(),
                span_id=?span_context.span_id(),
                "span_context invalid trace_id and span_id",
            );
        }
    }

    pub(crate) fn deserialize_headers(s: &[u8]) -> Result<HashMap<String, String>, HeadersError> {
        let s = std::str::from_utf8(s).map_err(HeadersError::ToStrError)?;
        let res = s
            .split(";")
            .filter_map(|kv| {
                if let Some((k, v)) = kv.split_once("=") {
                    Some((k.to_owned(), v.to_owned()))
                } else {
                    None
                }
            })
            .collect();
        Ok(res)
    }

    pub(crate) fn serialize_headers(extractor: &dyn Extractor) -> Vec<u8> {
        let serialized_str = extractor
            .keys()
            .iter()
            .map(|k| format!("{}={}", k, extractor.get(k).unwrap()))
            .join(";");
        serialized_str.into_bytes()
    }
}
