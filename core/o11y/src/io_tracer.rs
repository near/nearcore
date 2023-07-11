#![cfg(feature = "io_trace")]
//! `tracing` layer to collect the sequence of IO operations executed.
//!
//! This module contains a somewhat quirky way of collecting a detailed log of
//! which IO operations have been executed. The `tracing` crate is abused for
//! this, as it provides an easy way to globally emit such events and organize
//! them in a hierarchy of spans.
//!
//! The goal is to have something to look at to understand what requests are
//! sent to the DB and for what reason. Not necessarily timing of events.(Timing
//! might be affected by the sheer amount of details that this instrumentation
//! is capable of producing.) But rather something that makes it obvious, for
//! example, how effective an existing caching layer is for a workload by
//! showing when the cache was hit and when it was missed.
//!
//! Another, equally important, use case is replaying the output for further
//! analysis. The estimator has a replay command that understands the output
//! produced by the IO trace.

use base64::Engine;
use std::collections::HashMap;
use std::io::Write;
use tracing::{span, Subscriber};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

/// Tracing layer that produces a record of IO operations.
pub struct IoTraceLayer {
    make_writer: NonBlocking,
}

enum IoEventType {
    StorageOp(StorageOp),
    DbOp(DbOp),
}
#[derive(strum::Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
enum StorageOp {
    Read,
    Write,
    Remove,
    Exists,
    Other,
}
#[derive(strum::Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
enum DbOp {
    Get,
    Insert,
    Set,
    UpdateRc,
    Delete,
    DeleteAll,
    Other,
}

/// Formatted but not-yet printed output lines.
///
/// Events are bundled together and only printed after the enclosing span exits.
/// This allows to print information at the tpo that is only available later on.
///
/// Note: Type used as key in `AnyMap` inside span extensions.
struct OutputBuffer(Vec<BufferedLine>);

/// Formatted but not-yet printed output line.
struct BufferedLine {
    indent: usize,
    output_line: String,
}

/// Information added to a span through events happening within.
#[derive(Default)]
struct SpanInfo {
    key_values: Vec<String>,
    counts: HashMap<String, u64>,
}

impl<S: Subscriber + for<'span> LookupSpan<'span>> Layer<S> for IoTraceLayer {
    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx.span(id).unwrap();

        // Store span field values to be printed on exit, after they are
        // enhanced with additional information from events.
        let mut span_info = SpanInfo::default();
        attrs.record(&mut span_info);
        span.extensions_mut().insert(span_info);

        // This will be used to add lines that should be printed below the span
        // opening line.
        span.extensions_mut().insert(OutputBuffer(vec![]));
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() == "io_tracer_count" {
            // Events specifically added to add more info to spans in IO Tracer.
            // Marked with `target: "io_tracer_count"`.
            let mut span = ctx.event_span(event);
            while let Some(parent) = span {
                if let Some(span_info) = parent.extensions_mut().get_mut::<SpanInfo>() {
                    event.record(span_info);
                    break;
                } else {
                    span = parent.parent();
                }
            }
        } else {
            // All other events. These can be for target "io_tracer" or
            // something else, that has values for the `IoEventVisitor`.
            self.record_io_event(event, ctx);
        }
    }

    #[allow(clippy::integer_arithmetic)]
    fn on_exit(&self, id: &span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        // When the span exits, produce one line for the span itself that
        // includes key=value pairs from `SpanInfo`. Then also add indentation
        // to all lines buffered in the `OutputBuffer` extension.
        // If no parent span exists, print all buffered lines.
        let span = ctx.span(id).unwrap();
        let name = span.name();
        let span_line = {
            let mut span_info = span.extensions_mut().replace(SpanInfo::default()).unwrap();
            for (key, count) in span_info.counts.drain() {
                span_info.key_values.push(format!("{key}={count}"));
            }
            format!("{name} {}", span_info.key_values.join(" "))
        };

        let OutputBuffer(mut exiting_buffer) =
            span.extensions_mut().replace(OutputBuffer(vec![])).unwrap();

        if let Some(parent) = span.parent() {
            let mut ext = parent.extensions_mut();
            let OutputBuffer(parent_buffer) = ext.get_mut().unwrap();
            parent_buffer.push(BufferedLine { indent: 2, output_line: span_line });
            parent_buffer.extend(exiting_buffer.drain(..).map(|mut line| {
                line.indent += 2;
                line
            }));
        } else {
            let mut out = self.make_writer.make_writer();
            writeln!(out, "{span_line}").unwrap();
            for BufferedLine { indent, output_line } in exiting_buffer.drain(..) {
                writeln!(out, "{:indent$}{output_line}", "").unwrap();
            }
        }
    }

    fn on_record(
        &self,
        span: &span::Id,
        values: &span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        // Similar to how span fields are recorded on the `SpanInfo` extension,
        // add records to the same.
        let mut span = ctx.span(span);
        while let Some(parent) = span {
            if let Some(span_info) = parent.extensions_mut().get_mut::<SpanInfo>() {
                values.record(span_info);
                break;
            } else {
                span = parent.parent();
            }
        }
    }
}

impl IoTraceLayer {
    pub(crate) fn new<W: 'static + Write + Send + Sync>(out: W) -> (Self, WorkerGuard) {
        let (make_writer, guard) = NonBlocking::new(out);
        (Self { make_writer }, guard)
    }

    /// Print or buffer formatted tracing events that look like an IO event.
    ///
    /// IO events are:
    ///   - DB operations, emitted in core/store/src/lib.rs
    ///   - Storage operations, emitted in runtime/near-vm-logic/src/logic.rs
    fn record_io_event<S: Subscriber + for<'span> LookupSpan<'span>>(
        &self,
        event: &tracing::Event,
        ctx: tracing_subscriber::layer::Context<S>,
    ) {
        /// `Display`s ` size=<value>` if wrapped value is Some; nothing
        /// otherwise.
        struct FormattedSize(Option<u64>);

        impl std::fmt::Display for FormattedSize {
            fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.map_or(Ok(()), |size| write!(fmt, " size={size}"))
            }
        }

        let mut visitor = IoEventVisitor::default();
        event.record(&mut visitor);
        match visitor.t {
            Some(IoEventType::DbOp(db_op)) => {
                let col = visitor.col.as_deref().unwrap_or("?");
                let key = visitor.key.as_deref().unwrap_or("?");
                let size = FormattedSize(visitor.size);
                let output_line = format!("{db_op} {col} {key:?}{size}");
                if let Some(span) = ctx.event_span(event) {
                    span.extensions_mut()
                        .get_mut::<OutputBuffer>()
                        .unwrap()
                        .0
                        .push(BufferedLine { indent: 2, output_line });
                } else {
                    // Print top level unbuffered.
                    writeln!(self.make_writer.make_writer(), "{output_line}").unwrap();
                }
            }
            Some(IoEventType::StorageOp(storage_op)) => {
                let key_bytes = visitor.key.map(|key| {
                    base64::engine::general_purpose::STANDARD
                        .decode(key)
                        .expect("key was not properly base64-encoded")
                });
                let key = key_bytes
                    .as_ref()
                    .map(|k| format!("{}", near_fmt::Bytes(&*k)))
                    .unwrap_or_else(|| String::from("?"));
                let size = FormattedSize(visitor.size);
                let tn_db_reads = visitor.tn_db_reads.unwrap();
                let tn_mem_reads = visitor.tn_mem_reads.unwrap();

                let span_info =
                    format!("{storage_op} key={key}{size} tn_db_reads={tn_db_reads} tn_mem_reads={tn_mem_reads}");

                let span =
                    ctx.event_span(event).expect("storage operations must happen inside span");
                span.extensions_mut().get_mut::<SpanInfo>().unwrap().key_values.push(span_info);
            }
            None => {
                // Ignore irrelevant tracing events.
            }
        }
    }
}

/// Builder object to fill in field-by-field on traced events.
#[derive(Default)]
struct IoEventVisitor {
    t: Option<IoEventType>,
    key: Option<String>,
    col: Option<String>,
    size: Option<u64>,
    evicted_len: Option<u64>,
    tn_db_reads: Option<u64>,
    tn_mem_reads: Option<u64>,
}

impl tracing::field::Visit for IoEventVisitor {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "size" => self.size = Some(value),
            "evicted_len" => self.evicted_len = Some(value),
            "tn_db_reads" => self.tn_db_reads = Some(value),
            "tn_mem_reads" => self.tn_mem_reads = Some(value),
            _ => { /* Ignore other values, likely they are used in logging. */ }
        }
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if value >= 0 {
            self.record_u64(field, value as u64);
        }
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "key" => self.key = Some(value.to_owned()),
            "col" => self.col = Some(value.to_owned()),
            "storage_op" => {
                let op = match value {
                    "write" => StorageOp::Write,
                    "read" => StorageOp::Read,
                    "exists" => StorageOp::Exists,
                    "remove" => StorageOp::Remove,
                    _ => StorageOp::Other,
                };
                self.t = Some(IoEventType::StorageOp(op));
            }
            "size" => {
                self.size = value.parse().ok();
            }
            "db_op" => {
                let op = match value {
                    "get" => DbOp::Get,
                    "insert" => DbOp::Insert,
                    "set" => DbOp::Set,
                    "update_rc" => DbOp::UpdateRc,
                    "delete" => DbOp::Delete,
                    "delete_all" => DbOp::DeleteAll,
                    _ => DbOp::Other,
                };
                self.t = Some(IoEventType::DbOp(op));
            }
            _ => { /* Ignore other values, likely they are used in logging. */ }
        }
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.record_str(field, &format!("{value:?}"))
    }
}

impl tracing::field::Visit for SpanInfo {
    #[allow(clippy::integer_arithmetic)]
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        // "count" is a special field, everything else are key values pairs.
        if field.name() == "counter" {
            *self.counts.entry(value.to_string()).or_default() += 1;
        } else {
            self.record_debug(field, &value);
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let name = field.name();
        // Some fields are too verbose for the trace, ignore them on a case-by-case basis.
        let ignore = ["message", "node_counter"];
        if !ignore.contains(&name) {
            self.key_values.push(format!("{name}={value:?}"));
        }
    }
}
