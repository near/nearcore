//! Consumer of `tracing` data, which prints a hierarchical profile.
//!
//! Based on https://github.com/davidbarsky/tracing-tree, but does less, while
//! actually printing timings for spans.

use std::fmt;
use std::time::Instant;

use tracing::debug;
use tracing::field::{Field, Visit};
use tracing::span::Attributes;
use tracing::{Event, Id, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

pub fn enable() {
    let subscriber = tracing_subscriber::Registry::default().with(Timings);
    tracing::subscriber::set_global_default(subscriber)
        .unwrap_or_else(|_| debug!("Global subscriber is already set"));
}

struct Timings;

struct Data {
    start: Instant,
}

impl Data {
    fn new(attrs: &Attributes<'_>) -> Self {
        let mut span = Self { start: Instant::now() };
        attrs.record(&mut span);
        span
    }
}

impl Visit for Data {
    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

impl<S> Layer<S> for Timings
where
    S: Subscriber + for<'span> LookupSpan<'span> + fmt::Debug,
{
    fn new_span(&self, attrs: &Attributes, id: &Id, ctx: Context<S>) {
        let span = ctx.span(id).unwrap();

        let data = Data::new(attrs);
        span.extensions_mut().insert(data);
    }

    fn on_event(&self, _event: &Event<'_>, _ctx: Context<S>) {}

    fn on_close(&self, id: Id, ctx: Context<S>) {
        let span = ctx.span(&id).unwrap();
        let level = ctx.scope().count();

        let ext = span.extensions();
        let data = ext.get::<Data>().unwrap();

        let bold = "\u{001b}[1m";
        let reset = "\u{001b}[0m";
        eprintln!(
            "{:width$}  {:3.2?} {bold}{}{reset}",
            "",
            data.start.elapsed(),
            span.name(),
            bold = bold,
            reset = reset,
            width = level * 2
        );
        if level == 0 {
            eprintln!()
        }
    }
}
