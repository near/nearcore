use tracing::field::{Field, Visit};
use tracing_subscriber::field::{MakeVisitor, VisitFmt, VisitOutput};
use tracing_subscriber::fmt::format::{DefaultFields, Writer};

/// A wrapper around `DefaultFields` that does not escape ANSI sequences in the "message" field.
/// This allows terminal to interpret them correctly, working around the issue introduced in
/// `tracing-subscriber` 0.3.20, see https://github.com/tokio-rs/tracing/issues/3369.
/// TODO: This is a temporary workaround until `tracing-subscriber` provides a built-in solution.
pub struct SkipAnsiEscapingMessage {
    f: DefaultFields,
}

impl SkipAnsiEscapingMessage {
    pub fn new(f: DefaultFields) -> Self {
        SkipAnsiEscapingMessage { f }
    }
}

pub struct SkipAnsiEscapeMessageVisitor<'a> {
    f: <DefaultFields as MakeVisitor<Writer<'a>>>::Visitor,
    result: std::fmt::Result,
}

impl<'a> MakeVisitor<Writer<'a>> for SkipAnsiEscapingMessage {
    type Visitor = SkipAnsiEscapeMessageVisitor<'a>;

    fn make_visitor(&self, target: Writer<'a>) -> Self::Visitor {
        SkipAnsiEscapeMessageVisitor { f: self.f.make_visitor(target), result: Ok(()) }
    }
}

impl Visit for SkipAnsiEscapeMessageVisitor<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if self.result.is_err() {
            return;
        }
        if field.name() == "message" {
            // Actual workaround: do not escape ANSI sequences in the "message" field,
            // so that terminal can interpret them correctly.
            self.result = write!(self.f.writer(), "{:?}", value);
            return;
        }
        // Everything else is handled normally.
        self.f.record_debug(field, value);
    }
}

impl VisitFmt for SkipAnsiEscapeMessageVisitor<'_> {
    fn writer(&mut self) -> &mut dyn core::fmt::Write {
        self.f.writer()
    }
}

impl VisitOutput<std::fmt::Result> for SkipAnsiEscapeMessageVisitor<'_> {
    fn finish(self) -> std::fmt::Result {
        if self.result.is_err() {
            return self.result;
        }
        self.f.finish()
    }
}
