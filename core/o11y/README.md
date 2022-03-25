Observability (o11y) helpers for the NEAR codebase.

This crate contains all sorts of utilities to enable a more convenient observability implementation
in the NEAR codebase. Of particular interest to most will be standardized tracing subscriber setup
available via the [`default_subscriber`] function.

Among other things you should expect to find here eventually are utilities to work with prometheus
metrics (wrappers for additional metric types, a server publishing these metrics, etc.) as well as
NEAR-specific event and span subscriber implementations, etc.
