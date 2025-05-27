// Span tags are string constants that are set as the `tag` attribute on tracing spans,
// allowing related spans to be grouped and filtered together.
// For more information on filtering spans by attributes, see:
// https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives

pub const BLOCK_PRODUCTION: &str = "block_production";
