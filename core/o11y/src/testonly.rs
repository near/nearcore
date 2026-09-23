use crate::subscriber::use_color_auto;
use core::fmt::Result;
use std::time::Instant;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

fn setup_subscriber_from_filter(mut env_filter: EnvFilter) {
    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        for directive in rust_log.split(',').filter_map(|s| match s.parse() {
            Ok(directive) => Some(directive),
            Err(err) => {
                eprintln!("Ignoring directive `{}`: {}", s, err);
                None
            }
        }) {
            env_filter = env_filter.add_directive(directive);
        }
    }

    let _ = fmt::Subscriber::builder()
        .with_ansi(use_color_auto())
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .with_env_filter(env_filter)
        .with_writer(fmt::TestWriter::new())
        .with_timer(TestUptime::default())
        .try_init();
}

const TEST_LOG_ENV_FILTER: &str = "cranelift=warn,wasmtime=warn,h2=warn,tower=warn,trust_dns=warn,tokio_reactor=info,tokio_core=info,hyper=info,debug";

pub fn init_test_logger() {
    setup_subscriber_from_filter(EnvFilter::new(TEST_LOG_ENV_FILTER));
}

/// Like [`init_test_logger`] but layers extra comma-separated directives on top of the default
/// filter, e.g. `"test_loop=warn"` to mute the expensive per-event visualizer trace in long tests.
pub fn init_test_logger_with_directives(directives: &str) {
    let mut env_filter = EnvFilter::new(TEST_LOG_ENV_FILTER);
    for directive in directives.split(',').filter(|s| !s.is_empty()) {
        env_filter = env_filter.add_directive(directive.parse().unwrap());
    }
    setup_subscriber_from_filter(env_filter);
}

pub fn init_test_module_logger(module: &str) {
    let env_filter =
        EnvFilter::new("cranelift=warn,wasmtime=warn,h2=warn,tower=warn,trust_dns=warn,tokio_reactor=info,tokio_core=info,hyper=info,cranelift_wasm=warn,info")
            .add_directive(format!("{}=info", module).parse().unwrap());
    setup_subscriber_from_filter(env_filter);
}

pub fn init_integration_logger() {
    let env_filter = EnvFilter::new("cranelift=warn,wasmtime=warn,info");
    setup_subscriber_from_filter(env_filter);
}

/// Shameless copy paste of the Uptime timer in the tracing subscriber with
/// adjusted time formatting. It measures time since the subscriber is configured.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TestUptime {
    epoch: Instant,
}

impl Default for TestUptime {
    fn default() -> Self {
        TestUptime { epoch: Instant::now() }
    }
}

impl From<Instant> for TestUptime {
    fn from(epoch: Instant) -> Self {
        TestUptime { epoch }
    }
}

impl FormatTime for TestUptime {
    fn format_time(&self, w: &mut Writer<'_>) -> Result {
        let e = self.epoch.elapsed();
        write!(w, "{:2}.{:03}s", e.as_secs(), e.subsec_millis())
    }
}
