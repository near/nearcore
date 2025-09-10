use crate::reload::TracingLayer;
use near_crypto::PublicKey;
use near_primitives_core::types::AccountId;
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{BatchSpanProcessor, RandomIdGenerator, Sampler, SdkTracerProvider};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Layer, reload};

// Doesn't define WARN and ERROR, because the highest verbosity of spans is INFO.
#[derive(Copy, Clone, Debug, Default, clap::ValueEnum)]
pub enum OpenTelemetryLevel {
    #[default]
    OFF,
    INFO,
    DEBUG,
    TRACE,
}

/// Constructs an OpenTelemetryConfig which sends span data to an external collector.
//
// NB: this function is `async` because `tonic` (gRPC server) requires a tokio context to
// register timers and channels and whatnot.
pub(crate) async fn add_opentelemetry_layer<S>(
    opentelemetry_level: OpenTelemetryLevel,
    chain_id: String,
    node_public_key: PublicKey,
    account_id: Option<AccountId>,
    subscriber: S,
) -> (TracingLayer<S>, reload::Handle<EnvFilter, S>)
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    let filter = get_opentelemetry_filter(opentelemetry_level);
    let (filter, handle) = reload::Layer::<EnvFilter, S>::new(filter);

    let mut resource = vec![
        KeyValue::new("chain_id", chain_id),
        KeyValue::new("node_id", node_public_key.to_string()),
    ];
    // Prefer account name as the node name.
    // Fallback to a node public key if a validator key is unavailable.
    let service_name = if let Some(account_id) = account_id {
        resource.push(KeyValue::new("account_id", account_id.to_string()));
        format!("neard:{}", account_id)
    } else {
        format!("neard:{}", node_public_key)
    };
    resource.push(KeyValue::new(SERVICE_NAME, service_name));

    // In OpenTelemetry 0.30, the environment variables OTEL_BSP_MAX_QUEUE_SIZE and
    // OTEL_BSP_MAX_CONCURRENT_EXPORTS are automatically respected by the BatchConfig::default()
    // We can use the builder pattern with with_batch_exporter which handles the configuration
    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder().with_tonic().build().unwrap();
    let batch_processor = BatchSpanProcessor::builder(otlp_exporter).build();

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(Resource::builder().with_attributes(resource).build())
        .with_sampler(Sampler::AlwaysOn)
        .with_id_generator(RandomIdGenerator::default())
        .with_span_processor(batch_processor)
        .build();

    let tracer = tracer_provider.tracer("near-o11y");
    let layer = tracing_opentelemetry::layer().with_tracer(tracer).with_filter(filter);
    (subscriber.with(layer), handle)
}

pub(crate) fn get_opentelemetry_filter(opentelemetry_level: OpenTelemetryLevel) -> EnvFilter {
    match opentelemetry_level {
        OpenTelemetryLevel::OFF => EnvFilter::new("off"),
        OpenTelemetryLevel::INFO => EnvFilter::new("info"),
        OpenTelemetryLevel::DEBUG => EnvFilter::new("debug"),
        OpenTelemetryLevel::TRACE => EnvFilter::new("trace"),
    }
}
