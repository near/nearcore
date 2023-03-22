mod metrics;

use actix::{Actor, Addr, Context, Handler};
use awc::{Client, Connector};
use futures::FutureExt;
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext, WithSpanContextExt};
use near_performance_metrics_macros::perf;
use near_primitives::static_clock::StaticClock;
use std::ops::Sub;
use std::time::{Duration, Instant};

/// Timeout for establishing connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct TelemetryConfig {
    pub endpoints: Vec<String>,
    /// Only one request will be allowed in the specified time interval.
    #[serde(default = "default_reporting_interval")]
    pub reporting_interval: std::time::Duration,
}

fn default_reporting_interval() -> std::time::Duration {
    std::time::Duration::from_secs(10)
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self { endpoints: vec![], reporting_interval: default_reporting_interval() }
    }
}

/// Event to send over telemetry.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct TelemetryEvent {
    content: serde_json::Value,
}

pub struct TelemetryActor {
    config: TelemetryConfig,
    client: Client,
    last_telemetry_update: Instant,
}

impl Default for TelemetryActor {
    fn default() -> Self {
        Self::new(TelemetryConfig::default())
    }
}

impl TelemetryActor {
    pub fn new(config: TelemetryConfig) -> Self {
        for endpoint in config.endpoints.iter() {
            if endpoint.is_empty() {
                panic!(
                    "All telemetry endpoints must be valid URLs. Received: {:?}",
                    config.endpoints
                );
            }
        }

        let client = Client::builder()
            .timeout(CONNECT_TIMEOUT)
            .connector(Connector::new().max_http_version(awc::http::Version::HTTP_11))
            .finish();
        let reporting_interval = config.reporting_interval;
        Self {
            config,
            client,
            // Let the node report telemetry info at the startup.
            last_telemetry_update: std::time::Instant::now().sub(reporting_interval),
        }
    }
}

impl Actor for TelemetryActor {
    type Context = Context<Self>;
}

impl Handler<WithSpanContext<TelemetryEvent>> for TelemetryActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: WithSpanContext<TelemetryEvent>, _ctx: &mut Context<Self>) {
        // let (_span, msg) = handler_span!(target: "telemetry", tracing::Level::DEBUG, msg, );
        let (_span, msg) = handler_debug_span!(target: "telemetry", msg);
        let now = StaticClock::instant();
        if now.duration_since(self.last_telemetry_update) < self.config.reporting_interval {
            // Throttle requests to the telemetry endpoints, to at most one
            // request per `self.config.reporting_interval`.
            return;
        }
        for endpoint in self.config.endpoints.iter() {
            let endpoint = endpoint.clone();
            near_performance_metrics::actix::spawn(
                "telemetry",
                self.client
                    .post(endpoint.clone())
                    .insert_header(("Content-Type", "application/json"))
                    .send_json(&msg.content)
                    .map(move |response| {
                        let result = if let Err(error) = response {
                            tracing::warn!(
                                target: "telemetry",
                                err = ?error,
                                endpoint = ?endpoint,
                                "Failed to send telemetry data");
                            "failed"
                        } else {
                            "ok"
                        };
                        metrics::TELEMETRY_RESULT.with_label_values(&[result]).inc();
                    }),
            );
        }
        self.last_telemetry_update = now;
    }
}

/// Send telemetry event to all the endpoints.
pub fn telemetry(telemetry: &Addr<TelemetryActor>, content: serde_json::Value) {
    telemetry.do_send(TelemetryEvent { content }.with_span_context());
}
