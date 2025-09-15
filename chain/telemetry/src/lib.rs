mod metrics;

use futures::FutureExt;
use near_async::ActorSystem;
use near_async::futures::FutureSpawnerExt;
use near_async::messaging::{Actor, Handler};
use near_async::time::{Duration, Instant};
use near_async::tokio::TokioRuntimeHandle;
use near_performance_metrics as _; // Suppress cargo machete
use near_performance_metrics_macros::perf;
use reqwest::Client;
use std::ops::Sub;

/// Timeout for establishing connection.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct TelemetryConfig {
    pub endpoints: Vec<String>,
    /// Only one request will be allowed in the specified time interval.
    #[serde(default = "default_reporting_interval")]
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub reporting_interval: Duration,
}

fn default_reporting_interval() -> Duration {
    Duration::seconds(10)
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
    pub content: serde_json::Value,
}

pub struct TelemetryActor {
    handle: TokioRuntimeHandle<TelemetryActor>,
    config: TelemetryConfig,
    client: Client,
    last_telemetry_update: Instant,
}

impl Actor for TelemetryActor {}

impl TelemetryActor {
    fn new(handle: TokioRuntimeHandle<TelemetryActor>, config: TelemetryConfig) -> Self {
        for endpoint in &config.endpoints {
            if endpoint.is_empty() {
                panic!(
                    "All telemetry endpoints must be valid URLs. Received: {:?}",
                    config.endpoints
                );
            }
        }

        let client = Client::builder()
            .timeout(CONNECT_TIMEOUT)
            .build()
            .expect("Failed to create HTTP client for telemetry");

        let reporting_interval = config.reporting_interval;
        Self {
            handle,
            config,
            client,
            // Let the node report telemetry info at the startup.
            last_telemetry_update: Instant::now().sub(reporting_interval),
        }
    }

    pub fn spawn_tokio_actor(
        actor_system: ActorSystem,
        config: TelemetryConfig,
    ) -> TokioRuntimeHandle<TelemetryActor> {
        let builder = actor_system.new_tokio_builder();
        let handle = builder.handle();
        let actor = TelemetryActor::new(handle.clone(), config);
        builder.spawn_tokio_actor(actor);
        handle
    }
}

impl Handler<TelemetryEvent> for TelemetryActor {
    #[perf]
    fn handle(&mut self, msg: TelemetryEvent) {
        tracing::debug!(target: "telemetry", ?msg);
        let now = Instant::now();
        if now - self.last_telemetry_update < self.config.reporting_interval {
            // Throttle requests to the telemetry endpoints, to at most one
            // request per `self.config.reporting_interval`.
            return;
        }
        for endpoint in &self.config.endpoints {
            let endpoint = endpoint.clone();
            let client = self.client.clone();
            self.handle.spawn(
                "send telemetry",
                client
                    .post(endpoint.clone())
                    .header("Content-Type", "application/json")
                    .json(&msg.content)
                    .send()
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
