mod metrics;

use near_async::messaging::{Actor, Handler};
use near_async::time::{Duration, Instant};
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
    config: TelemetryConfig,
    last_telemetry_update: Instant,
    client: Client,
}

impl Default for TelemetryActor {
    fn default() -> Self {
        Self::new(TelemetryConfig::default())
    }
}

impl Actor for TelemetryActor {}

impl TelemetryActor {
    pub fn new(config: TelemetryConfig) -> Self {
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
            config,
            // Let the node report telemetry info at the startup.
            last_telemetry_update: Instant::now().sub(reporting_interval),
            client,
        }
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
            let content = msg.content.clone();

            tokio::spawn(async move {
                let result = match client
                    .post(&endpoint)
                    .header("Content-Type", "application/json")
                    .json(&content)
                    .send()
                    .await
                {
                    Ok(response) => {
                        if response.status().is_success() {
                            "ok"
                        } else {
                            tracing::warn!(
                                target: "telemetry",
                                status = %response.status(),
                                endpoint = %endpoint,
                                "Telemetry request failed with HTTP error");
                            "failed"
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: "telemetry",
                            err = ?error,
                            endpoint = %endpoint,
                            "Failed to send telemetry data");
                        "failed"
                    }
                };
                metrics::TELEMETRY_RESULT.with_label_values(&[result]).inc();
            });
        }
        self.last_telemetry_update = now;
    }
}
