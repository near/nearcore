mod metrics;

use actix::{Actor, Addr, Context, Handler, Message};
use awc::{Client, Connector};
use futures::FutureExt;
use near_performance_metrics_macros::perf;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Timeout for establishing connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct TelemetryConfig {
    pub endpoints: Vec<String>,
}

/// Event to send over telemetry.
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct TelemetryEvent {
    content: serde_json::Value,
}

pub struct TelemetryActor {
    config: TelemetryConfig,
    client: Client,
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
            .connector(Connector::new().max_http_version(actix_web::http::Version::HTTP_11))
            .finish();
        Self { config, client }
    }
}

impl Actor for TelemetryActor {
    type Context = Context<Self>;
}

impl Handler<TelemetryEvent> for TelemetryActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: TelemetryEvent, _ctx: &mut Context<Self>) {
        for endpoint in self.config.endpoints.iter() {
            near_performance_metrics::actix::spawn(
                "telemetry",
                self.client
                    .post(endpoint)
                    .insert_header(("Content-Type", "application/json"))
                    .send_json(&msg.content)
                    .map(|response| {
                        let result = if let Err(error) = response {
                            tracing::warn!(target: "telemetry", err=?error, "Failed to send telemetry data");
                            "failed"
                        } else {
                            "ok"
                        };
                        metrics::TELEMETRY_RESULT.with_label_values(&[result]).inc();
                    }),
            );
        }
    }
}

/// Send telemetry event to all the endpoints.
pub fn telemetry(telemetry: &Addr<TelemetryActor>, content: serde_json::Value) {
    telemetry.do_send(TelemetryEvent { content });
}
