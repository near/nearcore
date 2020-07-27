use std::time::Duration;

use actix::{Actor, Addr, Context, Handler, Message};
use actix_web::client::{Client, Connector};
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use tracing::info;

/// Timeout for establishing connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct TelemetryConfig {
    pub endpoints: Vec<String>,
}

/// Event to send over telemetry.
#[derive(Message)]
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
        openssl_probe::init_ssl_cert_env_vars();
        let client = Client::build()
            .timeout(CONNECT_TIMEOUT)
            .connector(
                Connector::new()
                    .conn_lifetime(Duration::from_secs(u64::max_value()))
                    .conn_keep_alive(Duration::from_secs(30))
                    .finish(),
            )
            .finish();
        Self { config, client }
    }
}

impl Actor for TelemetryActor {
    type Context = Context<Self>;
}

impl Handler<TelemetryEvent> for TelemetryActor {
    type Result = ();

    fn handle(&mut self, msg: TelemetryEvent, _ctx: &mut Context<Self>) {
        for endpoint in self.config.endpoints.iter() {
            actix::spawn(
                self.client
                    .post(endpoint)
                    .header("Content-Type", "application/json")
                    .send_json(&msg.content)
                    .map(|response| {
                        if let Err(error) = response {
                            info!(target: "telemetry", "Telemetry data could not be sent due to: {}", error);
                        }
                    }),
            );
        }
    }
}

/// Send telemetry event to all the endpoints.
pub fn telemetry(telemetry: &Addr<TelemetryActor>, content: serde_json::Value) {
    telemetry.do_send(TelemetryEvent { content });
}
