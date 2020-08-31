//! Client actor orchestrates Client and facilitates network connection.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use actix::{Actor, Addr, Arbiter, AsyncContext, Context};
use chrono::{DateTime, Utc};
use log::{error, info};

use near_chain::{ChainGenesis, RuntimeAdapter};
use near_chain_configs::ClientConfig;
use near_network::types::NetworkInfo;
use near_network::NetworkAdapter;
use near_primitives::validator_signer::ValidatorSigner;

use crate::client::Client;
use crate::types::Error;
use crate::ClientActor;

#[cfg(feature = "metric_recorder")]
use near_network::recorder::MetricRecorder;

pub struct ClientActorHelper {
    client: Client,
    network_info: NetworkInfo,

    client_actor: Addr<ClientActor>,
}

/// Blocks the program until given genesis time arrives.
fn wait_until_genesis(genesis_time: &DateTime<Utc>) {
    loop {
        // Get chrono::Duration::num_seconds() by deducting genesis_time from now.
        let duration = genesis_time.signed_duration_since(Utc::now());
        let chrono_seconds = duration.num_seconds();
        // Check if number of seconds in chrono::Duration larger than zero.
        if chrono_seconds <= 0 {
            break;
        }
        info!(target: "near", "Waiting until genesis: {}d {}h {}m {}s", duration.num_days(),
              (duration.num_hours() % 24),
              (duration.num_minutes() % 60),
              (duration.num_seconds() % 60));
        let wait =
            std::cmp::min(Duration::from_secs(10), Duration::from_secs(chrono_seconds as u64));
        thread::sleep(wait);
    }
}

impl ClientActorHelper {
    pub fn new(
        config: ClientConfig,
        chain_genesis: ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Arc<dyn NetworkAdapter>,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
        enable_doomslug: bool,
        client_actor: Addr<ClientActor>,
    ) -> Result<Self, Error> {
        wait_until_genesis(&chain_genesis.time);
        if let Some(vs) = &validator_signer {
            info!(target: "client", "Starting validator node: {}", vs.validator_id());
        }
        let client = Client::new(
            config,
            chain_genesis,
            runtime_adapter,
            network_adapter.clone(),
            validator_signer,
            enable_doomslug,
        )?;

        Ok(ClientActorHelper {
            client,
            network_info: NetworkInfo {
                active_peers: vec![],
                num_active_peers: 0,
                peer_max_count: 0,
                highest_height_peers: vec![],
                received_bytes_per_sec: 0,
                sent_bytes_per_sec: 0,
                known_producers: vec![],
                #[cfg(feature = "metric_recorder")]
                metric_recorder: MetricRecorder::default(),
            },
            client_actor,
        })
    }
}

impl Actor for ClientActorHelper {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start catchup job.
        self.catchup(ctx);
    }
}

impl ClientActorHelper {
    /// Runs catchup on repeat, if this client is a validator.
    fn catchup(&mut self, ctx: &mut Context<ClientActorHelper>) {
        match self.client.run_catchup_pt1(&self.network_info.highest_height_peers) {
            Ok(catchup_msg) => {
                self.client_actor.do_send(catchup_msg);
            }
            Err(err) => {
                error!(target: "client", "{:?} Error occurred during catchup for the next epoch: {:?}", self.client.validator_signer.as_ref().map(|vs| vs.validator_id()), err)
            }
        }

        ctx.run_later(self.client.config.catchup_step_period, move |act, ctx| {
            act.catchup(ctx);
        });
    }
}

/// Starts client in a separate Arbiter (thread).
pub fn start_client_helper(
    client_config: ClientConfig,
    chain_genesis: ChainGenesis,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    network_adapter: Arc<dyn NetworkAdapter>,
    validator_signer: Option<Arc<dyn ValidatorSigner>>,
    client_actor: Addr<ClientActor>,
) -> Arbiter {
    let client_helper_arbiter = Arbiter::current();
    ClientActorHelper::start_in_arbiter(&client_helper_arbiter, move |_ctx| {
        ClientActorHelper::new(
            client_config,
            chain_genesis,
            runtime_adapter,
            network_adapter,
            validator_signer,
            true,
            client_actor,
        )
        .unwrap()
    });
    client_helper_arbiter
}
