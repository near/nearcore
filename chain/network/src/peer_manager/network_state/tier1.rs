use crate::accounts_data;
use crate::config;
use crate::network_protocol::{AccountData, PeerMessage, SignedAccountData, SyncAccountsData};
use crate::peer_manager::peer_manager_actor::Event;
use crate::time;
use std::sync::Arc;

impl super::NetworkState {
    // Returns ValidatorConfig of this node iff it belongs to TIER1 according to `accounts_data`.
    pub fn tier1_validator_config(
        &self,
        accounts_data: &accounts_data::CacheSnapshot,
    ) -> Option<&config::ValidatorConfig> {
        if self.config.tier1.is_none() {
            return None;
        }
        self.config
            .validator
            .as_ref()
            .filter(|cfg| accounts_data.keys.contains(&cfg.signer.public_key()))
    }

    /// Tries to connect to ALL trusted proxies from the config, then broadcasts AccountData with
    /// the set of proxies it managed to connect to. This way other TIER1 nodes can just connect
    /// to ANY proxy of this node.
    pub async fn tier1_advertise_proxies(
        self: &Arc<Self>,
        clock: &time::Clock,
    ) -> Vec<Arc<SignedAccountData>> {
        let accounts_data = self.accounts_data.load();
        let vc = match self.tier1_validator_config(&accounts_data) {
            Some(it) => it,
            None => {
                return vec![];
            }
        };
        // TODO(gprusak): for now we just blindly broadcast the static list of proxies, however
        // here we should try to connect to the TIER1 proxies, before broadcasting them.
        let my_proxies = match &vc.proxies {
            config::ValidatorProxies::Dynamic(_) => vec![],
            config::ValidatorProxies::Static(proxies) => proxies.clone(),
        };
        let now = clock.now_utc();
        let version =
            self.accounts_data.load().data.get(&vc.signer.public_key()).map_or(0, |d| d.version)
                + 1;
        // This unwrap is safe, because we did signed a sample payload during
        // config validation. See config::Config::new().
        let my_data = Arc::new(
            AccountData {
                peer_id: self.config.node_id(),
                account_key: vc.signer.public_key(),
                proxies: my_proxies.clone(),
                timestamp: now,
                version,
            }
            .sign(vc.signer.as_ref())
            .unwrap(),
        );
        let (new_data, err) = self.accounts_data.insert(vec![my_data]).await;
        // Inserting node's own AccountData should never fail.
        if let Some(err) = err {
            panic!("inserting node's own AccountData to self.state.accounts_data: {err}");
        }
        if new_data.is_empty() {
            // If new_data is empty, it means that accounts_data contains entry newer than `version`.
            // This means that this node has been restarted and forgot what was the latest `version`
            // of accounts_data published AND it just learned about it from a peer.
            // TODO(gprusak): for better resiliency, consider persisting latest version in storage.
            // TODO(gprusak): consider broadcasting a new version immediately after learning about
            //   conflicting version.
            tracing::info!("received a conflicting version of AccountData (expected, iff node has been just restarted)");
            return vec![];
        }
        self.tier2.broadcast_message(Arc::new(PeerMessage::SyncAccountsData(SyncAccountsData {
            incremental: true,
            requesting_full_sync: true,
            accounts_data: new_data.clone(),
        })));
        self.config.event_sink.push(Event::Tier1AdvertiseProxies(new_data.clone()));
        new_data
    }
}
