use crate::sync::{highest_height_peer, BlockSync, HeaderSync, StateSync, StateSyncResult};
use actix::Message;
use actix::{Actor, Addr, Arbiter, Context, Handler};
use near_chain_configs::ClientConfig;
use near_client_primitives::types::SyncStatus;
use near_network::{NetworkAdapter, NetworkClientMessages, NetworkRequests};
use near_performance_metrics_macros::perf_with_debug;
use std::sync::{Arc, RwLock};
use strum::AsStaticStr;

// use delay_detector::DelayDetector;
use crate::ClientActor;
use log::{debug, error, info, warn};
use near_chain::test_utils::format_hash;
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode, RuntimeAdapter};
use near_chunks::ShardsManager;
#[cfg(feature = "metric_recorder")]
use near_network::recorder::MetricRecorder;
use near_network::types::NetworkInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use std::collections::HashMap;

pub struct StateSyncActor {
    state_sync: StateSync,
    pub config: ClientConfig,
    network_adapter: Arc<dyn NetworkAdapter>,
    pub chain: Chain,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub sync_status: SyncStatus, // TODO, not yet updated
    network_info: NetworkInfo,
    pub shards_mgr: ShardsManager,
    pub validator_signer: Option<Arc<dyn ValidatorSigner>>,
    /// Keeps track of syncing headers.
    pub header_sync: HeaderSync,
    /// Keeps track of syncing block.
    pub block_sync: BlockSync,
    client_addr: Option<Addr<ClientActor>>,
}

impl StateSyncActor {
    pub fn new(
        config: ClientConfig,
        network_adapter: Arc<dyn NetworkAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        chain_genesis: ChainGenesis,
        enable_doomslug: bool,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
    ) -> StateSyncActor {
        let doomslug_threshold_mode = if enable_doomslug {
            DoomslugThresholdMode::TwoThirds
        } else {
            DoomslugThresholdMode::NoApprovals
        };
        let state_sync = StateSync::new(network_adapter.clone(), config.state_sync_timeout);

        let chain =
            Chain::new(runtime_adapter.clone(), &chain_genesis, doomslug_threshold_mode).unwrap();
        let sync_status = SyncStatus::AwaitingPeers;
        let shards_mgr = ShardsManager::new(
            validator_signer.as_ref().map(|x| x.validator_id().clone()),
            runtime_adapter.clone(),
            network_adapter.clone(),
        );
        let header_sync = HeaderSync::new(
            network_adapter.clone(),
            config.header_sync_initial_timeout,
            config.header_sync_progress_timeout,
            config.header_sync_stall_ban_timeout,
            config.header_sync_expected_height_per_second,
        );
        let block_sync =
            BlockSync::new(network_adapter.clone(), config.block_fetch_horizon, config.archive);
        Self {
            state_sync,
            config,
            network_adapter,
            chain,
            runtime_adapter,
            sync_status,
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
                peer_counter: 0,
            },
            shards_mgr,
            validator_signer,
            header_sync,
            block_sync,
            client_addr: None,
        }
    }
}

#[derive(Clone, strum::AsRefStr, Message, AsStaticStr)]
#[rtype(result = "()")]
pub enum StateSyncActorRequests {
    ReceivedRequestedPart { part_id: u64, shard_id: u64, hash: CryptoHash },
    ClientAddr { addr: Addr<ClientActor> },
    NetworkInfo { network_info: NetworkInfo },
}

impl StateSyncActor {
    fn needs_syncing(&self, needs_syncing: bool) -> bool {
        #[cfg(feature = "adversarial")]
        {
            if self.adv.read().unwrap().adv_disable_header_sync {
                return false;
            }
        }

        needs_syncing
    }

    /// Select the block hash we are using to sync state. It will sync with the state before applying the
    /// content of such block.
    ///
    /// The selected block will always be the first block on a new epoch:
    /// https://github.com/nearprotocol/nearcore/issues/2021#issuecomment-583039862
    ///
    /// To prevent syncing from a fork, we move `state_fetch_horizon` steps backwards and use that epoch.
    /// Usually `state_fetch_horizon` is much less than the expected number of produced blocks on an epoch,
    /// so this is only relevant on epoch boundaries.
    fn find_sync_hash(&mut self) -> Result<CryptoHash, near_chain::Error> {
        let header_head = self.chain.header_head()?;
        let mut sync_hash = header_head.prev_block_hash;
        for _ in 0..self.config.state_fetch_horizon {
            sync_hash = *self.chain.get_block_header(&sync_hash)?.prev_hash();
        }
        let mut epoch_start_sync_hash =
            StateSync::get_epoch_start_sync_hash(&mut self.chain, &sync_hash)?;

        if &epoch_start_sync_hash == self.chain.genesis().hash() {
            // If we are within `state_fetch_horizon` blocks of the second epoch, the sync hash will
            // be the first block of the first epoch (or, the genesis block). Due to implementation
            // details of the state sync, we can't state sync to the genesis block, so redo the
            // search without going back `state_fetch_horizon` blocks.
            epoch_start_sync_hash = StateSync::get_epoch_start_sync_hash(
                &mut self.chain,
                &header_head.last_block_hash,
            )?;
            assert_ne!(&epoch_start_sync_hash, self.chain.genesis().hash());
        }
        Ok(epoch_start_sync_hash)
    }

    fn request_block_by_hash(&mut self, hash: CryptoHash, peer_id: PeerId) {
        match self.chain.block_exists(&hash) {
            Ok(false) => {
                self.network_adapter.do_send(NetworkRequests::BlockRequest { hash, peer_id });
            }
            Ok(true) => {
                debug!(target: "client", "send_block_request_to_peer: block {} already known", hash)
            }
            Err(e) => {
                error!(target: "client", "send_block_request_to_peer: failed to check block exists: {:?}", e)
            }
        }
    }

    /// Main syncing job responsible for syncing client with other peers.
    fn sync(&mut self, ctx: &mut Context<StateSyncActor>) {
        //#[cfg(feature = "delay_detector")]
        //let _d = DelayDetector::new("client sync".into());
        // Macro to schedule to call this function later if error occurred.
        macro_rules! unwrap_or_run_later(($obj: expr) => (match $obj {
            Ok(v) => v,
            Err(err) => {
                error!(target: "sync", "Sync: Unexpected error: {}", err);

            near_performance_metrics::actix::run_later(
                ctx,
                file!(),
                line!(),
                self.config.sync_step_period, move |act, ctx| {
                    act.sync(ctx);
                });
                return;
            }
        }));

        let mut wait_period = self.config.sync_step_period;

        let currently_syncing = self.sync_status.is_syncing();
        let (needs_syncing, highest_height) = unwrap_or_run_later!(self.syncing_info());

        if !self.needs_syncing(needs_syncing) {
            if currently_syncing {
                debug!(
                    target: "client",
                    "{:?} transitions to no sync",
                    self.validator_signer.as_ref().map(|vs| vs.validator_id()),
                );
                self.sync_status = SyncStatus::NoSync;

                // Initial transition out of "syncing" state.
                // Announce this client's account id if their epoch is coming up.
                let head = unwrap_or_run_later!(self.chain.head());

                self.client_addr
                    .clone()
                    .unwrap()
                    .do_send(NetworkClientMessages::CheckSendAnnounceAccount(head.prev_block_hash));
            }
            wait_period = self.config.sync_check_period;
        } else {
            // Run each step of syncing separately.
            unwrap_or_run_later!(self.header_sync.run(
                &mut self.sync_status,
                &mut self.chain,
                highest_height,
                &self.network_info.highest_height_peers
            ));
            // Only body / state sync if header height is close to the latest.
            let header_head = unwrap_or_run_later!(self.chain.header_head());

            // Sync state if already running sync state or if block sync is too far.
            let sync_state = match self.sync_status {
                SyncStatus::StateSync(_, _) => true,
                _ if header_head.height
                    >= highest_height.saturating_sub(self.config.block_header_fetch_horizon) =>
                {
                    unwrap_or_run_later!(self.block_sync.run(
                        &mut self.sync_status,
                        &mut self.chain,
                        highest_height,
                        &self.network_info.highest_height_peers
                    ))
                }
                _ => false,
            };
            if sync_state {
                let (sync_hash, mut new_shard_sync, just_enter_state_sync) = match &self.sync_status
                {
                    SyncStatus::StateSync(sync_hash, shard_sync) => {
                        (sync_hash.clone(), shard_sync.clone(), false)
                    }
                    _ => {
                        let sync_hash = unwrap_or_run_later!(self.find_sync_hash());
                        (sync_hash, HashMap::default(), true)
                    }
                };

                let me = self.validator_signer.as_ref().map(|x| x.validator_id().clone());
                let shards_to_sync = (0..self.runtime_adapter.num_shards())
                    .filter(|x| {
                        self.shards_mgr.cares_about_shard_this_or_next_epoch(
                            me.as_ref(),
                            &sync_hash,
                            *x,
                            true,
                        )
                    })
                    .collect();

                if !self.config.archive && just_enter_state_sync {
                    unwrap_or_run_later!(self.chain.reset_data_pre_state_sync(sync_hash));
                }

                match unwrap_or_run_later!(self.state_sync.run(
                    &me,
                    sync_hash,
                    &mut new_shard_sync,
                    &mut self.chain,
                    &self.runtime_adapter,
                    &self.network_info.highest_height_peers,
                    shards_to_sync,
                )) {
                    StateSyncResult::Unchanged => (),
                    StateSyncResult::Changed(fetch_block) => {
                        self.sync_status = SyncStatus::StateSync(sync_hash, new_shard_sync);
                        if fetch_block {
                            if let Some(peer_info) =
                                highest_height_peer(&self.network_info.highest_height_peers)
                            {
                                if let Ok(header) = self.chain.get_block_header(&sync_hash) {
                                    for hash in
                                        vec![*header.prev_hash(), *header.hash()].into_iter()
                                    {
                                        self.request_block_by_hash(
                                            hash,
                                            peer_info.peer_info.id.clone(),
                                        );
                                    }
                                }
                            }
                        }
                    }
                    StateSyncResult::Completed => {
                        info!(target: "sync", "State sync: all shards are done");

                        let accepted_blocks = Arc::new(RwLock::new(vec![]));
                        let blocks_missing_chunks = Arc::new(RwLock::new(vec![]));
                        let challenges = Arc::new(RwLock::new(vec![]));

                        unwrap_or_run_later!(self.chain.reset_heads_post_state_sync(
                            &me,
                            sync_hash,
                            |accepted_block| {
                                accepted_blocks.write().unwrap().push(accepted_block);
                            },
                            |missing_chunks| {
                                blocks_missing_chunks.write().unwrap().push(missing_chunks)
                            },
                            |challenge| challenges.write().unwrap().push(challenge)
                        ));

                        self.client_addr
                            .clone()
                            .unwrap()
                            .do_send(NetworkClientMessages::SendChallenges(challenges));

                        self.client_addr.clone().unwrap().do_send(
                            NetworkClientMessages::ProcessAcceptedBlocked(
                                accepted_blocks.write().unwrap().drain(..).collect(),
                            ),
                        );

                        self.shards_mgr.request_chunks(
                            blocks_missing_chunks.write().unwrap().drain(..).flatten(),
                            &self
                                .chain
                                .header_head()
                                .expect("header_head must be available during sync"),
                            // It is ok to pass the latest protocol version here since we are likely
                            // syncing old blocks, which means the protocol version will not change
                            // the logic.
                            PROTOCOL_VERSION,
                        );

                        self.sync_status =
                            SyncStatus::BodySync { current_height: 0, highest_height: 0 };
                    }
                }
            }
        }

        near_performance_metrics::actix::run_later(
            ctx,
            file!(),
            line!(),
            wait_period,
            move |act, ctx| {
                act.sync(ctx);
            },
        );
    }

    /// Starts syncing and then switches to either syncing or regular mode.
    fn start_sync(&mut self, ctx: &mut Context<StateSyncActor>) {
        // Wait for connections reach at least minimum peers unless skipping sync.
        if self.network_info.num_active_peers < self.config.min_num_peers
            && !self.config.skip_sync_wait
        {
            near_performance_metrics::actix::run_later(
                ctx,
                file!(),
                line!(),
                self.config.sync_step_period,
                move |act, ctx| {
                    act.start_sync(ctx);
                },
            );
            return;
        }
        // self.sync_started = true; TODO

        // Start main sync loop.
        self.sync(ctx);
    }

    /// Check whether need to (continue) sync.
    /// Also return higher height with known peers at that height.
    fn syncing_info(&self) -> Result<(bool, u64), near_chain::Error> {
        let head = self.chain.head()?;
        let mut is_syncing = self.sync_status.is_syncing();

        let full_peer_info = if let Some(full_peer_info) =
            highest_height_peer(&self.network_info.highest_height_peers)
        {
            full_peer_info
        } else {
            if !self.config.skip_sync_wait {
                warn!(target: "client", "Sync: no peers available, disabling sync");
            }
            return Ok((false, 0));
        };

        if is_syncing {
            if full_peer_info.chain_info.height <= head.height {
                info!(target: "client", "Sync: synced at {} [{}], {}, highest height peer: {}",
                      head.height, format_hash(head.last_block_hash),
                      full_peer_info.peer_info.id, full_peer_info.chain_info.height
                );
                is_syncing = false;
            }
        } else {
            if full_peer_info.chain_info.height > head.height + self.config.sync_height_threshold {
                info!(
                    target: "client",
                    "Sync: height: {}, peer id/height: {}/{}, enabling sync",
                    head.height,
                    full_peer_info.peer_info.id,
                    full_peer_info.chain_info.height,
                );
                is_syncing = true;
            }
        }
        Ok((is_syncing, full_peer_info.chain_info.height))
    }
}

impl Handler<StateSyncActorRequests> for StateSyncActor {
    type Result = ();

    #[perf_with_debug]
    fn handle(&mut self, msg: StateSyncActorRequests, _ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            StateSyncActorRequests::ReceivedRequestedPart { part_id, shard_id, hash } => {
                self.state_sync.received_requested_part(part_id, shard_id, hash);
            }
            StateSyncActorRequests::ClientAddr { addr } => {
                self.client_addr = Some(addr);
            }
            StateSyncActorRequests::NetworkInfo { network_info } => {
                self.network_info = network_info
            }
        }
    }
}

impl Actor for StateSyncActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.start_sync(ctx);
    }
}

pub fn start_state_sync_actor(
    config: ClientConfig,
    network_adapter: Arc<dyn NetworkAdapter>,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    chain_genesis: ChainGenesis,
    enable_doomslug: bool,
    validator_signer: Option<Arc<dyn ValidatorSigner>>,
) -> (Addr<StateSyncActor>, Arbiter) {
    let client_arbiter = Arbiter::current();
    let client_addr = StateSyncActor::start_in_arbiter(&client_arbiter, move |_ctx| {
        StateSyncActor::new(
            config,
            network_adapter,
            runtime_adapter,
            chain_genesis,
            enable_doomslug,
            validator_signer,
        )
    });
    (client_addr, client_arbiter)
}
