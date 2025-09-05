use account::Account;
use anyhow::Context as _;
use near_async::messaging::AsyncSender;
use near_client::{GetBlock, Query, QueryError};
use near_client_primitives::types::GetBlockError;
use near_crypto::PublicKey;
use near_network::client::{ProcessTxRequest, ProcessTxResponse};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, BlockReference};
use near_primitives::views::{BlockView, QueryRequest, QueryResponse, QueryResponseKind};
use node_runtime::metrics::TRANSACTION_PROCESSED_FAILED_TOTAL;
use rand::SeedableRng;
use rand::rngs::StdRng;
use serde_with::serde_as;
use std::panic;
use std::path::PathBuf;
use std::sync::{Arc, atomic};
use std::time::Duration;
use tokio::task::{self, JoinSet};

pub mod account;
pub mod actix_actor;

// Number of tasks to run producing and sending transactions
// We need several tasks to not get blocked by the sending latency.
// 4 is currently more than enough.
const TX_GENERATOR_TASK_COUNT: u64 = 8;

#[serde_as]
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct Load {
    tps: u64,
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(rename = "duration_s")]
    duration: Duration,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone)]
struct ControllerConfig {
    target_block_production_time_s: f64,
    bps_filter_window_length: usize,
    gain_proportional: f64,
    gain_integral: f64,
    gain_derivative: f64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Config {
    schedule: Vec<Load>,
    controller: Option<ControllerConfig>,
    accounts_path: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            schedule: Default::default(),
            controller: Default::default(),
            accounts_path: "".into(),
        }
    }
}

#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
pub struct ClientSender {
    pub tx_request_sender: AsyncSender<ProcessTxRequest, ProcessTxResponse>,
}

#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
pub struct ViewClientSender {
    pub block_request_sender: AsyncSender<GetBlock, Result<BlockView, GetBlockError>>,
    pub query_sender: AsyncSender<Query, Result<QueryResponse, QueryError>>,
}

pub struct TxGenerator {
    pub params: Config,
    client_sender: ClientSender,
    view_client_sender: ViewClientSender,
}

#[derive(Debug)]
struct Stats {
    pool_accepted: atomic::AtomicU64,
    pool_rejected: atomic::AtomicU64,
}

#[derive(Debug, Clone)]
struct StatsLocal {
    pool_accepted: u64,
    pool_rejected: u64,
    included_in_chunk: u64,
    failed: u64,
}

enum FilterStage {
    Init0,
    Init1 { data: u64, at_time: std::time::Instant },
    Ready { data: u64, rate: f64, at_time: std::time::Instant },
}

/// Exponential smoothing filter accepting the cumulative values and returning the rate estimate.
struct FilterRateExponentialSmoothing {
    gain: f64,
    stage: FilterStage,
}

impl FilterRateExponentialSmoothing {
    pub fn new(gain: f64) -> Self {
        Self { gain, stage: FilterStage::Init0 }
    }

    /// Given the cumulative value measurements returns the smoothed rate estimate.
    /// Assumes the value is measured at a time of a call.
    pub fn register(&mut self, new_data: u64) -> Option<f64> {
        let now = std::time::Instant::now();

        match &mut self.stage {
            FilterStage::Init0 => {
                self.stage = FilterStage::Init1 { data: new_data, at_time: now };
                None
            }
            FilterStage::Init1 { data, at_time } => {
                let rate = (new_data - *data) as f64 / now.duration_since(*at_time).as_secs_f64();
                self.stage = FilterStage::Ready { data: new_data, rate, at_time: now };
                None
            }
            FilterStage::Ready { data, rate, at_time } => {
                let new_rate =
                    (new_data - *data) as f64 / now.duration_since(*at_time).as_secs_f64();
                *rate += self.gain * (new_rate - *rate);
                *data = new_data;
                *at_time = now;
                tracing::debug!(target: "transaction-generator", rate=*rate, "filtered measurement");
                Some(*rate)
            }
        }
    }
}

struct FilterRateWindow {
    data: std::collections::VecDeque<(u64, std::time::Instant)>,
}

impl FilterRateWindow {
    pub fn new(window_len: usize) -> Self {
        Self {
            data: std::collections::VecDeque::<(u64, std::time::Instant)>::with_capacity(
                window_len,
            ),
        }
    }

    pub fn register(&mut self, new_data: u64) -> Option<f64> {
        let now = std::time::Instant::now();

        if self.data.len() == self.data.capacity() {
            let (old_value, at_time) = self.data.pop_front().unwrap();
            self.data.push_back((new_data, now));
            Some((new_data - old_value) as f64 / now.duration_since(at_time).as_secs_f64())
        } else {
            self.data.push_back((new_data, now));
            None
        }
    }
}

struct FilteredRateController {
    controller: pid_lite::Controller,
    filter: FilterRateWindow,
}

impl FilteredRateController {
    /// given the latest cumulative measurement returns the suggested parameter correction.
    pub fn register(&mut self, block_height_sampled: u64) -> f64 {
        if let Some(bps) = self.filter.register(block_height_sampled) {
            tracing::debug!(target: "transaction-generator", bps, "filtered measurement");
            return self.controller.update(1. / bps);
        }

        0.0
    }
}

impl From<&Stats> for StatsLocal {
    fn from(x: &Stats) -> Self {
        Self {
            pool_accepted: x.pool_accepted.load(atomic::Ordering::Relaxed),
            pool_rejected: x.pool_rejected.load(atomic::Ordering::Relaxed),
            included_in_chunk: 0,
            failed: 0,
        }
    }
}

impl std::ops::Sub for StatsLocal {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self {
            pool_accepted: self.pool_accepted - other.pool_accepted,
            pool_rejected: self.pool_rejected - other.pool_rejected,
            included_in_chunk: self.included_in_chunk - other.included_in_chunk,
            failed: self.failed - other.failed,
        }
    }
}

impl TxGenerator {
    pub fn new(
        params: Config,
        client_sender: ClientSender,
        view_client_sender: ViewClientSender,
    ) -> anyhow::Result<Self> {
        Ok(Self { params, client_sender, view_client_sender })
    }

    pub fn start(self: &mut Self) -> anyhow::Result<()> {
        let client_sender = self.client_sender.clone();
        let view_client_sender = self.view_client_sender.clone();

        if self.params.schedule.is_empty() {
            anyhow::bail!("tx generator idle: no schedule provided");
        }

        let stats = Arc::new(Stats { pool_accepted: 0.into(), pool_rejected: 0.into() });

        let block_rx = Self::start_block_updates(self.view_client_sender.clone());

        Self::start_transactions_loop(
            &self.params,
            client_sender,
            view_client_sender,
            Arc::clone(&stats),
            block_rx,
        )
        .context("start transactions loop")?;

        Self::start_report_updates(Arc::clone(&stats));

        Ok(())
    }

    /// Generates a transaction between two random (but different) accounts and pushes it to the `client_sender`
    async fn generate_send_transaction(
        rnd: &mut StdRng,
        accounts: &[account::Account],
        block_hash: &CryptoHash,
        client_sender: &ClientSender,
    ) -> bool {
        // each transaction will transfer this amount
        const AMOUNT: Balance = Balance::from_yoctonear(1);

        let idx = rand::seq::index::sample(rnd, accounts.len(), 2);
        let sender = &accounts[idx.index(0)];
        let nonce = sender.nonce.fetch_add(1, atomic::Ordering::Relaxed) + 1;
        let sender_id = sender.id.clone();
        let signer = sender.as_signer();

        let receiver = &accounts[idx.index(1)];
        let transaction = SignedTransaction::send_money(
            nonce,
            sender_id,
            receiver.id.clone(),
            &signer,
            AMOUNT,
            *block_hash,
        );

        match client_sender
            .tx_request_sender
            .send_async(ProcessTxRequest { transaction, is_forwarded: false, check_only: false })
            .await
        {
            Ok(res) => match res {
                ProcessTxResponse::ValidTx => true,
                _ => {
                    tracing::debug!(target: "transaction-generator",
                        request_rsp=?res);
                    false
                }
            },
            Err(err) => {
                tracing::debug!(target: "transaction-generator",
                    request_err=format!("{err}"), "error");
                false
            }
        }
    }

    async fn get_latest_block(
        view_client_sender: &ViewClientSender,
    ) -> anyhow::Result<(CryptoHash, u64)> {
        match view_client_sender
            .block_request_sender
            .send_async(GetBlock(BlockReference::latest()))
            .await
        {
            Ok(rsp) => {
                let rsp = rsp.context("get latest block")?;
                Ok((rsp.header.hash, rsp.header.height))
            }
            Err(err) => {
                anyhow::bail!("async send error: {err}");
            }
        }
    }

    fn start_block_updates(
        view_client_sender: ViewClientSender,
    ) -> tokio::sync::watch::Receiver<(CryptoHash, u64)> {
        let (tx_latest_block, rx_latest_block) = tokio::sync::watch::channel(Default::default());

        tokio::spawn(async move {
            let view_client = &view_client_sender;
            let mut block_update_interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                match Self::get_latest_block(view_client).await {
                    Ok((new_hash, new_height)) => {
                        tracing::debug!(target: "transaction-generator", new_height, "block update received");
                        let _ = tx_latest_block.send((new_hash, new_height));
                    }
                    Err(err) => {
                        tracing::warn!(target: "transaction-generator", "block_hash update failed: {err}");
                    }
                }
                block_update_interval.tick().await;
            }
        });

        rx_latest_block
    }

    fn prepare_accounts(
        accounts_path: &PathBuf,
        sender: ViewClientSender,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<Arc<Vec<Account>>>> {
        let mut accounts =
            account::accounts_from_path(accounts_path).context("accounts from path")?;
        if accounts.is_empty() {
            anyhow::bail!("No active accounts available");
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            for account in &mut accounts {
                let (id, pk) = (account.id.clone(), account.public_key.clone());
                match Self::get_client_nonce(sender.clone(), id, pk).await {
                    Ok(nonce) => {
                        account.nonce = nonce.into();
                    }
                    Err(err) => {
                        tracing::debug!(target: "transaction-generator",
                            nonce_update_failed=?err);
                    }
                }
            }
            tx.send(Arc::new(accounts)).unwrap();
        });

        Ok(rx)
    }

    async fn run_load_task(
        client_sender: ClientSender,
        accounts: Arc<Vec<Account>>,
        mut tx_interval: tokio::time::Interval,
        duration: tokio::time::Duration,
        mut rx_block: tokio::sync::watch::Receiver<(CryptoHash, u64)>,
        stats: Arc<Stats>,
    ) {
        let mut rnd: StdRng = SeedableRng::from_entropy();

        let _ = rx_block.wait_for(|(hash, _)| *hash != CryptoHash::default()).await.is_ok();
        let (mut latest_block_hash, _) = *rx_block.borrow();

        let ld = async {
            loop {
                tokio::select! {
                    _ = rx_block.changed() => {
                        (latest_block_hash, _) = *rx_block.borrow();
                    }
                    _ = tx_interval.tick() => {
                        let ok = Self::generate_send_transaction(
                            &mut rnd,
                            &accounts,
                            &latest_block_hash,
                            &client_sender,
                        )
                        .await;

                        if ok {
                            stats.pool_accepted.fetch_add(1, atomic::Ordering::Relaxed);
                        } else {
                            stats.pool_rejected.fetch_add(1, atomic::Ordering::Relaxed);
                        }
                    }
                }
            }
        };

        let _ = tokio::time::timeout(duration, ld).await;
    }

    async fn run_load(
        client_sender: ClientSender,
        accounts: Arc<Vec<Account>>,
        load: Load,
        rx_block: tokio::sync::watch::Receiver<(CryptoHash, u64)>,
        stats: Arc<Stats>,
    ) {
        tracing::info!(target: "transaction-generator", ?load, "starting the load");

        let mut tasks = JoinSet::new();

        for _ in 0..TX_GENERATOR_TASK_COUNT {
            tasks.spawn(Self::run_load_task(
                client_sender.clone(),
                Arc::clone(&accounts),
                tokio::time::interval(Duration::from_micros({
                    let load_tps = std::cmp::max(load.tps, 1);
                    1_000_000 * TX_GENERATOR_TASK_COUNT / load_tps
                })),
                load.duration,
                rx_block.clone(),
                Arc::clone(&stats),
            ));
        }
        tasks.join_all().await;
    }

    /// return channel with updates to the total tx rate
    async fn run_controller_loop(
        mut controller: FilteredRateController,
        initial_rate: u64,
        mut rx_block: tokio::sync::watch::Receiver<(CryptoHash, u64)>,
    ) -> tokio::sync::watch::Receiver<tokio::time::Duration> {
        let initial_rate = std::cmp::max(initial_rate, 1);
        let mut rate = initial_rate as f64;
        let (tx_tps_values, rx_tps_values) = tokio::sync::watch::channel(
            tokio::time::Duration::from_micros(1_000_000 * TX_GENERATOR_TASK_COUNT / initial_rate),
        );
        tracing::debug!(target: "transaction-generator", initial_rate, "starting controller");

        let _ = rx_block.wait_for(|(hash, _)| *hash != CryptoHash::default()).await.is_ok();
        let (_, height) = *rx_block.borrow();
        controller.register(height);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx_block.changed() => {
                        let (_, height) = *rx_block.borrow();
                        rate += controller.register(height);
                        tracing::debug!(target: "transaction-generator", rate, "tps updated");
                        let effective_rate = if rate.is_finite() && rate >= 1.0 { rate } else { 1.0 };
                        let micros = ((1_000_000.0 * TX_GENERATOR_TASK_COUNT as f64) / effective_rate)
                            .max(1.0) as u64;
                        tx_tps_values
                            .send(tokio::time::Duration::from_micros(micros))
                            .unwrap();
                    }
                }
            }
        });

        rx_tps_values
    }

    async fn run_controlled_loop(
        controller: FilteredRateController,
        initial_rate: u64,
        client_sender: ClientSender,
        accounts: Arc<Vec<Account>>,
        rx_block: tokio::sync::watch::Receiver<(CryptoHash, u64)>,
        stats: Arc<Stats>,
    ) {
        tracing::info!(target: "transaction-generator", "starting the controlled loop");

        let rx_intervals =
            Self::run_controller_loop(controller, initial_rate, rx_block.clone()).await;

        for _ in 0..TX_GENERATOR_TASK_COUNT {
            tokio::spawn(Self::controlled_loop_task(
                client_sender.clone(),
                Arc::clone(&accounts),
                rx_block.clone(),
                rx_intervals.clone(),
                Arc::clone(&stats),
            ));
        }
    }

    async fn controlled_loop_task(
        client_sender: ClientSender,
        accounts: Arc<Vec<Account>>,
        mut rx_block: tokio::sync::watch::Receiver<(CryptoHash, u64)>,
        mut tx_rates: tokio::sync::watch::Receiver<tokio::time::Duration>,
        stats: Arc<Stats>,
    ) {
        let mut rnd: StdRng = SeedableRng::from_entropy();

        let _ = rx_block.wait_for(|(hash, _)| *hash != CryptoHash::default()).await.is_ok();
        let (mut latest_block_hash, _) = *rx_block.borrow();
        let mut tx_interval = tokio::time::interval(*tx_rates.borrow());

        async {
            loop {
                tokio::select! {
                    _ = rx_block.changed() => {
                        (latest_block_hash, _) = *rx_block.borrow();
                    }
                    _ = tx_rates.changed() => {
                            tx_interval = tokio::time::interval(*tx_rates.borrow());
                        }
                    _ = tx_interval.tick() => {
                        let ok = Self::generate_send_transaction(
                            &mut rnd,
                            &accounts,
                            &latest_block_hash,
                            &client_sender,
                        )
                        .await;

                        if ok {
                            stats.pool_accepted.fetch_add(1, atomic::Ordering::Relaxed);
                        } else {
                            stats.pool_rejected.fetch_add(1, atomic::Ordering::Relaxed);
                        }
                    }
                }
            }
        }
        .await;
    }

    fn start_transactions_loop(
        config: &Config,
        client_sender: ClientSender,
        view_client_sender: ViewClientSender,
        stats: Arc<Stats>,
        rx_block: tokio::sync::watch::Receiver<(CryptoHash, u64)>,
    ) -> anyhow::Result<()> {
        let rx_accounts = Self::prepare_accounts(&config.accounts_path, view_client_sender)
            .context("prepare accounts")?;

        let schedule = config.schedule.clone();

        let controller = if let Some(controller_config) = &config.controller {
            Some(FilteredRateController {
                controller: pid_lite::Controller::new(
                    controller_config.target_block_production_time_s,
                    controller_config.gain_proportional,
                    controller_config.gain_integral,
                    controller_config.gain_derivative,
                ),
                filter: FilterRateWindow::new(controller_config.bps_filter_window_length),
            })
        } else {
            None
        };

        tokio::spawn(async move {
            let accounts = rx_accounts.await.unwrap();
            for load in &schedule {
                Self::run_load(
                    client_sender.clone(),
                    accounts.clone(),
                    load.clone(),
                    rx_block.clone(),
                    Arc::clone(&stats),
                )
                .await;
            }

            tracing::info!(target: "transaction-generator",
                "completed running the schedule"
            );

            if let Some(controller) = controller {
                Self::run_controlled_loop(
                    controller,
                    schedule.last().unwrap().tps,
                    client_sender,
                    accounts.clone(),
                    rx_block.clone(),
                    Arc::clone(&stats),
                )
                .await;
            } else {
                tracing::info!(target: "transaction-generator",
                "no 'controller' settings provided. stopping the `neard`..."
                );
                std::process::exit(0);
            }
        });

        Ok(())
    }

    fn start_report_updates(stats: Arc<Stats>) -> task::JoinHandle<()> {
        let mut report_interval = tokio::time::interval(Duration::from_secs(1));
        tokio::spawn(async move {
            let mut stats_prev = StatsLocal::from(&*stats);
            let mut tps_filter = FilterRateExponentialSmoothing::new(0.1);
            loop {
                report_interval.tick().await;
                let stats = {
                    let mut stats = StatsLocal::from(&*stats);
                    let chunk_tx_total =
                        near_client::metrics::CHUNK_TRANSACTIONS_TOTAL.with_label_values(&["0"]);
                    stats.included_in_chunk = chunk_tx_total.get();
                    stats.failed = TRANSACTION_PROCESSED_FAILED_TOTAL.get();
                    stats
                };
                tracing::info!(target: "transaction-generator", total=format!("{stats:?}"),);
                let diff = stats.clone() - stats_prev;
                let rate = tps_filter.register(stats.included_in_chunk);
                tracing::info!(target: "transaction-generator",
                    diff=format!("{:?}", diff),
                    rate,
                );
                stats_prev = stats.clone();
            }
        })
    }

    async fn get_client_nonce(
        view_client: ViewClientSender,
        account_id: AccountId,
        public_key: PublicKey,
    ) -> anyhow::Result<u64> {
        let q = Query {
            block_reference: BlockReference::latest(),
            request: QueryRequest::ViewAccessKey { account_id, public_key },
        };

        match view_client.query_sender.send_async(q).await {
            Ok(Ok(QueryResponse { kind, .. })) => {
                if let QueryResponseKind::AccessKey(access_key) = kind {
                    Ok(access_key.nonce)
                } else {
                    panic!("wrong response type received from neard");
                }
            }
            Ok(Err(err)) => Err(err.into()),
            Err(err) => {
                anyhow::bail!("request to ViewAccessKey failed: {err}");
            }
        }
    }
}
