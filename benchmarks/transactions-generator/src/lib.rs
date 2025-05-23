use account::Account;
use near_async::messaging::AsyncSender;
use near_client::{GetBlock, Query, QueryError};
use near_client_primitives::types::GetBlockError;
use near_crypto::PublicKey;
use near_network::client::{ProcessTxRequest, ProcessTxResponse};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockReference};
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
#[cfg(feature = "with_actix")]
pub mod actix_actor;
mod welford;

#[serde_as]
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct Load {
    tps: u64,
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(rename = "duration_s")]
    duration: Duration,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Config {
    schedule: Vec<Load>,
    accounts_path: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self { schedule: Default::default(), accounts_path: "".into() }
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
        )?;

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
        const AMOUNT: near_primitives::types::Balance = 1;

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

    async fn get_latest_block(view_client_sender: &ViewClientSender) -> anyhow::Result<CryptoHash> {
        match view_client_sender
            .block_request_sender
            .send_async(GetBlock(BlockReference::latest()))
            .await
        {
            Ok(rsp) => Ok(rsp?.header.hash),
            Err(err) => {
                anyhow::bail!("async send error: {err}");
            }
        }
    }

    fn start_block_updates(
        view_client_sender: ViewClientSender,
    ) -> tokio::sync::watch::Receiver<CryptoHash> {
        let (tx_latest_block, rx_latest_block) = tokio::sync::watch::channel(Default::default());

        tokio::spawn(async move {
            let view_client = &view_client_sender;
            loop {
                match Self::get_latest_block(view_client).await {
                    Ok(new_hash) => {
                        let _ = tx_latest_block.send(new_hash);
                        tokio::time::interval(Duration::from_secs(3)).tick().await;
                    }
                    Err(err) => {
                        tracing::warn!(target: "transaction-generator", "block_hash update failed: {err}");
                        tokio::time::interval(Duration::from_millis(200)).tick().await;
                    }
                }
            }
        });

        rx_latest_block
    }

    fn prepare_accounts(
        accounts_path: &PathBuf,
        sender: ViewClientSender,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<Arc<Vec<Account>>>> {
        let mut accounts = account::accounts_from_path(accounts_path)?;
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
        mut rx_block: tokio::sync::watch::Receiver<CryptoHash>,
        stats: Arc<Stats>,
    ) {
        let mut rnd: StdRng = SeedableRng::from_entropy();

        let _ = rx_block.wait_for(|hash| *hash != CryptoHash::default()).await.is_ok();
        let mut latest_block_hash = *rx_block.borrow();

        let ld = async {
            loop {
                tokio::select! {
                    _ = rx_block.changed() => {
                        latest_block_hash = *rx_block.borrow();
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
        rx_block: tokio::sync::watch::Receiver<CryptoHash>,
        stats: Arc<Stats>,
    ) {
        tracing::info!(target: "transaction-generator", ?load, "starting the load");

        // Number of tasks to run producing and sending transactions
        // We need several tasks to not get blocked by the sending latency.
        // 4 is currently more than enough.
        const TASK_COUNT: u64 = 4;
        let mut tasks = JoinSet::new();

        for _ in 0..TASK_COUNT {
            tasks.spawn(Self::run_load_task(
                client_sender.clone(),
                Arc::clone(&accounts),
                tokio::time::interval(Duration::from_micros(1_000_000 * TASK_COUNT / load.tps)),
                load.duration,
                rx_block.clone(),
                Arc::clone(&stats),
            ));
        }
        tasks.join_all().await;
    }

    fn start_transactions_loop(
        config: &Config,
        client_sender: ClientSender,
        view_client_sender: ViewClientSender,
        stats: Arc<Stats>,
        rx_block: tokio::sync::watch::Receiver<CryptoHash>,
    ) -> anyhow::Result<()> {
        let rx_accounts = Self::prepare_accounts(&config.accounts_path, view_client_sender)?;

        let schedule = config.schedule.clone();
        tokio::spawn(async move {
            let accounts = rx_accounts.await.unwrap();
            for load in schedule {
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
                "completed running the schedule. stopping the neard..."
            );
            std::process::exit(0);
        });

        Ok(())
    }

    fn start_report_updates(stats: Arc<Stats>) -> task::JoinHandle<()> {
        let mut report_interval = tokio::time::interval(Duration::from_secs(1));
        tokio::spawn(async move {
            let mut stats_prev = StatsLocal::from(&*stats);
            let mut mean_diff = welford::Mean::<i64>::new();
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
                mean_diff.add_measurement(diff.included_in_chunk as i64);
                tracing::info!(target: "transaction-generator",
                    diff=format!("{:?}", diff),
                    rate_processed=mean_diff.mean(),
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
