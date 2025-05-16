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
use parking_lot::Mutex;
use rand::SeedableRng;
use rand::rngs::StdRng;
use serde_with::serde_as;
use std::path::PathBuf;
use std::sync::{Arc, atomic};
use std::time::Duration;
use tokio::task;

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

#[derive(Clone)]
struct RunnerState {
    stats: Arc<Mutex<Stats>>,
}

#[derive(Debug, Clone)]
struct Stats {
    pool_accepted: u64,
    pool_rejected: u64,
    included_in_chunk: u64,
    failed: u64,
}

impl std::ops::Sub for Stats {
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

        let runner_state = RunnerState {
            stats: Arc::new(Mutex::new(Stats {
                pool_accepted: 0,
                pool_rejected: 0,
                included_in_chunk: 0,
                failed: 0,
            })),
        };

        let block_rx = Self::start_block_updates(self.view_client_sender.clone());
        
        Self::start_transactions_loop(
            &self.params,
            client_sender,
            view_client_sender.clone(),
            runner_state.clone(),
            block_rx,
        )?;

        Self::start_report_updates(runner_state);

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
    )-> tokio::sync::watch::Receiver<CryptoHash> 
    {
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
   

    async fn run_the_load(
        load: &Load,
        block_rx: tokio::sync::watch::Receiver<CryptoHash>,
    ) {
        
    }

    fn prepare_accounts(
        accounts_path: &PathBuf,
        sender: ViewClientSender,   
    )-> anyhow::Result<Arc<Vec<Account>>> {
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
        
        Ok(rx.blocking_recv()?)
    }

    fn start_transactions_loop(
        config: &Config,
        client_sender: ClientSender,
        view_client_sender: ViewClientSender,
        runner_state: RunnerState,
        rx_block: tokio::sync::watch::Receiver<CryptoHash>,
    ) -> anyhow::Result<()> {
        let accounts = Self::prepare_accounts(&config.accounts_path, view_client_sender.clone())?;
        let (load_tx, _) = tokio::sync::broadcast::channel(config.schedule.len());

        /// Number of tasks to run producing and sending transactions
        /// We need several tasks to not get blocked by the sending latency.
        /// 4 is currently more than enough.
        const TASK_COUNT: u64 = 4;
        for _ in 0..TASK_COUNT {
            let accounts = accounts.clone();
            let mut rx_load = load_tx.subscribe();
            let client_sender = client_sender.clone();
            let runner_state = runner_state.clone();
            let mut rx_block = rx_block.clone();
            tokio::spawn(async move {
                let mut rnd: StdRng = SeedableRng::from_entropy();
                
                let load: Load = rx_load.recv().await.unwrap();
                let mut tx_interval =
                    tokio::time::interval(Duration::from_micros(1_000_000 * TASK_COUNT / load.tps));

                let _ = rx_block.wait_for(|hash| *hash != CryptoHash::default() ).await.is_ok();
                let mut latest_block_hash = rx_block.borrow().clone();
                loop {
                    tokio::select!{
                        _ = rx_block.changed() => {
                            latest_block_hash = rx_block.borrow().clone();
                        }
                        _ = tx_interval.tick() => {
                            let ok = Self::generate_send_transaction(
                                &mut rnd,
                                &accounts,
                                &latest_block_hash,
                                &client_sender,
                            )
                            .await;

                            let mut stats = runner_state.stats.lock();
                            if ok {
                                stats.pool_accepted += 1;
                            } else {
                                stats.pool_rejected += 1;
                            }         
                        }
                    }
                }
            });
        }

        let schedule = config.schedule.clone();
        tokio::spawn(async move {
            for load in schedule {
                load_tx.send(load).unwrap();
            }
        });

        Ok(())
    }

    fn start_report_updates(runner_state: RunnerState) -> task::JoinHandle<()> {
        let mut report_interval = tokio::time::interval(Duration::from_secs(1));
        tokio::spawn(async move {
            let mut stats_prev = runner_state.stats.lock().clone();
            let mut mean_diff = welford::Mean::<i64>::new();
            loop {
                report_interval.tick().await;
                let stats = {
                    let chunk_tx_total =
                        near_client::metrics::CHUNK_TRANSACTIONS_TOTAL.with_label_values(&["0"]);
                    let included_in_chunk = chunk_tx_total.get();

                    let failed = TRANSACTION_PROCESSED_FAILED_TOTAL.get();

                    let mut stats = runner_state.stats.lock();
                    stats.included_in_chunk = included_in_chunk;
                    stats.failed = failed;
                    stats.clone()
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
