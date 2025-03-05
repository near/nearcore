use near_async::messaging::AsyncSender;
use near_client::GetBlock;
use near_client_primitives::types::GetBlockError;
use near_network::client::{ProcessTxRequest, ProcessTxResponse};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::BlockReference;
use near_primitives::views::BlockView;
use node_runtime::metrics::TRANSACTION_PROCESSED_FAILED_TOTAL;
use node_runtime::metrics::TRANSACTION_PROCESSED_SUCCESSFULLY_TOTAL;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task;

pub mod account;
#[cfg(feature = "with_actix")]
pub mod actix_actor;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct TxGeneratorConfig {
    tps: u64,
    volume: u64,
    accounts_path: PathBuf,
}

impl Default for TxGeneratorConfig {
    fn default() -> Self {
        Self { tps: 0, volume: 0, accounts_path: "".into() }
    }
}

#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
pub struct ClientSender {
    pub tx_request_sender: AsyncSender<ProcessTxRequest, ProcessTxResponse>,
}

#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
pub struct ViewClientSender {
    pub block_request_sender: AsyncSender<GetBlock, Result<BlockView, GetBlockError>>,
}

pub struct TxGenerator {
    pub params: TxGeneratorConfig,
    client_sender: ClientSender,
    view_client_sender: ViewClientSender,
    runner: Option<(task::JoinHandle<()>, task::JoinHandle<()>, task::JoinHandle<()>)>,
}

#[derive(Clone)]
struct RunnerState {
    block_hash: Arc<std::sync::Mutex<CryptoHash>>,
    stats: Arc<std::sync::Mutex<Stats>>,
}

#[derive(Debug, Clone)]
struct Stats {
    pool_accepted: u64,
    pool_rejected: u64,
    processed: u64,
    failed: u64,
}

impl std::ops::Sub for Stats {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self {
            pool_accepted: self.pool_accepted - other.pool_accepted,
            pool_rejected: self.pool_rejected - other.pool_rejected,
            processed: self.processed - other.processed,
            failed: self.failed - other.failed,
        }
    }
}

impl TxGenerator {
    pub fn new(
        params: TxGeneratorConfig,
        client_sender: ClientSender,
        view_client_sender: ViewClientSender,
    ) -> anyhow::Result<Self> {
        Ok(Self { params, client_sender, view_client_sender, runner: None })
    }

    pub fn start(self: &mut Self) -> anyhow::Result<()> {
        if self.runner.is_some() {
            anyhow::bail!("attempt to (re)start the running transaction generator");
        }
        let client_sender = self.client_sender.clone();
        let view_client_sender = self.view_client_sender.clone();

        if self.params.tps == 0 {
            anyhow::bail!("target TPS should be > 0");
        }

        let runner_state = RunnerState {
            block_hash: Arc::new(std::sync::Mutex::new(CryptoHash::default())),
            stats: Arc::new(std::sync::Mutex::new(Stats {
                pool_accepted: 0,
                pool_rejected: 0,
                processed: 0,
                failed: 0,
            })),
        };

        let transactions = Self::start_transactions_loop(
            &self.params,
            client_sender,
            view_client_sender.clone(),
            runner_state.clone(),
        )?;

        let block_updates = Self::start_block_updates(view_client_sender, runner_state.clone());

        let report_updates = Self::start_report_updates(runner_state);

        self.runner = Some((transactions, block_updates, report_updates));
        Ok(())
    }

    /// Generates a transaction between two random (but different) accounts and pushes it to the `client_sender`
    async fn generate_send_transaction(
        rnd: &mut StdRng,
        accounts: &mut [account::Account],
        block_hash: &CryptoHash,
        client_sender: &ClientSender,
    ) -> bool {
        const AMOUNT: near_primitives::types::Balance = 1_000;

        let idx = rand::seq::index::sample(rnd, accounts.len(), 2);
        let sender = &mut accounts[idx.index(0)];
        sender.nonce += 1;
        let nonce = sender.nonce;
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
                        request_rsp=format!("res?:"));
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

    fn start_transactions_loop(
        params: &TxGeneratorConfig,
        client_sender: ClientSender,
        view_client_sender: ViewClientSender,
        runner_state: RunnerState,
    ) -> anyhow::Result<task::JoinHandle<()>> {
        let mut tx_interval = tokio::time::interval(Duration::from_micros(1_000_000 / params.tps));
        // TODO(slavas): generate accounts on the fly?
        let mut accounts = account::accounts_from_dir(&params.accounts_path)?;
        if accounts.is_empty() {
            anyhow::bail!("No active accounts available");
        }

        Ok(tokio::spawn(async move {
            let mut rnd: StdRng = SeedableRng::from_entropy();

            match Self::get_latest_block(&view_client_sender).await {
                Ok(new_hash) => {
                    let mut block_hash = runner_state.block_hash.lock().unwrap();
                    *block_hash = new_hash;
                }
                Err(err) => {
                    tracing::error!("failed initializing the block hash: {err}");
                }
            }

            let block_hash = runner_state.block_hash.clone();
            loop {
                tx_interval.tick().await;
                let block_hash = *block_hash.lock().unwrap();
                let ok = Self::generate_send_transaction(
                    &mut rnd,
                    &mut accounts,
                    &block_hash,
                    &client_sender,
                )
                .await;

                let mut stats = runner_state.stats.lock().unwrap();
                if ok {
                    stats.pool_accepted += 1;
                } else {
                    stats.pool_rejected += 1;
                }
            }
        }))
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
        runner_state: RunnerState,
    ) -> task::JoinHandle<()> {
        let mut block_interval = tokio::time::interval(Duration::from_secs(5));
        tokio::spawn(async move {
            let view_client = &view_client_sender;
            loop {
                block_interval.tick().await;
                match Self::get_latest_block(view_client).await {
                    Ok(new_hash) => {
                        let mut block_hash = runner_state.block_hash.lock().unwrap();
                        *block_hash = new_hash;
                    }
                    Err(err) => {
                        tracing::warn!(target: "transaction-generator", "block_hash update failed: {err}");
                    }
                }
            }
        })
    }

    fn start_report_updates(runner_state: RunnerState) -> task::JoinHandle<()> {
        let mut report_interval = tokio::time::interval(Duration::from_secs(1));
        tokio::spawn(async move {
            let mut stats_prev = runner_state.stats.lock().unwrap().clone();
            loop {
                report_interval.tick().await;
                let stats = {
                    let processed = TRANSACTION_PROCESSED_SUCCESSFULLY_TOTAL.get();
                    let failed = TRANSACTION_PROCESSED_FAILED_TOTAL.get();

                    let mut stats = runner_state.stats.lock().unwrap();
                    stats.processed = processed;
                    stats.failed = failed;
                    stats.clone()
                };
                tracing::info!(target: "transaction-generator",
                    total=format!("{stats:?}"),);
                tracing::info!(target: "transaction-generator",
                    diff=format!("{:?}", stats.clone() - stats_prev),);
                stats_prev = stats.clone();
            }
        })
    }
}
