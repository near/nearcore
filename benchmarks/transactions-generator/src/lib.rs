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
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::path::PathBuf;
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
    runner: Option<task::JoinHandle<()>>,
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
        // TODO(slavas): generate accounts on the fly?
        let mut accounts = account::accounts_from_dir(&self.params.accounts_path)?;
        if accounts.is_empty() {
            anyhow::bail!("No active accounts available");
        }
        let client_sender = self.client_sender.clone();
        let view_client_sender = self.view_client_sender.clone();

        if self.params.tps == 0 {
            anyhow::bail!("target TPS should be > 0");
        }
        let mut stats = Stats { pool_accepted: 0, pool_rejected: 0, processed: 0, failed: 0 };
        let mut stats_prev = stats.clone();
        let mut tx_interval =
            tokio::time::interval(Duration::from_micros(1_000_000 / self.params.tps));
        let mut block_interval = tokio::time::interval(Duration::from_secs(5));
        let mut report_interval = tokio::time::interval(Duration::from_secs(1));

        let handle = tokio::spawn(async move {
            let mut rnd: StdRng = SeedableRng::from_entropy();
            let mut block_hash = CryptoHash::default();
            tracing::debug!(target: "transaction-generator", "tx generation loop starting");
            loop {
                tokio::select! {
                    _ = tx_interval.tick() => {
                        Self::generate_send_transaction(
                            &mut rnd,
                            &mut accounts,
                            &block_hash,
                            &client_sender,
                            &mut stats).await;
                    }
                    _ = block_interval.tick() => {
                        if let Ok(Ok(block_view)) = view_client_sender.block_request_sender.send_async( GetBlock(BlockReference::latest())).await {
                             block_hash = block_view.header.hash;
                             tracing::trace!(target: "transaction-generator",
                                 block_hash=format!("{block_hash:?}"), "update");
                        } else {
                            tracing::warn!(target: "transaction-generator", "block_hash update failed");
                        }
                    }
                    _ = report_interval.tick() => {
                        stats.processed = TRANSACTION_PROCESSED_SUCCESSFULLY_TOTAL.get();
                        stats.failed = TRANSACTION_PROCESSED_FAILED_TOTAL.get();
                        tracing::info!(target: "transaction-generator",
                            total=format!("{stats:?}"),);
                        tracing::info!(target: "transaction-generator",
                            diff=format!("{:?}", stats.clone() - stats_prev),);
                        stats_prev = stats.clone();
                    }
                }
            }
        });

        self.runner = Some(handle);
        Ok(())
    }

    /// Generates a transaction between two random (but different) accounts and pushes it to the `client_sender`
    async fn generate_send_transaction(
        rnd: &mut StdRng,
        accounts: &mut [account::Account],
        block_hash: &CryptoHash,
        client_sender: &ClientSender,
        stats: &mut Stats,
    ) {
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
                ProcessTxResponse::NoResponse => {
                    stats.pool_rejected += 1;
                    tracing::debug!(target: "transaction-generator",
                            processTxRequest="NoResponse", "error");
                }
                ProcessTxResponse::ValidTx => {
                    stats.pool_accepted += 1;
                }
                ProcessTxResponse::InvalidTx(err) => {
                    stats.pool_rejected += 1;
                    tracing::debug!(target: "transaction-generator",
                            processTxRequest=format!("{err:?}"), "error");
                }
                ProcessTxResponse::RequestRouted => {
                    stats.pool_rejected += 1;
                    tracing::debug!(target: "transaction-generator",
                            processTxRequest="routed", "error");
                }
                ProcessTxResponse::DoesNotTrackShard => {
                    stats.pool_rejected += 1;
                    tracing::debug!(target: "transaction-generator",
                            processTxRequest="DoesNotTrackShard", "error");
                }
            },
            Err(err) => {
                stats.pool_rejected += 1;
                tracing::debug!(target: "transaction-generator",
                    processTxRequest=format!("{err}"), "error");
            }
        }
    }
} // impl TxGenerator
