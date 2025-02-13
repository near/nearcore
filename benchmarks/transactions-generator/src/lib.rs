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
use rand::distributions::{Distribution, Uniform};
use rand::rngs::StdRng;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::oneshot;
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
        Self { tps: 0, volume: 40000, accounts_path: "".into() }
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
    runner: Option<(task::JoinHandle<()>, oneshot::Sender<bool>)>,
}

struct Stats {
    accepted: u64,
    rejected: u64,
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
        if let Some(_) = self.runner {
            anyhow::bail!("attempt to (re)start the running transaction generator");
        }
        // TODO(slavas): generate accounts on the fly?
        let mut accounts = account::accounts_from_dir(&self.params.accounts_path)?;
        if accounts.is_empty() {
            anyhow::bail!("No active accounts available");
        }
        let client_sender = self.client_sender.clone();
        let view_client_sender = self.view_client_sender.clone();
        let (tx, mut _rx) = oneshot::channel::<bool>();

        if self.params.tps == 0 {
            anyhow::bail!("target TPS should be > 0");
        }
        let mut stats = Stats { accepted: 0, rejected: 0 };
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
                        tracing::info!(target: "transaction-generator",
                            pool_accepted=stats.accepted,
                            pool_rejected=stats.rejected,
                            total_processed=TRANSACTION_PROCESSED_SUCCESSFULLY_TOTAL.get(),
                            total_failed=TRANSACTION_PROCESSED_FAILED_TOTAL.get(),
                            "transactions",);
                    }
                    // r = &mut rx => {} // triggered out of control
                }
            }
        });

        self.runner = Some((handle, tx));
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

        let id_sender = Uniform::from(0..accounts.len()).sample(rnd);
        let id_recv = loop {
            let candidate = Uniform::from(0..accounts.len()).sample(rnd);
            if candidate != id_sender {
                break candidate;
            }
        };

        let sender = &mut accounts[id_sender];
        sender.nonce += 1;
        let nonce = sender.nonce;
        let sender_id = sender.id.clone();
        let signer = sender.as_signer();

        let receiver = &accounts[id_recv];
        let transaction = SignedTransaction::send_money(
            nonce,
            sender_id,
            receiver.id.clone(),
            &signer,
            AMOUNT,
            *block_hash,
        );

        tracing::trace!(target: "transaction-generator", "generated");
        match client_sender
            .tx_request_sender
            .send_async(ProcessTxRequest { transaction, is_forwarded: false, check_only: false })
            .await
        {
            Ok(res) => {
                match res {
                    ProcessTxResponse::NoResponse => {
                        stats.rejected += 1;
                        tracing::debug!(target: "transaction-generator",
                            processTxRequest="NoResponse", "error");
                    }
                    ProcessTxResponse::ValidTx => {
                        stats.accepted += 1;
                    }
                    ProcessTxResponse::InvalidTx(err) => {
                        stats.rejected += 1;
                        tracing::debug!(target: "transaction-generator",
                            processTxRequest=format!("{err:?}"), "error");
                    }
                    ProcessTxResponse::RequestRouted => {
                        stats.rejected += 1;
                        tracing::debug!(target: "transaction-generator",
                            processTxRequest="routed", "error");
                    }
                    ProcessTxResponse::DoesNotTrackShard => {
                        stats.rejected += 1;
                        tracing::debug!(target: "transaction-generator",
                            processTxRequest="DoesNotTrackShard", "error");
                    }
                }
                tracing::trace!(target: "transaction-generator", "transaction pushed");
            }
            Err(err) => {
                stats.rejected += 1;
                tracing::debug!(target: "transaction-generator",
                    processTxRequest=format!("{err}"), "error");
            }
        }
    }
} // impl TxGenerator
