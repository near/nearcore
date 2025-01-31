use near_async::messaging::AsyncSender;
use near_client::GetBlock;
use near_client_primitives::types::GetBlockError;
use near_network::client::{ProcessTxRequest, ProcessTxResponse};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::views::BlockView;
use near_primitives::types::BlockReference;
use rand::SeedableRng;
use rand::distributions::{Distribution, Uniform};
use rand::rngs::StdRng;
use std::path::PathBuf;
use std::time::Duration;
use tokio::task;
use tokio::sync::oneshot;


pub mod account;
#[cfg(feature="with_actix")]
pub mod actix_actor;


#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct TxGeneratorConfig {
    tps: u64,
    volume: u64,
    accounts_path: PathBuf,
}

impl Default for TxGeneratorConfig {
    fn default()-> Self {
        Self {
            tps: 0,
            volume: 40000,
            accounts_path: "".into(),
        }
    }
}

#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
pub struct ClientSender{
    pub tx_request_sender: AsyncSender<ProcessTxRequest, ProcessTxResponse>,
}

#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
pub struct ViewClientSender{
    pub block_request_sender: AsyncSender<GetBlock, Result<BlockView, GetBlockError>>,
}

pub struct TxGenerator {
    pub params: TxGeneratorConfig,
    client_sender: ClientSender,
    view_client_sender: ViewClientSender,
    runner: Option<(task::JoinHandle<()>, oneshot::Sender<bool>)>,
}

impl TxGenerator {
    pub fn new(
        params: TxGeneratorConfig,
        client_sender: ClientSender,
        view_client_sender: ViewClientSender
    )-> anyhow::Result<Self> 
    {
        Ok(Self {
            params, client_sender, view_client_sender, runner: None,
        })
    }

    pub fn start(self: &mut Self)-> anyhow::Result<()> {
        if let Some(_) = self.runner {
            anyhow::bail!("attempt to (re)start the running transaction generator");
        }
        // TODO(ssavenko): generate accounts on the fly?
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
        let mut tx_interval = tokio::time::interval(Duration::from_micros(1_000_000/self.params.tps));
        let mut block_interval = tokio::time::interval(Duration::from_secs(5));
        
        let handle = tokio::spawn(async move {
            let mut rnd: StdRng = SeedableRng::from_entropy();
            let mut block_hash = CryptoHash::default();
            tracing::trace!(target: "transaction-generator", "tx generation loop starting");
            loop {
                tokio::select! {
                    _ = tx_interval.tick() => {
                        tracing::trace!(target: "transaction-generator", "tick");
                        Self::generate_send_transaction(&mut rnd, &mut accounts, &block_hash, &client_sender).await;
                    }
                    _ = block_interval.tick() => {
                        if let Ok(Ok(block_view)) = view_client_sender.block_request_sender.send_async( GetBlock(BlockReference::latest())).await {
                             block_hash = block_view.header.hash;
                             tracing::trace!(target: "transaction-generator",
                                 block_hash=format!("{block_hash:?}"), "block hash updated");
                        } else {
                            tracing::warn!(target: "transaction-generator", "failed to update the transactions validity range");
                        }
                    }
                    // _ = &mut rx => {
                    //     tracing::trace!(target: "transaction-generator", "received stop signal");
                    //     break;
                    // }
                }
            }
            // tracing::trace!(target: "transaction-generator", "tx generation loop completed");  
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
    ){
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
            block_hash.clone(),
        );

        tracing::trace!(target: "transaction-generator", "generated");
        match client_sender.tx_request_sender.send_async(ProcessTxRequest{
                transaction, is_forwarded: false, check_only: false}).await {
            Ok(_) => {
                tracing::trace!(target: "transaction-generator", "transaction pushed");    
            },
            Err(err) => {
                tracing::debug!(target: "transaction-generator", "failed sending the transaction: {err}");
            },
        }
    }    
}
