use crate::account::Account;

use near_async::messaging::AsyncSender;
use near_client::{GetBlock, Query, QueryError};
use near_client_primitives::types::GetBlockError;
use near_crypto::{KeyType, PublicKey, SecretKey};
use near_network::client::{ProcessTxRequest, ProcessTxResponse};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockReference};
use near_primitives::views::{BlockView, QueryRequest, QueryResponse, QueryResponseKind};
use node_runtime::metrics::TRANSACTION_PROCESSED_FAILED_TOTAL;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::path::PathBuf;
use std::sync::{Arc, atomic};
use std::time::Duration;
use tokio::task;

pub mod account;
#[cfg(feature = "with_actix")]
pub mod actix_actor;
mod welford;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct AccountsConfig {
    pub accounts_path: PathBuf,
    pub signer_key_path: PathBuf,
    pub prefixes: Vec<String>,
    pub num_accounts: usize,
    pub tps: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct TxGeneratorConfig {
    tps: u64,
    accounts_config: AccountsConfig,
}

impl Default for TxGeneratorConfig {
    fn default() -> Self {
        Self {
            tps: 0,
            accounts_config: AccountsConfig {
                accounts_path: "".into(),
                signer_key_path: "".into(),
                prefixes: Default::default(),
                num_accounts: 0,
                tps: 0,
            },
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
    pub params: TxGeneratorConfig,
    client_sender: ClientSender,
    view_client_sender: ViewClientSender,
    tasks: Vec<task::JoinHandle<()>>,
}

#[derive(Clone)]
struct RunnerState {
    stats: Arc<std::sync::Mutex<Stats>>,
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
        params: TxGeneratorConfig,
        client_sender: ClientSender,
        view_client_sender: ViewClientSender,
    ) -> anyhow::Result<Self> {
        Ok(Self { params, client_sender, view_client_sender, tasks: Vec::new() })
    }

    pub fn start(self: &mut Self) -> anyhow::Result<()> {
        if !self.tasks.is_empty() {
            anyhow::bail!("attempt to (re)start the running transaction generator");
        }
        let client_sender = self.client_sender.clone();
        let view_client_sender = self.view_client_sender.clone();

        if self.params.tps == 0 {
            anyhow::bail!("target TPS should be > 0");
        }

        let runner_state = RunnerState {
            stats: Arc::new(std::sync::Mutex::new(Stats {
                pool_accepted: 0,
                pool_rejected: 0,
                included_in_chunk: 0,
                failed: 0,
            })),
        };

        let (tx_block, rx_block) = tokio::sync::watch::channel(CryptoHash::default());

        self.tasks.push(Self::start_block_updates(view_client_sender.clone(), tx_block.clone()));

        self.tasks = Self::start_transactions_loop(
            &self.params,
            client_sender,
            view_client_sender,
            rx_block.clone(),
            runner_state.clone(),
        )?;

        self.tasks.push(Self::start_report_updates(runner_state));

        Ok(())
    }

    async fn generate_create_account(
        block_hash: &CryptoHash,
        client_sender: &ClientSender,
        signer: &Account,
        new_account: AccountId,
        deposit: u128,
    ) -> Option<Account> {
        let new_account_key = SecretKey::from_random(KeyType::ED25519);

        let transaction = SignedTransaction::create_account(
            signer.nonce.fetch_add(1, atomic::Ordering::Relaxed),
            signer.signer.account_id.clone(),
            new_account.clone(),
            deposit,
            new_account_key.public_key().clone(),
            &signer.as_signer(),
            block_hash.clone(),
        );

        match client_sender
            .tx_request_sender
            .send_async(ProcessTxRequest { transaction, is_forwarded: false, check_only: false })
            .await
        {
            Ok(response) => match response {
                ProcessTxResponse::ValidTx => Some(Account::new(new_account, new_account_key, 1000000000u64)),
                _ => {
                    tracing::debug!(target: "transaction-generator",
                        ?response, "request to create account failed",
                    );
                    None
                }
            },
            Err(error) => {
                tracing::debug!(target:"transaction-generator", ?error,);
                None
            }
        }
    }


    fn update_nonces(
        mut accounts: Vec<Account>,
        view_client_sender: ViewClientSender,
    ) -> tokio::sync::oneshot::Receiver<Box<Vec<Account>>>
    {
        let (tx_accounts_deserialized, rx_accounts_deserialized) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {            
            // update nonces for the accounts read from disk
            tracing::info!(target: "transaction-generator",
                num_accounts=accounts.len(),
                "updating the deserialized account nonces");
            for account in accounts.iter_mut() {
                let (id, pk) = (account.signer.account_id.clone(), account.signer.public_key.clone());
                match Self::get_client_nonce(view_client_sender.clone(), id, pk).await {
                    Ok(nonce) => {
                        account.nonce = nonce.into();
                        break;
                    }
                    Err(err) => {
                        tracing::debug!(target: "transaction-generator",
                            ?err, "nonce update failed");
                    }
                }
            }

            tx_accounts_deserialized.send(Box::new(accounts)).unwrap();
        });
        
        rx_accounts_deserialized
    }

    fn load_or_generate_accounts(
        config: &AccountsConfig,
        client_sender: ClientSender,
        view_client_sender: ViewClientSender,
        mut block_receiver: tokio::sync::watch::Receiver<CryptoHash>,
    ) -> tokio::sync::broadcast::Receiver<Arc<Vec<Account>>> {

        let mut accounts = account::accounts_from_path(&config.accounts_path)
            .unwrap_or_else(|_| panic!("failed to read the accounts from: {:?}", config.accounts_path));
        let num_accounts = config.num_accounts;
        if num_accounts != 0 && num_accounts < accounts.len() {
            accounts.truncate(num_accounts);
        }

        let mut next_account_index = accounts.len();
        
        let rx_accounts_deserialized = Self::update_nonces(accounts, view_client_sender.clone());

        let prefixes = {
            let mut prefixes = config.prefixes.clone();
            if prefixes.is_empty() {
                prefixes.push(String::from("a"));
            }
            prefixes
        };

        let deposit: u128 = 95306060187500000001000;
        let signer_key_path = config.signer_key_path.clone();
        let signer = std::cell::OnceCell::new();
        let mut tx_interval = tokio::time::interval(Duration::from_micros(1_000_000 / config.tps));
        let (tx_accounts_created, rx_accounts_created) = tokio::sync::oneshot::channel();

        let view_client_sender1 = view_client_sender.clone();
        tokio::spawn(async move {
            let accounts_to_create = num_accounts - next_account_index;
            tracing::info!(target: "transaction-generator", accounts_to_create, "creating accounts");
            let mut accounts: Vec<Account> = Vec::with_capacity(accounts_to_create);

            block_receiver.mark_unchanged();
            block_receiver.changed().await.unwrap();
            let mut block_hash = block_receiver.borrow_and_update().clone();

            let signer = signer.get_or_init(|| { 
                Account::from_file(signer_key_path.as_path())
                    .expect("failed to read the signer from: {signer_key_path}")
            });
            
            let nonce = match Self::get_client_nonce(
                view_client_sender1.clone(),
                signer.signer.account_id.clone(),
                signer.signer.public_key.clone(),
            ).await {
                Ok(nonce) => { nonce },
                Err(err) => {
                    panic!("failed to update the signer account nonce from network: {}", err); 
                },
            };
            signer.nonce.store(nonce+1, atomic::Ordering::Release);

            while next_account_index < num_accounts {
                tokio::select! {
                    _ = &mut Box::pin(block_receiver.changed()) => {
                        block_hash = block_receiver.borrow_and_update().clone();
                        tracing::info!(target: "transaction-generator",
                            next_account_index,
                            "new latest block received");
                    }
                    _ = &mut Box::pin(tx_interval.tick()) => {
                        let prefix = &prefixes[accounts.len() % prefixes.len()];
                        let new_account = format!("{prefix}_user_{}.{}",
                            next_account_index,
                            signer.signer.account_id).parse().unwrap();

                        if let Some(new_account) = Self::generate_create_account(
                            &block_hash,
                            &client_sender,
                            &signer,
                            new_account,
                            deposit).await
                        {
                            accounts.push(new_account);
                            next_account_index += 1;
                        }
                    }
                }
            }
            tracing::info!(target: "transaction-generator", accounts_created=accounts_to_create, "accounts created");
            tx_accounts_created.send(Box::new(accounts)).unwrap();
        });

        let (tx_accounts_ready, rx_accounts_ready) = tokio::sync::broadcast::channel(1);
        tokio::spawn(async move {
            let mut deserialized = rx_accounts_deserialized.await.unwrap();
            let mut created = rx_accounts_created.await.unwrap();
            // let rx_new_accounts_updated = Self::update_nonces(*created, view_client_sender.clone());
            // let mut created = rx_new_accounts_updated.await
            //     .expect("failed to update the nonces of newly created accounts");

            deserialized.append(&mut *created);
            tx_accounts_ready.send(Arc::new(*deserialized)).unwrap();
        });

        rx_accounts_ready
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
        let sender_id = sender.signer.account_id.clone();
        let signer = sender.as_signer();

        let receiver = &accounts[idx.index(1)];
        let transaction = SignedTransaction::send_money(
            nonce,
            sender_id,
            receiver.signer.account_id.clone(),
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

    fn start_transactions_loop(
        config: &TxGeneratorConfig,
        client_sender: ClientSender,
        view_client_sender: ViewClientSender,
        block_receiver: tokio::sync::watch::Receiver<CryptoHash>,
        runner_state: RunnerState,
    ) -> anyhow::Result<Vec<task::JoinHandle<()>>> 
    {    
        let tx_accounts = Self::load_or_generate_accounts(
            &config.accounts_config,
            client_sender.clone(),
            view_client_sender.clone(),
            block_receiver.clone(),
        );

        let mut tasks = Vec::<task::JoinHandle<()>>::new();
        const TASK_COUNT:u64 = 4;
        for _ in 0..TASK_COUNT {
            let client_sender = client_sender.clone();
            let runner_state = runner_state.clone();
            let mut tx_interval = tokio::time::interval(Duration::from_micros(
                1_000_000 * TASK_COUNT / config.tps,
            ));
            let mut block_receiver = block_receiver.clone();
            let mut tx_accounts = tx_accounts.resubscribe();
            tasks.push(tokio::spawn(async move {
                let mut rnd: StdRng = SeedableRng::from_entropy();

                block_receiver.mark_unchanged();
                block_receiver.changed().await.unwrap();
                let mut block_hash = block_receiver.borrow_and_update().clone();

                let accounts = tx_accounts.recv().await.unwrap();

                loop {
                    tokio::select! {
                        _ = &mut Box::pin(block_receiver.changed()) => {
                            block_hash = block_receiver.borrow_and_update().clone();
                        }
                        _ = &mut Box::pin(tx_interval.tick()) => {
                            let ok = Self::generate_send_transaction(
                                &mut rnd,
                                &accounts,
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
                    }
                }
            }));
        }

        Ok(tasks)
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
        sender: tokio::sync::watch::Sender<CryptoHash>,
    ) -> task::JoinHandle<()> {
        let mut block_interval = tokio::time::interval(Duration::from_secs(5));
        tokio::spawn(async move {
            let view_client = &view_client_sender;
            loop {
                block_interval.tick().await;
                match Self::get_latest_block(view_client).await {
                    Ok(new_hash) => {
                        sender.send(new_hash).unwrap();
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
            let mut mean_diff = welford::Mean::<i64>::new();
            loop {
                report_interval.tick().await;
                let stats = {
                    let chunk_tx_total =
                        near_client::metrics::CHUNK_TRANSACTIONS_TOTAL.with_label_values(&["0"]);
                    let included_in_chunk = chunk_tx_total.get();

                    let failed = TRANSACTION_PROCESSED_FAILED_TOTAL.get();

                    let mut stats = runner_state.stats.lock().unwrap();
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
