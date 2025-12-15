use account::Account;
use anyhow::Context as _;
use choice::{Choice, TxAccountsSelector};
use near_async::messaging::AsyncSender;
use near_client::{GetBlock, Query, QueryError};
use near_client_primitives::types::GetBlockError;
use near_crypto::PublicKey;
use near_network::client::{ProcessTxRequest, ProcessTxResponse, TxStatusRequest};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    Action, CreateAccountAction, DeployContractAction, FunctionCallAction, SignedTransaction,
    TransferAction,
};
use near_primitives::types::{AccountId, Balance, BlockReference};
use near_primitives::views::{
    BlockHeaderView, BlockView, FinalExecutionOutcomeView, FinalExecutionStatus, QueryRequest,
    QueryResponse, QueryResponseKind,
};
// use near_primitives::borsh::BorshSerialize; // if needed
use node_runtime::metrics::TRANSACTION_PROCESSED_FAILED_TOTAL;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use serde_with::serde_as;
use std::panic;
use std::path::PathBuf;
use std::sync::{Arc, atomic};
use std::time::{Duration, Instant};
use tokio::task::{self, JoinSet};

pub mod account;
pub mod actor;
mod choice;

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
    block_pause_threshold_ms: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
enum TransactionTypeConfig {
    NativeToken,
    FungibleToken { wasm_path: PathBuf },
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Config {
    schedule: Vec<Load>,
    controller: Option<ControllerConfig>,
    accounts_path: PathBuf,
    receiver_accounts_path: Option<PathBuf>,
    #[serde(default = "default_transaction_type")]
    transaction_type: TransactionTypeConfig,
    /// ratio of receivers chosen from the senders accounts [0.0, 1.0];
    /// 0.0: receivers are chosen from the receiver accounts (default). The receiver accounts may actually have the senders accounts as a subset
    /// 1.0: receivers are chosen only from the senders accounts
    #[serde(default = "default_receivers_from_senders_ratio")]
    receivers_from_senders_ratio: f64,
    #[serde(default = "default_sender_accounts_zipf_skew")]
    sender_accounts_zipf_skew: f64,
    #[serde(default = "default_receiver_accounts_zipf_skew")]
    receiver_accounts_zipf_skew: f64,
}

fn default_transaction_type() -> TransactionTypeConfig {
    TransactionTypeConfig::NativeToken
}

fn default_sender_accounts_zipf_skew() -> f64 {
    0.0 // uniform distribution
}

fn default_receiver_accounts_zipf_skew() -> f64 {
    0.0 // uniform distribution
}

fn default_receivers_from_senders_ratio() -> f64 {
    0.0 // all receivers are selected from the sender accounts
}

impl Default for Config {
    fn default() -> Self {
        Self {
            schedule: Default::default(),
            controller: Default::default(),
            accounts_path: "".into(),
            receiver_accounts_path: None,
            transaction_type: TransactionTypeConfig::NativeToken,
            receivers_from_senders_ratio: default_receivers_from_senders_ratio(),
            sender_accounts_zipf_skew: default_sender_accounts_zipf_skew(),
            receiver_accounts_zipf_skew: default_receiver_accounts_zipf_skew(),
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
    pub tx_status_request: AsyncSender<TxStatusRequest, Option<Box<FinalExecutionOutcomeView>>>,
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

struct ValueAtTime {
    value: u64,
    at_time_ns: u64,
}
struct FilterRateWindow {
    data: std::collections::VecDeque<ValueAtTime>,
}

/// Sliding window filter accepting the cumulative values and returning the rate estimate.
/// Returns None until the window is filled.
impl FilterRateWindow {
    pub fn new(window_len: usize) -> Self {
        Self { data: std::collections::VecDeque::<_>::with_capacity(window_len) }
    }

    pub fn register(&mut self, new_data: u64, timestamp_ns: u64) -> Option<f64> {
        if self.data.len() == self.data.capacity() {
            let ValueAtTime { value: old_value, at_time_ns: old_timestamp_ns } =
                self.data.pop_front().unwrap();
            self.data.push_back(ValueAtTime { value: new_data, at_time_ns: timestamp_ns });
            let time_diff_ns = timestamp_ns.checked_sub(old_timestamp_ns).unwrap();
            if time_diff_ns == 0 {
                tracing::warn!(target: "transaction-generator", "zero time difference in rate measurement");
                return None;
            }
            Some((new_data - old_value) as f64 * 1e9 / (time_diff_ns as f64))
        } else {
            self.data.push_back(ValueAtTime { value: new_data, at_time_ns: timestamp_ns });
            None
        }
    }
}

struct FilteredRateController {
    controller: pid_lite::Controller,
    filter: FilterRateWindow,
    block_pause_threshold_ms: Option<u64>,
}

impl FilteredRateController {
    /// given the latest cumulative measurement returns the suggested parameter correction.
    pub fn register(&mut self, block_height_sampled: u64, block_at_ns: u64) -> f64 {
        if let Some(bps) = self.filter.register(block_height_sampled, block_at_ns) {
            if bps < 0.1 {
                tracing::warn!(target: "transaction-generator", bps, "filtered bps measurement is too low ");
                // this will cause the controller to suggest a large decrease in tps
                return self.controller.update(1000.0);
            }
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

enum TransactionSender {
    Native(NativeTokenTxSender),
    Fungible(FungibleTokenTxSender),
}

impl TransactionSender {
    /// Generates a transaction between two random (but different) accounts and pushes it to the `client_sender`
    pub async fn generate_send_transaction(
        &self,
        rnd: &mut StdRng,
        block_hash: &CryptoHash,
    ) -> bool {
        match self {
            TransactionSender::Native(native_load) => {
                native_load.generate_send_transaction(rnd, block_hash).await
            }
            TransactionSender::Fungible(ft_load) => {
                ft_load.generate_ft_transfer_transaction(rnd, block_hash).await
            }
        }
    }
}

struct NativeTokenTxSender {
    client_sender: ClientSender,
    sender_accounts: Arc<Vec<Account>>,
    receiver_ids: Arc<Vec<AccountId>>,
    choice: TxAccountsSelector,
}

impl NativeTokenTxSender {
    pub fn new(
        client_sender: ClientSender,
        sender_accounts: Arc<Vec<Account>>,
        receiver_ids: Arc<Vec<AccountId>>,
        choice: TxAccountsSelector,
    ) -> Self {
        Self { client_sender, sender_accounts, receiver_ids, choice }
    }

    /// Generates a transaction between two random (but different) accounts and pushes it to the `client_sender`
    pub async fn generate_send_transaction(
        &self,
        rnd: &mut StdRng,
        block_hash: &CryptoHash,
    ) -> bool {
        // each transaction will transfer this amount
        const AMOUNT: Balance = Balance::from_yoctonear(1);

        let Choice { sender_idx, receiver_idx } = self.choice.sample(rnd);

        let sender = &self.sender_accounts[sender_idx];
        let nonce = sender.nonce.fetch_add(1, atomic::Ordering::Relaxed) + 1;
        let sender_id = sender.id.clone();
        let signer = sender.as_signer();

        let receiver_id = match receiver_idx {
            choice::FromSendersOrReceivers::FromSenders(idx) => &self.sender_accounts[idx].id,
            choice::FromSendersOrReceivers::FromReceivers(idx) => &self.receiver_ids[idx],
        };

        let transaction = SignedTransaction::send_money(
            nonce,
            sender_id,
            receiver_id.clone(),
            &signer,
            AMOUNT,
            *block_hash,
        );

        match self
            .client_sender
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
}

struct FungibleTokenTxSender {
    client_sender: ClientSender,
    sender_accounts: Arc<Vec<Account>>,
    receiver_ids: Arc<Vec<AccountId>>,
    account_selector: TxAccountsSelector,
    ft_contract_account_id: AccountId,
}

/// poll for transaction final execution outcome until final status becomes available or timeout
async fn wait_for_transaction_finalization(
    view_client_sender: &ViewClientSender,
    tx_hash: CryptoHash,
    creator_id: AccountId,
    timeout: Duration,
) -> anyhow::Result<bool> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Ok(false);
        }
        if let Some(outcome) = view_client_sender
            .tx_status_request
            .send_async(TxStatusRequest { tx_hash, signer_account_id: creator_id.clone() })
            .await?
        {
            match outcome.status {
                FinalExecutionStatus::SuccessValue(_) => {
                    return Ok(true);
                }
                FinalExecutionStatus::Failure(err) => {
                    return Err(err.into());
                }
                FinalExecutionStatus::NotStarted | FinalExecutionStatus::Started => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Create + transfer + deploy contract in a single transaction (create account + fund + deploy)
async fn ft_contract_account_create(
    client_sender: &ClientSender,
    view_client_sender: &ViewClientSender,
    creator: &Account,
    wasm_path: PathBuf,
) -> anyhow::Result<AccountId> {
    let creator_id = creator.id.clone();
    let creator_signer = creator.as_signer();
    let prefix = creator_id.as_str().split('_').next().unwrap_or(creator_id.as_str());
    let ft_contract_account_id: AccountId =
        format!("{}_ft_contract.{}", prefix, creator_id).parse()?;

    tracing::info!(target: "transaction-generator",
        wasm_path=?&wasm_path,
        owner_id=?&creator_id,
        ft_contract_account_id=?&ft_contract_account_id,
        "creating ft contract account",
    );

    let wasm_code = std::fs::read(&wasm_path).context("reading the wasm code")?;

    let block_hash = TxGenerator::get_latest_block(&view_client_sender).await?.hash;
    let create_nonce = creator.nonce.fetch_add(1, atomic::Ordering::Relaxed) + 1;
    let initial_balance_for_contract: Balance = Balance::from_near(2); // deploying contract needs some extra balance for storage costs

    let create_deploy_actions = vec![
        Action::CreateAccount(CreateAccountAction {}),
        Action::Transfer(TransferAction { deposit: initial_balance_for_contract }),
        Action::DeployContract(DeployContractAction { code: wasm_code.clone() }),
    ];

    let create_deploy_tx = SignedTransaction::from_actions(
        create_nonce,
        creator_id.clone(),             // signer_id
        ft_contract_account_id.clone(), // receiver_id
        &creator_signer,
        create_deploy_actions,
        block_hash,
        0, // priority fee
    );

    // send and wait for the create+deploy to be accepted
    let tx_hash = create_deploy_tx.get_hash();
    match client_sender
        .tx_request_sender
        .send_async(ProcessTxRequest {
            transaction: create_deploy_tx,
            is_forwarded: false,
            check_only: false,
        })
        .await
        .context("send create+deploy tx")?
    {
        ProcessTxResponse::ValidTx => {}
        rsp => {
            anyhow::bail!("create+deploy tx rejected: {:?}", rsp);
        }
    }

    match wait_for_transaction_finalization(
        &view_client_sender,
        tx_hash,
        creator_id.clone(),
        Duration::from_secs(10),
    )
    .await
    {
        Ok(true) => {
            tracing::info!(target: "transaction-generator", "create+deploy tx accepted");
            Ok(ft_contract_account_id)
        }
        Ok(false) => {
            // there are issues with transaction finalization on mocknet
            // Err(anyhow::anyhow!("timeout waiting for create+deploy tx to be finalized")),
            tracing::error!(target: "transaction-generator", "create+deploy tx timeout");
            Ok(ft_contract_account_id)
        }
        Err(err) => Err(err),
    }
}

/// Initialize the FT contract by calling `new` with owner_id and metadata
async fn ft_contract_account_init(
    client_sender: &ClientSender,
    view_client_sender: &ViewClientSender,
    creator: &Account,
    ft_contract_account_id: &AccountId,
) -> anyhow::Result<()> {
    tracing::info!(target: "transaction-generator",
        ft_contract_account_id=?&ft_contract_account_id,
        "initializing ft contract"
    );
    let creator_signer = creator.as_signer();

    let init_args = serde_json::json!({
        "owner_id": creator.id.to_string(),
        "total_supply": "1000000000000000000000000", // 1 million tokens with 18 decimals
        "metadata": {
            "spec": "ft-1.0.0",
            "name": "Benchmark Token",
            "symbol": "BMT",
            "decimals": 18u8
        },
    })
    .to_string()
    .into_bytes();

    let init_nonce = creator.nonce.fetch_add(1, atomic::Ordering::Relaxed) + 1;

    let init_action = Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "new".to_string(),
        args: init_args,
        gas: Gas::from_teragas(200),
        deposit: Balance::from_yoctonear(0),
    }));

    let block_hash = TxGenerator::get_latest_block(&view_client_sender).await?.hash;
    let init_tx = SignedTransaction::from_actions(
        init_nonce,
        creator.id.clone(),
        ft_contract_account_id.clone(),
        &creator_signer,
        vec![init_action],
        block_hash,
        0,
    );

    let init_tx_hash = init_tx.get_hash();
    match client_sender
        .tx_request_sender
        .send_async(ProcessTxRequest {
            transaction: init_tx,
            is_forwarded: false,
            check_only: false,
        })
        .await
        .context("send ft init tx")?
    {
        ProcessTxResponse::ValidTx => {}
        rsp => {
            anyhow::bail!("ft init tx rejected: {:?}", rsp);
        }
    };

    // wait for init to finalize
    match wait_for_transaction_finalization(
        &view_client_sender,
        init_tx_hash,
        creator.id.clone(),
        Duration::from_secs(10),
    )
    .await
    {
        Ok(true) => {
            tracing::info!(target: "transaction-generator", "ft init tx accepted");
            Ok(())
        }
        Ok(false) => {
            // there are issues with transaction finalization on mocknet
            // Err(anyhow::anyhow!("timeout waiting for ft init tx to be finalized")),
            tracing::error!(target: "transaction-generator", "ft init tx timeout");
            Ok(())
        }
        Err(err) => Err(err),
    }
}

/// Register contract-using accounts.
/// Creator pays a storage_deposit on behalf of each account, because we do not have the private keys for the receiver accounts.
async fn ft_contract_register_accounts(
    client_sender: &ClientSender,
    view_client_sender: &ViewClientSender,
    creator: &Account,
    ft_contract_account_id: &AccountId,
    receiver_ids: &Vec<AccountId>,
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();

    tracing::info!(target: "transaction-generator",
        total_receiver_accounts=receiver_ids.len(),
        "registering accounts with the ft contract"
    );

    let mut tx_interval = tokio::time::interval(Duration::from_micros(250));

    for receiver_id in receiver_ids {
        let client_sender = client_sender.clone();
        let view_client = view_client_sender.clone();
        let creator_signer = creator.as_signer().clone();
        let ft_account = ft_contract_account_id.clone();
        const DEPOSIT: Balance = Balance::from_millinear(125);
        let receiver_id = receiver_id.clone();
        let creator_id = creator.id.clone();
        let nonce = creator.nonce.fetch_add(1, atomic::Ordering::Relaxed) + 1;

        // spawn a future per account
        tasks.spawn(async move {
            // prepare and send storage_deposit transaction (creator pays)
            let block_hash = TxGenerator::get_latest_block(&view_client).await?.hash;

            let args = serde_json::to_vec(&serde_json::json!({
                "account_id": receiver_id.to_string(),
                "registration_only": true
            }))
            .unwrap();

            let action = Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "storage_deposit".to_string(),
                args,
                gas: Gas::from_teragas(30),
                deposit: DEPOSIT,
            }));

            let tx = SignedTransaction::from_actions(
                nonce,
                creator_id.clone(),
                ft_account.clone(),
                &creator_signer,
                vec![action],
                block_hash,
                0,
            );

            let tx_hash = tx.get_hash();
            match client_sender
                .tx_request_sender
                .send_async(ProcessTxRequest {
                    transaction: tx,
                    is_forwarded: false,
                    check_only: false,
                })
                .await
                .map_err(|e| anyhow::anyhow!("send storage_deposit tx failed: {:?}", e))?
            {
                ProcessTxResponse::ValidTx => {}
                rsp => {
                    return Err(anyhow::anyhow!("storage_deposit tx rejected: {:?}", rsp));
                }
            }

            // wait for tx finalization
            match wait_for_transaction_finalization(
                &view_client,
                tx_hash,
                creator_id.clone(),
                Duration::from_secs(100),
            )
            .await
            {
                Ok(true) => Ok(()),
                Ok(false) => {
                    // there are issues with transaction finalization on mocknet
                    tracing::error!(target: "transaction-generator",
                        "timeout waiting for storage_deposit tx to be finalized for {}",
                        receiver_id);
                    Ok(())
                    //     Err(anyhow::anyhow!(
                    //     "timeout waiting for storage_deposit tx to be finalized for {}",
                    //     receiver_id
                    // ))
                }
                Err(err) => {
                    Err(anyhow::anyhow!("storage_deposit tx failed for {}: {:?}", receiver_id, err))
                }
            }
        });

        tx_interval.tick().await; // small delay to avoid overloading the tx processing
    }

    // wait for all registration tasks to finish and propagate any error
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                return Err(err);
            }
            Err(join_err) => {
                return Err(join_err.into());
            }
        }
    }

    Ok(())
}

/// Fund contract-using accounts using the
async fn ft_contract_fund_accounts(
    client_sender: &ClientSender,
    view_client_sender: &ViewClientSender,
    creator: &Account,
    ft_contract_account_id: &AccountId,
    receiver_ids: &Vec<AccountId>,
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();

    tracing::info!(target: "transaction-generator",
        total_receiver_accounts=receiver_ids.len(),
        "funding accounts with the ft contract"
    );

    let mut tx_interval = tokio::time::interval(Duration::from_micros(250));

    for receiver_id in receiver_ids {
        if receiver_id == &creator.id {
            continue;
        }
        let client_sender = client_sender.clone();
        let view_client = view_client_sender.clone();
        let creator_signer = creator.as_signer();
        let ft_account = ft_contract_account_id.clone();
        let receiver_id = receiver_id.clone();
        const AMOUNT: u128 = 1_000_000_000_000_000_000; // 1 token with 18 decimals
        let creator_id = creator.id.clone();
        let nonce = creator.nonce.fetch_add(1, atomic::Ordering::Relaxed) + 1;

        // spawn a future per account
        tasks.spawn(async move {
            // prepare and send ft_transfer transaction (creator pays)
            let block_hash = TxGenerator::get_latest_block(&view_client).await?.hash;

            let args = serde_json::to_vec(&serde_json::json!({
                "receiver_id": receiver_id.to_string(),
                "amount": AMOUNT.to_string(),
            }))
            .unwrap();

            let action = Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "ft_transfer".to_string(),
                args,
                gas: Gas::from_teragas(100),
                deposit: Balance::from_yoctonear(1), // 1 yoctoNEAR required for ft_transfer
            }));

            let tx = SignedTransaction::from_actions(
                nonce,
                creator_id.clone(),
                ft_account.clone(),
                &creator_signer,
                vec![action],
                block_hash,
                0,
            );

            let tx_hash = tx.get_hash();
            match client_sender
                .tx_request_sender
                .send_async(ProcessTxRequest {
                    transaction: tx,
                    is_forwarded: false,
                    check_only: false,
                })
                .await
                .map_err(|e| anyhow::anyhow!("send ft_transfer tx failed: {:?}", e))?
            {
                ProcessTxResponse::ValidTx => {}
                rsp => {
                    return Err(anyhow::anyhow!("ft_transfer tx rejected: {:?}", rsp));
                }
            }

            // wait for tx finalization
            match wait_for_transaction_finalization(
                &view_client,
                tx_hash,
                creator_id.clone(),
                Duration::from_secs(10),
            )
            .await
            {
                Ok(true) => Ok(()),
                Ok(false) => {
                    // there are issues with transaction finalization on mocknet
                    tracing::error!(target: "transaction-generator",
                        "timeout waiting for ft_transfer tx to be finalized for {}",
                        receiver_id);
                    Ok(())
                    //     Err(anyhow::anyhow!(
                    //     "timeout waiting for ft_transfer tx to be finalized for {}",
                    //     receiver_id
                    // ))
                }
                Err(err) => {
                    Err(anyhow::anyhow!("ft_transfer tx failed for {}: {:?}", receiver_id, err))
                }
            }
        });

        tx_interval.tick().await; // small delay to avoid overloading the tx processing
    }

    // wait for all funding tasks to finish and propagate any error
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                return Err(err);
            }
            Err(join_err) => {
                return Err(join_err.into());
            }
        }
    }

    Ok(())
}

impl FungibleTokenTxSender {
    pub async fn new(
        client_sender: ClientSender,
        view_client_sender: ViewClientSender,
        sender_accounts: Arc<Vec<Account>>,
        receiver_ids: Arc<Vec<AccountId>>,
        account_selector: TxAccountsSelector,
        wasm_path: PathBuf,
    ) -> anyhow::Result<Self> {
        // pick a creator account (use first sender account)
        let creator = &sender_accounts[0];

        let ft_contract_account_id =
            ft_contract_account_create(&client_sender, &view_client_sender, creator, wasm_path)
                .await?;

        ft_contract_account_init(
            &client_sender,
            &view_client_sender,
            creator,
            &ft_contract_account_id,
        )
        .await?;

        ft_contract_register_accounts(
            &client_sender,
            &view_client_sender,
            creator,
            &ft_contract_account_id,
            &receiver_ids,
        )
        .await?;

        ft_contract_fund_accounts(
            &client_sender,
            &view_client_sender,
            creator,
            &ft_contract_account_id,
            &receiver_ids,
        )
        .await?;

        tracing::info!(target: "transaction-generator",
            ft_contract_account_id=?&ft_contract_account_id,
            "fungible token contract is ready",
        );

        Ok(Self {
            client_sender,
            sender_accounts,
            receiver_ids,
            account_selector,
            ft_contract_account_id,
        })
    }

    /// Generates a transaction between two random (but different) accounts and pushes it to the `client_sender`
    pub async fn generate_ft_transfer_transaction(
        &self,
        rnd: &mut StdRng,
        block_hash: &CryptoHash,
    ) -> bool {
        let Choice { sender_idx, receiver_idx } = self.account_selector.sample(rnd);

        let sender = &self.sender_accounts[sender_idx];
        let nonce = sender.nonce.fetch_add(1, atomic::Ordering::Relaxed) + 1;
        let sender_id = sender.id.clone();
        let signer = sender.as_signer();

        let receiver_id = match receiver_idx {
            choice::FromSendersOrReceivers::FromSenders(idx) => &self.sender_accounts[idx].id,
            choice::FromSendersOrReceivers::FromReceivers(idx) => &self.receiver_ids[idx],
        };

        const TRANSFER_AMOUNT: u128 = 1_000_000;

        let ft_transfer_args = serde_json::json!({
            "receiver_id": receiver_id.to_string(),
            "amount": TRANSFER_AMOUNT.to_string(),
        })
        .to_string()
        .into_bytes();

        let action = Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ft_transfer".to_string(),
            args: ft_transfer_args,
            gas: Gas::from_teragas(100),
            deposit: Balance::from_yoctonear(1), // 1 yoctoNEAR required for ft_transfer
        }));

        let transaction = SignedTransaction::from_actions(
            nonce,
            sender_id,
            self.ft_contract_account_id.clone(),
            &signer,
            vec![action],
            *block_hash,
            0,
        );

        match self
            .client_sender
            .tx_request_sender
            .send_async(ProcessTxRequest { transaction, is_forwarded: false, check_only: false })
            .await
        {
            Ok(ProcessTxResponse::ValidTx) => true,
            Ok(rsp) => {
                tracing::debug!(target: "transaction-generator", ?rsp, "invalid transaction");
                false
            }
            Err(err) => {
                tracing::debug!(target: "transaction-generator", ?err, "error");
                false
            }
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
        std::thread::sleep(std::time::Duration::from_secs(20)); // sleep to let the chain to start/stabilize
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

    async fn get_latest_block(
        view_client_sender: &ViewClientSender,
    ) -> anyhow::Result<BlockHeaderView> {
        match view_client_sender
            .block_request_sender
            .send_async(GetBlock(BlockReference::latest()))
            .await
        {
            Ok(rsp) => {
                let rsp = rsp.context("get latest block")?;
                Ok(rsp.header)
            }
            Err(err) => {
                anyhow::bail!("async send error: {err}");
            }
        }
    }

    fn start_block_updates(
        view_client_sender: ViewClientSender,
    ) -> tokio::sync::watch::Receiver<BlockHeaderView> {
        let (tx_latest_block, rx_latest_block) = tokio::sync::watch::channel(Default::default());

        tokio::spawn(async move {
            let view_client = &view_client_sender;
            let mut block_update_interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                match Self::get_latest_block(view_client).await {
                    Ok(block_header_view) => {
                        tracing::debug!(target: "transaction-generator", height=block_header_view.height, "block update received");
                        let _ = tx_latest_block.send(block_header_view);
                    }
                    Err(err) => {
                        tracing::warn!(target: "transaction-generator", ?err, "block hash update failed");
                    }
                }
                block_update_interval.tick().await;
            }
        });

        rx_latest_block
    }

    fn prepare_accounts(
        accounts_path: &PathBuf,
        receiver_accounts_path: &Option<PathBuf>,
        sender: ViewClientSender,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<(Arc<Vec<Account>>, Arc<Vec<AccountId>>)>>
    {
        let mut accounts =
            account::accounts_from_path(accounts_path).context("accounts from path")?;
        tracing::info!(target: "transaction-generator",
            total_accounts=accounts.len(),
            path=?accounts_path,
            "loaded source accounts"
        );

        if accounts.is_empty() {
            anyhow::bail!("No active accounts available");
        }

        let receiver_ids = if let Some(receiver_accounts_path) = receiver_accounts_path {
            let mut receivers = account::account_ids_from_path(receiver_accounts_path)
                .context("loading receiver account ids")?;
            tracing::info!(target: "transaction-generator",
                total_receiver_accounts = receivers.len(),
                path = ?receiver_accounts_path,
                "loaded receiver accounts"
            );
            if receivers.is_empty() {
                anyhow::bail!("No receiver accounts available");
            }

            // shuffle the receivers to avoid shard hot spots from non-uniform account id distribution
            receivers.shuffle(&mut StdRng::from_entropy());
            receivers
        } else {
            tracing::info!(target: "transaction-generator",
                "no receiver accounts path provided, using source accounts as receivers"
            );

            accounts.iter().map(|acc| acc.id.clone()).collect()
        };

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
            tx.send((Arc::new(accounts), Arc::new(receiver_ids))).unwrap();
        });

        Ok(rx)
    }

    async fn run_load_task(
        mut tx_interval: tokio::time::Interval,
        duration: tokio::time::Duration,
        mut rx_block: tokio::sync::watch::Receiver<BlockHeaderView>,
        stats: Arc<Stats>,
        token_load: Arc<TransactionSender>,
    ) {
        let mut rnd: StdRng = SeedableRng::from_entropy();

        let _ = rx_block
            .wait_for(|BlockHeaderView { hash, .. }| *hash != CryptoHash::default())
            .await
            .is_ok();
        let BlockHeaderView { hash: mut latest_block_hash, .. } = *rx_block.borrow();

        let ld = async {
            loop {
                tokio::select! {
                    _ = rx_block.changed() => {
                        BlockHeaderView{hash: latest_block_hash, .. } = *rx_block.borrow();
                    }
                    _ = tx_interval.tick() => {

                        let ok = token_load.generate_send_transaction(
                            &mut rnd,
                            &latest_block_hash,
                        ).await;

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
        load: Load,
        rx_block: tokio::sync::watch::Receiver<BlockHeaderView>,
        stats: Arc<Stats>,
        tx_generator: Arc<TransactionSender>,
    ) {
        tracing::info!(target: "transaction-generator", ?load, "starting the load");

        let mut tasks = JoinSet::new();

        for _ in 0..TX_GENERATOR_TASK_COUNT {
            tasks.spawn(Self::run_load_task(
                tokio::time::interval(Duration::from_micros({
                    let load_tps = std::cmp::max(load.tps, 1);
                    1_000_000 * TX_GENERATOR_TASK_COUNT / load_tps
                })),
                load.duration,
                rx_block.clone(),
                Arc::clone(&stats),
                Arc::clone(&tx_generator),
            ));
        }
        tasks.join_all().await;
    }

    /// return channel with updates to the total tx rate
    async fn run_controller_loop(
        mut controller: FilteredRateController,
        initial_rate: u64,
        mut rx_block: tokio::sync::watch::Receiver<BlockHeaderView>,
    ) -> tokio::sync::watch::Receiver<tokio::time::Duration> {
        let mut rate = std::cmp::max(initial_rate, 1) as f64;
        let (tx_tps_values, rx_tps_values) = tokio::sync::watch::channel(
            tokio::time::Duration::from_micros(1_000_000 * TX_GENERATOR_TASK_COUNT / rate as u64),
        );
        tracing::debug!(target: "transaction-generator", rate, "starting controller");

        let _ = rx_block
            .wait_for(|BlockHeaderView { hash, .. }| *hash != CryptoHash::default())
            .await
            .is_ok();
        let BlockHeaderView { height, timestamp_nanosec, .. } = *rx_block.borrow();

        // initialize the controller with the current height
        controller.register(height, timestamp_nanosec);

        tokio::spawn(async move {
            let mut last_height_update: Option<(u64, tokio::time::Instant)> = None;
            loop {
                tokio::select! {
                    _ = rx_block.changed() => {
                        let BlockHeaderView{height, timestamp_nanosec, ..} = *rx_block.borrow();
                        let now = tokio::time::Instant::now();

                        if let Some((last_height, last_update_time)) = last_height_update {
                            if height == last_height {
                                // if the time between blocks is too long, skip the controller update and
                                // stop generating transactions for a while
                                // the trigger value (3s) needs to be outside of the normal operation range to
                                // avoid getting into a mode where we spit out the chunk of
                                // transactions at a very high rate and then wait for chain to recover,
                                // and then repeat the cycle.
                                let dt = now.duration_since(last_update_time);
                                if let Some(threshold_ms) = controller.block_pause_threshold_ms {
                                    if Duration::from_millis(threshold_ms) < dt {
                                        tx_tps_values
                                            .send(tokio::time::Duration::from_secs(100))
                                            .unwrap();
                                        tracing::warn!(target: "transaction-generator", dt=?dt, "long delay between blocks, skipping controller update and pausing transaction generation");
                                    }
                                }

                                // no change in height, ignore sample
                                continue;
                            }
                        }
                        last_height_update = Some((height, now));

                        rate += controller.register(height, timestamp_nanosec);
                        if rate < 1.0 || rate > 100000.0 {
                            tracing::warn!(target: "transaction-generator", rate, "controller suggested tps is out of range, clamping");
                            rate = rate.clamp(1.0, 100000.0);
                        }
                        let _span = tracing::debug_span!(
                            "update_tx_generator_rate",
                            %height,
                            %rate,
                            tag_block_production = true)
                        .entered();
                        let micros = ((1_000_000.0 * TX_GENERATOR_TASK_COUNT as f64) / rate) as u64;
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
        rx_block: tokio::sync::watch::Receiver<BlockHeaderView>,
        stats: Arc<Stats>,
        token_load: Arc<TransactionSender>,
    ) {
        tracing::info!(target: "transaction-generator", "starting the controlled loop");

        let rx_intervals =
            Self::run_controller_loop(controller, initial_rate, rx_block.clone()).await;

        for _ in 0..TX_GENERATOR_TASK_COUNT {
            tokio::spawn(Self::controlled_loop_task(
                rx_block.clone(),
                rx_intervals.clone(),
                Arc::clone(&stats),
                Arc::clone(&token_load),
            ));
        }
    }

    async fn controlled_loop_task(
        mut rx_block: tokio::sync::watch::Receiver<BlockHeaderView>,
        mut tx_rates: tokio::sync::watch::Receiver<tokio::time::Duration>,
        stats: Arc<Stats>,
        token_load: Arc<TransactionSender>,
    ) {
        let mut rnd: StdRng = SeedableRng::from_entropy();

        let _ = rx_block
            .wait_for(|BlockHeaderView { hash, .. }| *hash != CryptoHash::default())
            .await
            .is_ok();
        let BlockHeaderView { hash: mut latest_block_hash, .. } = *rx_block.borrow();
        let mut tx_interval = tokio::time::interval(*tx_rates.borrow());

        async {
            loop {
                tokio::select! {
                    _ = rx_block.changed() => {
                        BlockHeaderView{hash: latest_block_hash, .. } = *rx_block.borrow();
                    }
                    _ = tx_rates.changed() => {
                            tx_interval = tokio::time::interval(*tx_rates.borrow());
                        }
                    _ = tx_interval.tick() => {
                        let ok = token_load.generate_send_transaction(
                            &mut rnd,
                            &latest_block_hash,
                        ).await;

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

    /// Starts the native token transactions loop
    fn start_transactions_loop(
        config: &Config,
        client_sender: ClientSender,
        view_client_sender: ViewClientSender,
        stats: Arc<Stats>,
        rx_block: tokio::sync::watch::Receiver<BlockHeaderView>,
    ) -> anyhow::Result<()> {
        let rx_accounts = Self::prepare_accounts(
            &config.accounts_path,
            &config.receiver_accounts_path,
            view_client_sender.clone(),
        )
        .context("prepare accounts")?;

        let controller = if let Some(controller_config) = &config.controller {
            Some(FilteredRateController {
                controller: pid_lite::Controller::new(
                    controller_config.target_block_production_time_s,
                    controller_config.gain_proportional,
                    controller_config.gain_integral,
                    controller_config.gain_derivative,
                ),
                filter: FilterRateWindow::new(controller_config.bps_filter_window_length),
                block_pause_threshold_ms: controller_config.block_pause_threshold_ms,
            })
        } else {
            None
        };

        let (sender_accounts_zipf_skew, receiver_accounts_zipf_skew) =
            (config.sender_accounts_zipf_skew, config.receiver_accounts_zipf_skew);
        let schedule = config.schedule.clone();
        let receivers_from_senders_ratio = config.receivers_from_senders_ratio;
        let config = config.clone();
        tokio::spawn(async move {
            let (source_accounts, receiver_ids) = rx_accounts.await.unwrap();

            let account_selector = TxAccountsSelector::new(
                source_accounts.len(),
                receiver_ids.len(),
                sender_accounts_zipf_skew,
                receiver_accounts_zipf_skew,
                receivers_from_senders_ratio,
            );

            let tx_generator = match config.transaction_type {
                TransactionTypeConfig::NativeToken => {
                    Arc::new(TransactionSender::Native(NativeTokenTxSender::new(
                        client_sender.clone(),
                        Arc::clone(&source_accounts),
                        Arc::clone(&receiver_ids),
                        account_selector,
                    )))
                }
                TransactionTypeConfig::FungibleToken { wasm_path } => {
                    let tx_sender = Arc::new(TransactionSender::Fungible(
                        FungibleTokenTxSender::new(
                            client_sender.clone(),
                            view_client_sender.clone(),
                            Arc::clone(&source_accounts),
                            Arc::clone(&receiver_ids),
                            account_selector,
                            wasm_path,
                        )
                        .await
                        .expect("failed to create fungible token tx sender"),
                    ));

                    tx_sender
                }
            };

            // execute the scheduled loads
            for load in &schedule {
                Self::run_load(
                    load.clone(),
                    rx_block.clone(),
                    Arc::clone(&stats),
                    Arc::clone(&tx_generator),
                )
                .await;
            }

            tracing::info!(target: "transaction-generator",
                "completed running the schedule"
            );

            // if controller is configured, start the controlled loop
            if let Some(controller) = controller {
                Self::run_controlled_loop(
                    controller,
                    schedule.last().unwrap().tps,
                    rx_block.clone(),
                    Arc::clone(&stats),
                    Arc::clone(&tx_generator),
                )
                .await;
            } else {
                tracing::info!(target: "transaction-generator",
                "no 'controller' settings provided, stopping the `neard`"
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
                tracing::info!(target: "transaction-generator", total = ?stats);
                let diff = stats.clone() - stats_prev;
                let rate = tps_filter.register(stats.included_in_chunk);
                tracing::info!(target: "transaction-generator",
                    ?diff,
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
