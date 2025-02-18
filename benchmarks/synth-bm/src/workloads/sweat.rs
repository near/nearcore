use crate::account::{accounts_from_dir, create_sub_accounts, Account, CreateSubAccountsArgs};
use crate::block_service::BlockService;
use crate::rpc::{ResponseCheckSeverity, RpcResponseHandler};
use clap::{Args, Subcommand};
use log::info;
use near_crypto::{KeyType, SecretKey};
use near_jsonrpc_client::methods::send_tx::RpcSendTransactionRequest;
use near_jsonrpc_client::JsonRpcClient;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::views::TxExecutionStatus;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use serde::Serialize;
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time;

#[derive(Subcommand, Debug)]
pub enum SweatCommand {
    /// Creates oracle accounts
    CreateOracles(CreateOraclesArgs),
    /// Creates passive users for each oracle
    CreateUsers(CreateUsersArgs),
    /// Runs the benchmark with batch recording
    RunBenchmark(RunBenchmarkArgs),
}

#[derive(Args, Debug)]
pub struct CreateOraclesArgs {
    #[arg(long)]
    pub rpc_url: String,

    #[arg(long)]
    pub num_oracles: u64,

    #[arg(long)]
    pub oracle_deposit: u128,

    #[arg(long)]
    pub sweat_contract_id: String,

    #[arg(long)]
    pub user_data_dir: PathBuf,

    #[arg(long)]
    pub signer_key_path: PathBuf,
}

#[derive(Args, Debug)]
pub struct CreateUsersArgs {
    #[arg(long)]
    pub rpc_url: String,

    #[arg(long)]
    pub sweat_contract_id: String,

    #[arg(long)]
    pub oracle_data_dir: PathBuf,

    #[arg(long, default_value = "1000")]
    pub users_per_oracle: u64,

    #[arg(long)]
    pub user_data_dir: PathBuf,

    #[arg(long)]
    pub deposit: u128,
}

#[derive(Args, Debug)]
pub struct RunBenchmarkArgs {
    #[arg(long)]
    pub rpc_url: String,

    #[arg(long)]
    pub sweat_contract_id: String,

    #[arg(long)]
    pub oracle_data_dir: PathBuf,

    #[arg(long)]
    pub user_data_dir: PathBuf,

    #[arg(long, default_value = "750")]
    pub batch_size: u64,

    #[arg(long, default_value = "1000")]
    pub steps_min: u64,

    #[arg(long, default_value = "3000")]
    pub steps_max: u64,

    #[arg(long)]
    pub requests_per_second: u64,

    #[arg(long)]
    pub total_batches: u64,
}

#[derive(Args, Debug)]
pub struct BenchmarkSweatArgs {
    #[command(subcommand)]
    pub command: SweatCommand,
}

const TOTAL_GAS: u64 = 300_000_000_000_000; // 300 TGAS
const CHUNK_SIZE: usize = 150;

#[derive(Serialize)]
struct StepsBatch {
    steps_batch: Vec<(AccountId, u64)>,
}

pub async fn benchmark_sweat(args: &BenchmarkSweatArgs) -> anyhow::Result<()> {
    match &args.command {
        SweatCommand::CreateOracles(create_args) => create_oracles(create_args).await,
        SweatCommand::CreateUsers(users_args) => create_users(users_args).await,
        SweatCommand::RunBenchmark(benchmark_args) => run_benchmark(benchmark_args).await,
    }
}

pub async fn create_oracles(args: &CreateOraclesArgs) -> anyhow::Result<()> {
    // Use existing create_sub_accounts with oracle prefix
    let create_args = CreateSubAccountsArgs {
        rpc_url: args.rpc_url.clone(),
        num_sub_accounts: args.num_oracles,
        deposit: args.oracle_deposit,
        sub_account_prefixes: Some(
            ["2", "c", "h", "m", "x"].into_iter().map(|s| s.to_string()).collect(),
        ),
        user_data_dir: args.user_data_dir.clone(),
        channel_buffer_size: 1000,
        requests_per_second: 10, // we don't have many oracles anyway
        nonce: 1,
        signer_key_path: args.signer_key_path.clone(),
    };

    create_sub_accounts(&create_args).await
}

pub async fn create_users(args: &CreateUsersArgs) -> anyhow::Result<()> {
    let client = JsonRpcClient::connect(&args.rpc_url);
    let block_service = Arc::new(BlockService::new(client.clone()).await);
    block_service.clone().start().await;

    // Load oracle accounts
    let oracles = accounts_from_dir(&args.oracle_data_dir)?;

    for oracle in oracles {
        info!("Creating users for oracle {}", oracle.id);
        let users = create_passive_users(
            &client,
            &block_service,
            &oracle,
            &args.sweat_contract_id,
            args.users_per_oracle,
            1000,
            args.deposit,
        )
        .await?;

        // Save user accounts
        for user in users {
            user.write_to_dir(&args.user_data_dir)?;
        }
    }

    Ok(())
}

async fn create_passive_users(
    client: &JsonRpcClient,
    block_service: &Arc<BlockService>,
    oracle: &Account,
    sweat_contract_id: &str,
    num_users: u64,
    channel_size: usize,
    deposit: u128,
) -> anyhow::Result<Vec<Account>> {
    let mut users = Vec::with_capacity(num_users as usize);
    let (tx, rx) = mpsc::channel(channel_size);

    // Similar to create_sub_accounts but with FT registration
    let mut interval = time::interval(Duration::from_micros(1_000_000 / 100)); // 100 TPS
    let wait_until = TxExecutionStatus::ExecutedOptimistic;
    let wait_until_channel = wait_until.clone();
    let num_expected_responses = num_users * 2; // Two transactions per user
    let response_handler_task = tokio::task::spawn(async move {
        let mut rpc_response_handler = RpcResponseHandler::new(
            rx,
            wait_until_channel,
            ResponseCheckSeverity::Assert,
            num_expected_responses,
        );
        rpc_response_handler.handle_all_responses().await;
    });

    for i in 0..num_users {
        interval.tick().await;

        let user_key = SecretKey::from_random(KeyType::ED25519);
        let user_id: AccountId = format!("user_{}.{}", i, oracle.id).parse()?;

        // Create user account
        let create_account_tx = SignedTransaction::create_account(
            oracle.nonce + i,
            oracle.id.clone(),
            user_id.clone(),
            deposit,
            user_key.public_key(),
            &oracle.as_signer(),
            block_service.get_block_hash(),
        );
        let create_account_request = RpcSendTransactionRequest {
            signed_transaction: create_account_tx,
            wait_until: wait_until.clone(),
        };

        // Register user in Sweat contract
        let register_tx = SignedTransaction::call(
            oracle.nonce + i + num_users,
            oracle.id.clone(),
            sweat_contract_id.parse()?,
            &oracle.as_signer(),
            0,
            "storage_deposit".to_string(),
            serde_json::to_vec(&json!({
                "account_id": user_id
            }))?,
            300_000_000_000_000,
            block_service.get_block_hash(),
        );
        let register_request = RpcSendTransactionRequest {
            signed_transaction: register_tx,
            wait_until: wait_until.clone(),
        };

        let client1 = client.clone();
        let client2 = client.clone();
        let tx1 = tx.clone();
        let tx2 = tx.clone();

        tokio::spawn(async move {
            let res = client1.call(create_account_request).await;
            tx1.send(res).await.unwrap();
        });

        tokio::spawn(async move {
            let res = client2.call(register_request).await;
            tx2.send(res).await.unwrap();
        });

        users.push(Account::new(user_id, user_key, 0));
    }

    // Wait for all transactions
    response_handler_task.await.expect("response handler tasks should succeed");

    Ok(users)
}

async fn run_benchmark(args: &RunBenchmarkArgs) -> anyhow::Result<()> {
    // Load oracle accounts
    let oracles = accounts_from_dir(&args.oracle_data_dir)?;
    assert!(!oracles.is_empty(), "at least one oracle required");

    // Load user accounts that will receive steps
    let users = accounts_from_dir(&args.user_data_dir)?;
    assert!(users.len() >= args.batch_size as usize, "need at least as many users as batch_size");

    let client = JsonRpcClient::connect(&args.rpc_url);
    let block_service = Arc::new(BlockService::new(client.clone()).await);
    block_service.clone().start().await;

    let mut interval = time::interval(Duration::from_micros(1_000_000 / args.requests_per_second));
    let timer = Instant::now();
    let mut rng = thread_rng();

    // Channel for handling responses
    let (channel_tx, channel_rx) = mpsc::channel(1000);

    let wait_until = TxExecutionStatus::ExecutedOptimistic;
    let wait_until_channel = wait_until.clone();
    let num_expected_responses = args.total_batches;
    let response_handler_task = tokio::task::spawn(async move {
        let mut rpc_response_handler = RpcResponseHandler::new(
            channel_rx,
            wait_until_channel,
            ResponseCheckSeverity::Log,
            num_expected_responses,
        );
        rpc_response_handler.handle_all_responses().await;
    });

    // Round-robin through oracles
    for i in 0..args.total_batches {
        interval.tick().await;

        let oracle = &oracles[i as usize % oracles.len()];

        // Generate random batch of users and steps
        let batch_receivers: Vec<(AccountId, u64)> = users
            .choose_multiple(&mut rng, args.batch_size as usize)
            .map(|user| {
                let steps = rng.gen_range(args.steps_min..=args.steps_max);
                (user.id.clone(), steps)
            })
            .collect();

        // Split into chunks to match Python implementation's chunking
        let chunks: Vec<Vec<(AccountId, u64)>> =
            batch_receivers.chunks(CHUNK_SIZE).map(|c| c.to_vec()).collect();

        // Create multiple function calls in a single transaction
        let actions: Vec<near_primitives::transaction::Action> = chunks
            .iter()
            .map(|chunk| -> Result<near_primitives::transaction::Action, serde_json::Error> {
                let steps_batch = StepsBatch { steps_batch: chunk.to_vec() };
                let args = serde_json::to_vec(&steps_batch)?;
                Ok(near_primitives::transaction::Action::FunctionCall(Box::new(
                    near_primitives::transaction::FunctionCallAction {
                        method_name: "record_batch".to_string(),
                        args,
                        gas: TOTAL_GAS / chunks.len() as u64, // Split gas equally between calls
                        deposit: 0,
                    },
                )))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Create single transaction with multiple actions
        let transaction = SignedTransaction::from_actions(
            oracle.nonce + (i / oracles.len() as u64),
            oracle.id.clone(),
            args.sweat_contract_id.parse()?,
            &oracle.as_signer(),
            actions,
            block_service.get_block_hash(),
            0,
        );

        let request = RpcSendTransactionRequest {
            signed_transaction: transaction,
            wait_until: wait_until.clone(),
        };

        let permit = channel_tx.clone().reserve_owned().await.unwrap();
        let client = client.clone();
        tokio::spawn(async move {
            let res = client.call(request).await;
            permit.send(res);
        });

        if i > 0 && i % 200 == 0 {
            info!("sent {} batches in {:.2} seconds", i, timer.elapsed().as_secs_f64());
        }
    }

    info!(
        "Done sending {} batches in {:.2} seconds",
        args.total_batches,
        timer.elapsed().as_secs_f64()
    );

    // Wait for all responses
    response_handler_task.await.expect("response handler tasks should succeed");
    info!("Received all RPC responses after {:.2} seconds", timer.elapsed().as_secs_f64());

    Ok(())
}
