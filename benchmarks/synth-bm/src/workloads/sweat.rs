use crate::account::Account;
use crate::block_service::BlockService;
use clap::{Args, Subcommand};
use log::info;
use near_crypto::{InMemorySigner, KeyType, SecretKey};
use near_jsonrpc_client::JsonRpcClient;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use rand::seq::SliceRandom;
use rand::Rng;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time;

#[derive(Subcommand)]
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

pub async fn benchmark_sweat(args: &BenchmarkSweatArgs) -> anyhow::Result<()> {
    match &args.command {
        SweatCommand::CreateOracles(create_args) => create_oracles(create_args).await,
        SweatCommand::CreateUsers(users_args) => create_users(users_args).await,
        SweatCommand::RunBenchmark(benchmark_args) => run_benchmark(benchmark_args).await,
    }
}

async fn create_sweat_users(
    client: &JsonRpcClient,
    block_service: &Arc<BlockService>,
    oracle: &InMemorySigner,
    num_users: u64,
) -> anyhow::Result<Vec<Account>> {
    // Implementation similar to create_sub_accounts but with
    // Sweat contract registration
    // ...
    Ok(vec![])
}

pub async fn create_oracles(args: &CreateOraclesArgs) -> anyhow::Result<()> {
    // Use existing create_sub_accounts with oracle prefix
    let create_args = CreateSubAccountsArgs {
        rpc_url: args.rpc_url.clone(),
        num_sub_accounts: args.num_oracles,
        deposit: args.oracle_deposit,
        sub_account_prefixes: Some(vec!["oracle".to_string()]),
        user_data_dir: args.user_data_dir.clone(),
        // ... other fields
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
) -> anyhow::Result<Vec<Account>> {
    let mut users = Vec::with_capacity(num_users as usize);
    let (tx, rx) = mpsc::channel(channel_size);

    // Similar to create_sub_accounts but with FT registration
    let mut interval = time::interval(Duration::from_micros(1_000_000 / 100)); // 100 TPS

    for i in 0..num_users {
        interval.tick().await;

        let user_key = SecretKey::from_random(KeyType::ED25519);
        let user_id = format!("user_{}.{}", i, oracle.id).parse()?;

        // Create user account
        let create_account_tx = SignedTransaction::create_account(
            oracle.nonce + i,
            oracle.id.clone(),
            user_id.clone(),
            INIT_BALANCE,
            user_key.public_key(),
            block_service.get_block_hash(),
            &oracle.as_signer(),
        );

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

        let client1 = client.clone();
        let client2 = client.clone();
        let tx1 = tx.clone();
        let tx2 = tx.clone();

        tokio::spawn(async move {
            let res = client1.broadcast_tx_async(create_account_tx).await;
            tx1.send(res).await.unwrap();
        });

        tokio::spawn(async move {
            let res = client2.broadcast_tx_async(register_tx).await;
            tx2.send(res).await.unwrap();
        });

        users.push(Account::new(user_id, user_key, 0));
    }

    // Wait for all transactions
    let response_handler = spawn_response_handler(rx, num_users * 2);
    response_handler.await?;

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
    let response_handler_task = tokio::task::spawn(async move {
        let mut rpc_response_handler = RpcResponseHandler::new(
            channel_rx,
            wait_until_channel,
            ResponseCheckSeverity::Log,
            args.total_batches,
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

        // Create record_batch transaction
        let transaction = SignedTransaction::call(
            oracle.nonce + (i / oracles.len() as u64),
            oracle.id.clone(),
            args.sweat_contract_id.parse()?,
            &oracle.as_signer(),
            0,
            "record_batch".to_string(),
            serde_json::to_vec(&batch_receivers)?,
            300_000_000_000_000,
            block_service.get_block_hash(),
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
    response_handler_task.await.expect("response handler task should succeed");
    info!("Received all RPC responses after {:.2} seconds", timer.elapsed().as_secs_f64());

    Ok(())
}
