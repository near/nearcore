use crate::account::{
    accounts_from_dir, create_sub_accounts, update_account_nonces, Account, CreateSubAccountsArgs,
};
use crate::block_service::BlockService;
use crate::rpc::{check_response, check_tx_response, ResponseCheckSeverity, RpcResponseHandler};
use clap::{Args, Subcommand};
use log::info;
use near_crypto::{InMemorySigner, KeyType, SecretKey};
use near_jsonrpc_client::methods::send_tx::RpcSendTransactionRequest;
use near_jsonrpc_client::JsonRpcClient;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::views::TxExecutionStatus;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use serde::Serialize;
use serde_json::json;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time;

#[derive(Subcommand, Debug)]
pub enum SweatCommand {
    /// Creates oracle accounts
    CreateContracts(CreateContractsArgs),
    /// Creates passive users for each oracle
    CreateUsers(CreateUsersArgs),
    /// Runs the benchmark with batch recording
    RunBenchmark(RunBenchmarkArgs),
}

#[derive(Args, Debug)]
pub struct CreateContractsArgs {
    #[arg(long)]
    pub rpc_url: String,

    #[arg(long)]
    pub num_oracles: u64,

    #[arg(long)]
    pub oracle_deposit: u128,

    #[arg(long)]
    pub user_data_dir: PathBuf,

    #[arg(long)]
    pub signer_key_path: PathBuf,

    #[arg(long)]
    pub wasm_file: PathBuf,

    #[arg(long, default_value = "1")]
    pub nonce: u64,
}

#[derive(Args, Debug)]
pub struct CreateUsersArgs {
    #[arg(long)]
    pub rpc_url: String,

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

#[derive(Debug)]
enum AccountState {
    ToCreate(Account),
    Inflight(Account, u64), // Account and nonce used
    ToRefresh(Account),
    Done,
}

pub async fn benchmark_sweat(args: &BenchmarkSweatArgs) -> anyhow::Result<()> {
    match &args.command {
        SweatCommand::CreateContracts(create_args) => create_contracts(create_args).await,
        SweatCommand::CreateUsers(users_args) => create_users(users_args).await,
        SweatCommand::RunBenchmark(benchmark_args) => run_benchmark(benchmark_args).await,
    }
}

pub async fn create_contracts(args: &CreateContractsArgs) -> anyhow::Result<()> {
    info!("Starting contract creation with {} oracles", args.num_oracles);

    // First create the oracle accounts using existing functionality
    let create_args = CreateSubAccountsArgs {
        rpc_url: args.rpc_url.clone(),
        num_sub_accounts: args.num_oracles,
        deposit: args.oracle_deposit,
        sub_account_prefixes: Some(
            ["2", "c", "h", "m", "x"].into_iter().map(|s| s.to_string()).collect(),
        ),
        user_data_dir: args.user_data_dir.clone(),
        channel_buffer_size: 1000,
        requests_per_second: 10,
        nonce: args.nonce,
        signer_key_path: args.signer_key_path.clone(),
    };

    create_sub_accounts(&create_args).await?;

    let client = JsonRpcClient::connect(&args.rpc_url);
    let block_service = Arc::new(BlockService::new(client.clone()).await);
    block_service.clone().start().await;

    info!("Created oracle accounts, loading from directory");
    let mut oracles = accounts_from_dir(&args.user_data_dir)?;
    info!("Loaded {} oracle accounts", oracles.len());
    let master_signer = InMemorySigner::from_file(&args.signer_key_path)?;

    // Update nonces from network before deployment
    oracles = update_account_nonces(
        client.clone(),
        oracles,
        10, // Lower RPS for deployment
        Some(&args.user_data_dir),
    )
    .await?;

    info!("Reading WASM bytes from {}", args.wasm_file.display());
    let wasm_bytes = std::fs::read(&args.wasm_file)?;
    info!("Read {} bytes of WASM code", wasm_bytes.len());

    // Deploy contract to each oracle account
    for (i, oracle) in oracles.iter().enumerate() {
        info!("Deploying contract to oracle {} ({}/{})", oracle.id, i + 1, oracles.len());

        // Deploy contract
        let deploy_tx = SignedTransaction::deploy_contract(
            oracle.nonce + 1,
            &oracle.id,
            wasm_bytes.clone(),
            &oracle.as_signer(),
            block_service.get_block_hash(),
        );

        info!("Sending deploy transaction for {}", oracle.id);
        let deploy_request = RpcSendTransactionRequest {
            signed_transaction: deploy_tx,
            wait_until: TxExecutionStatus::ExecutedOptimistic,
        };

        match client.call(deploy_request).await {
            Ok(outcome) => {
                info!("Deploy result for {}: {:?}", oracle.id, outcome);
                check_tx_response(
                    outcome,
                    TxExecutionStatus::ExecutedOptimistic,
                    ResponseCheckSeverity::Assert,
                );
            }
            Err(e) => {
                log::error!("Deploy failed for {}: {}", oracle.id, e);
                return Err(anyhow::anyhow!("Deploy failed for {}: {}", oracle.id, e));
            }
        }

        info!("Initializing contract for {}", oracle.id);
        // Initialize contract with proper metadata
        let init_args = json!({
            "owner_id": master_signer.account_id,
            "total_supply": format!("{}", 10u128.pow(33)),
            "metadata": {
                "spec": "ft-1.0.0",
                "name": format!("SWEAT_{}", i),
                "symbol": format!("SWEAT_{}", i),
                "decimals": 18,
                "icon": "",
                "reference": "",
                "reference_hash": ""
            }
        });
        info!("Init args for {}: {}", oracle.id, init_args);

        let init_tx = SignedTransaction::call(
            oracle.nonce + 2,
            oracle.id.clone(),
            oracle.id.clone(),
            &oracle.as_signer(),
            0,
            "new".to_string(),
            serde_json::to_vec(&init_args)?,
            TOTAL_GAS,
            block_service.get_block_hash(),
        );

        let init_request = RpcSendTransactionRequest {
            signed_transaction: init_tx,
            wait_until: TxExecutionStatus::ExecutedOptimistic,
        };

        match client.call(init_request).await {
            Ok(outcome) => {
                info!("Init result for {}: {:?}", oracle.id, outcome);
                check_tx_response(
                    outcome,
                    TxExecutionStatus::ExecutedOptimistic,
                    ResponseCheckSeverity::Assert,
                );
            }
            Err(e) => {
                log::error!("Init failed for {}: {}", oracle.id, e);
                return Err(anyhow::anyhow!("Init failed for {}: {}", oracle.id, e));
            }
        }

        // Register the oracle account
        let register_oracle_args = json!({
            "account_id": oracle.id
        });

        let register_oracle_tx = SignedTransaction::call(
            oracle.nonce + 3,
            oracle.id.clone(),
            oracle.id.clone(),
            &oracle.as_signer(),
            0,
            "add_oracle".to_string(),
            serde_json::to_vec(&register_oracle_args)?,
            TOTAL_GAS,
            block_service.get_block_hash(),
        );

        let register_oracle_request = RpcSendTransactionRequest {
            signed_transaction: register_oracle_tx,
            wait_until: TxExecutionStatus::ExecutedOptimistic,
        };

        match client.call(register_oracle_request).await {
            Ok(outcome) => {
                info!("Oracle registration result for {}: {:?}", oracle.id, outcome);
                check_tx_response(
                    outcome,
                    TxExecutionStatus::ExecutedOptimistic,
                    ResponseCheckSeverity::Assert,
                );
            }
            Err(e) => {
                log::error!("Oracle registration failed for {}: {}", oracle.id, e);
                return Err(anyhow::anyhow!("Oracle registration failed for {}: {}", oracle.id, e));
            }
        }
    }

    info!("Updating nonces after deployment");
    update_account_nonces(client.clone(), oracles, 10, Some(&args.user_data_dir)).await?;

    info!("Contract creation completed successfully");
    Ok(())
}

pub async fn create_users(args: &CreateUsersArgs) -> anyhow::Result<()> {
    let client = JsonRpcClient::connect(&args.rpc_url);
    let block_service = Arc::new(BlockService::new(client.clone()).await);
    block_service.clone().start().await;

    // Load oracle accounts
    let mut oracles = accounts_from_dir(&args.oracle_data_dir)?;
    oracles = update_account_nonces(client.clone(), oracles, 10, Some(&args.user_data_dir)).await?;

    // Create users for all oracles in parallel
    let mut handles = Vec::new();
    for oracle in oracles {
        let client = client.clone();
        let block_service = block_service.clone();
        let user_data_dir = args.user_data_dir.clone();

        let handle = tokio::spawn({
            let users_per_oracle = args.users_per_oracle;
            let deposit = args.deposit;

            async move {
                info!("Creating users for oracle {}", oracle.id);
                let users = create_passive_users(
                    &client,
                    &block_service,
                    &oracle,
                    oracle.id.as_str(),
                    users_per_oracle,
                    1000,
                    deposit,
                    &user_data_dir,
                )
                .await?;

                // Save user accounts
                for user in users {
                    user.write_to_dir(&user_data_dir)?;
                }
                Ok::<_, anyhow::Error>(())
            }
        });
        handles.push(handle);
    }

    // Wait for all oracles to finish creating their users
    info!("Waiting for all oracle user creation tasks to complete...");
    for (i, handle) in handles.into_iter().enumerate() {
        info!("Waiting for oracle task {}", i + 1);
        match handle.await {
            Ok(result) => match result {
                Ok(_) => info!("Oracle task {} completed successfully", i + 1),
                Err(e) => {
                    log::error!("Oracle task {} failed with error: {}", i + 1, e);
                    return Err(e);
                }
            },
            Err(e) => {
                log::error!("Oracle task {} join error: {}", i + 1, e);
                return Err(anyhow::anyhow!("Join error: {}", e));
            }
        };
    }

    info!("Loading oracle accounts for final nonce update");
    let oracles = accounts_from_dir(&args.oracle_data_dir)?;
    info!("Updating final nonces for {} oracle accounts", oracles.len());
    update_account_nonces(client.clone(), oracles, 10, Some(&args.oracle_data_dir)).await?;
    info!("User creation completed successfully");

    Ok(())
}

async fn create_passive_users(
    client: &JsonRpcClient,
    block_service: &Arc<BlockService>,
    oracle: &Account,
    sweat_contract_id: &str,
    num_users: u64,
    _channel_size: usize,
    deposit: u128,
    user_data_dir: &PathBuf,
) -> anyhow::Result<Vec<Account>> {
    info!("Starting to create {} users for oracle {}", num_users, oracle.id);
    let mut users = Vec::with_capacity(num_users as usize);

    // Track states for each account
    let mut account_states = Vec::with_capacity(num_users as usize);
    let mut current_nonce = oracle.nonce;
    let mut retry_accounts = HashSet::new();

    // Initialize accounts and their states
    for i in 0..num_users {
        let user_key = SecretKey::from_random(KeyType::ED25519);
        let user_id: AccountId = format!("user_{}.{}", i, oracle.id).parse().map_err(|e| {
            log::error!("Failed to parse account ID for user_{}.{}: {}", i, oracle.id, e);
            e
        })?;

        let user = Account::new(user_id.clone(), user_key.clone(), 0);

        // Check if account exists
        let account_exists = client
            .call(near_jsonrpc_client::methods::query::RpcQueryRequest {
                block_reference: near_primitives::types::Finality::Final.into(),
                request: near_primitives::views::QueryRequest::ViewAccount {
                    account_id: user_id.clone(),
                },
            })
            .await
            .is_ok();

        if account_exists {
            continue;
        }

        account_states.push((user.clone(), AccountState::ToCreate(user)));
        expected_responses += 2; // Both create_account and storage_deposit needed
    }

    let wait_until = TxExecutionStatus::ExecutedOptimistic;

    let mut interval = time::interval(Duration::from_micros(1_000_000 / 100)); // 100 TPS
    const BATCH_SIZE: usize = 20;

    // Process accounts through state machine in batches
    while !account_states.iter().all(|(_, state)| matches!(state, AccountState::Done)) {
        interval.tick().await;

        // Process ToCreate states in batches
        let to_create: Vec<_> = account_states
            .iter_mut()
            .filter(|(_, state)| matches!(state, AccountState::ToCreate(_)))
            .take(BATCH_SIZE)
            .collect();

        if !to_create.is_empty() {
            info!("Processing creation batch of {} accounts", to_create.len());
        }

        for (_, state) in to_create {
            if let AccountState::ToCreate(account) = state {
                if retry_accounts.contains(&account.id) {
                    continue;
                }

                info!("Creating account {}", account.id);
                let create_account_tx = SignedTransaction::create_account(
                    current_nonce + 1,
                    oracle.id.clone(),
                    account.id.clone(),
                    deposit,
                    account.public_key.clone(),
                    &oracle.as_signer(),
                    block_service.get_block_hash(),
                );

                let create_request = RpcSendTransactionRequest {
                    signed_transaction: create_account_tx,
                    wait_until: wait_until.clone(),
                };

                match client.call(create_request).await {
                    Ok(outcome) => {
                        let response_success = check_response(outcome);
                        match response_success {
                            Ok(true) => {
                                info!("Successfully created account {}", account.id);
                                *state = AccountState::Inflight(account.clone(), current_nonce + 1);
                                current_nonce += 1;
                            }
                            Ok(false) => {
                                log::error!(
                                    "Failed to create account {}: unknown error",
                                    account.id
                                );
                                retry_accounts.insert(account.id.clone());
                            }
                            Err(e) => {
                                log::error!("Failed to create account {}: {}", account.id, e);
                                retry_accounts.insert(account.id.clone());
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to create account {}: {}", account.id, e);
                        retry_accounts.insert(account.id.clone());
                    }
                }
            }
        }

        // Process Inflight states in batches
        let inflight: Vec<_> = account_states
            .iter_mut()
            .filter(|(_, state)| matches!(state, AccountState::Inflight(_, _)))
            .take(BATCH_SIZE)
            .collect();

        if !inflight.is_empty() {
            info!("Processing storage deposit batch of {} accounts", inflight.len());
        }

        for (_, state) in inflight {
            if let AccountState::Inflight(account, _) = state {
                let register_tx = SignedTransaction::call(
                    current_nonce + 1,
                    oracle.id.clone(),
                    sweat_contract_id.parse()?,
                    &oracle.as_signer(),
                    1_250_000_000_000_000_000_000,
                    "storage_deposit".to_string(),
                    serde_json::to_vec(&json!({ "account_id": account.id }))?,
                    300_000_000_000_000,
                    block_service.get_block_hash(),
                );

                let register_request = RpcSendTransactionRequest {
                    signed_transaction: register_tx,
                    wait_until: wait_until.clone(),
                };

                match client.call(register_request).await {
                    Ok(outcome) => {
                        let response_success = check_response(outcome);
                        match response_success {
                            Ok(true) => {
                                *state = AccountState::ToRefresh(account.clone());
                                current_nonce += 1;
                            }
                            Ok(false) => {
                                log::error!(
                                    "Failed to register account {} in contract: unknown error",
                                    account.id
                                );
                                retry_accounts.insert(account.id.clone());
                            }
                            Err(e) => {
                                log::error!(
                                    "Failed to register account {} in contract: {}",
                                    account.id,
                                    e
                                );
                                retry_accounts.insert(account.id.clone());
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to register account {} in contract: {}", account.id, e);
                        retry_accounts.insert(account.id.clone());
                        *state = AccountState::ToCreate(account.clone());
                    }
                }
            }
        }

        // Process ToRefresh states in batches
        let to_refresh: Vec<_> = account_states
            .iter_mut()
            .filter(|(_, state)| matches!(state, AccountState::ToRefresh(_)))
            .take(BATCH_SIZE)
            .collect();

        for (_, state) in to_refresh {
            if let AccountState::ToRefresh(account) = state {
                account.write_to_dir(user_data_dir)?;
                users.push(account.clone());
                *state = AccountState::Done;
            }
        }

        // Clear retry accounts for next iteration
        retry_accounts.clear();
    }

    Ok(users)
}

async fn run_benchmark(args: &RunBenchmarkArgs) -> anyhow::Result<()> {
    // Load oracle accounts
    let mut oracles = accounts_from_dir(&args.oracle_data_dir)?;
    assert!(!oracles.is_empty(), "at least one oracle required");

    let client = JsonRpcClient::connect(&args.rpc_url);

    // Update oracle nonces from network before starting
    oracles = update_account_nonces(
        client.clone(),
        oracles,
        10, // Lower RPS for nonce updates
        Some(&args.oracle_data_dir),
    )
    .await?;

    // Load user accounts that will receive steps
    let users = accounts_from_dir(&args.user_data_dir)?;
    assert!(users.len() >= args.batch_size as usize, "need at least as many users as batch_size");

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
        info!("Initiating batch {} with oracle {}", i + 1, oracle.id);

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
        info!("Created batch {} with {} chunks", i + 1, chunks.len());

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
            oracle.nonce + 1 + (i / oracles.len() as u64),
            oracle.id.clone(),
            oracle.id.clone(),
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
