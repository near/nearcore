use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::account::{Account, accounts_from_dir, update_account_nonces};
use crate::block_service::{BlockService, read_rpc_urls};
use crate::rpc::{ResponseCheckSeverity, RpcResponseHandler};
use clap::Args;
use near_jsonrpc_client::JsonRpcClient;
use near_jsonrpc_client::methods::send_tx::RpcSendTransactionRequest;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::views::TxExecutionStatus;
use rand::distributions::{Alphanumeric, DistString, Distribution, Uniform};
use rand::rngs::ThreadRng;
use rand::{Rng, thread_rng};
use serde::Serialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{error, info};

#[derive(Args, Debug)]
pub struct BenchmarkMpcSignArgs {
    /// RPC node to which transactions are sent.
    #[arg(long)]
    pub rpc_url: String,
    /// Directory containing data of the users that sign transactions.
    #[arg(long)]
    pub user_data_dir: PathBuf,
    /// The number of transactions to send per second. May be lower when reaching hardware limits or
    /// network congestion.
    #[arg(long)]
    pub requests_per_second: u64,
    /// The total number of transactions to send.
    #[arg(long)]
    pub num_transactions: u64,
    /// The id of the account to which the MPC contract has been deployed.
    #[arg(long)]
    pub receiver_id: AccountId,
    /// The `key_version` passed as argument to `sign`.
    #[arg(long)]
    pub key_version: u32,
    /// Acts as upper bound on the number of concurrently open RPC requests.
    #[arg(long)]
    pub channel_buffer_size: usize,
    /// The gas (in yoctoNEAR) attached to each `sign` function call transaction.
    #[arg(long)]
    pub gas: u64,
    /// The deposit (in yoctoNEAR) attached to each `sign` function call transaction.
    #[arg(long)]
    pub deposit: u128,

    /// If set, this flag updates the nonce values from the network.
    #[arg(default_value_t = false, long)]
    pub read_nonces_from_network: bool,

    /// If set, this flag reads additional RPC URLs from a file.
    #[arg(long)]
    pub rpc_urls_file: Option<PathBuf>,
}

pub async fn benchmark_mpc_sign_impl(
    args: &BenchmarkMpcSignArgs,
    client: JsonRpcClient,
    accounts: &mut Vec<Account>,
) -> anyhow::Result<()> {
    // Pick interval to achieve desired TPS.
    let mut interval = time::interval(Duration::from_micros(1_000_000 / args.requests_per_second));

    let block_service = Arc::new(BlockService::new(client.clone()).await);
    block_service.clone().start().await;
    let mut rng = thread_rng();

    // Before a request is made, a permit to send into the channel is awaited. Hence buffer size
    // limits the number of outstanding requests. This helps to avoid congestion.
    let (channel_tx, channel_rx) = mpsc::channel(args.channel_buffer_size);

    // Current network capacity for MPC `sign` calls is known to be around 100 TPS. At that
    // rate, neither the network nor the RPC should be a bottleneck.
    // Hence `wait_until: EXECUTED_OPTIMISTIC` as it provides most insights.
    let wait_until = TxExecutionStatus::ExecutedOptimistic;
    let wait_until_channel = wait_until.clone();
    let num_expected_responses = args.num_transactions;
    let response_handler_task = tokio::task::spawn(async move {
        let mut rpc_response_handler = RpcResponseHandler::new(
            channel_rx,
            wait_until_channel,
            ResponseCheckSeverity::Log,
            num_expected_responses,
        );
        rpc_response_handler.handle_all_responses().await;
    });

    info!("Setup complete, starting to send transactions");
    let timer = Instant::now();
    for i in 0..args.num_transactions {
        let sender_idx = usize::try_from(i).unwrap() % accounts.len();
        let sender = &accounts[sender_idx];

        let transaction = SignedTransaction::call(
            sender.nonce,
            sender.id.clone(),
            args.receiver_id.clone(),
            &sender.as_signer(),
            args.deposit,
            "sign".to_string(),
            new_random_mpc_sign_args(&mut rng, args.key_version).to_string().into_bytes(),
            args.gas,
            block_service.get_block_hash(),
        );
        let request = RpcSendTransactionRequest {
            signed_transaction: transaction,
            wait_until: wait_until.clone(),
        };

        // Let time pass to meet TPS target.
        interval.tick().await;

        // Await permit before sending the request to make channel buffer size a limit for the
        // number of outstanding requests.
        let permit = channel_tx.clone().reserve_owned().await.unwrap();
        let client = client.clone();
        tokio::spawn(async move {
            let res = client.call(request).await;
            permit.send(res);
        });

        if i > 0 && i % 200 == 0 {
            info!("sent {i} transactions in {:.2} seconds", timer.elapsed().as_secs_f64());
        }

        let sender = accounts.get_mut(sender_idx).unwrap();
        sender.nonce += 1;
    }

    info!(
        "Done sending {} transactions in {:.2} seconds",
        args.num_transactions,
        timer.elapsed().as_secs_f64()
    );

    info!("Awaiting RPC responses");
    response_handler_task.await.expect("response handler tasks should succeed");
    info!("Received all RPC responses after {:.2} seconds", timer.elapsed().as_secs_f64());

    Ok(())
}

pub async fn benchmark_mpc_sign(args: &BenchmarkMpcSignArgs) -> anyhow::Result<()> {
    let mut accounts: Vec<_> = accounts_from_dir(&args.user_data_dir)?.into_iter().collect();
    assert!(
        !accounts.is_empty(),
        "at least one account required in {:?} to send transactions",
        args.user_data_dir
    );

    // Create the primary RPC client
    let client = JsonRpcClient::connect(&args.rpc_url);

    // Initialize a vector containing all RPC clients, starting with the primary one
    let mut all_rpc_clients = vec![client.clone()];

    // Read additional RPC URLs if provided
    if let Some(ref rpc_urls_file) = args.rpc_urls_file {
        match read_rpc_urls(rpc_urls_file) {
            Ok(urls) => {
                info!(
                    "Loaded {} additional RPC URLs from {}: {:?}",
                    urls.len(),
                    rpc_urls_file.display(),
                    urls
                );
                all_rpc_clients.extend(urls.into_iter().map(|url| JsonRpcClient::connect(&url)));
                info!(
                    "Using {} validators for random transaction distribution",
                    all_rpc_clients.len()
                );
            }
            Err(e) => {
                error!("Failed to read RPC URLs from {}: {}", rpc_urls_file.display(), e);
                // Continue with just the primary client
            }
        }
    } else {
        info!("Using single RPC endpoint: {}", args.rpc_url);
    }

    if args.read_nonces_from_network {
        accounts = update_account_nonces(
            all_rpc_clients.clone(),
            accounts.to_vec(),
            args.requests_per_second,
            Some(&args.user_data_dir),
        )
        .await?;
    }

    // Pick a random validator for block service
    let mut rng = thread_rng();
    let validator_between = Uniform::from(0..all_rpc_clients.len());
    let idx = validator_between.sample(&mut rng);
    let random_client = all_rpc_clients[idx].clone();

    let result = benchmark_mpc_sign_impl(args, random_client, &mut accounts).await;

    info!("Writing updated nonces to {:?}", args.user_data_dir);
    for account in accounts.iter() {
        account.write_to_dir(&args.user_data_dir)?;
    }

    result
}

/// Constructs the parameters according to
/// https://github.com/near/mpc/blob/79ec50759146221e7ad8bb04520f13333b75ca07/chain-signatures/contract/src/lib.rs#L127
fn new_random_mpc_sign_args(rng: &mut ThreadRng, key_version: u32) -> serde_json::Value {
    #[derive(Serialize)]
    struct SignRequest {
        pub payload: [u8; 32],
        pub path: String,
        pub key_version: u32,
    }

    let mut payload: [u8; 32] = [0; 32];
    rng.fill(&mut payload);
    let path = Alphanumeric.sample_string(rng, 16);

    json!({
        "request": SignRequest { payload, path, key_version },
    })
}
