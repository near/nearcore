use std::fs::File;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::account::{accounts_from_dir, update_account_nonces};
use crate::block_service::BlockService;
use crate::metrics::TransactionStatisticsService;
use crate::rpc::{ResponseCheckSeverity, RpcResponseHandler};
use clap::Args;
use near_jsonrpc_client::JsonRpcClient;
use near_jsonrpc_client::methods::send_tx::RpcSendTransactionRequest;
use near_primitives::transaction::SignedTransaction;
use near_primitives::views::TxExecutionStatus;
use rand::distributions::{Distribution, Uniform};
use tokio::sync::mpsc;
use tokio::time;
use tracing::{error, info};

#[derive(Args, Debug)]
pub struct BenchmarkArgs {
    /// TODO try to have single arg for all commands
    #[arg(long)]
    pub rpc_url: String,
    #[arg(long)]
    pub user_data_dir: PathBuf,
    #[arg(long)]
    pub num_transfers: u64,
    /// Acts as upper bound on the number of concurrently open RPC requests.
    #[arg(long)]
    pub channel_buffer_size: usize,

    /// Upper bound on request rate to the network for transaction (and other auxiliary) calls. The actual rate may be lower in case of congestions.
    #[arg(long)]
    pub requests_per_second: u64,

    #[arg(long)]
    pub amount: u128,

    /// If set, this flag updates the nonce values from the network.
    #[arg(default_value_t = false, long)]
    pub read_nonces_from_network: bool,

    /// Enable measuring and reports of transaction statistics.
    #[arg(default_value_t = false, long)]
    pub transaction_statistics_service: bool,

    /// File containing a list of RPC URLs to use for sending transactions
    #[arg(long)]
    pub rpc_urls_file: Option<PathBuf>,
}

/// Read RPC URLs from a file, one URL per line
fn read_rpc_urls(file_path: &PathBuf) -> io::Result<Vec<String>> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    let mut urls = Vec::new();

    for line in reader.lines() {
        let url = line?;
        let trimmed = url.trim();
        if !trimmed.is_empty() && !trimmed.starts_with('#') {
            // Check if the line is just an IP address without protocol and port
            if trimmed.chars().all(|c| c.is_digit(10) || c == '.') {
                urls.push(format!("http://{}:3030", trimmed));
            } else if trimmed.contains('|') {
                // Handle complex format like "mocknet-mainnet-138038233-hoptnet-573c|us-central1-a|35.238.229.51"
                // Extract the IP address which is the last part after the last pipe
                let parts: Vec<&str> = trimmed.split('|').collect();
                if let Some(ip) = parts.last() {
                    if !ip.is_empty() {
                        urls.push(format!("http://{}:3030", ip.trim()));
                    }
                }
            } else {
                urls.push(trimmed.to_string());
            }
        }
    }

    Ok(urls)
}

pub async fn benchmark(args: &BenchmarkArgs) -> anyhow::Result<()> {
    let mut accounts = accounts_from_dir(&args.user_data_dir)?;
    assert!(accounts.len() >= 2);

    let mut interval = time::interval(Duration::from_micros(1_000_000 / args.requests_per_second));
    let timer = Instant::now();

    let between = Uniform::from(0..accounts.len());
    let mut rng = rand::thread_rng();

    // Create the primary RPC client
    let client = JsonRpcClient::connect(&args.rpc_url);

    // Read additional RPC URLs if provided
    let rpc_clients = if let Some(ref rpc_urls_file) = args.rpc_urls_file {
        match read_rpc_urls(rpc_urls_file) {
            Ok(urls) => {
                info!(
                    "Loaded {} additional RPC URLs from {}: {:?}",
                    urls.len(),
                    rpc_urls_file.display(),
                    urls
                );
                urls.into_iter().map(|url| JsonRpcClient::connect(&url)).collect::<Vec<_>>()
            }
            Err(e) => {
                error!("Failed to read RPC URLs from {}: {}", rpc_urls_file.display(), e);
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    // If we have additional RPC clients, use them
    let use_random_validators = !rpc_clients.is_empty();
    if use_random_validators {
        info!("Using {} validators for random transaction distribution", rpc_clients.len());
    } else {
        info!("Using single RPC endpoint: {}", args.rpc_url);
    }

    let block_service = Arc::new(BlockService::new(client.clone()).await);

    if args.read_nonces_from_network {
        accounts = update_account_nonces(
            client.clone(),
            accounts,
            args.requests_per_second,
            Some(&args.user_data_dir),
        )
        .await?;
    }

    block_service.clone().start().await;

    // Before a request is made, a permit to send into the channel is awaited. Hence buffer size
    // limits the number of outstanding requests. This helps to avoid congestion.
    // TODO find reasonable buffer size.
    let (channel_tx, channel_rx) = mpsc::channel(args.channel_buffer_size);

    let wait_until = TxExecutionStatus::None;
    let wait_until_channel = wait_until.clone();
    let num_expected_responses = args.num_transfers;
    let response_handler_task = tokio::task::spawn(async move {
        let mut rpc_response_handler = RpcResponseHandler::new(
            channel_rx,
            wait_until_channel,
            ResponseCheckSeverity::Log,
            num_expected_responses,
        );
        rpc_response_handler.handle_all_responses().await;
    });

    let transaction_stat_handle = if args.transaction_statistics_service {
        let service =
            TransactionStatisticsService::new(args.rpc_url.clone(), Duration::from_secs(1));
        Some(tokio::spawn(async move { service.start().await }))
    } else {
        None
    };

    // Create a uniform distribution for selecting random validators
    let validator_between =
        if use_random_validators { Some(Uniform::from(0..rpc_clients.len())) } else { None };

    for i in 0..args.num_transfers {
        let idx_sender = usize::try_from(i % u64::try_from(accounts.len()).unwrap()).unwrap();
        let idx_receiver = {
            let mut idx = between.sample(&mut rng);
            if idx == idx_sender {
                // Avoid creating a transaction where an account sends NEAR to itself.
                // Relies on accounts.len() > 2 (asserted above).
                if idx < accounts.len() - 1 {
                    idx += 1;
                } else {
                    idx = 0
                }
            }
            idx
        };

        let sender = &accounts[idx_sender];
        let receiver = &accounts[idx_receiver];
        let transaction = SignedTransaction::send_money(
            sender.nonce + 1,
            sender.id.clone(),
            receiver.id.clone(),
            &sender.as_signer(),
            args.amount,
            block_service.get_block_hash(),
        );
        let request = RpcSendTransactionRequest {
            signed_transaction: transaction,
            wait_until: wait_until.clone(),
        };

        interval.tick().await;

        // Select a random validator client if available
        let selected_client = if use_random_validators && validator_between.is_some() {
            let idx = validator_between.as_ref().unwrap().sample(&mut rng);
            rpc_clients[idx].clone()
        } else {
            client.clone()
        };

        // Await permit before sending the request to make channel buffer size a limit for the
        // number of outstanding requests.
        let permit = channel_tx.clone().reserve_owned().await.unwrap();
        tokio::spawn(async move {
            let res = selected_client.call(request).await;
            permit.send(res);
        });
        if i > 0 && i % 10000 == 0 {
            info!("num txs sent: {}", i);
        }

        let sender = accounts.get_mut(idx_sender).unwrap();
        sender.nonce += 1;
    }

    info!("Sent {} txs in {:.2} seconds", args.num_transfers, timer.elapsed().as_secs_f64());

    for account in accounts.iter() {
        account.write_to_dir(&args.user_data_dir)?;
    }

    // Ensure all rpc responses are handled.
    response_handler_task.await.expect("response handler tasks should succeed");

    if let Some(handle) = transaction_stat_handle {
        // Ensure transaction stats are collected until all transactions are processed.
        if let Err(err) = handle.await {
            error!("Transaction statistics service failed with: {err}");
        }
    }

    Ok(())
}
