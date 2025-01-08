use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::account::accounts_from_dir;
use crate::block_service::BlockService;
use crate::rpc::{ResponseCheckSeverity, RpcResponseHandler};
use clap::Args;
use log::info;
use near_jsonrpc_client::methods::send_tx::RpcSendTransactionRequest;
use near_jsonrpc_client::JsonRpcClient;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::views::TxExecutionStatus;
use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time;

#[derive(Args, Debug)]
pub struct BenchmarkMpcSignArgs {
    #[arg(long)]
    pub rpc_url: String,
    #[arg(long)]
    pub user_data_dir: PathBuf,
    #[arg(long)]
    pub transactions_per_second: u64,
    #[arg(long)]
    pub num_transfers: u64,
    #[arg(long)]
    pub receiver_id: AccountId,
    /// The `key_version` passed as argument to `sign`.
    #[arg(long)]
    pub key_version: u32,
    #[arg(long)]
    pub channel_buffer_size: usize,
    #[arg(long)]
    pub gas: u64,
    #[arg(long)]
    pub deposit: u128,
}

pub async fn benchmark_mpc_sign(args: &BenchmarkMpcSignArgs) -> anyhow::Result<()> {
    let mut accounts = accounts_from_dir(&args.user_data_dir)?;
    assert!(
        accounts.len() > 0,
        "at least one account required in {:?} to send transactions",
        args.user_data_dir
    );

    // Pick interval to achieve desired TPS.
    let mut interval =
        time::interval(Duration::from_micros(1_000_000 / args.transactions_per_second));

    let client = JsonRpcClient::connect(&args.rpc_url);
    let block_service = Arc::new(BlockService::new(client.clone()).await);
    block_service.clone().start().await;
    let mut rng = thread_rng();

    // Before a request is made, a permit to send into the channel is awaited. Hence buffer size
    // limits the number of outstanding requests. This helps to avoid congestion.
    let (channel_tx, channel_rx) = mpsc::channel(args.channel_buffer_size);

    // Current network capacity for MPC `sign` calls is known to be around 100 TPS. At that
    // rate, neither the network nor the RPC should be a bottleneck.
    // Hence `wait_until: EXECUTED_OPTIMISTIC` as it provides most insights.
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

    info!("Setup complete, starting to send transactions");
    let timer = Instant::now();
    for i in 0..args.num_transfers {
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
        args.num_transfers,
        timer.elapsed().as_secs_f64()
    );

    info!("Awaiting RPC responses");
    response_handler_task.await.expect("response handler tasks should succeed");
    info!("Received all RPC responses after {:.2} seconds", timer.elapsed().as_secs_f64());

    info!("Writing updated nonces to {:?}", args.user_data_dir);
    for account in accounts.iter() {
        account.write_to_dir(&args.user_data_dir)?;
    }

    Ok(())
}

fn new_random_mpc_sign_args(rng: &mut ThreadRng, key_version: u32) -> serde_json::Value {
    let mut payload: [u8; 32] = [0; 32];
    rng.fill(&mut payload);

    json!({
        "payload": payload,
        "path": Alphanumeric.sample_string(rng, 16),
        "key_version": key_version,
    })
}
