use crate::account::Account;
use crate::block_service::BlockService;
use clap::Args;
use log::info;
use near_crypto::{InMemorySigner, KeyType, SecretKey};
use near_jsonrpc_client::JsonRpcClient;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use rand::seq::SliceRandom;
use rand::Rng;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time;

#[derive(Args, Debug)]
pub struct BenchmarkSweatArgs {
    #[arg(long)]
    pub rpc_url: String,

    #[arg(long)]
    pub sweat_contract_id: String,

    #[arg(long)]
    pub oracle_account_id: String,

    #[arg(long, default_value = "1000")]
    pub num_users: u64,

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

pub async fn benchmark_sweat(args: &BenchmarkSweatArgs) -> anyhow::Result<()> {
    // Generate random key for oracle
    let secret_key = SecretKey::from_random(KeyType::ED25519);
    let oracle_signer =
        InMemorySigner::from_secret_key(args.oracle_account_id.parse()?, secret_key);
    let client = JsonRpcClient::connect(&args.rpc_url);
    let block_service = Arc::new(BlockService::new(client.clone()).await);
    block_service.clone().start().await;

    // Create passive users first (similar to create_sub_accounts)
    let mut users =
        create_sweat_users(&client, &block_service, &oracle_signer, args.num_users).await?;

    let mut interval = time::interval(Duration::from_micros(1_000_000 / args.requests_per_second));
    let timer = Instant::now();
    let mut rng = rand::thread_rng();

    // Channel for handling responses
    let (tx, rx) = mpsc::channel(100);
    let response_handler = spawn_response_handler(rx, args.total_batches);

    for i in 0..args.total_batches {
        interval.tick().await;

        // Generate random batch of users and steps
        let batch_receivers: Vec<(AccountId, u64)> = users
            .choose_multiple(&mut rng, args.batch_size as usize)
            .map(|user| {
                let steps = rng.gen_range(args.steps_min..=args.steps_max);
                (user.id.clone(), steps)
            })
            .collect();

        // Create SweatMintBatch transaction
        let transaction = SignedTransaction::call(
            i,
            oracle_signer.account_id.clone(),
            args.sweat_contract_id.parse()?,
            &oracle_signer.as_signer(),
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

        let client = client.clone();
        let tx_sender = transaction.clone();

        tokio::spawn(async move {
            let result = client.broadcast_tx_async(transaction).await;
            tx_sender.send(result).await.unwrap();
        });
    }

    // Wait for all responses
    response_handler.await?;

    info!("Sent {} batches in {:.2} seconds", args.total_batches, timer.elapsed().as_secs_f64());

    Ok(())
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
