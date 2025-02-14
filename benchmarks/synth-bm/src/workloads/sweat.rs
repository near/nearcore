use clap::Args;
use near_primitives::types::AccountId;
use std::time::{Duration, Instant};
use tokio::time;

#[derive(Args, Debug)]
pub struct BenchmarkSweatArgs {
    #[arg(long)]
    pub rpc_url: String,

    #[arg(long)]
    pub sweat_contract_id: String,

    #[arg(long)]
    pub oracle_key_path: String,

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
    let oracle_signer = InMemorySigner::from_file(&args.oracle_key_path)?;
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
                (user.account_id.clone(), steps)
            })
            .collect();

        // Create SweatMintBatch transaction
        let tx = Transaction::new(
            oracle_signer.account_id.clone(),
            oracle_signer.public_key.clone(),
            args.sweat_contract_id.parse()?,
            i, // nonce
            block_service.get_block_hash(),
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "record_batch".to_string(),
                args: serde_json::to_vec(&batch_receivers)?,
                gas: 300_000_000_000_000, // 300 TGas
                deposit: 0,
            })],
        );

        let client = client.clone();
        let tx_sender = tx.clone();

        tokio::spawn(async move {
            let result = client.broadcast_tx_async(tx).await;
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
}
