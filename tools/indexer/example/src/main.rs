use actix;

use anyhow::Result;
use clap::Parser;
use tokio::sync::mpsc;
use tracing::info;

use configs::{Opts, SubCommand};
use near_indexer;

mod configs;

async fn listen_blocks(mut stream: mpsc::Receiver<near_indexer::StreamerMessage>) {
    while let Some(streamer_message) = stream.recv().await {
        // TODO: handle data as you need
        // Example of `StreamerMessage` with all the data (the data is synthetic)
        //
        // Note that `outcomes` for a given transaction won't be included into the same block.
        // Execution outcomes are included into the blocks after the transaction or receipt
        // are recorded on a chain; in most cases, it is the next block after the one that has
        // the transaction or receipt.
        //
        // StreamerMessage {
        //     block: BlockView {
        //         author: "test.near",
        //         header: BlockHeaderView {
        //             height: 63596,
        //             epoch_id: `Bk7pvZWUTfHRRZtfgTDjnQ6y5cV8yG2h3orCqJvUbiym`,
        //             next_epoch_id: `3JuBZ4Gz5Eauf7PzQegfqSEDyvws3eKJYPbfGHAYmeR5`,
        //             hash: `5X37niQWWcihDGQjsvDMHYKLCurNJyQLxCeLgneDb8mk`,
        //             prev_hash: `2vJNJca72pBiq2eETq2xvuoc6caKDaUkdRgtdefyutbA`,
        //             prev_state_root: `GkdxSBf4Kfq8V16N4Kqn3YdcThG1f5KG1KLBmXpMzP1k`,
        //             chunk_receipts_root: `9ETNjrt6MkwTgSVMMbpukfxRshSD1avBUUa4R4NuqwHv`,
        //             chunk_headers_root: `C7dVr9KdXYKt31yF2BkeAu115fpo79zYTqeU3FzqbFak`,
        //             chunk_tx_root: `7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t`,
        //             outcome_root: `7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t`,
        //             chunks_included: 1,
        //             challenges_root: `11111111111111111111111111111111`,
        //             timestamp: 1618558205803345000,
        //             timestamp_nanosec: 1618558205803345000,
        //             random_value: `3cAa93XmoLaKAJQgWz3K7SiKwnA3uaxi8MGgLM78HTNS`,
        //             validator_proposals: [],
        //             chunk_mask: [
        //                 true,
        //             ],
        //             gas_price: 1000000000,
        //             rent_paid: 0,
        //             validator_reward: 0,
        //             total_supply: 2050206401403887985811862247311434,
        //             challenges_result: [],
        //             last_final_block: `DCkMmXYHqibzcMjgFjRXJP7eckAMLrA4ijggSApMNwKu`,
        //             last_ds_final_block: `2vJNJca72pBiq2eETq2xvuoc6caKDaUkdRgtdefyutbA`,
        //             next_bp_hash: `4DJWnxRbUhRrsXK6EBkx4nFeXHKgJWqteDnJ7Hv4MZ6M`,
        //             block_merkle_root: `Bvn5K89fJ3uPNsj3324Ls9TXAGUVteHPpfKwKqL1La6W`,
        //             approvals: [
        //                 Some(
        //                     ed25519:F816hgJod7nPfD2qQz5yhaKDMn1JXmvzj2iXegsJpsmPNnYYZpKYJXgyuVTVJ4TKQbcJ2Q3USCGZF6fX2TcwBBv,
        //                 ),
        //             ],
        //             signature: ed25519:239NbE4BuJaxneQA3AEsPrsGY7v3wBgaezbgg56HER69zPrBoc3a4fbyVWPXeoKE3LvgGma1g6pSHk9QHkmETCZY,
        //             latest_protocol_version: 43,
        //         },
        //         chunks: [
        //             ChunkHeaderView {
        //                 chunk_hash: `2M2oeNFBbUUnHfkU1UuBr8EKBCLMH9xr2vfsGRpyiBmA`,
        //                 prev_block_hash: `2vJNJca72pBiq2eETq2xvuoc6caKDaUkdRgtdefyutbA`,
        //                 outcome_root: `11111111111111111111111111111111`,
        //                 prev_state_root: `3gZPPijaumgMRCvMuuZZM1Ab2LoHTSfYigMKwLqZ67m6`,
        //                 encoded_merkle_root: `79Bt7ivt9Qhp3c6dJYnueaTyPVweYxZRpQHASRRAiyuy`,
        //                 encoded_length: 8,
        //                 height_created: 63596,
        //                 height_included: 63596,
        //                 shard_id: 0,
        //                 gas_used: 0,
        //                 gas_limit: 1000000000000000,
        //                 rent_paid: 0,
        //                 validator_reward: 0,
        //                 balance_burnt: 0,
        //                 outgoing_receipts_root: `H4Rd6SGeEBTbxkitsCdzfu9xL9HtZ2eHoPCQXUeZ6bW4`,
        //                 tx_root: `11111111111111111111111111111111`,
        //                 validator_proposals: [],
        //                 signature: ed25519:2vWNayBzEoW5DRc7gTdhxdLbkKuK6ACQ78p3JGpKSAZZCarnLroeoALPAFwpr9ZNPxBqdVYh9QLBe7WHZebsS17Z,
        //             },
        //         ],
        //     },
        //     shards: [
        //         IndexerShard {
        //             shard_id: 0,
        //             chunk: Some(
        //                 IndexerChunkView {
        //                     author: "test.near",
        //                     header: ChunkHeaderView {
        //                         chunk_hash: `2M2oeNFBbUUnHfkU1UuBr8EKBCLMH9xr2vfsGRpyiBmA`,
        //                         prev_block_hash: `2vJNJca72pBiq2eETq2xvuoc6caKDaUkdRgtdefyutbA`,
        //                         outcome_root: `11111111111111111111111111111111`,
        //                         prev_state_root: `3gZPPijaumgMRCvMuuZZM1Ab2LoHTSfYigMKwLqZ67m6`,
        //                         encoded_merkle_root: `79Bt7ivt9Qhp3c6dJYnueaTyPVweYxZRpQHASRRAiyuy`,
        //                         encoded_length: 8,
        //                         height_created: 63596,
        //                         height_included: 0,
        //                         shard_id: 0,
        //                         gas_used: 0,
        //                         gas_limit: 1000000000000000,
        //                         rent_paid: 0,
        //                         validator_reward: 0,
        //                         balance_burnt: 0,
        //                         outgoing_receipts_root: `H4Rd6SGeEBTbxkitsCdzfu9xL9HtZ2eHoPCQXUeZ6bW4`,
        //                         tx_root: `11111111111111111111111111111111`,
        //                         validator_proposals: [],
        //                         signature: ed25519:2vWNayBzEoW5DRc7gTdhxdLbkKuK6ACQ78p3JGpKSAZZCarnLroeoALPAFwpr9ZNPxBqdVYh9QLBe7WHZebsS17Z,
        //                     },
        //                     transactions: [
        //                         IndexerTransactionWithOutcome {
        //                             transaction: SignedTransactionView {
        //                                 signer_id: "test.near",
        //                                 public_key: ed25519:8NA7mh6TAWzy2qz68bHp62QHTEQ6nJLfiYeKDRwEbU3X,
        //                                 nonce: 1,
        //                                 receiver_id: "some.test.near",
        //                                 actions: [
        //                                     CreateAccount,
        //                                     Transfer {
        //                                         deposit: 40000000000000000000000000,
        //                                     },
        //                                     AddKey {
        //                                         public_key: ed25519:2syGhqwJ8ba2nUGmP9tkZn9m1DYZPYYobpufiERVnug8,
        //                                         access_key: AccessKeyView {
        //                                             nonce: 0,
        //                                             permission: FullAccess,
        //                                         },
        //                                     },
        //                                 ],
        //                                 signature: ed25519:Qniuu7exnr6xbe6gKafV5vDhuwM1jt9Bn7sCTF6cHfPpYWVJ4Q6kq8RAxKSeLoxbCreVp1XzMMJmXt8YcUqmMYw,
        //                                 hash: `8dNv9S8rAFwso9fLwfDQXmw5yv5zscDjQpta96pMF6Bi`,
        //                             },
        //                             outcome: IndexerExecutionOutcomeWithReceipt {
        //                                 execution_outcome: ExecutionOutcomeWithIdView {
        //                                     proof: [],
        //                                     block_hash: `G9v6Fsv94xaa7BRY2N5PFF5PJwT7ec6DPzQK73Yf3CZ6`,
        //                                     id: `8dNv9S8rAFwso9fLwfDQXmw5yv5zscDjQpta96pMF6Bi`,
        //                                     outcome: ExecutionOutcomeView {
        //                                         logs: [],
        //                                         receipt_ids: [
        //                                         `CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz`,
        //                                         ],
        //                                         gas_burnt: 424555062500,
        //                                         tokens_burnt: 424555062500000000000,
        //                                         executor_id: "test.near",
        //                                         status: SuccessReceiptId(CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz),
        //                                     },
        //                                 },
        //                                 receipt: None,
        //                             },
        //                         },
        //                     ],
        //                     receipts: [
        //                         ReceiptView {
        //                             predecessor_id: "test.near",
        //                             receiver_id: "some.test.near",
        //                             receipt_id: `CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz`,
        //                             receipt: Action {
        //                                 signer_id: "test.near",
        //                                 signer_public_key: ed25519:8NA7mh6TAWzy2qz68bHp62QHTEQ6nJLfiYeKDRwEbU3X,
        //                                 gas_price: 1030000000,
        //                                 output_data_receivers: [],
        //                                 input_data_ids: [],
        //                                 actions: [
        //                                     CreateAccount,
        //                                     Transfer {
        //                                         deposit: 40000000000000000000000000,
        //                                     },
        //                                     AddKey {
        //                                         public_key: ed25519:2syGhqwJ8ba2nUGmP9tkZn9m1DYZPYYobpufiERVnug8,
        //                                         access_key: AccessKeyView {
        //                                             nonce: 0,
        //                                             permission: FullAccess,
        //                                         },
        //                                     },
        //                                 ],
        //                             },
        //                         },
        //                     ],
        //                 },
        //             ),
        //             receipt_execution_outcomes: [
        //                 IndexerExecutionOutcomeWithReceipt {
        //                     execution_outcome: ExecutionOutcomeWithIdView {
        //                         proof: [],
        //                         block_hash: `BXPB6DQGmBrjARvcgYwS8qKLkyto6dk9NfawGSmfjE9Q`,
        //                         id: `CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz`,
        //                         outcome: ExecutionOutcomeView {
        //                             logs: [],
        //                             receipt_ids: [
        //                             `8vJ1QWM4pffRDnW3c5CxFFV5cMx8wiqxsAqmZTitHvfh`,
        //                             ],
        //                             gas_burnt: 424555062500,
        //                             tokens_burnt: 424555062500000000000,
        //                             executor_id: "some.test.near",
        //                             status: SuccessValue(``),
        //                         },
        //                     },
        //                     receipt: ReceiptView {
        //                         predecessor_id: "test.near",
        //                         receiver_id: "some.test.near",
        //                         receipt_id: `CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz`,
        //                         receipt: Action {
        //                             signer_id: "test.near",
        //                             signer_public_key: ed25519:8NA7mh6TAWzy2qz68bHp62QHTEQ6nJLfiYeKDRwEbU3X,
        //                             gas_price: 1030000000,
        //                             output_data_receivers: [],
        //                             input_data_ids: [],
        //                             actions: [
        //                                 CreateAccount,
        //                                 Transfer {
        //                                     deposit: 40000000000000000000000000,
        //                                 },
        //                                 AddKey {
        //                                     public_key: ed25519:2syGhqwJ8ba2nUGmP9tkZn9m1DYZPYYobpufiERVnug8,
        //                                     access_key: AccessKeyView {
        //                                         nonce: 0,
        //                                         permission: FullAccess,
        //                                     },
        //                                 },
        //                             ],
        //                         },
        //                     },
        //                 },
        //             ],
        //         },
        //     ],
        //     state_changes: [
        //         StateChangeWithCauseView {
        //             cause: ValidatorAccountsUpdate,
        //             value: AccountUpdate {
        //                 account_id: "test.near",
        //                 account: AccountView {
        //                     amount: 1000000000000000000000000000000000,
        //                     locked: 50000000000000000000000000000000,
        //                     code_hash: `11111111111111111111111111111111`,
        //                     storage_usage: 182,
        //                     storage_paid_at: 0,
        //                 },
        //             },
        //         },
        //     ],
        // }
        info!(
            target: "indexer_example",
            "#{} {} Shards: {}, Transactions: {}, Receipts: {}, ExecutionOutcomes: {}",
            streamer_message.block.header.height,
            streamer_message.block.header.hash,
            streamer_message.shards.len(),
            streamer_message.shards.iter().map(|shard| if let Some(chunk) = &shard.chunk { chunk.transactions.len() } else { 0usize }).sum::<usize>(),
            streamer_message.shards.iter().map(|shard| if let Some(chunk) = &shard.chunk { chunk.receipts.len() } else { 0usize }).sum::<usize>(),
            streamer_message.shards.iter().map(|shard| shard.receipt_execution_outcomes.len()).sum::<usize>(),
        );
    }
}

fn main() -> Result<()> {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();
    let env_filter = near_o11y::tracing_subscriber::EnvFilter::new(
        "nearcore=info,indexer_example=info,tokio_reactor=info,near=info,\
         stats=info,telemetry=info,indexer=info,near-performance-metrics=info",
    );
    let _subscriber = near_o11y::default_subscriber(env_filter, &Default::default()).global();
    let opts: Opts = Opts::parse();

    let home_dir = opts.home_dir.unwrap_or(near_indexer::get_default_home());

    match opts.subcmd {
        SubCommand::Run => {
            let indexer_config = near_indexer::IndexerConfig {
                home_dir,
                sync_mode: near_indexer::SyncModeEnum::FromInterruption,
                await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
                validate_genesis: true,
            };
            let system = actix::System::new();
            system.block_on(async move {
                let indexer = near_indexer::Indexer::new(indexer_config).expect("Indexer::new()");
                let stream = indexer.streamer();
                actix::spawn(listen_blocks(stream));
            });
            system.run()?;
        }
        SubCommand::Init(config) => near_indexer::indexer_init_configs(&home_dir, config.into())?,
    }
    Ok(())
}
