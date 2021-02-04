use actix;

use clap::Clap;
use tokio::sync::mpsc;
use tracing::info;

use configs::{init_logging, Opts, SubCommand};
use near_indexer;

mod configs;

async fn listen_blocks(mut stream: mpsc::Receiver<near_indexer::StreamerMessage>) {
    while let Some(streamer_message) = stream.recv().await {
        // TODO: handle data as you need
        // Example of `block` with all the data
        //
        // Note that `outcomes` for a given transaction won't be included into the same block.
        // Execution outcomes are included into the blocks after the transaction or receipt
        // are recorded on a chain; in most cases, it is the next block after the one that has
        // the transaction or receipt.
        //
        // BlockResponse {
        //     block: BlockView {
        //         author: "test.near",
        //         header: BlockHeaderView {
        //             height: 426,
        //             epoch_id: `11111111111111111111111111111111`,
        //             next_epoch_id: `9dH4uF6d3bQtXa7v8CyLPhPXGBUikCCdWB26JWVFRsBY`,
        //             hash: `99fpSqxeiMe8iTfh72reaLvx4R16kPYAr1TYuxS3zqkB`,
        //             prev_hash: `8try83LRTx76jfbmPv8SxJchgW8XeQH3gt2piKT6Ykpj`,
        //             prev_state_root: `DjCJizTpo86umJHv6urA37RJqXkztV1VXpRQysTaKpZA`,
        //             chunk_receipts_root: `GcXz5GG5oTYvdYK7jzqUt2gGQtw36gE2Hu2MxXxuBh7`,
        //             chunk_headers_root: `GibE7k6ychYbjJHaURHV369WSGR2jjfgCCF3xL4Qgi9Y`,
        //             chunk_tx_root: `7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t`,
        //             outcome_root: `JfGt9hY94ftG2sswDQduL5U3GVb7drdX14oJZvGpuoV`,
        //             chunks_included: 1,
        //             challenges_root: `11111111111111111111111111111111`,
        //             timestamp: 1594306903797198000,
        //             timestamp_nanosec: 1594306903797198000,
        //             random_value: `EhqGHhUP8W4ULtHF6N7pjB6hSJRE7noUKgmQvy6kfZTZ`,
        //             validator_proposals: [],
        //             chunk_mask: [
        //                 true,
        //             ],
        //             gas_price: 5000,
        //             rent_paid: 0,
        //             validator_reward: 0,
        //             total_supply: 2049999999999999997877224687500000,
        //             challenges_result: [],
        //             last_final_block: `2eiUwiZxqo5fRSKPqJ5Nq4oSYRQZE5gRuA9p7TcjFRSJ`,
        //             last_ds_final_block: `8try83LRTx76jfbmPv8SxJchgW8XeQH3gt2piKT6Ykpj`,
        //             next_bp_hash: `BsoSx2Ea1Vcomv3Ygw95E8ZeNq5QYrZLdcsYdbs3SWpC`,
        //             block_merkle_root: `EUmovh7K8yRgboG6vXxCP2dN3ChMLByX846MZG1y6xwG`,
        //             approvals: [
        //                 Some(
        //                     ed25519:42QiF81ZvRx5PfFzZxKgYC3yBfFJ4nSJCSwyiiZoztf34NXx8ottoz9jj3urtuwCHV8u6gJ9GHxUhNqbB1KpTeCH,
        //                 ),
        //             ],
        //             signature: ed25519:27iPMfiR3fh5nC4wmWA3XXjXvA6yffNcnF7PMeMKRnLDpeHytV6GPtzrNNyDsuLVEWjFJvQLfp1kPw8S16zexy2d,
        //             latest_protocol_version: 29,
        //         },
        //         chunks: [
        //             ChunkHeaderView {
        //                 chunk_hash: `7Qz2B7MumKt68iNFjSwahG35cynVLrkCfXjopFZQFLg4`,
        //                 prev_block_hash: `8try83LRTx76jfbmPv8SxJchgW8XeQH3gt2piKT6Ykpj`,
        //                 outcome_root: `86VPrDDpopYnn5pXSZWh6LqHjBUui2reaQT3kPYpthUs`,
        //                 prev_state_root: `F53dYSv5z9ejgDEt1keCY2JxEeDSTUPeGsEkVg21QbBH`,
        //                 encoded_merkle_root: `6s1QWaxhbL7EwzE7ccmhqAAebYBaziXBbumNCEqkKL76`,
        //                 encoded_length: 208,
        //                 height_created: 426,
        //                 height_included: 426,
        //                 shard_id: 0,
        //                 gas_used: 424555062500,
        //                 gas_limit: 1000000000000000,
        //                 rent_paid: 0,
        //                 validator_reward: 0,
        //                 balance_burnt: 2122775312500000,
        //                 outgoing_receipts_root: `7LstzSPfxFErjyxZg8nhvcXUxMUpDBysEF53w3uFF5dc`,
        //                 tx_root: `11111111111111111111111111111111`,
        //                 validator_proposals: [],
        //                 signature: ed25519:4P2mYEGHU5L2JW2smLuy92DBRJ1iZmmjFmbHNV6PddCD46UW9Nmb5E285AKcK2XCjishc9NLyByMudursGxCatkf,
        //             },
        //         ],
        //     },
        //     chunks: [
        //         IndexerChunkView {
        //         author: "test.near",
        //         header: ChunkHeaderView {
        //             chunk_hash: `EF4KJYRKzmtgncQwPkJx3ed2XG3u42e7Dw8G2qLFizYS`,
        //             prev_block_hash: `GXkHtzYqzkNN11RJajcGMyW18wsn3Joa2Z8aRoRzy6GK`,
        //             outcome_root: `11111111111111111111111111111111`,
        //             prev_state_root: `2Aw5LFRbDpcSji61VPeCeT9yX7bSToRmbzVk5vyPhCfp`,
        //             encoded_merkle_root: `7SWuaCHcRSMt56B9MwhMtmKUhZLwAkKPUtnShTrvHaqc`,
        //             encoded_length: 242,
        //             height_created: 1214,
        //             height_included: 0,
        //             shard_id: 0,
        //             gas_used: 0,
        //             gas_limit: 1000000000000000,
        //             rent_paid: 0,
        //             validator_reward: 0,
        //             balance_burnt: 0,
        //             outgoing_receipts_root: `H4Rd6SGeEBTbxkitsCdzfu9xL9HtZ2eHoPCQXUeZ6bW4`,
        //             tx_root: `E1CbQD6XaRPG5ji7K6z4eAhdjAEpTonUo6jK3FN1Bjk`,
        //             validator_proposals: [],
        //             signature: ed25519:4oRiWAB9hmSUKpn6ADNpckGXiPYF5SYvHeSWDEqbQdzDWEqoRLRzRQCDXwfRrnLcqodvR97CkoDUrfwmrMJydcpu,
        //         },
        //         transactions: [
        //             IndexerTransactionWithOutcome {
        //             transaction: SignedTransactionView {
        //                 signer_id: "test.near",
        //                 public_key: ed25519:8NA7mh6TAWzy2qz68bHp62QHTEQ6nJLfiYeKDRwEbU3X,
        //                 nonce: 1,
        //                 receiver_id: "some.test.near",
        //                 actions: [
        //                     CreateAccount,
        //                 Transfer {
        //                     deposit: 40000000000000000000000000,
        //                 },
        //                 AddKey {
        //                     public_key: ed25519:2syGhqwJ8ba2nUGmP9tkZn9m1DYZPYYobpufiERVnug8,
        //                     access_key: AccessKeyView {
        //                         nonce: 0,
        //                         permission: FullAccess,
        //                     },
        //                 },
        //                 ],
        //                 signature: ed25519:Qniuu7exnr6xbe6gKafV5vDhuwM1jt9Bn7sCTF6cHfPpYWVJ4Q6kq8RAxKSeLoxbCreVp1XzMMJmXt8YcUqmMYw,
        //                 hash: `8dNv9S8rAFwso9fLwfDQXmw5yv5zscDjQpta96pMF6Bi`,
        //             },
        //             outcome: IndexerExecutionOutcomeWithReceipt {
        //                 execution_outcome: ExecutionOutcomeWithIdView {
        //                     proof: [],
        //                     block_hash: `G9v6Fsv94xaa7BRY2N5PFF5PJwT7ec6DPzQK73Yf3CZ6`,
        //                     id: `8dNv9S8rAFwso9fLwfDQXmw5yv5zscDjQpta96pMF6Bi`,
        //                     outcome: ExecutionOutcomeView {
        //                         logs: [],
        //                         receipt_ids: [
        //                         `CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz`,
        //                         ],
        //                         gas_burnt: 424555062500,
        //                         tokens_burnt: 424555062500000000000,
        //                         executor_id: "test.near",
        //                         status: SuccessReceiptId(CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz),
        //                     },
        //                 },
        //                 receipt: None,
        //             },
        //         },
        //         ],
        //         receipts: [
        //             ReceiptView {
        //             predecessor_id: "test.near",
        //             receiver_id: "some.test.near",
        //             receipt_id: `CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz`,
        //             receipt: Action {
        //                 signer_id: "test.near",
        //                 signer_public_key: ed25519:8NA7mh6TAWzy2qz68bHp62QHTEQ6nJLfiYeKDRwEbU3X,
        //                 gas_price: 1030000000,
        //                 output_data_receivers: [],
        //                 input_data_ids: [],
        //                 actions: [
        //                     CreateAccount,
        //                 Transfer {
        //                     deposit: 40000000000000000000000000,
        //                 },
        //                 AddKey {
        //                     public_key: ed25519:2syGhqwJ8ba2nUGmP9tkZn9m1DYZPYYobpufiERVnug8,
        //                     access_key: AccessKeyView {
        //                         nonce: 0,
        //                         permission: FullAccess,
        //                     },
        //                 },
        //                 ],
        //             },
        //         },
        //         ],
        //         receipt_execution_outcomes: [
        //             IndexerExecutionOutcomeWithReceipt {
        //             execution_outcome: ExecutionOutcomeWithIdView {
        //                 proof: [],
        //                 block_hash: `BXPB6DQGmBrjARvcgYwS8qKLkyto6dk9NfawGSmfjE9Q`,
        //                 id: `CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz`,
        //                 outcome: ExecutionOutcomeView {
        //                     logs: [],
        //                     receipt_ids: [
        //                     `8vJ1QWM4pffRDnW3c5CxFFV5cMx8wiqxsAqmZTitHvfh`,
        //                     ],
        //                     gas_burnt: 424555062500,
        //                     tokens_burnt: 424555062500000000000,
        //                     executor_id: "some.test.near",
        //                     status: SuccessValue(``),
        //                 },
        //             },
        //             receipt: Some(
        //                 ReceiptView {
        //                 predecessor_id: "test.near",
        //                 receiver_id: "some.test.near",
        //                 receipt_id: `CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz`,
        //                 receipt: Action {
        //                     signer_id: "test.near",
        //                     signer_public_key: ed25519:8NA7mh6TAWzy2qz68bHp62QHTEQ6nJLfiYeKDRwEbU3X,
        //                     gas_price: 1030000000,
        //                     output_data_receivers: [],
        //                     input_data_ids: [],
        //                     actions: [
        //                         CreateAccount,
        //                     Transfer {
        //                         deposit: 40000000000000000000000000,
        //                     },
        //                     AddKey {
        //                         public_key: ed25519:2syGhqwJ8ba2nUGmP9tkZn9m1DYZPYYobpufiERVnug8,
        //                         access_key: AccessKeyView {
        //                             nonce: 0,
        //                             permission: FullAccess,
        //                         },
        //                     },
        //                     ],
        //                 },
        //             },
        //             ),
        //         },
        //         ],
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
        //     ]
        // }
        info!(
            target: "indexer_example",
            "#{} {} Chunks: {}, Transactions: {}, Receipts: {}, ExecutionOutcomes: {}",
            streamer_message.block.header.height,
            streamer_message.block.header.hash,
            streamer_message.chunks.len(),
            streamer_message.chunks.iter().map(|chunk| chunk.transactions.len()).sum::<usize>(),
            streamer_message.chunks.iter().map(|chunk| chunk.receipts.len()).sum::<usize>(),
            streamer_message.chunks.iter().map(|chunk| chunk.receipt_execution_outcomes.len()).sum::<usize>(),
        );
    }
}

fn main() {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();
    init_logging();

    let opts: Opts = Opts::parse();

    let home_dir =
        opts.home_dir.unwrap_or(std::path::PathBuf::from(near_indexer::get_default_home()));

    match opts.subcmd {
        SubCommand::Run => {
            let indexer_config = near_indexer::IndexerConfig {
                home_dir,
                sync_mode: near_indexer::SyncModeEnum::FromInterruption,
                await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
            };
            actix::System::builder()
                .stop_on_panic(true)
                .run(move || {
                    let indexer = near_indexer::Indexer::new(indexer_config);
                    let stream = indexer.streamer();
                    actix::spawn(listen_blocks(stream));
                })
                .unwrap();
        }
        SubCommand::Init(config) => near_indexer::init_configs(
            &home_dir,
            config.chain_id.as_ref().map(AsRef::as_ref),
            config.account_id.as_ref().map(AsRef::as_ref),
            config.test_seed.as_ref().map(AsRef::as_ref),
            config.num_shards,
            config.fast,
            config.genesis.as_ref().map(AsRef::as_ref),
            config.download,
            config.download_genesis_url.as_ref().map(AsRef::as_ref),
        ),
    }
}
