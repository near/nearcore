use std::env;
use std::io;

use actix;

use tokio::sync::mpsc;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::EnvFilter;

use near_indexer;

fn init_logging(verbose: bool) {
    let mut env_filter = EnvFilter::new("tokio_reactor=info,near=info,stats=info,telemetry=info");

    if verbose {
        env_filter = env_filter
            .add_directive("cranelift_codegen=warn".parse().unwrap())
            .add_directive("cranelift_codegen=warn".parse().unwrap())
            .add_directive("h2=warn".parse().unwrap())
            .add_directive("trust_dns_resolver=warn".parse().unwrap())
            .add_directive("trust_dns_proto=warn".parse().unwrap());

        env_filter = env_filter.add_directive(LevelFilter::DEBUG.into());
    } else {
        env_filter = env_filter.add_directive(LevelFilter::WARN.into());
    }

    if let Ok(rust_log) = env::var("RUST_LOG") {
        for directive in rust_log.split(',').filter_map(|s| match s.parse() {
            Ok(directive) => Some(directive),
            Err(err) => {
                eprintln!("Ignoring directive `{}`: {}", s, err);
                None
            }
        }) {
            env_filter = env_filter.add_directive(directive);
        }
    }
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(io::stderr)
        .init();
}

async fn listen_blocks(mut stream: mpsc::Receiver<near_indexer::BlockResponse>) {
    while let Some(block) = stream.recv().await {
        // TODO: handle data as you need
        // Example of `block` with all the data
        // Note that `outcomes` for specific transaction won't be in the same block
        // (it should be in the next one)
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
        //         ChunkView {
        //             author: "test.near",
        //             header: ChunkHeaderView {
        //                 chunk_hash: `7Qz2B7MumKt68iNFjSwahG35cynVLrkCfXjopFZQFLg4`,
        //                 prev_block_hash: `8try83LRTx76jfbmPv8SxJchgW8XeQH3gt2piKT6Ykpj`,
        //                 outcome_root: `86VPrDDpopYnn5pXSZWh6LqHjBUui2reaQT3kPYpthUs`,
        //                 prev_state_root: `F53dYSv5z9ejgDEt1keCY2JxEeDSTUPeGsEkVg21QbBH`,
        //                 encoded_merkle_root: `6s1QWaxhbL7EwzE7ccmhqAAebYBaziXBbumNCEqkKL76`,
        //                 encoded_length: 208,
        //                 height_created: 426,
        //                 height_included: 0,
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
        //             transactions: [
        //                 SignedTransactionView {
        //                     signer_id: "test.near",
        //                     public_key: ed25519:8dM8rHT6bzGY2y5aZt6rMDFPeW3ZMsDtGxzEzAb6okPs,
        //                     nonce: 2,
        //                     receiver_id: "inx04.test.near",
        //                     actions: [
        //                         CreateAccount,
        //                         Transfer {
        //                             deposit: 10000000000000000000000000000,
        //                         },
        //                         AddKey {
        //                             public_key: ed25519:Df3CyHVsEdSfRtYDa286BFJN6nYePmoNvdgoxJ9F7PDM,
        //                             access_key: AccessKeyView {
        //                                 nonce: 0,
        //                                 permission: FullAccess,
        //                             },
        //                         },
        //                     ],
        //                     signature: ed25519:2kZ6eckZAAJCmx69zJNkShWGju41nRZJYoktRdSQahZmyH4xvtg8mMbrhjoPxyUCJQJTaGh3NpLisgpRiXCrySuL,
        //                     hash: `GaZk7PVHQ7FPdn69tGKdLoYgH5sFYqidBxKFDvyvCncw`,
        //                 },
        //             ],
        //             receipts: [
        //                 ReceiptView {
        //                     predecessor_id: "test.near",
        //                     receiver_id: "inx03.test.near",
        //                     receipt_id: `6yAABgeyTYcf9pauF7yvRnnxxsSGVF9RcDt5TK3cLVGg`,
        //                     receipt: Action {
        //                         signer_id: "test.near",
        //                         signer_public_key: ed25519:8dM8rHT6bzGY2y5aZt6rMDFPeW3ZMsDtGxzEzAb6okPs,
        //                         gas_price: 5150,
        //                         output_data_receivers: [],
        //                         input_data_ids: [],
        //                         actions: [
        //                             CreateAccount,
        //                             Transfer {
        //                                 deposit: 10000000000000000000000000000,
        //                             },
        //                             AddKey {
        //                                 public_key: ed25519:DRR2T8XKzVBLyeADvcVpLki9aonwJN2CdMBxbJijwvbW,
        //                                 access_key: AccessKeyView {
        //                                     nonce: 0,
        //                                     permission: FullAccess,
        //                                 },
        //                             },
        //                         ],
        //                     },
        //                 },
        //             ],
        //         },
        //     ],
        //     outcomes: [
        //         Transaction(
        //             ExecutionOutcomeWithIdView {
        //                 proof: [],
        //                 block_hash: `99fpSqxeiMe8iTfh72reaLvx4R16kPYAr1TYuxS3zqkB`,
        //                 id: `FTeMWryKrKaKSgCXhgMe7rtMyxbpsxscGAiCjbRZndit`,
        //                 outcome: ExecutionOutcomeView {
        //                     logs: [],
        //                     receipt_ids: [
        //                         `6yAABgeyTYcf9pauF7yvRnnxxsSGVF9RcDt5TK3cLVGg`,
        //                     ],
        //                     gas_burnt: 424555062500,
        //                     tokens_burnt: 2122775312500000,
        //                     executor_id: "test.near",
        //                     status: SuccessReceiptId(6yAABgeyTYcf9pauF7yvRnnxxsSGVF9RcDt5TK3cLVGg),
        //                 },
        //             },
        //         ),
        //     ],
        // }
        eprintln!("{:#?}", block);
    }
}

fn main() {
    init_logging(true);
    let indexer = near_indexer::Indexer::new(None);
    let stream = indexer.streamer();
    actix::spawn(listen_blocks(stream));
    indexer.start();
}
