//! Integration smoke test for immediate V2 partial witness resolution via the
//! grandparent anchor. A witness part for the chunk at height H naturally races
//! the parent block H-1 (the producer emits parts right after chunk production),
//! so on every height the receiving validator resolves the producer from the
//! signed `prev_prev_block_hash` (H-2, already processed) and signature-verifies
//! the part immediately — there is no defer/replay machinery anymore. If the
//! resolution failed (anchor row missing or wrongly dropped parts), the chunk
//! could never be endorsed — witness parts are not retransmitted — and its mask
//! bit would stay false. Nightly-gated: the V2 wire path needs
//! `ProtocolFeature::EarlyKickout`.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance};

/// Two clients: account0 produces blocks/chunks, account1 is the chunk
/// validator that receives V2 witnesses.
const PRODUCER: &str = "account0";
const RECEIVER: &str = "account1";

fn make_env() -> TestLoopEnv {
    let accounts = [PRODUCER, RECEIVER];
    let clients = accounts.iter().map(|s| s.parse::<AccountId>().unwrap()).collect_vec();
    let validators_spec = ValidatorsSpec::desired_roles(&[PRODUCER], &[RECEIVER]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(50)
        .shard_layout(ShardLayout::single_shard())
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&clients, Balance::from_near(1_000_000))
        .genesis_height(10_000)
        .build();
    let epoch_config_store =
        TestEpochConfigBuilder::from_genesis(&genesis).build_store_for_genesis_protocol_version();
    TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
}

#[test]
// Spice distributes witnesses via its own data-distribution path, not the V2
// partial-witness path this test drives.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_v2_witness_immediate_resolution() {
    init_test_logger();
    let mut env = make_env();

    let producer_client_handle = env
        .node_datas
        .iter()
        .find(|n| n.account_id.as_str() == PRODUCER)
        .expect("producer node datas")
        .client_sender
        .actor_handle();

    let genesis_height = 10_000;
    let target_height = genesis_height + 20;
    env.test_loop.run_until(
        |data| {
            let chain = &data.get(&producer_client_handle).client.chain;
            chain.head().unwrap().height >= target_height
        },
        Duration::seconds(30),
    );

    // Every produced block past the first must carry its chunk: a single missed
    // mask bit means the receiver failed to endorse, i.e. the witness part was
    // dropped or its producer resolution failed.
    let chain = &env.test_loop.data.get(&producer_client_handle).client.chain;
    let mut hash = chain.head().unwrap().last_block_hash;
    loop {
        let block = chain.get_block(&hash).unwrap();
        if block.header().height() <= genesis_height + 1 {
            break;
        }
        assert!(
            block.header().chunk_mask().iter().all(|&mask| mask),
            "chunk missing at height {} — witness part dropped or unresolved",
            block.header().height(),
        );
        hash = *block.header().prev_hash();
    }

    drop(env);
}
