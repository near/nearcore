use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::{Genesis, GenesisConfig, GenesisRecords};
use near_client::test_utils::TestEnv;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Tip;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::AccountInfo;
use near_primitives_core::account::Account;
use near_primitives_core::checked_feature;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::AccountId;
use near_primitives_core::version::PROTOCOL_VERSION;
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use std::collections::HashSet;

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

#[test]
fn test_chunk_validation_basic() {
    init_test_logger();

    if !checked_feature!("stable", ChunkValidation, PROTOCOL_VERSION) {
        println!("Test not applicable without ChunkValidation enabled");
        return;
    }

    let validator_stake = 1000000 * ONE_NEAR;
    let accounts =
        (0..9).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let mut genesis_config = GenesisConfig {
        // Use the latest protocol version. Otherwise, the version may be too
        // old that e.g. blocks don't even store previous heights.
        protocol_version: PROTOCOL_VERSION,
        // Some arbitrary starting height. Doesn't matter.
        genesis_height: 10000,
        // We'll use four shards for this test.
        shard_layout: ShardLayout::get_simple_nightshade_layout(),
        // Make 8 validators, which means 2 will be assigned as chunk validators
        // for each chunk.
        validators: accounts
            .iter()
            .take(8)
            .map(|account_id| AccountInfo {
                account_id: account_id.clone(),
                public_key: create_test_signer(account_id.as_str()).public_key(),
                amount: validator_stake,
            })
            .collect(),
        // We don't care about epoch transitions in this test.
        epoch_length: 10000,
        // The genesis requires this, so set it to something arbitrary.
        protocol_treasury_account: accounts[8].clone(),
        // Simply make all validators block producers.
        num_block_producer_seats: 8,
        // Make all validators produce chunks for all shards.
        minimum_validators_per_shard: 8,
        // Even though not used for the most recent protocol version,
        // this must still have the same length as the number of shards,
        // or else the genesis fails validation.
        num_block_producer_seats_per_shard: vec![8, 8, 8, 8],
        ..Default::default()
    };

    // Set up the records corresponding to the validator accounts.
    let mut records = Vec::new();
    for (i, account) in accounts.iter().enumerate() {
        // The staked amount must be consistent with validators from genesis.
        let staked = if i < 8 { validator_stake } else { 0 };
        records.push(StateRecord::Account {
            account_id: account.clone(),
            account: Account::new(0, staked, CryptoHash::default(), 0),
        });
        // The total supply must be correct to pass validation.
        genesis_config.total_supply += staked;
    }
    let genesis = Genesis::new(genesis_config, GenesisRecords(records)).unwrap();
    let chain_genesis = ChainGenesis::new(&genesis);

    let mut env = TestEnv::builder(chain_genesis)
        .clients(accounts.iter().take(8).cloned().collect())
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    for round in 0..10 {
        let heads = env
            .clients
            .iter()
            .map(|client| client.chain.head().unwrap().last_block_hash)
            .collect::<HashSet<_>>();
        assert_eq!(heads.len(), 1, "All clients should have the same head");
        let tip = env.clients[0].chain.head().unwrap();

        let block_producer = get_block_producer(&env, &tip, 1);
        println!("Producing block at height {} by {}", tip.height + 1, block_producer);
        let block = env.client(&block_producer).produce_block(tip.height + 1).unwrap().unwrap();
        if round > 1 {
            for i in 0..4 {
                let chunks = block.chunks();
                let chunk = chunks.get(i).unwrap();
                assert_eq!(chunk.height_created(), chunk.height_included());
            }
        }

        // Apply the block.
        for i in 0..env.clients.len() {
            println!(
                "  Applying block at height {} at {}",
                block.header().height(),
                env.get_client_id(i)
            );
            let blocks_processed =
                env.clients[i].process_block_test(block.clone().into(), Provenance::NONE).unwrap();
            assert_eq!(blocks_processed, vec![*block.hash()]);
        }

        env.process_partial_encoded_chunks();
        for j in 0..env.clients.len() {
            env.process_shards_manager_responses_and_finish_processing_blocks(j);
        }
        env.propagate_chunk_state_witnesses();
    }

    // Wait a bit and check that we've received at least some chunk approvals.
    // TODO(#10265): We need to make this not time-based, and we need to assert
    // exactly how many approvals (or total stake) we have.
    std::thread::sleep(std::time::Duration::from_secs(1));
    let approvals = env.get_all_chunk_endorsements();
    assert!(!approvals.is_empty());
}

// Returns the block producer for the height of head + height_offset.
fn get_block_producer(env: &TestEnv, head: &Tip, height_offset: u64) -> AccountId {
    let client = &env.clients[0];
    let epoch_manager = &client.epoch_manager;
    let parent_hash = &head.last_block_hash;
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash).unwrap();
    let height = head.height + height_offset;
    let block_producer = epoch_manager.get_block_producer(&epoch_id, height).unwrap();
    block_producer
}
