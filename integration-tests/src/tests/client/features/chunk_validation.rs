use assert_matches::assert_matches;
use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::{Genesis, GenesisConfig, GenesisRecords};
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType};
use near_o11y::testonly::init_integration_logger;
use near_primitives::block::Tip;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountInfo;
use near_primitives::views::FinalExecutionStatus;
use near_primitives_core::account::{AccessKey, Account};
use near_primitives_core::checked_feature;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, NumSeats};
use near_primitives_core::version::PROTOCOL_VERSION;
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use std::collections::HashSet;

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

#[test]
fn test_chunk_validation_basic() {
    init_integration_logger();

    if !checked_feature!("stable", ChunkValidation, PROTOCOL_VERSION) {
        println!("Test not applicable without ChunkValidation enabled");
        return;
    }

    let initial_balance = 100 * ONE_NEAR;
    let validator_stake = 1000000 * ONE_NEAR;
    let blocks_to_produce = 20;
    let num_accounts = 9;
    let accounts = (0..num_accounts)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();
    // We'll use four shards for this test.
    let shard_layout = ShardLayout::get_simple_nightshade_layout();
    let num_shards = shard_layout.shard_ids().count();
    let num_validators = 8;
    let mut genesis_config = GenesisConfig {
        // Use the latest protocol version. Otherwise, the version may be too
        // old that e.g. blocks don't even store previous heights.
        protocol_version: PROTOCOL_VERSION,
        // Some arbitrary starting height. Doesn't matter.
        genesis_height: 10000,
        shard_layout,
        validators: accounts
            .iter()
            .take(num_validators)
            .map(|account_id| AccountInfo {
                account_id: account_id.clone(),
                public_key: create_test_signer(account_id.as_str()).public_key(),
                amount: validator_stake,
            })
            .collect(),
        // We don't care about epoch transitions in this test.
        epoch_length: 10000,
        // The genesis requires this, so set it to something arbitrary.
        protocol_treasury_account: accounts[num_validators].clone(),
        // Simply make all validators block producers.
        num_block_producer_seats: num_validators as NumSeats,
        // Make all validators produce chunks for all shards.
        minimum_validators_per_shard: num_validators as NumSeats,
        // Even though not used for the most recent protocol version,
        // this must still have the same length as the number of shards,
        // or else the genesis fails validation.
        num_block_producer_seats_per_shard: vec![8; num_shards],
        gas_limit: 10u64.pow(15),
        transaction_validity_period: 120,
        ..Default::default()
    };

    // Set up the records corresponding to the validator accounts.
    let mut records = Vec::new();
    for (i, account) in accounts.iter().enumerate() {
        // The staked amount must be consistent with validators from genesis.
        let staked = if i < num_validators { validator_stake } else { 0 };
        records.push(StateRecord::Account {
            account_id: account.clone(),
            account: Account::new(initial_balance, staked, CryptoHash::default(), 0),
        });
        records.push(StateRecord::AccessKey {
            account_id: account.clone(),
            public_key: create_test_signer(account.as_str()).public_key(),
            access_key: AccessKey::full_access(),
        });
        // The total supply must be correct to pass validation.
        genesis_config.total_supply += initial_balance + staked;
    }
    let genesis = Genesis::new(genesis_config, GenesisRecords(records)).unwrap();
    let chain_genesis = ChainGenesis::new(&genesis);

    let mut env = TestEnv::builder(chain_genesis)
        .clients(accounts.iter().take(8).cloned().collect())
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();
    let mut tx_hashes = vec![];

    for round in 0..blocks_to_produce {
        let heads = env
            .clients
            .iter()
            .map(|client| client.chain.head().unwrap().last_block_hash)
            .collect::<HashSet<_>>();
        assert_eq!(heads.len(), 1, "All clients should have the same head");
        let tip = env.clients[0].chain.head().unwrap();

        let sender_account = accounts[round % num_accounts].clone();
        let receiver_account = accounts[(round + 1) % num_accounts].clone();
        let signer = InMemorySigner::from_seed(
            sender_account.clone(),
            KeyType::ED25519,
            sender_account.as_ref(),
        );
        // Give each transaction 10 blocks to be fully executed.
        if round > 1 && blocks_to_produce - round >= 10 {
            let tx = SignedTransaction::send_money(
                round as u64,
                sender_account,
                receiver_account,
                &signer,
                ONE_NEAR,
                tip.last_block_hash,
            );
            tx_hashes.push(tx.get_hash());
            let _ = env.clients[0].process_tx(tx, false, false);
        }

        let block_producer = get_block_producer(&env, &tip, 1);
        tracing::debug!(
            target: "chunk_validation",
            "Producing block at height {} by {}", tip.height + 1, block_producer
        );
        let block = env.client(&block_producer).produce_block(tip.height + 1).unwrap().unwrap();
        if round > 1 {
            for i in 0..num_shards {
                let chunks = block.chunks();
                let chunk = chunks.get(i).unwrap();
                assert!(chunk.is_new_chunk(block.header().height()));
            }
        }

        // Apply the block.
        for i in 0..env.clients.len() {
            tracing::debug!(
                target: "chunk_validation",
                "Applying block at height {} at {}", block.header().height(), env.get_client_id(i)
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

    for tx_hash in tx_hashes {
        let outcome = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
        assert_matches!(outcome.status, FinalExecutionStatus::SuccessValue(_));
    }
    // Check that number of chunk endorsements is correct.
    // There should be `(blocks_to_produce - 1) * num_shards` chunks, because
    // for first block after genesis chunk production was not triggered.
    // Then, each chunk is validated by each validator.
    // TODO(#10265): divide validators separately between shards.
    let expected_endorsements = (blocks_to_produce - 1) * num_shards * num_validators;
    let approvals = env.take_chunk_endorsements(expected_endorsements);
    assert!(approvals.len() >= expected_endorsements);
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
