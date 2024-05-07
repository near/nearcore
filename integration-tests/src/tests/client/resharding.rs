use crate::tests::client::process_blocks::set_block_protocol_version;
use assert_matches::assert_matches;
use near_chain::near_chain_primitives::Error;
use near_chain::test_utils::wait_for_all_blocks_in_processing;
use near_chain::{ChainStoreAccess, Provenance};
use near_chain_configs::{Genesis, NEAR_BASE};
use near_client::test_utils::{run_catchup, TestEnv};
use near_client::{Client, ProcessTxResponse};
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::id::AccountId;
use near_primitives::block::{Block, Tip};
use near_primitives::epoch_manager::{AllEpochConfig, AllEpochConfigTestOverrides, EpochConfig};
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::to_base64;
use near_primitives::shard_layout::{account_id_to_shard_id, account_id_to_shard_uid, ShardLayout};
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives::types::{BlockHeight, NumShards, ProtocolVersion, ShardId};
use near_primitives::utils::MaybeValidated;
use near_primitives::version::ProtocolFeature;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{
    ExecutionStatusView, FinalExecutionOutcomeView, FinalExecutionStatus, QueryRequest,
};
use near_primitives_core::num_rational::Rational32;
use near_store::flat::FlatStorageStatus;
use near_store::metadata::DbKind;
use near_store::test_utils::{gen_account, gen_shard_accounts, gen_unique_accounts};
use near_store::trie::SnapshotError;
use near_store::{DBCol, ShardUId};
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap, HashSet};
use tracing::debug;

#[cfg(feature = "nightly")]
use near_parameters::RuntimeConfig;

const SIMPLE_NIGHTSHADE_PROTOCOL_VERSION: ProtocolVersion =
    ProtocolFeature::SimpleNightshade.protocol_version();

const SIMPLE_NIGHTSHADE_V2_PROTOCOL_VERSION: ProtocolVersion =
    ProtocolFeature::SimpleNightshadeV2.protocol_version();

const SIMPLE_NIGHTSHADE_V3_PROTOCOL_VERSION: ProtocolVersion =
    ProtocolFeature::SimpleNightshadeV3.protocol_version();

#[cfg(feature = "nightly")]
const SIMPLE_NIGHTSHADE_TESTONLY_PROTOCOL_VERSION: ProtocolVersion =
    ProtocolFeature::SimpleNightshadeTestonly.protocol_version();

const P_CATCHUP: f64 = 0.2;

#[derive(Clone, Copy)]
enum ReshardingType {
    // In the V0->V1 resharding outgoing receipts are reassigned to receiver.
    V1,
    // In the V1->V2 resharding outgoing receipts are reassigned to lowest index child.
    V2,
    // In the V2->V3 resharding outgoing receipts are reassigned to lowest index child.
    V3,
    // In V3->TESTONLY resharding outgoing receipts are reassigned to lowest index child.
    #[cfg(feature = "nightly")]
    TESTONLY,
}

fn get_target_protocol_version(resharding_type: &ReshardingType) -> ProtocolVersion {
    match resharding_type {
        ReshardingType::V1 => SIMPLE_NIGHTSHADE_PROTOCOL_VERSION,
        ReshardingType::V2 => SIMPLE_NIGHTSHADE_V2_PROTOCOL_VERSION,
        ReshardingType::V3 => SIMPLE_NIGHTSHADE_V3_PROTOCOL_VERSION,
        #[cfg(feature = "nightly")]
        ReshardingType::TESTONLY => SIMPLE_NIGHTSHADE_TESTONLY_PROTOCOL_VERSION,
    }
}

fn get_genesis_protocol_version(resharding_type: &ReshardingType) -> ProtocolVersion {
    get_target_protocol_version(resharding_type) - 1
}

fn get_parent_shard_uids(resharding_type: &ReshardingType) -> Vec<ShardUId> {
    let shard_layout = match resharding_type {
        ReshardingType::V1 => ShardLayout::v0_single_shard(),
        ReshardingType::V2 => ShardLayout::get_simple_nightshade_layout(),
        ReshardingType::V3 => ShardLayout::get_simple_nightshade_layout_v2(),
        #[cfg(feature = "nightly")]
        ReshardingType::TESTONLY => ShardLayout::get_simple_nightshade_layout_v3(),
    };
    shard_layout.shard_uids().collect()
}

// Return the expected number of shards.
fn get_expected_shards_num(
    epoch_length: u64,
    height: BlockHeight,
    resharding_type: &ReshardingType,
) -> u64 {
    if height <= 2 * epoch_length {
        match resharding_type {
            ReshardingType::V1 => 1,
            ReshardingType::V2 => 4,
            ReshardingType::V3 => 5,
            #[cfg(feature = "nightly")]
            ReshardingType::TESTONLY => 6,
        }
    } else {
        match resharding_type {
            ReshardingType::V1 => 4,
            ReshardingType::V2 => 5,
            ReshardingType::V3 => 6,
            #[cfg(feature = "nightly")]
            ReshardingType::TESTONLY => 7,
        }
    }
}

/// The condition that determines if a chunk should be produced of dropped.
/// Can be probabilistic, predefined based on height and shard id or both.
struct DropChunkCondition {
    probability: f64,
    by_height_shard_id: HashSet<(BlockHeight, ShardId)>,
}

impl DropChunkCondition {
    fn should_drop_chunk(
        &self,
        rng: &mut StdRng,
        height: BlockHeight,
        shard_ids: &Vec<ShardId>,
    ) -> bool {
        if rng.gen_bool(self.probability) {
            return true;
        }

        // One chunk producer may be responsible for producing multiple chunks.
        // Ensure that the by_height_shard_id is consistent in that it doesn't
        // try to produce one chunk and drop another chunk, produced by the same
        // chunk producer.
        // It shouldn't happen in setups where num_validators >= num_shards.
        let mut all_true = true;
        let mut all_false = true;
        for shard_id in shard_ids {
            match self.by_height_shard_id.contains(&(height, *shard_id)) {
                true => all_false = false,
                false => all_true = false,
            }
        }
        if all_false {
            return false;
        }
        if all_true {
            return true;
        }

        tracing::warn!("Inconsistent test setup. Chunk producer configured to produce one of its chunks and to skip another. This is not supported, skipping all. height {} shard_ids {:?}", height, shard_ids);
        return true;
    }

    /// Returns a DropChunkCondition that doesn't drop any chunks.
    fn new() -> Self {
        Self { probability: 0.0, by_height_shard_id: HashSet::new() }
    }

    fn with_probability(probability: f64) -> Self {
        Self { probability, by_height_shard_id: HashSet::new() }
    }

    fn with_by_height_shard_id(by_height_shard_id: HashSet<(u64, ShardId)>) -> Self {
        Self { probability: 0.0, by_height_shard_id: by_height_shard_id }
    }
}

/// Test environment prepared for testing the sharding upgrade.
/// Epoch 0, blocks 1-5  : genesis shard layout
/// Epoch 1, blocks 6-10 : genesis shard layout, resharding happens
/// Epoch 2: blocks 10-15: target shard layout, shard layout is upgraded
/// Epoch 3: blocks 16-20: target shard layout,
///
/// Note: if the test is extended to more epochs, garbage collection will
/// kick in and delete data that is checked at the end of the test.
struct TestReshardingEnv {
    env: TestEnv,
    initial_accounts: Vec<AccountId>,
    init_txs: Vec<SignedTransaction>,
    txs_by_height: BTreeMap<u64, Vec<SignedTransaction>>,
    epoch_length: u64,
    num_clients: usize,
    rng: StdRng,
    resharding_type: Option<ReshardingType>,
}

impl TestReshardingEnv {
    fn new(
        epoch_length: u64,
        num_validators: usize,
        num_clients: usize,
        num_init_accounts: usize,
        gas_limit: Option<u64>,
        genesis_protocol_version: ProtocolVersion,
        rng_seed: u64,
        state_snapshot_enabled: bool,
        resharding_type: Option<ReshardingType>,
    ) -> Self {
        let mut rng = SeedableRng::seed_from_u64(rng_seed);
        let validators: Vec<AccountId> =
            (0..num_validators).map(|i| format!("test{}", i).parse().unwrap()).collect();
        let shard_accounts = gen_shard_accounts();
        let other_accounts = gen_unique_accounts(&mut rng, num_init_accounts, num_init_accounts);
        let initial_accounts = [validators, shard_accounts, other_accounts].concat();
        let genesis = setup_genesis(
            epoch_length,
            num_validators as u64,
            initial_accounts.clone(),
            gas_limit,
            genesis_protocol_version,
        );
        let builder = if state_snapshot_enabled {
            TestEnv::builder(&genesis.config).use_state_snapshots()
        } else {
            TestEnv::builder(&genesis.config)
        };
        // Set the kickout thresholds to zero. In some tests we have chunk
        // producers missing chunks but we don't want any of the clients to get
        // kicked.
        // One of the reasons for not kicking is that the current test infra
        // doesn't support requesting chunks and non-validators wouldn't be able
        // to obtain the chunks at all.
        // Same needs to be set in the genesis.
        let epoch_config_test_overrides = Some(AllEpochConfigTestOverrides {
            block_producer_kickout_threshold: Some(0),
            chunk_producer_kickout_threshold: Some(0),
        });
        let env = builder
            .clients_count(num_clients)
            .validator_seats(num_validators)
            .real_stores()
            .epoch_managers_with_test_overrides(epoch_config_test_overrides)
            .nightshade_runtimes(&genesis)
            .track_all_shards()
            .build();
        assert_eq!(env.validators.len(), num_validators);
        Self {
            env,
            initial_accounts,
            epoch_length,
            num_clients,
            init_txs: vec![],
            txs_by_height: BTreeMap::new(),
            rng,
            resharding_type,
        }
    }

    /// `init_txs` are added before any block is produced
    fn set_init_tx(&mut self, init_txs: Vec<SignedTransaction>) {
        self.init_txs = init_txs;
    }

    /// `txs_by_height` is a hashmap from block height to transactions to be
    /// included at block at that height
    fn set_tx_at_height(&mut self, height: u64, txs: Vec<SignedTransaction>) {
        debug!(target: "test", "setting txs at height {} txs: {:?}", height, txs.iter().map(|x|x.get_hash()).collect::<Vec<_>>());
        self.txs_by_height.insert(height, txs);
    }

    fn step(
        &mut self,
        drop_chunk_condition: &DropChunkCondition,
        protocol_version: ProtocolVersion,
    ) {
        self.step_err(drop_chunk_condition, protocol_version).unwrap();
    }

    /// produces and processes the next block also checks that all accounts in
    /// initial_accounts are intact
    ///
    /// allows for changing the protocol version in the middle of the test the
    /// testing_v2 argument means whether the test should expect the sharding
    /// layout V2 to be used once the appropriate protocol version is reached
    fn step_err(
        &mut self,
        drop_chunk_condition: &DropChunkCondition,
        protocol_version: ProtocolVersion,
    ) -> Result<(), near_client::Error> {
        let env = &mut self.env;
        let head = env.clients[0].chain.head().unwrap();
        let next_height = head.height + 1;
        let expected_num_shards = self
            .resharding_type
            .map(|t| get_expected_shards_num(self.epoch_length, next_height, &t));

        tracing::debug!(target: "test", next_height, expected_num_shards, "step");

        // add transactions for the next block
        if next_height == 1 {
            for tx in self.init_txs.iter() {
                Self::process_tx(env, tx);
            }
        }

        // At every step, chunks for the next block are produced after the current block is processed
        // (inside env.process_block)
        // Therefore, if we want a transaction to be included at the block at `height+1`, we must add
        // it when we are producing the block at `height`
        if let Some(txs) = self.txs_by_height.get(&(next_height + 1)) {
            for tx in txs {
                Self::process_tx(env, tx);
            }
        }

        // produce block
        let block = {
            let block_producer = get_block_producer(env, &head);
            let _span = tracing::debug_span!(target: "test", "", client=?block_producer).entered();
            let block_producer_client = env.client(&block_producer);
            let mut block = block_producer_client.produce_block(next_height).unwrap().unwrap();
            set_block_protocol_version(&mut block, block_producer.clone(), protocol_version);

            block
        };

        // Mapping from the chunk producer account id to the list of chunks that
        // it should produce. When processing block at height `next_height` the
        // chunk producers will produce chunks at height `next_height + 1`.
        let mut chunk_producer_to_shard_id: HashMap<AccountId, Vec<ShardId>> = HashMap::new();
        for shard_id in 0..block.chunks().len() {
            let shard_id = shard_id as ShardId;
            let validator_id = get_chunk_producer(env, &block, shard_id);
            chunk_producer_to_shard_id.entry(validator_id).or_default().push(shard_id);
        }

        let should_catchup = Self::should_catchup(&mut self.rng, self.epoch_length, next_height);

        // process block, this also triggers chunk producers for the next block to produce chunks
        for j in 0..self.num_clients {
            let client = &mut env.clients[j];
            let _span = tracing::debug_span!(target: "test", "process block", client=j).entered();

            let shard_ids = chunk_producer_to_shard_id
                .get(client.validator_signer.as_ref().unwrap().validator_id())
                .cloned()
                .unwrap_or_default();
            let should_produce_chunk =
                !drop_chunk_condition.should_drop_chunk(&mut self.rng, next_height + 1, &shard_ids);
            tracing::info!(target: "test", ?next_height, ?should_produce_chunk, ?shard_ids, "should produce chunk");

            // Here we don't just call self.clients[i].process_block_sync_with_produce_chunk_options
            // because we want to call run_catchup before finish processing this block. This simulates
            // that catchup and block processing run in parallel.
            let block = MaybeValidated::from(block.clone());
            client.start_process_block(block, Provenance::NONE, None).unwrap();
            if should_catchup {
                run_catchup(client, &[])?;
            }
            while wait_for_all_blocks_in_processing(&mut client.chain) {
                let (_, errors) = client.postprocess_ready_blocks(None, should_produce_chunk);
                assert!(errors.is_empty(), "unexpected errors: {:?}", errors);
            }
            // manually invoke gc
            let gc_config = client.config.gc.clone();
            client.chain.clear_data(&gc_config).unwrap();
            if should_catchup {
                run_catchup(&mut env.clients[j], &[])?;
            }
        }

        {
            if let Some(expected_num_shards) = expected_num_shards {
                let num_shards = env.clients[0]
                    .epoch_manager
                    .get_shard_layout_from_prev_block(block.header().prev_hash())
                    .unwrap()
                    .shard_ids()
                    .count() as NumShards;
                assert_eq!(num_shards, expected_num_shards);
            }
        }

        env.process_partial_encoded_chunks();
        for j in 0..self.num_clients {
            env.process_shards_manager_responses_and_finish_processing_blocks(j);
        }

        // after resharding, check chunk extra exists and the states are correct
        for account_id in self.initial_accounts.iter() {
            check_account(env, account_id, &block);
        }

        Ok(())
    }

    fn should_catchup(rng: &mut StdRng, epoch_length: u64, next_height: u64) -> bool {
        // Always do catchup before the end of the epoch to ensure it happens at
        // least once. Doing it in the very last block triggers some error conditions.
        if (next_height + 1) % epoch_length == 0 {
            return true;
        }

        // Don't do catchup in the very first block of the epoch. This is
        // primarily for the error handling test where we want to corrupt the
        // databse before catchup happens.
        if next_height % epoch_length == 1 {
            return false;
        }

        rng.gen_bool(P_CATCHUP)
    }

    // Submit the tx to all clients for processing and checks:
    // Clients that track the relevant shard should return ValidTx
    // Clients that do not track the relevenat shard should return RequestRouted
    // At least one client should process it and return ValidTx.
    fn process_tx(env: &mut TestEnv, tx: &SignedTransaction) {
        let mut response_valid_count = 0;
        let mut response_routed_count = 0;
        for j in 0..env.validators.len() {
            let response = env.clients[j].process_tx(tx.clone(), false, false);
            tracing::trace!(target: "test", client=j, tx=?tx.get_hash(), ?response, "process tx");
            match response {
                ProcessTxResponse::ValidTx => response_valid_count += 1,
                ProcessTxResponse::RequestRouted => response_routed_count += 1,
                response => {
                    panic!("invalid tx response {response:?} {tx:?}");
                }
            }
        }
        assert_ne!(response_valid_count, 0);
        assert_eq!(response_valid_count + response_routed_count, env.validators.len());
    }

    /// check that all accounts in `accounts` exist in the current state
    fn check_accounts(&mut self, accounts: Vec<&AccountId>) {
        tracing::debug!(target: "test", "checking accounts");

        let head = self.env.clients[0].chain.head().unwrap();
        let block = self.env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
        for account_id in accounts {
            check_account(&mut self.env, account_id, &block)
        }
    }

    /// Check that chain.get_next_block_hash_with_new_chunk function returns the expected
    /// result with sharding upgrade
    /// Specifically, the function calls `get_next_block_with_new_chunk` for the block at height
    /// `height` for all shards in this block, and verifies that the returned result
    /// 1) If it is not empty (`new_block_hash`, `target_shard_id`),
    ///    - the chunk at `target_shard_id` is a new chunk
    ///    - `target_shard_id` is either the original shard or a split shard of the original shard
    ///    - all blocks before the returned `new_block_hash` do not have new chunk for the corresponding
    ///      shards
    /// 2) If it is empty
    ///    - all blocks after the block at `height` in the current canonical chain do not have
    ///      new chunks for the corresponding shards
    fn check_next_block_with_new_chunk(&mut self, height: BlockHeight) {
        let client = &self.env.clients[0];
        let block = client.chain.get_block_by_height(height).unwrap();
        let block_hash = block.hash();
        let num_shards = block.chunks().len();
        for shard_id in 0..num_shards {
            // get hash of the last block that we need to check that it has empty chunks for the shard
            // if `get_next_block_hash_with_new_chunk` returns None, that would be the lastest block
            // on chain, otherwise, that would be the block before the `block_hash` that the function
            // call returns
            let next_block_hash_with_new_chunk = client
                .chain
                .get_next_block_hash_with_new_chunk(block_hash, shard_id as ShardId)
                .unwrap();

            let mut last_block_hash_with_empty_chunk = match next_block_hash_with_new_chunk {
                Some((new_block_hash, target_shard_id)) => {
                    let new_block = client.chain.get_block(&new_block_hash).unwrap().clone();
                    let chunks = new_block.chunks();
                    // check that the target chunk in the new block is new
                    assert_eq!(
                        chunks.get(target_shard_id as usize).unwrap().height_included(),
                        new_block.header().height(),
                    );
                    if chunks.len() == num_shards {
                        assert_eq!(target_shard_id, shard_id as ShardId);
                    }
                    *new_block.header().prev_hash()
                }
                None => client.chain.head().unwrap().last_block_hash,
            };

            // Check that the target chunks are not new for all blocks between
            // last_block_hash_with_empty_chunk (inclusive) and
            // block_hash (exclusive).
            while &last_block_hash_with_empty_chunk != block_hash {
                let last_block_hash = last_block_hash_with_empty_chunk;
                let last_block = client.chain.get_block(&last_block_hash).unwrap().clone();
                let chunks = last_block.chunks();

                let target_shard_ids = if chunks.len() == num_shards {
                    // same shard layout between block and last_block
                    vec![shard_id as ShardId]
                } else {
                    // different shard layout between block and last_block
                    let shard_layout = client
                        .epoch_manager
                        .get_shard_layout_from_prev_block(&last_block_hash)
                        .unwrap();

                    shard_layout.get_children_shards_ids(shard_id as ShardId).unwrap()
                };

                for target_shard_id in target_shard_ids {
                    let chunk = chunks.get(target_shard_id as usize).unwrap();
                    assert_ne!(chunk.height_included(), last_block.header().height());
                }

                last_block_hash_with_empty_chunk = *last_block.header().prev_hash();
            }
        }
    }

    /// This functions checks that the outcomes of all transactions and associated receipts
    /// have successful status
    /// If `allow_not_started` is true, allow transactions status to be NotStarted
    /// Returns a map from successful transaction hashes to the transaction outcomes
    fn check_tx_outcomes(
        &mut self,
        allow_not_started: bool,
    ) -> HashMap<CryptoHash, FinalExecutionOutcomeView> {
        tracing::debug!(target: "test", "checking tx outcomes");
        let env = &mut self.env;
        let head = env.clients[0].chain.head().unwrap();
        let block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
        // check execution outcomes
        let shard_layout = env.clients[0]
            .epoch_manager
            .get_shard_layout_from_prev_block(&head.last_block_hash)
            .unwrap();
        let mut txs_to_check = vec![];
        txs_to_check.extend(&self.init_txs);
        for (_, txs) in self.txs_by_height.iter() {
            txs_to_check.extend(txs);
        }

        let mut successful_txs = HashMap::new();
        for tx in txs_to_check {
            let id = &tx.get_hash();

            let signer_account_id = &tx.transaction.signer_id;
            let shard_uid = account_id_to_shard_uid(signer_account_id, &shard_layout);

            tracing::trace!(target: "test", tx=?id, ?signer_account_id, ?shard_uid, "checking tx");

            for (i, validator_account_id) in env.validators.iter().enumerate() {
                let client = &env.clients[i];

                let cares_about_shard = client.shard_tracker.care_about_shard(
                    Some(validator_account_id),
                    block.header().prev_hash(),
                    shard_uid.shard_id(),
                    true,
                );
                if !cares_about_shard {
                    continue;
                }
                let execution_outcomes = client.chain.get_transaction_execution_result(id).unwrap();
                if execution_outcomes.is_empty() {
                    tracing::error!(target: "test", tx=?id, client=i, "tx not processed");
                    assert!(allow_not_started, "tx {:?} not processed", id);
                    continue;
                }
                let final_outcome = client.chain.get_final_transaction_result(id).unwrap();
                for outcome in &final_outcome.receipts_outcome {
                    assert_matches!(
                        outcome.outcome.status,
                        ExecutionStatusView::SuccessValue(_)
                            | ExecutionStatusView::SuccessReceiptId(_)
                    );
                }

                let outcome_status = final_outcome.status.clone();
                if matches!(outcome_status, FinalExecutionStatus::SuccessValue(_)) {
                    successful_txs.insert(tx.get_hash(), final_outcome);
                } else {
                    tracing::error!(target: "test", tx=?id, client=i, "tx failed");
                    panic!("tx failed {:?}", final_outcome);
                }
            }
        }

        successful_txs
    }

    // Check the receipt_id_to_shard_id mappings are correct for all outgoing receipts in the
    // latest block
    fn check_receipt_id_to_shard_id(&mut self) {
        let env = &mut self.env;
        let head = env.clients[0].chain.head().unwrap();
        let shard_layout = env.clients[0]
            .epoch_manager
            .get_shard_layout_from_prev_block(&head.last_block_hash)
            .unwrap();
        let block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();

        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            if chunk_header.height_included() != block.header().height() {
                continue;
            }
            let shard_id = shard_id as ShardId;

            for (i, me) in env.validators.iter().enumerate() {
                let client = &mut env.clients[i];
                let care_about_shard = client.shard_tracker.care_about_shard(
                    Some(me),
                    &head.prev_block_hash,
                    shard_id,
                    true,
                );
                if !care_about_shard {
                    continue;
                }

                let outgoing_receipts = client
                    .chain
                    .mut_chain_store()
                    .get_outgoing_receipts(&head.last_block_hash, shard_id)
                    .unwrap()
                    .clone();
                for receipt in outgoing_receipts.iter() {
                    let target_shard_id =
                        client.chain.get_shard_id_for_receipt_id(&receipt.receipt_id).unwrap();
                    assert_eq!(
                        target_shard_id,
                        account_id_to_shard_id(&receipt.receiver_id, &shard_layout)
                    );
                }
            }
        }
    }

    /// Check that after resharding is finished, the artifacts stored in storage is removed
    fn check_resharding_artifacts(&mut self, client_id: usize) {
        tracing::debug!(target: "test", "checking resharding artifacts");

        let client = &mut self.env.clients[client_id];
        let head = client.chain.head().unwrap();
        for height in 0..head.height {
            let (block_hash, num_shards) = {
                let block = client.chain.get_block_by_height(height).unwrap();
                (*block.hash(), block.chunks().len() as NumShards)
            };
            for shard_id in 0..num_shards {
                let res = client
                    .chain
                    .chain_store()
                    .get_state_changes_for_resharding(&block_hash, shard_id);
                assert_matches!(res, Err(error) => {
                    assert_matches!(error, Error::DBNotFoundErr(_));
                })
            }
        }
    }

    fn check_outgoing_receipts_reassigned(&self, resharding_type: &ReshardingType) {
        tracing::debug!(target: "test", "checking outgoing receipts reassigned");
        let env = &self.env;

        // height 20 is after the resharding is finished
        let num_shards = get_expected_shards_num(self.epoch_length, 20, resharding_type);

        // last height before resharding took place
        let last_height_included = 10;
        for client in &env.clients {
            for shard_id in 0..num_shards {
                check_outgoing_receipts_reassigned_impl(
                    client,
                    shard_id,
                    last_height_included,
                    resharding_type,
                );
            }
        }
    }

    // function to check whether the state snapshot exists or not and whether it exists at the correct
    // hash for both cases when state snapshot is enabled and disabled
    fn check_snapshot(&mut self, state_snapshot_enabled: bool) {
        let env = &mut self.env;
        let head = env.clients[0].chain.head().unwrap();
        let block_header = env.clients[0].chain.get_block_header(&head.prev_block_hash).unwrap();
        let snapshot = env.clients[0]
            .chain
            .runtime_adapter
            .get_tries()
            .get_state_snapshot(&block_header.prev_hash());

        // There are two main cases we would like to check, when state_snapshot is enabled and disabled
        // 1. with state snapshot enabled, we expect to create a new snapshot with every epoch boundry
        // 2. with state snapshot disabled, we should ONLY be creating the snapshot at the first epoch boundry
        //    i.e. when resharding happens, and subsequently, at the next epoch boundry, the state snapshot
        //    must be deleted.
        if head.height <= self.epoch_length {
            // No snapshot for height less than epoch length, i.e. first epoch
            assert!(snapshot
                .is_err_and(|e| e == SnapshotError::SnapshotNotFound(*block_header.prev_hash())));
        } else if head.height == self.epoch_length + 1 {
            // At the epoch boundary, right before resharding, snapshot should exist
            assert!(snapshot.is_ok());
        } else if head.height <= 2 * self.epoch_length {
            // In the resharding epoch there are two cases to consider
            // * While the resharding is in progress, snapshot should exist but
            //   not at the current block hash and we should get
            //   IncorrectSnapshotRequested
            // * Once resharding is finished the snapshot should not exist at
            //   all and we should get SnapshotNotFound.
            let snapshot_block_header =
                env.clients[0].chain.get_block_header_by_height(self.epoch_length).unwrap();
            let Err(err) = snapshot else { panic!("snapshot should not exist at given hash") };

            match err {
                SnapshotError::IncorrectSnapshotRequested(requested, current) => {
                    assert_eq!(requested, *block_header.prev_hash());
                    assert_eq!(current, *snapshot_block_header.prev_hash());
                }
                SnapshotError::SnapshotNotFound(requested) => {
                    assert_eq!(requested, *block_header.prev_hash());
                }
                err => {
                    panic!("unexpected snapshot error: {:?}", err);
                }
            }
        } else if (head.height - 1) % self.epoch_length == 0 {
            // At other epoch boundries, snapshot should exist only if state_snapshot_enabled is true
            assert_eq!(state_snapshot_enabled, snapshot.is_ok());
        } else {
            // And for the rest of the height which are not at the epoch boundary, snapshot should
            // 1. Either not exist if state_snapshot_enabled is false OR
            // 2. snapshot should exist (but with hash as of last block of prev epoch) if state_snapshot is enabled
            assert!(snapshot.is_err_and(|err| {
                match err {
                    SnapshotError::SnapshotNotFound(_) => !state_snapshot_enabled,
                    SnapshotError::IncorrectSnapshotRequested(_, _) => state_snapshot_enabled,
                    _ => false,
                }
            }));
        }
    }

    fn check_trie_and_flat_state(&self, expect_deleted: bool) {
        let tries = self.env.clients[0].chain.runtime_adapter.get_tries();
        let flat_storage_manager =
            self.env.clients[0].chain.runtime_adapter.get_flat_storage_manager();
        let store = tries.get_store();
        for shard_uid in get_parent_shard_uids(&self.resharding_type.unwrap()) {
            // verify we have no keys in State and FlatState column
            let key_prefix = shard_uid.to_bytes();
            if expect_deleted {
                assert!(store.iter_prefix(DBCol::State, &key_prefix).next().is_none());
                assert!(store.iter_prefix(DBCol::FlatState, &key_prefix).next().is_none());
            }
            // verify that flat storage status says Empty
            let status = flat_storage_manager.get_flat_storage_status(shard_uid);
            match status {
                FlatStorageStatus::Empty => assert!(expect_deleted, "flat storage status Empty"),
                _ => assert!(!expect_deleted, "unexpected flat storage status: {:?}", status),
            }
        }
    }
}

// Returns the block producer for the next block after the current head.
fn get_block_producer(env: &TestEnv, head: &Tip) -> AccountId {
    let client = &env.clients[0];
    let epoch_manager = &client.epoch_manager;
    let parent_hash = &head.last_block_hash;
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash).unwrap();
    let height = head.height + 1;
    let block_producer = epoch_manager.get_block_producer(&epoch_id, height).unwrap();
    block_producer
}

// Returns the chunk producer for the next chunk after the given block.
fn get_chunk_producer(env: &TestEnv, block: &Block, shard_id: ShardId) -> AccountId {
    let client = &env.clients[0];
    let epoch_manager = &client.epoch_manager;
    let parent_hash = block.header().prev_hash();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash).unwrap();
    let height = block.header().height() + 1;
    let chunk_producer = epoch_manager.get_chunk_producer(&epoch_id, height, shard_id).unwrap();
    chunk_producer
}

fn check_outgoing_receipts_reassigned_impl(
    client: &Client,
    shard_id: ShardId,
    last_height_included: BlockHeight,
    resharding_type: &ReshardingType,
) {
    let chain = &client.chain;
    let prev_block = chain.get_block_by_height(last_height_included).unwrap();
    let prev_block_hash = *prev_block.hash();

    let outgoing_receipts = chain
        .get_outgoing_receipts_for_shard(prev_block_hash, shard_id, last_height_included)
        .unwrap();
    let shard_layout =
        client.epoch_manager.get_shard_layout_from_prev_block(&prev_block_hash).unwrap();

    match resharding_type {
        ReshardingType::V1 => {
            // In V0->V1 resharding the outgoing receipts should be reassigned
            // to the receipt receiver's shard id.
            for receipt in outgoing_receipts {
                let receiver = receipt.receiver_id;
                let receiver_shard_id = account_id_to_shard_id(&receiver, &shard_layout);
                assert_eq!(receiver_shard_id, shard_id);
            }
        }
        ReshardingType::V2 => {
            // In V1->V2 resharding the outgoing receipts should be reassigned
            // to the lowest index child of the parent shard.
            // We can't directly check that here but we can check that the
            // non-lowest-index shards are not assigned any receipts.
            // We check elsewhere that no receipts are lost so this should be sufficient.
            if shard_id == 4 {
                assert!(outgoing_receipts.is_empty());
            }
        }
        ReshardingType::V3 => {
            // In V2->V3 resharding the outgoing receipts should be reassigned
            // to the lowest index child of the parent shard.
            // We can't directly check that here but we can check that the
            // non-lowest-index shards are not assigned any receipts.
            // We check elsewhere that no receipts are lost so this should be sufficient.
            if shard_id == 3 {
                assert!(outgoing_receipts.is_empty());
            }
        }
        #[cfg(feature = "nightly")]
        ReshardingType::TESTONLY => {
            // In V3->TESTONLY resharding the outgoing receipts should be reassigned
            // to the lowest index child of the parent shard.
            // We can't directly check that here but we can check that the
            // non-lowest-index shards are not assigned any receipts.
            // We check elsewhere that no receipts are lost so this should be sufficient.
            if shard_id == 5 {
                assert!(outgoing_receipts.is_empty());
            }
        }
    }
}

/// Checks that account exists in the state after `block` is processed
/// This function checks both state_root from chunk extra and state root from chunk header, if
/// the corresponding chunk is included in the block
fn check_account(env: &TestEnv, account_id: &AccountId, block: &Block) {
    tracing::trace!(target: "test", ?account_id, block_height=block.header().height(), "checking account");
    let prev_hash = block.header().prev_hash();
    let shard_layout =
        env.clients[0].epoch_manager.get_shard_layout_from_prev_block(prev_hash).unwrap();
    let shard_uid = account_id_to_shard_uid(account_id, &shard_layout);
    let shard_id = shard_uid.shard_id();
    for (i, me) in env.validators.iter().enumerate() {
        let client = &env.clients[i];
        let care_about_shard =
            client.shard_tracker.care_about_shard(Some(me), prev_hash, shard_id, true);
        if !care_about_shard {
            continue;
        }
        let chunk_extra = &client.chain.get_chunk_extra(block.hash(), &shard_uid).unwrap();
        let state_root = *chunk_extra.state_root();
        client
            .runtime_adapter
            .query(
                shard_uid,
                &state_root,
                block.header().height(),
                0,
                prev_hash,
                block.hash(),
                block.header().epoch_id(),
                &QueryRequest::ViewAccount { account_id: account_id.clone() },
            )
            .unwrap();

        let chunk = &block.chunks()[shard_id as usize];
        if chunk.height_included() == block.header().height() {
            client
                .runtime_adapter
                .query(
                    shard_uid,
                    &chunk.prev_state_root(),
                    block.header().height(),
                    0,
                    block.header().prev_hash(),
                    block.hash(),
                    block.header().epoch_id(),
                    &QueryRequest::ViewAccount { account_id: account_id.clone() },
                )
                .unwrap();
        }
    }
}

fn setup_genesis(
    epoch_length: u64,
    num_validators: u64,
    initial_accounts: Vec<AccountId>,
    gas_limit: Option<u64>,
    genesis_protocol_version: ProtocolVersion,
) -> Genesis {
    let mut genesis = Genesis::test(initial_accounts, num_validators);
    // No kickout, since we are going to test missing chunks.
    // Same needs to be set in the AllEpochConfigTestOverrides.
    genesis.config.block_producer_kickout_threshold = 0;
    genesis.config.chunk_producer_kickout_threshold = 0;
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = genesis_protocol_version;
    genesis.config.use_production_config = true;
    if let Some(gas_limit) = gas_limit {
        genesis.config.gas_limit = gas_limit;
    }

    // The block producer assignment often happens to be unlucky enough to not
    // include one of the validators in the first epoch. When that happens the
    // new protocol version gets only 75% of the votes which is lower that the
    // default 80% threshold for upgrading. Delaying the upgrade also delays
    // resharding and makes it harder to predict. The threshold is set slightly
    // lower here to always ensure that upgrade takes place as soon as possible.
    // This was not fun to debug.
    genesis.config.protocol_upgrade_stake_threshold = Rational32::new(7, 10);

    let default_epoch_config = EpochConfig::from(&genesis.config);
    let all_epoch_config = AllEpochConfig::new(true, default_epoch_config, "test-chain");
    let epoch_config = all_epoch_config.for_protocol_version(genesis_protocol_version);

    genesis.config.shard_layout = epoch_config.shard_layout;
    genesis.config.num_block_producer_seats_per_shard =
        epoch_config.num_block_producer_seats_per_shard;
    genesis.config.avg_hidden_validator_seats_per_shard =
        epoch_config.avg_hidden_validator_seats_per_shard;

    genesis
}

fn generate_create_accounts_txs(
    mut rng: &mut StdRng,
    genesis_hash: CryptoHash,
    initial_accounts: &Vec<AccountId>,
    accounts_to_check: &mut Vec<AccountId>,
    all_accounts: &mut HashSet<AccountId>,
    nonce: &mut u64,
    max_size: usize,
    check_accounts: bool,
) -> Vec<SignedTransaction> {
    let size = rng.gen_range(0..max_size) + 1;
    std::iter::repeat_with(|| loop {
        let signer_account = initial_accounts.choose(&mut rng).unwrap();
        let signer0 = InMemorySigner::from_seed(
            signer_account.clone(),
            KeyType::ED25519,
            signer_account.as_ref(),
        );
        let account_id = gen_account(&mut rng);
        if all_accounts.insert(account_id.clone()) {
            let signer = InMemorySigner::from_seed(
                account_id.clone(),
                KeyType::ED25519,
                account_id.as_ref(),
            );
            let tx = SignedTransaction::create_account(
                *nonce,
                signer_account.clone(),
                account_id.clone(),
                NEAR_BASE,
                signer.public_key(),
                &signer0,
                genesis_hash,
            );
            if check_accounts {
                accounts_to_check.push(account_id.clone());
            }
            *nonce += 1;
            tracing::trace!(target: "test", ?account_id, tx=?tx.get_hash(), "adding create account tx");
            return tx;
        }
    })
    .take(size)
    .collect()
}

fn test_shard_layout_upgrade_simple_impl(
    resharding_type: ReshardingType,
    rng_seed: u64,
    state_snapshot_enabled: bool,
) {
    init_test_logger();
    tracing::info!(target: "test", "test_shard_layout_upgrade_simple_impl starting");

    let genesis_protocol_version = get_genesis_protocol_version(&resharding_type);
    let target_protocol_version = get_target_protocol_version(&resharding_type);

    // setup
    let epoch_length = 5;
    let mut test_env = TestReshardingEnv::new(
        epoch_length,
        2,
        2,
        100,
        None,
        genesis_protocol_version,
        rng_seed,
        state_snapshot_enabled,
        Some(resharding_type),
    );
    test_env.set_init_tx(vec![]);

    let mut nonce = 100;
    let genesis_hash = *test_env.env.clients[0].chain.genesis_block().hash();
    let mut all_accounts: HashSet<_> = test_env.initial_accounts.clone().into_iter().collect();
    let mut accounts_to_check: Vec<_> = vec![];
    let initial_accounts = test_env.initial_accounts.clone();

    // add transactions until after sharding upgrade finishes
    for height in 2..3 * epoch_length {
        let txs = generate_create_accounts_txs(
            &mut test_env.rng,
            genesis_hash,
            &initial_accounts,
            &mut accounts_to_check,
            &mut all_accounts,
            &mut nonce,
            10,
            true,
        );

        test_env.set_tx_at_height(height, txs);
    }

    let drop_chunk_condition = DropChunkCondition::new();
    for _ in 1..4 * epoch_length {
        test_env.step(&drop_chunk_condition, target_protocol_version);
        test_env.check_receipt_id_to_shard_id();
        test_env.check_snapshot(state_snapshot_enabled);
    }

    test_env.check_tx_outcomes(false);
    test_env.check_accounts(accounts_to_check.iter().collect());
    test_env.check_resharding_artifacts(0);
    test_env.check_outgoing_receipts_reassigned(&resharding_type);
    tracing::info!(target: "test", "test_shard_layout_upgrade_simple_impl finished");
}

// TODO(congestion_control) - integration with resharding
// set congestion control for resharding and un-ignore all integration tests

#[ignore]
#[test]
fn test_shard_layout_upgrade_simple_v1() {
    test_shard_layout_upgrade_simple_impl(ReshardingType::V1, 42, false);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_simple_v1_with_snapshot_enabled() {
    test_shard_layout_upgrade_simple_impl(ReshardingType::V1, 42, true);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_simple_v2_seed_42() {
    test_shard_layout_upgrade_simple_impl(ReshardingType::V2, 42, false);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_simple_v2_seed_43() {
    test_shard_layout_upgrade_simple_impl(ReshardingType::V2, 43, false);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_simple_v2_seed_44() {
    test_shard_layout_upgrade_simple_impl(ReshardingType::V2, 44, false);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_simple_v3_seed_42() {
    test_shard_layout_upgrade_simple_impl(ReshardingType::V3, 42, false);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_simple_v3_seed_43() {
    test_shard_layout_upgrade_simple_impl(ReshardingType::V3, 43, false);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_simple_v3_seed_44() {
    test_shard_layout_upgrade_simple_impl(ReshardingType::V3, 44, false);
}

#[cfg(feature = "nightly")]
#[ignore]
#[test]
fn test_shard_layout_upgrade_simple_testonly_seed_42() {
    test_shard_layout_upgrade_simple_impl(ReshardingType::TESTONLY, 42, false);
}

#[cfg(feature = "nightly")]
#[ignore]
#[test]
fn test_shard_layout_upgrade_simple_testonly_seed_43() {
    test_shard_layout_upgrade_simple_impl(ReshardingType::TESTONLY, 43, false);
}

#[cfg(feature = "nightly")]
#[ignore]
#[test]
fn test_shard_layout_upgrade_simple_testonly_seed_44() {
    test_shard_layout_upgrade_simple_impl(ReshardingType::TESTONLY, 44, false);
}

fn test_resharding_with_different_db_kind_impl(resharding_type: ReshardingType) {
    init_test_logger();

    let genesis_protocol_version = get_genesis_protocol_version(&resharding_type);
    let target_protocol_version = get_target_protocol_version(&resharding_type);

    let epoch_length = 5;
    let mut test_env = TestReshardingEnv::new(
        epoch_length,
        3,
        3,
        10,
        None,
        genesis_protocol_version,
        42,
        true,
        Some(resharding_type),
    );

    // Set three different DbKind versions
    test_env.env.clients[0].chain.chain_store().store().set_db_kind(DbKind::Hot).unwrap();
    test_env.env.clients[1].chain.chain_store().store().set_db_kind(DbKind::Archive).unwrap();
    test_env.env.clients[2].chain.chain_store().store().set_db_kind(DbKind::RPC).unwrap();

    let drop_chunk_condition = DropChunkCondition::new();
    for _ in 1..4 * epoch_length {
        test_env.step(&drop_chunk_condition, target_protocol_version);
    }

    test_env.check_resharding_artifacts(0);
    test_env.check_resharding_artifacts(1);
    test_env.check_resharding_artifacts(2);
}

#[ignore]
#[test]
fn test_resharding_with_different_db_kind_v2() {
    test_resharding_with_different_db_kind_impl(ReshardingType::V2);
}

#[ignore]
#[test]
fn test_resharding_with_different_db_kind_v3() {
    test_resharding_with_different_db_kind_impl(ReshardingType::V3);
}

#[cfg(feature = "nightly")]
#[ignore]
#[test]
fn test_resharding_with_different_db_kind_testonly() {
    test_resharding_with_different_db_kind_impl(ReshardingType::TESTONLY);
}

/// In this test we are checking whether we are properly deleting trie state and flat state
/// from the old shard layout after resharding. This is handled as a part of Garbage Collection (GC)
fn test_shard_layout_upgrade_gc_impl(resharding_type: ReshardingType, rng_seed: u64) {
    init_test_logger();
    tracing::info!(target: "test", "test_shard_layout_upgrade_gc_impl starting");

    let genesis_protocol_version = get_genesis_protocol_version(&resharding_type);
    let target_protocol_version = get_target_protocol_version(&resharding_type);

    let epoch_length = 5;
    let mut test_env: TestReshardingEnv = TestReshardingEnv::new(
        epoch_length,
        2,
        2,
        100,
        None,
        genesis_protocol_version,
        rng_seed,
        false,
        Some(resharding_type),
    );

    // GC period is about 5 epochs. We should expect to see state deleted at the end of the 7th epoch
    // Epoch 0, blocks 1-5  : genesis shard layout
    // Epoch 1, blocks 6-10 : genesis shard layout, resharding happens
    // Epoch 2: blocks 10-15: target shard layout, shard layout is upgraded
    // Epoch 3-7: target shard layout, waiting for GC to happen
    // Epoch 8: block 37: GC happens, state is deleted
    let drop_chunk_condition = DropChunkCondition::new();
    for _ in 0..7 * epoch_length + 1 {
        // we expect the trie state and flat state to NOT be deleted before GC
        test_env.check_trie_and_flat_state(false);
        test_env.step(&drop_chunk_condition, target_protocol_version);
    }
    // Once GC is done, we expect the trie state and flat state to be deleted
    test_env.check_trie_and_flat_state(true);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_gc() {
    test_shard_layout_upgrade_gc_impl(ReshardingType::V1, 44);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_gc_v2() {
    test_shard_layout_upgrade_gc_impl(ReshardingType::V2, 44);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_gc_v3() {
    test_shard_layout_upgrade_gc_impl(ReshardingType::V3, 44);
}

#[cfg(feature = "nightly")]
#[ignore]
#[test]
fn test_shard_layout_upgrade_gc_testonly() {
    test_shard_layout_upgrade_gc_impl(ReshardingType::TESTONLY, 44);
}

const GAS_1: u64 = 300_000_000_000_000;
const GAS_2: u64 = GAS_1 / 3;

fn create_test_env_for_cross_contract_test(
    genesis_protocol_version: ProtocolVersion,
    epoch_length: u64,
    rng_seed: u64,
    resharding_type: Option<ReshardingType>,
) -> TestReshardingEnv {
    TestReshardingEnv::new(
        epoch_length,
        4,
        4,
        100,
        Some(100_000_000_000_000),
        genesis_protocol_version,
        rng_seed,
        false,
        resharding_type,
    )
}

/// Return test_env and a map from tx hash to the new account that will be added by this transaction
fn setup_test_env_with_cross_contract_txs(
    test_env: &mut TestReshardingEnv,
    epoch_length: u64,
) -> HashMap<CryptoHash, AccountId> {
    let genesis_hash = *test_env.env.clients[0].chain.genesis_block().hash();

    // Use the shard accounts where there should be one account per shard to get
    // traffic in every shard.
    let contract_accounts = gen_shard_accounts();

    let mut init_txs = vec![];
    for account_id in &contract_accounts {
        let signer =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, account_id.as_ref());
        let actions = vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::backwards_compatible_rs_contract().to_vec(),
        })];
        let tx = SignedTransaction::from_actions(
            1,
            account_id.clone(),
            account_id.clone(),
            &signer,
            actions,
            genesis_hash,
        );
        init_txs.push(tx);
    }
    test_env.set_init_tx(init_txs);

    let mut nonce = 100;
    let mut all_accounts: HashSet<_> = test_env.initial_accounts.clone().into_iter().collect();
    let mut new_accounts = HashMap::new();

    // add a bunch of transactions before the two epoch boundaries
    for height in [
        epoch_length - 3,
        epoch_length - 2,
        epoch_length - 1,
        epoch_length,
        2 * epoch_length - 3,
        2 * epoch_length - 2,
        2 * epoch_length - 1,
        2 * epoch_length,
    ] {
        let txs = generate_cross_contract_txs(
            &mut test_env.rng,
            genesis_hash,
            &contract_accounts,
            &mut all_accounts,
            &mut new_accounts,
            &mut nonce,
            15,
            20,
        );

        test_env.set_tx_at_height(height, txs);
    }

    // adds some transactions after sharding change finishes
    // but do not add too many because I want all transactions to
    // finish processing before epoch 5
    for height in 2 * epoch_length + 1..3 * epoch_length {
        if test_env.rng.gen_bool(0.3) {
            let txs = generate_cross_contract_txs(
                &mut test_env.rng,
                genesis_hash,
                &contract_accounts,
                &mut all_accounts,
                &mut new_accounts,
                &mut nonce,
                5,
                8,
            );
            test_env.set_tx_at_height(height, txs);
        }
    }

    new_accounts
}

fn generate_cross_contract_txs(
    rng: &mut StdRng,
    genesis_hash: CryptoHash,
    contract_accounts: &Vec<AccountId>,
    all_accounts: &mut HashSet<AccountId>,
    new_accounts: &mut HashMap<CryptoHash, AccountId>,
    nonce: &mut u64,
    min_size: usize,
    max_size: usize,
) -> Vec<SignedTransaction> {
    let size = rng.gen_range(min_size..max_size + 1);
    let mut transactions = vec![];
    for _ in 0..size {
        if let Some(tx) = generate_cross_contract_tx(
            rng,
            genesis_hash,
            contract_accounts,
            all_accounts,
            new_accounts,
            nonce,
        ) {
            transactions.push(tx);
        }
    }
    transactions
}

fn generate_cross_contract_tx(
    rng: &mut StdRng,
    genesis_hash: CryptoHash,
    contract_accounts: &Vec<AccountId>,
    all_accounts: &mut HashSet<AccountId>,
    new_accounts: &mut HashMap<CryptoHash, AccountId>,
    nonce: &mut u64,
) -> Option<SignedTransaction> {
    let account_id = gen_account(rng);
    if !all_accounts.insert(account_id.clone()) {
        return None;
    }
    *nonce += 1;
    // randomly shuffle contract accounts
    // so that transactions are send to different shards
    let mut contract_accounts = contract_accounts.clone();
    contract_accounts.shuffle(rng);
    let tx = gen_cross_contract_tx_impl(
        &contract_accounts[0],
        &contract_accounts[1],
        &contract_accounts[2],
        &contract_accounts[3],
        &account_id,
        *nonce,
        &genesis_hash,
    );
    new_accounts.insert(tx.get_hash(), account_id);
    tracing::trace!(target: "test", tx=?tx.get_hash(), "generated tx");
    Some(tx)
}

// create a transaction signed by `account0` and calls a contract on `account1`
// the contract creates a promise that executes a cross contract call on "account2"
// then executes another contract call on "account3" that creates a new account
fn gen_cross_contract_tx_impl(
    account0: &AccountId,
    account1: &AccountId,
    account2: &AccountId,
    account3: &AccountId,
    new_account: &AccountId,
    nonce: u64,
    block_hash: &CryptoHash,
) -> SignedTransaction {
    let signer0 = InMemorySigner::from_seed(account0.clone(), KeyType::ED25519, account0.as_ref());
    let signer_new_account =
        InMemorySigner::from_seed(new_account.clone(), KeyType::ED25519, new_account.as_ref());
    let data = serde_json::json!([
        {"create": {
        "account_id": account2.to_string(),
        "method_name": "call_promise",
        "arguments": [],
        "amount": "0",
        "gas": GAS_2,
        }, "id": 0 },
        {"then": {
        "promise_index": 0,
        "account_id": account3.to_string(),
        "method_name": "call_promise",
        "arguments": [
                {"batch_create": { "account_id": new_account.to_string() }, "id": 0 },
                {"action_create_account": {
                    "promise_index": 0, },
                    "id": 0 },
                {"action_transfer": {
                    "promise_index": 0,
                    "amount": NEAR_BASE.to_string(),
                }, "id": 0 },
                {"action_add_key_with_full_access": {
                    "promise_index": 0,
                    "public_key": to_base64(&borsh::to_vec(&signer_new_account.public_key).unwrap()),
                    "nonce": 0,
                }, "id": 0 }
            ],
        "amount": NEAR_BASE.to_string(),
        "gas": GAS_2,
        }, "id": 1}
    ]);

    SignedTransaction::from_actions(
        nonce,
        account0.clone(),
        account1.clone(),
        &signer0,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        }))],
        *block_hash,
    )
}

// Test cross contract calls
// This test case tests postponed receipts and delayed receipts
fn test_shard_layout_upgrade_cross_contract_calls_impl(
    resharding_type: ReshardingType,
    rng_seed: u64,
) {
    init_test_logger();

    // setup
    let epoch_length = 10;
    let genesis_protocol_version = get_genesis_protocol_version(&resharding_type);
    let target_protocol_version = get_target_protocol_version(&resharding_type);

    let mut test_env = create_test_env_for_cross_contract_test(
        genesis_protocol_version,
        epoch_length,
        rng_seed,
        Some(resharding_type),
    );

    let new_accounts = setup_test_env_with_cross_contract_txs(&mut test_env, epoch_length);

    let drop_chunk_condition = DropChunkCondition::new();
    for _ in 1..5 * epoch_length {
        test_env.step(&drop_chunk_condition, target_protocol_version);
        test_env.check_receipt_id_to_shard_id();
    }

    let successful_txs = test_env.check_tx_outcomes(false);
    let new_accounts =
        successful_txs.iter().flat_map(|(tx_hash, _)| new_accounts.get(tx_hash)).collect();

    test_env.check_accounts(new_accounts);

    test_env.check_resharding_artifacts(0);
}

// Test cross contract calls
// This test case tests postponed receipts and delayed receipts
#[ignore]
#[test]
fn test_shard_layout_upgrade_cross_contract_calls_v1() {
    test_shard_layout_upgrade_cross_contract_calls_impl(ReshardingType::V1, 42);
}

// Test cross contract calls
// This test case tests postponed receipts and delayed receipts
#[ignore]
#[test]
fn test_shard_layout_upgrade_cross_contract_calls_v2_seed_42() {
    test_shard_layout_upgrade_cross_contract_calls_impl(ReshardingType::V2, 42);
}

// Test cross contract calls
// This test case tests postponed receipts and delayed receipts
#[ignore]
#[test]
fn test_shard_layout_upgrade_cross_contract_calls_v2_seed_43() {
    test_shard_layout_upgrade_cross_contract_calls_impl(ReshardingType::V2, 43);
}

// Test cross contract calls
// This test case tests postponed receipts and delayed receipts
#[ignore]
#[test]
fn test_shard_layout_upgrade_cross_contract_calls_v2_seed_44() {
    test_shard_layout_upgrade_cross_contract_calls_impl(ReshardingType::V2, 44);
}

// Test cross contract calls
// This test case tests postponed receipts and delayed receipts
#[ignore]
#[test]
fn test_shard_layout_upgrade_cross_contract_calls_v3_seed_42() {
    test_shard_layout_upgrade_cross_contract_calls_impl(ReshardingType::V3, 42);
}

// Test cross contract calls
// This test case tests postponed receipts and delayed receipts
#[ignore]
#[test]
fn test_shard_layout_upgrade_cross_contract_calls_v3_seed_43() {
    test_shard_layout_upgrade_cross_contract_calls_impl(ReshardingType::V3, 43);
}

// Test cross contract calls
// This test case tests postponed receipts and delayed receipts
#[ignore]
#[test]
fn test_shard_layout_upgrade_cross_contract_calls_v3_seed_44() {
    test_shard_layout_upgrade_cross_contract_calls_impl(ReshardingType::V3, 44);
}

// Test cross contract calls
// This test case tests postponed receipts and delayed receipts
#[cfg(feature = "nightly")]
#[ignore]
#[test]
fn test_shard_layout_upgrade_cross_contract_calls_testonly_seed_42() {
    test_shard_layout_upgrade_cross_contract_calls_impl(ReshardingType::TESTONLY, 42);
}

// Test cross contract calls
// This test case tests postponed receipts and delayed receipts
#[cfg(feature = "nightly")]
#[ignore]
#[test]
fn test_shard_layout_upgrade_cross_contract_calls_testonly_seed_43() {
    test_shard_layout_upgrade_cross_contract_calls_impl(ReshardingType::TESTONLY, 43);
}

// Test cross contract calls
// This test case tests postponed receipts and delayed receipts
#[cfg(feature = "nightly")]
#[ignore]
#[test]
fn test_shard_layout_upgrade_cross_contract_calls_testonly_seed_44() {
    test_shard_layout_upgrade_cross_contract_calls_impl(ReshardingType::TESTONLY, 44);
}

#[cfg(feature = "nightly")]
fn generate_yield_create_tx(
    account_id: &AccountId,
    callback_method_name: String,
    nonce: u64,
    block_hash: &CryptoHash,
) -> SignedTransaction {
    let signer =
        InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, account_id.as_ref());

    SignedTransaction::from_actions(
        nonce,
        account_id.clone(),
        account_id.clone(),
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: callback_method_name,
            args: vec![],
            gas: GAS_1,
            deposit: 0,
        }))],
        *block_hash,
    )
}

#[cfg(feature = "nightly")]
fn setup_test_env_with_promise_yield_txs(
    test_env: &mut TestReshardingEnv,
    epoch_length: u64,
) -> Vec<CryptoHash> {
    let genesis_hash = *test_env.env.clients[0].chain.genesis_block().hash();

    // Generates an account for each shard
    let contract_accounts = gen_shard_accounts();

    // Add transactions deploying nightly_rs_contract to each account
    let mut init_txs = vec![];
    for account_id in &contract_accounts {
        let signer =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, account_id.as_ref());
        let actions = vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::nightly_rs_contract().to_vec(),
        })];
        let init_tx = SignedTransaction::from_actions(
            1,
            account_id.clone(),
            account_id.clone(),
            &signer,
            actions,
            genesis_hash,
        );
        init_txs.push(init_tx);
    }
    test_env.set_init_tx(init_txs);

    let mut yield_tx_hashes = vec![];
    let mut nonce = 100;

    // In these tests we set the epoch length equal to the yield timeout length.
    assert!(
        epoch_length
            == RuntimeConfig::test().wasm_config.limit_config.yield_timeout_length_in_blocks,
    );
    // Add transactions invoking promise_yield_create near the epoch boundaries.
    for height in [
        epoch_length - 2, // create in first epoch, trigger timeout during resharding epoch
        epoch_length - 1,
        epoch_length,
        2 * epoch_length - 2, // create during resharding epoch, trigger timeout on upgraded layout
        2 * epoch_length - 1,
        2 * epoch_length,
        3 * epoch_length - 2, // both the create and the timeout will occur on upgraded layout
        3 * epoch_length - 1,
        3 * epoch_length,
    ] {
        let mut txs = vec![];
        for account_id in &contract_accounts {
            let tx = generate_yield_create_tx(
                account_id,
                "call_yield_create_return_promise".to_string(),
                nonce,
                &genesis_hash,
            );

            nonce += 1;

            yield_tx_hashes.push(tx.get_hash());
            txs.push(tx);
        }

        test_env.set_tx_at_height(height, txs);
    }

    yield_tx_hashes
}

// Test delivery of promise yield timeouts
#[cfg(feature = "nightly")]
fn test_shard_layout_upgrade_promise_yield_impl(resharding_type: ReshardingType, rng_seed: u64) {
    init_test_logger();

    // setup
    let epoch_length =
        RuntimeConfig::test().wasm_config.limit_config.yield_timeout_length_in_blocks;
    let genesis_protocol_version = get_genesis_protocol_version(&resharding_type);
    let target_protocol_version = get_target_protocol_version(&resharding_type);

    // reuse the test env for cross contract calls
    let mut test_env = create_test_env_for_cross_contract_test(
        genesis_protocol_version,
        epoch_length,
        rng_seed,
        Some(resharding_type),
    );

    let yield_tx_hashes = setup_test_env_with_promise_yield_txs(&mut test_env, epoch_length);

    let drop_chunk_condition = DropChunkCondition::new();
    for _ in 1..5 * epoch_length {
        test_env.step(&drop_chunk_condition, target_protocol_version);
        test_env.check_receipt_id_to_shard_id();
    }

    let tx_outcomes = test_env.check_tx_outcomes(false);
    for tx_hash in yield_tx_hashes {
        // The yield callback returns a specific value when it is invoked by timeout
        assert_eq!(
            tx_outcomes.get(&tx_hash).unwrap().status,
            FinalExecutionStatus::SuccessValue(vec![23u8]),
        );
    }

    test_env.check_resharding_artifacts(0);
}

#[cfg(feature = "nightly")]
#[ignore]
#[test]
fn test_shard_layout_upgrade_promise_yield() {
    test_shard_layout_upgrade_promise_yield_impl(ReshardingType::TESTONLY, 42);
}

fn test_shard_layout_upgrade_incoming_receipts_impl(
    resharding_type: ReshardingType,
    rng_seed: u64,
) {
    init_test_logger();

    // setup
    let epoch_length = 10;
    let genesis_protocol_version = get_genesis_protocol_version(&resharding_type);
    let target_protocol_version = get_target_protocol_version(&resharding_type);

    let mut test_env = create_test_env_for_cross_contract_test(
        genesis_protocol_version,
        epoch_length,
        rng_seed,
        Some(resharding_type),
    );

    let new_accounts = setup_test_env_with_cross_contract_txs(&mut test_env, epoch_length);

    // Drop one of the chunks in the last block before switching to the new
    // shard layout.
    let drop_height = 2 * epoch_length;
    let old_shard_num = get_expected_shards_num(epoch_length, drop_height, &resharding_type);
    let new_shard_num = get_expected_shards_num(epoch_length, drop_height + 1, &resharding_type);
    assert_ne!(old_shard_num, new_shard_num);

    // Drop the chunk from the shard with the highest shard id since it's the
    // one that is split in both V1 and V2 reshardings.
    let drop_shard_id = old_shard_num - 1;

    let by_height_shard_id = HashSet::from([(drop_height, drop_shard_id)]);
    let drop_chunk_condition = DropChunkCondition::with_by_height_shard_id(by_height_shard_id);
    for _ in 1..5 * epoch_length {
        test_env.step(&drop_chunk_condition, target_protocol_version);
        test_env.check_receipt_id_to_shard_id();
    }

    let successful_txs = test_env.check_tx_outcomes(false);
    let new_accounts =
        successful_txs.iter().flat_map(|(tx_hash, _)| new_accounts.get(tx_hash)).collect();

    test_env.check_accounts(new_accounts);
    test_env.check_resharding_artifacts(0);
}

// This test doesn't make much sense for the V1 resharding. That is because in
// V1 resharding there is only one shard before resharding. Even if that chunk
// is missing there aren't any other chunks so there aren't any incoming
// receipts at all.
#[ignore]
#[test]
fn test_shard_layout_upgrade_incoming_receipts_v1() {
    test_shard_layout_upgrade_incoming_receipts_impl(ReshardingType::V1, 42);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_incoming_receipts_v2_seed_42() {
    test_shard_layout_upgrade_incoming_receipts_impl(ReshardingType::V2, 42);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_incoming_receipts_v2_seed_43() {
    test_shard_layout_upgrade_incoming_receipts_impl(ReshardingType::V2, 43);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_incoming_receipts_v2_seed_44() {
    test_shard_layout_upgrade_incoming_receipts_impl(ReshardingType::V2, 44);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_incoming_receipts_v3_seed_42() {
    test_shard_layout_upgrade_incoming_receipts_impl(ReshardingType::V3, 42);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_incoming_receipts_v3_seed_43() {
    test_shard_layout_upgrade_incoming_receipts_impl(ReshardingType::V3, 43);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_incoming_receipts_v3_seed_44() {
    test_shard_layout_upgrade_incoming_receipts_impl(ReshardingType::V3, 44);
}

#[cfg(feature = "nightly")]
#[ignore]
#[test]
fn test_shard_layout_upgrade_incoming_receipts_testonly_seed_42() {
    test_shard_layout_upgrade_incoming_receipts_impl(ReshardingType::TESTONLY, 42);
}

#[cfg(feature = "nightly")]
#[ignore]
#[test]
fn test_shard_layout_upgrade_incoming_receipts_testonly_seed_43() {
    test_shard_layout_upgrade_incoming_receipts_impl(ReshardingType::TESTONLY, 43);
}

#[cfg(feature = "nightly")]
#[ignore]
#[test]
fn test_shard_layout_upgrade_incoming_receipts_testonly_seed_44() {
    test_shard_layout_upgrade_incoming_receipts_impl(ReshardingType::TESTONLY, 44);
}

// Test cross contract calls
// This test case tests when there are missing chunks in the produced blocks
// This is to test that all the chunk management logic is correct, e.g. inclusion of outgoing
// receipts into next chunks
fn test_missing_chunks(
    test_env: &mut TestReshardingEnv,
    p_missing: f64,
    target_protocol_version: ProtocolVersion,
    epoch_length: u64,
) {
    // setup
    let new_accounts = setup_test_env_with_cross_contract_txs(test_env, epoch_length);

    // randomly dropping chunks at the first few epochs when sharding splits happens
    // make sure initial txs (deploy smart contracts) are processed successfully
    let drop_chunk_condition = DropChunkCondition::new();
    for _ in 1..3 {
        test_env.step(&drop_chunk_condition, target_protocol_version);
    }

    let drop_chunk_condition = DropChunkCondition::with_probability(p_missing);
    for _ in 3..3 * epoch_length {
        test_env.step(&drop_chunk_condition, target_protocol_version);
        let last_height = test_env.env.clients[0].chain.head().unwrap().height;
        for height in last_height - 3..=last_height {
            test_env.check_next_block_with_new_chunk(height);
        }
        test_env.check_receipt_id_to_shard_id();
    }

    // make sure all included transactions finished processing
    let drop_chunk_condition = DropChunkCondition::new();
    for _ in 3 * epoch_length..5 * epoch_length {
        test_env.step(&drop_chunk_condition, target_protocol_version);
        let last_height = test_env.env.clients[0].chain.head().unwrap().height;
        for height in last_height - 3..=last_height {
            test_env.check_next_block_with_new_chunk(height);
        }
        test_env.check_receipt_id_to_shard_id();
    }

    let successful_txs = test_env.check_tx_outcomes(true);
    let new_accounts: Vec<_> =
        successful_txs.iter().flat_map(|(tx_hash, _)| new_accounts.get(tx_hash)).collect();
    test_env.check_accounts(new_accounts);

    test_env.check_resharding_artifacts(0);
}

fn test_shard_layout_upgrade_missing_chunks(
    resharding_type: ReshardingType,
    p_missing: f64,
    rng_seed: u64,
) {
    init_test_logger();

    let genesis_protocol_version = get_genesis_protocol_version(&resharding_type);
    let target_protocol_version = get_target_protocol_version(&resharding_type);
    let epoch_length = 10;

    let mut test_env = create_test_env_for_cross_contract_test(
        genesis_protocol_version,
        epoch_length,
        rng_seed,
        Some(resharding_type),
    );
    test_missing_chunks(&mut test_env, p_missing, target_protocol_version, epoch_length)
}

// Use resharding setup to run protocol for couple epochs with given probability
// of missing chunk. In particular, checks that all txs generated in env have
// final outcome in the end.
// TODO: remove logical dependency on resharding.
fn test_latest_protocol_missing_chunks(p_missing: f64, rng_seed: u64) {
    init_test_logger();
    let epoch_length = 10;
    let mut test_env =
        create_test_env_for_cross_contract_test(PROTOCOL_VERSION, epoch_length, rng_seed, None);
    test_missing_chunks(&mut test_env, p_missing, PROTOCOL_VERSION, epoch_length)
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_low_missing_prob_v1() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V1, 0.1, 42);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_mid_missing_prob_v1() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V1, 0.5, 42);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_high_missing_prob_v1() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V1, 0.9, 42);
}

// V2, low missing prob

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_low_missing_prob_v2_seed_42() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V2, 0.1, 42);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_low_missing_prob_v2_seed_43() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V2, 0.1, 43);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_low_missing_prob_v2_seed_44() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V2, 0.1, 44);
}

// V2, mid missing prob

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_mid_missing_prob_v2_seed_42() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V2, 0.5, 42);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_mid_missing_prob_v2_seed_43() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V2, 0.5, 43);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_mid_missing_prob_v2_seed_44() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V2, 0.5, 44);
}

// V2, high missing prob

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_high_missing_prob_v2_seed_42() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V2, 0.9, 42);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_high_missing_prob_v2_seed_43() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V2, 0.9, 43);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_high_missing_prob_v2_seed_44() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V2, 0.9, 44);
}

// V3 tests

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_low_missing_prob_v3() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V3, 0.1, 42);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_mid_missing_prob_v3() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V3, 0.5, 42);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_missing_chunks_high_missing_prob_v3() {
    test_shard_layout_upgrade_missing_chunks(ReshardingType::V3, 0.9, 42);
}

// latest protocol

#[ignore]
#[test]
fn test_latest_protocol_missing_chunks_low_missing_prob() {
    test_latest_protocol_missing_chunks(0.1, 25);
}

#[ignore]
#[test]
fn test_latest_protocol_missing_chunks_mid_missing_prob() {
    test_latest_protocol_missing_chunks(0.5, 26);
}

#[ignore]
#[test]
fn test_latest_protocol_missing_chunks_high_missing_prob() {
    test_latest_protocol_missing_chunks(0.9, 27);
}

fn test_shard_layout_upgrade_error_handling_impl(
    resharding_type: ReshardingType,
    rng_seed: u64,
    state_snapshot_enabled: bool,
) {
    init_test_logger();
    tracing::info!(target: "test", "test_shard_layout_upgrade_simple_impl starting");

    let genesis_protocol_version = get_genesis_protocol_version(&resharding_type);
    let target_protocol_version = get_target_protocol_version(&resharding_type);

    // setup
    let epoch_length = 5;
    let mut test_env = TestReshardingEnv::new(
        epoch_length,
        2,
        2,
        100,
        None,
        genesis_protocol_version,
        rng_seed,
        state_snapshot_enabled,
        Some(resharding_type),
    );
    test_env.set_init_tx(vec![]);

    let mut nonce = 100;
    let genesis_hash = *test_env.env.clients[0].chain.genesis_block().hash();
    let mut all_accounts: HashSet<_> = test_env.initial_accounts.clone().into_iter().collect();
    let mut accounts_to_check: Vec<_> = vec![];
    let initial_accounts = test_env.initial_accounts.clone();

    // add transactions until after sharding upgrade finishes
    for height in 2..3 * epoch_length {
        let txs = generate_create_accounts_txs(
            &mut test_env.rng,
            genesis_hash,
            &initial_accounts,
            &mut accounts_to_check,
            &mut all_accounts,
            &mut nonce,
            10,
            true,
        );

        test_env.set_tx_at_height(height, txs);
    }

    let drop_chunk_condition = DropChunkCondition::new();
    for _ in 1..4 * epoch_length {
        let result = test_env.step_err(&drop_chunk_condition, target_protocol_version);
        if let Err(err) = result {
            tracing::info!(target: "test", ?err, "step failed, as expected");
            return;
        }

        // corrupt the state snapshot if available to make resharding fail
        corrupt_state_snapshot(&test_env);
    }

    assert!(false, "no error was recorded, something is wrong in error handling");
}

fn corrupt_state_snapshot(test_env: &TestReshardingEnv) {
    let tries = test_env.env.clients[0].runtime_adapter.get_tries();
    let Ok(snapshot_hash) = tries.get_state_snapshot_hash() else { return };
    let (store, flat_storage_manager) = tries.get_state_snapshot(&snapshot_hash).unwrap();
    let shard_uids = flat_storage_manager.get_shard_uids();
    let mut store_update = store.store_update();
    for shard_uid in shard_uids {
        flat_storage_manager.remove_flat_storage_for_shard(shard_uid, &mut store_update).unwrap();
    }
    store_update.commit().unwrap();
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_error_handling_v1() {
    test_shard_layout_upgrade_error_handling_impl(ReshardingType::V1, 42, false);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_error_handling_v2() {
    test_shard_layout_upgrade_error_handling_impl(ReshardingType::V2, 42, false);
}

#[ignore]
#[test]
fn test_shard_layout_upgrade_error_handling_v3() {
    test_shard_layout_upgrade_error_handling_impl(ReshardingType::V3, 42, false);
}

// TODO(resharding) add a test with missing blocks
// TODO(resharding) add a test with deleting accounts and delayed receipts check
