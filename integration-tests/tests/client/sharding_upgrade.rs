use borsh::BorshSerialize;

use crate::process_blocks::{create_nightshade_runtimes, set_block_protocol_version};
use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_logger_utils::init_test_logger;
use near_primitives::account::id::AccountId;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout, ShardUId};
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives::types::ProtocolVersion;
use near_primitives::version::ProtocolFeature;
use near_primitives::views::QueryRequest;
use near_primitives::views::{ExecutionStatusView, FinalExecutionStatus};
use near_store::test_utils::{gen_account, gen_unique_accounts};
use nearcore::config::GenesisExt;
use nearcore::NEAR_BASE;
use tracing::debug;

use assert_matches::assert_matches;
use near_store::get_delayed_receipt_indices;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use std::collections::{HashMap, HashSet};

const SIMPLE_NIGHTSHADE_PROTOCOL_VERSION: ProtocolVersion =
    ProtocolFeature::SimpleNightshade.protocol_version();

const P_CATCHUP: f64 = 0.2;

struct TestShardUpgradeEnv {
    env: TestEnv,
    initial_accounts: Vec<AccountId>,
    init_txs: Vec<SignedTransaction>,
    txs_by_height: HashMap<u64, Vec<SignedTransaction>>,
    epoch_length: u64,
    num_validators: usize,
    num_clients: usize,
}

/// Test shard layout upgrade. This function runs `env` to produce and process blocks
/// from 1 to 3 * epoch_length + 1, ie, to the beginning of epoch 3.
/// Epoch 0: 1 shard
/// Epoch 1: 1 shard, state split happens
/// Epoch 2: shard layout upgrades to simple_night_shade_shard,
impl TestShardUpgradeEnv {
    fn new(
        epoch_length: u64,
        num_validators: usize,
        num_clients: usize,
        num_init_accounts: usize,
        gas_limit: Option<u64>,
    ) -> Self {
        let mut rng = thread_rng();
        let validators: Vec<AccountId> = (0..num_validators)
            .map(|i| format!("test{}", i).to_string().parse().unwrap())
            .collect();
        let initial_accounts =
            [validators, gen_unique_accounts(&mut rng, num_init_accounts)].concat();
        let genesis =
            setup_genesis(epoch_length, num_validators as u64, initial_accounts.clone(), gas_limit);
        let chain_genesis = ChainGenesis::from(&genesis);
        let env = TestEnv::builder(chain_genesis)
            .clients_count(num_clients)
            .validator_seats(num_validators)
            .runtime_adapters(create_nightshade_runtimes(&genesis, num_clients))
            .build();
        Self {
            env,
            initial_accounts,
            epoch_length,
            num_validators,
            num_clients,
            init_txs: vec![],
            txs_by_height: HashMap::new(),
        }
    }

    /// `init_txs` are added before any block is produced
    fn set_init_tx(&mut self, init_txs: Vec<SignedTransaction>) {
        self.init_txs = init_txs;
    }

    /// `txs_by_height` is a hashmap from block height to transactions to be included at block at
    /// that height
    fn set_tx_at_height(&mut self, height: u64, txs: Vec<SignedTransaction>) {
        debug!(target:"test", "adding txs at height {} txs: {:?}", height, txs.iter().map(|x|x.get_hash()).collect::<Vec<_>>());
        self.txs_by_height.insert(height, txs);
    }

    /// produces and processes the next block
    /// also checks that all accounts in initial_accounts are intact
    fn step(&mut self, p_drop_chunk: f64) {
        let env = &mut self.env;
        let mut rng = thread_rng();
        let head = env.clients[0].chain.head().unwrap();
        let height = head.height + 1;

        // add transactions for the next block
        if height == 1 {
            for tx in self.init_txs.iter() {
                for j in 0..self.num_validators {
                    env.clients[j].process_tx(tx.clone(), false, false);
                }
            }
        }

        // At every step, chunks for the next block are produced after the current block is processed
        // (inside env.process_block)
        // Therefore, if we want a transaction to be included at the block at `height+1`, we must add
        // it when we are producing the block at `height`
        if let Some(txs) = self.txs_by_height.get(&(height + 1)) {
            for tx in txs {
                for j in 0..self.num_validators {
                    env.clients[j].process_tx(tx.clone(), false, false);
                }
            }
        }

        // produce block
        let block_producer = {
            let epoch_id = env.clients[0]
                .runtime_adapter
                .get_epoch_id_from_prev_block(&head.last_block_hash)
                .unwrap();
            env.clients[0].runtime_adapter.get_block_producer(&epoch_id, height).unwrap()
        };
        let block_producer_client = env.client(&block_producer);
        let mut block = block_producer_client.produce_block(height).unwrap().unwrap();
        set_block_protocol_version(
            &mut block,
            block_producer.clone(),
            SIMPLE_NIGHTSHADE_PROTOCOL_VERSION,
        );
        // make sure that catchup is done before the end of each epoch, but when it is done is
        // by chance. This simulates when catchup takes a long time to be done
        let should_catchup = rng.gen_bool(P_CATCHUP) || height % self.epoch_length == 0;
        // process block, this also triggers chunk producers for the next block to produce chunks
        for j in 0..self.num_clients {
            let produce_chunks = !rng.gen_bool(p_drop_chunk);
            env.process_block_with_options(
                j as usize,
                block.clone(),
                Provenance::NONE,
                should_catchup,
                produce_chunks,
            );
        }

        let expected_num_shards = if height < 2 * self.epoch_length { 1 } else { 4 };
        assert_eq!(
            env.clients[0]
                .runtime_adapter
                .get_shard_layout_from_prev_block(block.hash())
                .unwrap()
                .num_shards(),
            expected_num_shards
        );

        env.process_partial_encoded_chunks();

        // after state split, check chunk extra exists and the states are correct
        for account_id in self.initial_accounts.iter() {
            check_account(env, account_id, &block);
        }
    }

    /// check that all accounts in `accounts` exist in the current state
    fn check_accounts(&mut self, accounts: Vec<&AccountId>) {
        let head = self.env.clients[0].chain.head().unwrap();
        let block = self.env.clients[0].chain.get_block(&head.last_block_hash).unwrap().clone();
        for account_id in accounts {
            check_account(&mut self.env, account_id, &block)
        }
    }

    /// This functions checks that the outcomes of all transactions and associated receipts
    /// have successful status
    /// If `allow_not_started` is true, allow transactions status to be NotStarted
    /// Skips checking transactions added at `skip_heights`
    /// Return successful transaction hashes
    fn check_tx_outcomes(
        &mut self,
        allow_not_started: bool,
        skip_heights: Vec<u64>,
    ) -> Vec<CryptoHash> {
        let env = &mut self.env;
        let head = env.clients[0].chain.head().unwrap();
        let block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap().clone();
        // check execution outcomes
        let shard_layout = env.clients[0]
            .runtime_adapter
            .get_shard_layout_from_prev_block(&head.last_block_hash)
            .unwrap();
        let mut txs_to_check = vec![];
        txs_to_check.extend(&self.init_txs);
        for (height, txs) in self.txs_by_height.iter() {
            if !skip_heights.contains(height) {
                txs_to_check.extend(txs);
            }
        }

        let mut successful_txs = Vec::new();
        for tx in txs_to_check {
            let id = &tx.get_hash();
            let account_id = &tx.transaction.signer_id;
            let shard_uid = account_id_to_shard_uid(account_id, &shard_layout);
            for (i, account_id) in env.validators.iter().enumerate() {
                let cares_about_shard = env.clients[i].runtime_adapter.cares_about_shard(
                    Some(account_id),
                    block.header().prev_hash(),
                    shard_uid.shard_id(),
                    true,
                );
                if cares_about_shard {
                    let execution_outcomes =
                        env.clients[i].chain.get_transaction_execution_result(id).unwrap();
                    if execution_outcomes.is_empty() {
                        assert!(allow_not_started, "transaction {:?} not processed", id);
                    } else {
                        let final_outcome =
                            env.clients[i].chain.get_final_transaction_result(id).unwrap();

                        let outcome_status = final_outcome.status.clone();
                        if matches!(outcome_status, FinalExecutionStatus::SuccessValue(_)) {
                            successful_txs.push(tx.get_hash());
                        } else {
                            panic!("tx failed {:?}", final_outcome);
                        }
                        for outcome in final_outcome.receipts_outcome {
                            assert_matches!(
                                outcome.outcome.status,
                                ExecutionStatusView::SuccessValue(_)
                            );
                        }
                    }
                }
            }
        }
        successful_txs
    }
}

/// Checks that account exists in the state after `block` is processed
/// This function checks both state_root from chunk extra and state root from chunk header, if
/// the corresponding chunk is included in the block
fn check_account(env: &mut TestEnv, account_id: &AccountId, block: &Block) {
    let prev_hash = block.header().prev_hash();
    let shard_layout =
        env.clients[0].runtime_adapter.get_shard_layout_from_prev_block(prev_hash).unwrap();
    let shard_uid = account_id_to_shard_uid(account_id, &shard_layout);
    let shard_id = shard_uid.shard_id();
    for (i, me) in env.validators.iter().enumerate() {
        if env.clients[i].runtime_adapter.cares_about_shard(Some(me), prev_hash, shard_id, true) {
            let state_root = env.clients[i]
                .chain
                .get_chunk_extra(block.hash(), &shard_uid)
                .unwrap()
                .state_root()
                .clone();
            env.clients[i]
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
                env.clients[i]
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
}

fn setup_genesis(
    epoch_length: u64,
    num_validators: u64,
    initial_accounts: Vec<AccountId>,
    gas_limit: Option<u64>,
) -> Genesis {
    let mut genesis = Genesis::test(initial_accounts, num_validators);
    // No kickout, since we are going to test missing chunks
    genesis.config.chunk_producer_kickout_threshold = 0;
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = SIMPLE_NIGHTSHADE_PROTOCOL_VERSION - 1;
    genesis.config.simple_nightshade_shard_layout = Some(ShardLayout::v1_test());

    if let Some(gas_limit) = gas_limit {
        genesis.config.gas_limit = gas_limit;
    }

    genesis
}

// test some shard layout upgrade with some simple transactions to create accounts
#[test]
fn test_shard_layout_upgrade_simple() {
    init_test_logger();

    let mut rng = thread_rng();

    // setup
    let epoch_length = 5;
    let mut test_env = TestShardUpgradeEnv::new(epoch_length, 2, 2, 100, None);
    test_env.set_init_tx(vec![]);

    let mut nonce = 100;
    let genesis_hash = test_env.env.clients[0].chain.genesis_block().hash().clone();
    let mut all_accounts: HashSet<_> = test_env.initial_accounts.clone().into_iter().collect();
    let mut accounts_to_check: Vec<_> = vec![];
    let initial_accounts = test_env.initial_accounts.clone();
    let generate_create_accounts_txs: &mut dyn FnMut(usize, bool) -> Vec<SignedTransaction> =
        &mut |max_size: usize, check_accounts: bool| -> Vec<SignedTransaction> {
            let size = rng.gen_range(0, max_size) + 1;
            std::iter::repeat_with(|| loop {
                let signer_account = initial_accounts.choose(&mut rng).unwrap();
                let signer0 = InMemorySigner::from_seed(
                    signer_account.clone(),
                    KeyType::ED25519,
                    &signer_account.to_string(),
                );
                let account_id = gen_account(&mut rng, b"abcdefghijkmn");
                if all_accounts.insert(account_id.clone()) {
                    let signer = InMemorySigner::from_seed(
                        account_id.clone(),
                        KeyType::ED25519,
                        account_id.as_ref(),
                    );
                    let tx = SignedTransaction::create_account(
                        nonce,
                        signer_account.clone(),
                        account_id.clone(),
                        NEAR_BASE,
                        signer.public_key(),
                        &signer0,
                        genesis_hash.clone(),
                    );
                    if check_accounts {
                        accounts_to_check.push(account_id);
                    }
                    nonce += 1;
                    return tx;
                }
            })
            .take(size)
            .collect()
        };

    // add transactions until after sharding upgrade finishes
    for height in 2..3 * epoch_length {
        test_env.set_tx_at_height(
            height,
            generate_create_accounts_txs(10, height != 2 * epoch_length + 1),
        );
    }

    for _ in 1..5 * epoch_length {
        test_env.step(0.);
    }

    // transactions added for height = 2 * epoch_length + 1 will not be processed, that's a known
    // issue for the shard upgrade implementation. It is because transaction pools are stored by
    // shard id and we do not migrate transactions that are still in the pool at the end of the
    // sharding upgrade
    test_env.check_tx_outcomes(false, vec![2 * epoch_length + 1]);
    test_env.check_accounts(accounts_to_check.iter().collect());
}

const GAS_1: u64 = 300_000_000_000_000;
const GAS_2: u64 = GAS_1 / 3;

// create a transaction signed by `account0` and calls a contract on `account1`
// the contract creates a promise that executes a cross contract call on "account2"
// then executes another contract call on "account3" that creates a new account
fn gen_cross_contract_transaction(
    account0: &AccountId,
    account1: &AccountId,
    account2: &AccountId,
    account3: &AccountId,
    new_account: &AccountId,
    nonce: u64,
    block_hash: &CryptoHash,
) -> SignedTransaction {
    let signer0 =
        InMemorySigner::from_seed(account0.clone(), KeyType::ED25519, &account0.to_string());
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
                    "amount": format!("{}", NEAR_BASE),
                }, "id": 0 },
                {"action_add_key_with_full_access": {
                    "promise_index": 0,
                    "public_key": base64::encode(&signer_new_account.public_key.try_to_vec().unwrap()),
                    "nonce": 0,
                }, "id": 0 }
            ],
        "amount": format!("{}", NEAR_BASE),
        "gas": GAS_2,
        }, "id": 1}
    ]);

    SignedTransaction::from_actions(
        nonce,
        account0.clone(),
        account1.clone(),
        &signer0,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        })],
        block_hash.clone(),
    )
}

/// Return test_env and a map from tx hash to the new account that will be added by this transaction
fn setup_test_env_with_cross_contract_txs(
    epoch_length: u64,
) -> (TestShardUpgradeEnv, HashMap<CryptoHash, AccountId>) {
    let mut test_env = TestShardUpgradeEnv::new(epoch_length, 4, 4, 100, Some(100_000_000_000_000));
    let mut rng = thread_rng();

    let genesis_hash = test_env.env.clients[0].chain.genesis_block().hash().clone();
    // use test0, test1 and two random accounts to deploy contracts because we want accounts on different shards
    let contract_accounts = vec![
        test_env.initial_accounts[0].clone(),
        test_env.initial_accounts[1].clone(),
        test_env.initial_accounts[rng.gen_range(0, test_env.initial_accounts.len())].clone(),
        test_env.initial_accounts[rng.gen_range(0, test_env.initial_accounts.len())].clone(),
    ];
    test_env.set_init_tx(
        contract_accounts
            .iter()
            .map(|account_id| {
                let signer = InMemorySigner::from_seed(
                    account_id.clone(),
                    KeyType::ED25519,
                    &account_id.to_string(),
                );
                SignedTransaction::from_actions(
                    1,
                    account_id.clone(),
                    account_id.clone(),
                    &signer,
                    vec![Action::DeployContract(DeployContractAction {
                        code: near_test_contracts::rs_contract().to_vec(),
                    })],
                    genesis_hash.clone(),
                )
            })
            .collect(),
    );

    let mut nonce = 100;
    let mut all_accounts: HashSet<_> = test_env.initial_accounts.clone().into_iter().collect();
    let mut new_accounts = HashMap::new();
    let generate_txs: &mut dyn FnMut(usize, usize) -> Vec<SignedTransaction> =
        &mut |min_size: usize, max_size: usize| -> Vec<SignedTransaction> {
            let mut rng = thread_rng();
            let size = rng.gen_range(min_size, max_size + 1);
            std::iter::repeat_with(|| loop {
                let account_id = gen_account(&mut rng, b"abcdefghijkmn");
                if all_accounts.insert(account_id.clone()) {
                    nonce += 1;
                    // randomly shuffle contract accounts
                    // so that transactions are send to different shards
                    let mut contract_accounts = contract_accounts.clone();
                    contract_accounts.shuffle(&mut rng);
                    let tx = gen_cross_contract_transaction(
                        &contract_accounts[0],
                        &contract_accounts[1],
                        &contract_accounts[2],
                        &contract_accounts[3],
                        &account_id,
                        nonce,
                        &genesis_hash,
                    );
                    new_accounts.insert(tx.get_hash(), account_id.clone());
                    return tx;
                }
            })
            .take(size)
            .collect()
        };

    // add a bunch of transactions before the two epoch boundaries
    for height in vec![
        epoch_length - 2,
        epoch_length - 1,
        epoch_length,
        2 * epoch_length - 2,
        2 * epoch_length - 1,
        2 * epoch_length,
    ] {
        test_env.set_tx_at_height(height, generate_txs(5, 8));
    }

    // adds some transactions after sharding change finishes
    // but do not add too many because I want all transactions to
    // finish processing before epoch 5
    for height in 2 * epoch_length + 1..3 * epoch_length {
        if rng.gen_bool(0.3) {
            test_env.set_tx_at_height(height, generate_txs(5, 8));
        }
    }

    (test_env, new_accounts)
}

// Test cross contract calls
// This test case tests postponed receipts and delayed receipts
#[test]
fn test_shard_layout_upgrade_cross_contract_calls() {
    init_test_logger();

    // setup
    let epoch_length = 5;

    let (mut test_env, new_accounts) = setup_test_env_with_cross_contract_txs(epoch_length);

    for i in 1..5 * epoch_length {
        test_env.step(0.);
        if i == epoch_length || i == 2 * epoch_length {
            // check that there are delayed receipts
            let client = &mut test_env.env.clients[0];
            let block_hash = client.chain.head().unwrap().last_block_hash;
            let chunk_extra =
                client.chain.get_chunk_extra(&block_hash, &ShardUId::default()).unwrap();
            let trie_update = client
                .runtime_adapter
                .get_tries()
                .new_trie_update_view(ShardUId::default(), *chunk_extra.state_root());
            let delayed_receipt_indices = get_delayed_receipt_indices(&trie_update).unwrap();
            assert_ne!(
                delayed_receipt_indices.first_index,
                delayed_receipt_indices.next_available_index
            );
        }
    }

    let successful_txs = test_env.check_tx_outcomes(false, vec![2 * epoch_length + 1]);
    let new_accounts: Vec<_> =
        successful_txs.iter().flat_map(|tx_hash| new_accounts.get(&tx_hash)).collect();
    test_env.check_accounts(new_accounts);
}

// Test cross contract calls
// This test case tests when there are missing chunks in the produced blocks
// This is to test that all the chunk management logic in sharding split is correct
fn test_shard_layout_upgrade_missing_chunks(p_missing: f64) {
    init_test_logger();

    // setup
    let epoch_length = 10;
    let (mut test_env, new_accounts) = setup_test_env_with_cross_contract_txs(epoch_length);

    // randomly dropping chunks at the first few epochs when sharding splits happens
    // make sure initial txs (deploy smart contracts) are processed succesfully
    for _ in 1..3 {
        test_env.step(0.);
    }

    for _ in 3..3 * epoch_length {
        test_env.step(p_missing);
    }

    // make sure all included transactions finished processing
    for _ in 3 * epoch_length..5 * epoch_length {
        test_env.step(0.);
    }

    let successful_txs = test_env.check_tx_outcomes(true, vec![2 * epoch_length + 1]);
    let new_accounts: Vec<_> =
        successful_txs.iter().flat_map(|tx_hash| new_accounts.get(&tx_hash)).collect();
    test_env.check_accounts(new_accounts);
}

#[test]
fn test_shard_layout_upgrade_missing_chunks_low_missing_prob() {
    test_shard_layout_upgrade_missing_chunks(0.1);
}

#[test]
fn test_shard_layout_upgrade_missing_chunks_mid_missing_prob() {
    test_shard_layout_upgrade_missing_chunks(0.5);
}

#[test]
fn test_shard_layout_upgrade_missing_chunks_high_missing_prob() {
    test_shard_layout_upgrade_missing_chunks(0.9);
}
