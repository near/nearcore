use crate::test_loop::env::{TestData, TestLoopEnv};
use assert_matches::assert_matches;
use itertools::Itertools;
use near_async::messaging::{AsyncSendError, CanSend, SendAsync};
use near_async::test_loop::futures::TestLoopFutureSpawner;
use near_async::test_loop::sender::TestLoopSender;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_client::client_actor::ClientActorInner;
use near_client::test_utils::test_loop::ClientQueries;
use near_client::{Client, ProcessTxResponse};
use near_crypto::Signer;
use near_network::client::ProcessTxRequest;
use near_primitives::block::Tip;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::views::{
    FinalExecutionOutcomeView, FinalExecutionStatus, QueryRequest, QueryResponseKind,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use super::{ONE_NEAR, TGAS};
use near_async::futures::FutureSpawnerExt;

/// See `execute_money_transfers`. Debug is implemented so .unwrap() can print
/// the error.
#[derive(Debug)]
pub(crate) struct BalanceMismatchError {
    #[allow(unused)]
    pub account: AccountId,
    #[allow(unused)]
    pub expected: u128,
    #[allow(unused)]
    pub actual: u128,
}

// Returns the head with the smallest height
pub(crate) fn get_smallest_height_head(clients: &[&Client]) -> Tip {
    clients
        .iter()
        .map(|client| client.chain.head().unwrap())
        .min_by_key(|head| head.height)
        .unwrap()
}

// Transactions have to be built on top of some block in chain. To make
// sure all clients accept them, we select the head of the client with
// the smallest height.
pub(crate) fn get_anchor_hash(clients: &[&Client]) -> CryptoHash {
    get_smallest_height_head(clients).last_block_hash
}

/// Get next available nonce for the account's public key.
pub fn get_next_nonce(env: &mut TestLoopEnv, account_id: &AccountId) -> u64 {
    let signer: Signer = create_user_test_signer(&account_id).into();
    let public_key = signer.public_key();
    let clients = env
        .datas
        .iter()
        .map(|data| &env.test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    let response = clients.runtime_query(
        account_id,
        QueryRequest::ViewAccessKey { account_id: account_id.clone(), public_key },
    );
    let QueryResponseKind::AccessKey(access_key) = response.kind else {
        panic!("Expected AccessKey response");
    };
    access_key.nonce + 1
}

/// Execute money transfers within given `TestLoop` between given accounts.
/// Runs chain long enough for the transfers to be optimistically executed.
/// Used to generate state changes and check that chain is able to update
/// balances correctly.
///
/// If balances are incorrect, returns an error.
///
/// TODO: consider resending transactions which may be dropped because of
/// missing chunks.
pub(crate) fn execute_money_transfers(
    test_loop: &mut TestLoopV2,
    node_data: &[TestData],
    accounts: &[AccountId],
) -> Result<(), BalanceMismatchError> {
    let clients = node_data
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    let mut balances = accounts
        .iter()
        .map(|account| (account.clone(), clients.query_balance(&account)))
        .collect::<HashMap<_, _>>();
    let num_clients = clients.len();
    drop(clients);

    let node_data = Arc::new(node_data.to_vec());

    for i in 0..accounts.len() {
        let amount = ONE_NEAR * (i as u128 + 1);
        let sender = accounts[i].clone();
        let receiver = accounts[(i + 1) % accounts.len()].clone();
        let node_data = node_data.clone();
        *balances.get_mut(&sender).unwrap() -= amount;
        *balances.get_mut(&receiver).unwrap() += amount;
        test_loop.send_adhoc_event_with_delay(
            format!("transaction {}", i),
            Duration::milliseconds(300 * i as i64),
            move |data| {
                let clients = node_data
                    .iter()
                    .map(|test_data| &data.get(&test_data.client_sender.actor_handle()).client)
                    .collect_vec();

                let anchor_hash = get_anchor_hash(&clients);

                let tx = SignedTransaction::send_money(
                    // TODO: set correct nonce.
                    1,
                    sender.clone(),
                    receiver.clone(),
                    &create_user_test_signer(&sender).into(),
                    amount,
                    anchor_hash,
                );
                let process_tx_request =
                    ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };
                node_data[i % num_clients].client_sender.send(process_tx_request);
            },
        );
    }

    // Give plenty of time for these transactions to complete.
    // TODO: consider explicitly waiting for all execution outcomes.
    test_loop.run_for(Duration::milliseconds(300 * accounts.len() as i64 + 20_000));

    let clients = node_data
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    for account in accounts {
        let expected = *balances.get(account).unwrap();
        let actual = clients.query_balance(account);
        if expected != actual {
            return Err(BalanceMismatchError { account: account.clone(), expected, actual });
        }
    }
    Ok(())
}

pub fn do_create_account(
    env: &mut TestLoopEnv,
    rpc_id: &AccountId,
    originator: &AccountId,
    new_account_id: &AccountId,
    amount: u128,
) {
    tracing::info!(target: "test", "Creating account.");
    let tx = create_account(env, rpc_id, originator, new_account_id, amount);
    env.test_loop.run_for(Duration::seconds(5));
    check_txs(&env.test_loop, &env.datas, rpc_id, &[tx]);
}

pub fn do_delete_account(
    env: &mut TestLoopEnv,
    rpc_id: &AccountId,
    account_id: &AccountId,
    beneficiary_id: &AccountId,
) {
    tracing::info!(target: "test", "Deleting account.");
    let tx = delete_account(env, rpc_id, account_id, beneficiary_id);
    env.test_loop.run_for(Duration::seconds(5));
    check_txs(&env.test_loop, &env.datas, rpc_id, &[tx]);
}

pub fn do_deploy_contract(
    env: &mut TestLoopEnv,
    rpc_id: &AccountId,
    contract_id: &AccountId,
    code: Vec<u8>,
) {
    tracing::info!(target: "test", "Deploying contract.");
    let nonce = get_next_nonce(env, contract_id);
    let tx = deploy_contract(&mut env.test_loop, &env.datas, rpc_id, contract_id, code, nonce);
    env.test_loop.run_for(Duration::seconds(2));
    check_txs(&env.test_loop, &env.datas, rpc_id, &[tx]);
}

pub fn do_call_contract(
    env: &mut TestLoopEnv,
    rpc_id: &AccountId,
    sender_id: &AccountId,
    contract_id: &AccountId,
    method_name: String,
    args: Vec<u8>,
) {
    tracing::info!(target: "test", "Calling contract.");
    let nonce = get_next_nonce(env, contract_id);
    let tx = call_contract(
        &mut env.test_loop,
        &env.datas,
        rpc_id,
        sender_id,
        contract_id,
        method_name,
        args,
        nonce,
    );
    env.test_loop.run_for(Duration::seconds(2));
    check_txs(&env.test_loop, &env.datas, rpc_id, &[tx]);
}

pub fn create_account(
    env: &mut TestLoopEnv,
    rpc_id: &AccountId,
    originator: &AccountId,
    new_account_id: &AccountId,
    amount: u128,
) -> CryptoHash {
    let block_hash = get_shared_block_hash(&env.datas, &env.test_loop);

    let nonce = get_next_nonce(env, originator);
    let signer = create_user_test_signer(&originator).into();
    let new_signer: Signer = create_user_test_signer(&new_account_id).into();

    let tx = SignedTransaction::create_account(
        nonce,
        originator.clone(),
        new_account_id.clone(),
        amount,
        new_signer.public_key(),
        &signer,
        block_hash,
    );

    let tx_hash = tx.get_hash();
    submit_tx(&env.datas, rpc_id, tx);
    tracing::debug!(target: "test", ?originator, ?new_account_id, ?tx_hash, "created account");
    tx_hash
}

pub fn delete_account(
    env: &mut TestLoopEnv,
    rpc_id: &AccountId,
    account_id: &AccountId,
    beneficiary_id: &AccountId,
) -> CryptoHash {
    let signer: Signer = create_user_test_signer(&account_id).into();
    let nonce = get_next_nonce(env, account_id);
    let block_hash = get_shared_block_hash(&env.datas, &env.test_loop);

    let tx = SignedTransaction::delete_account(
        nonce,
        account_id.clone(),
        account_id.clone(),
        beneficiary_id.clone(),
        &signer,
        block_hash,
    );

    let tx_hash = tx.get_hash();
    submit_tx(&env.datas, rpc_id, tx);
    tracing::debug!(target: "test", ?account_id, ?beneficiary_id, ?tx_hash, "deleted account");
    tx_hash
}

/// Deploy the test contract to the provided contract_id account. The contract
/// account should already exist. The contract will be deployed from the contract
/// account itself.
///
/// This function does not wait until the transactions is executed.
pub fn deploy_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &[TestData],
    rpc_id: &AccountId,
    contract_id: &AccountId,
    code: Vec<u8>,
    nonce: u64,
) -> CryptoHash {
    let block_hash = get_shared_block_hash(node_datas, test_loop);

    let signer = create_user_test_signer(&contract_id).into();

    let tx = SignedTransaction::deploy_contract(nonce, contract_id, code, &signer, block_hash);
    let tx_hash = tx.get_hash();
    submit_tx(node_datas, rpc_id, tx);

    tracing::debug!(target: "test", ?contract_id, ?tx_hash, "deployed contract");
    tx_hash
}

/// Call the contract deployed at contract id from the sender id.
///
/// This function does not wait until the transactions is executed.
pub fn call_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &[TestData],
    rpc_id: &AccountId,
    sender_id: &AccountId,
    contract_id: &AccountId,
    method_name: String,
    args: Vec<u8>,
    nonce: u64,
) -> CryptoHash {
    let block_hash = get_shared_block_hash(node_datas, test_loop);
    let signer = create_user_test_signer(sender_id);
    let attach_gas = 300 * TGAS;
    let deposit = 0;

    let tx = SignedTransaction::call(
        nonce,
        sender_id.clone(),
        contract_id.clone(),
        &signer.into(),
        deposit,
        method_name,
        args,
        attach_gas,
        block_hash,
    );

    let tx_hash = tx.get_hash();
    submit_tx(node_datas, rpc_id, tx);
    tracing::debug!(target: "test", ?sender_id, ?contract_id, ?tx_hash, "called contract");
    tx_hash
}

/// Submit a transaction to the rpc node with the given account id.
/// Doesn't wait for the result, it must be requested separately.
pub fn submit_tx(node_datas: &[TestData], rpc_id: &AccountId, tx: SignedTransaction) {
    let process_tx_request =
        ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };

    let rpc_node_data = get_node_data(node_datas, rpc_id);
    let rpc_node_data_sender = &rpc_node_data.client_sender;

    let future = rpc_node_data_sender.send_async(process_tx_request);
    drop(future);
}

/// Check the status of the transactions and assert that they are successful.
///
/// Please note that it's important to use an rpc node that tracks all shards.
/// Otherwise, the transactions may not be found.
pub fn check_txs(
    test_loop: &TestLoopV2,
    node_datas: &[TestData],
    rpc_id: &AccountId,
    txs: &[CryptoHash],
) {
    let rpc = rpc_client(test_loop, node_datas, rpc_id);

    for &tx in txs {
        let tx_outcome = rpc.chain.get_partial_transaction_result(&tx);
        let status = tx_outcome.as_ref().map(|o| o.status.clone());
        let status = status.unwrap();
        tracing::info!(target: "test", ?tx, ?status, "transaction status");
        assert_matches!(status, FinalExecutionStatus::SuccessValue(_));
    }
}

/// Get the client for the provided rpd node account id.
fn rpc_client<'a>(
    test_loop: &'a TestLoopV2,
    node_datas: &'a [TestData],
    rpc_id: &AccountId,
) -> &'a Client {
    let node_data = get_node_data(node_datas, rpc_id);
    let client_actor_handle = node_data.client_sender.actor_handle();
    let client_actor = test_loop.data.get(&client_actor_handle);
    &client_actor.client
}

/// Finds a block that all clients have on their chain and return its hash.
pub fn get_shared_block_hash(node_datas: &[TestData], test_loop: &TestLoopV2) -> CryptoHash {
    let clients = node_datas
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();

    let (_, block_hash) = clients
        .iter()
        .map(|client| {
            let head = client.chain.head().unwrap();
            (head.height, head.last_block_hash)
        })
        .min_by_key(|&(height, _)| height)
        .unwrap();
    block_hash
}

/// Returns the test data of for the node with the given account id.
pub fn get_node_data<'a>(node_datas: &'a [TestData], account_id: &AccountId) -> &'a TestData {
    for node_data in node_datas {
        if &node_data.account_id == account_id {
            return node_data;
        }
    }
    panic!("RPC client not found");
}

/// Run a transaction until completion and assert that the result is "success".
/// Returns the transaction result.
pub fn run_tx(
    test_loop: &mut TestLoopV2,
    tx: SignedTransaction,
    node_datas: &[TestData],
    maximum_duration: Duration,
) -> Vec<u8> {
    let tx_res = execute_tx(test_loop, tx, node_datas, maximum_duration).unwrap();
    assert_matches!(tx_res.status, FinalExecutionStatus::SuccessValue(_));
    match tx_res.status {
        FinalExecutionStatus::SuccessValue(res) => res,
        _ => unreachable!(),
    }
}

/// Run multiple transactions in parallel and wait for all of them to complete.
/// The transactions are expected to be valid, the function will panic if any transaction fails.
pub fn run_txs_parallel(
    test_loop: &mut TestLoopV2,
    txs: Vec<SignedTransaction>,
    node_datas: &[TestData],
    maximum_duration: Duration,
) {
    let mut tx_runners = txs.into_iter().map(|tx| TransactionRunner::new(tx, true)).collect_vec();

    let client_sender = &node_datas[0].client_sender;
    let future_spawner = test_loop.future_spawner();

    test_loop.run_until(
        |tl_data| {
            let client = &tl_data.get(&node_datas[0].client_sender.actor_handle()).client;
            let mut all_ready = true;
            for runner in tx_runners.iter_mut() {
                match runner.poll_assert_success(client_sender, client, &future_spawner) {
                    Poll::Pending => all_ready = false,
                    Poll::Ready(_) => {}
                }
            }

            all_ready
        },
        maximum_duration,
    );
}

/// Submit a transaction and wait for the execution result.
/// For invalid transactions returns an error.
/// For valid transactions returns the execution result (which could have an execution error inside, check it!).
pub fn execute_tx(
    test_loop: &mut TestLoopV2,
    tx: SignedTransaction,
    node_datas: &[TestData],
    maximum_duration: Duration,
) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
    // Last node is usually the rpc node
    let rpc_node_id = node_datas.len().checked_sub(1).unwrap();

    let client_sender = &node_datas[rpc_node_id].client_sender;
    let future_spawner = test_loop.future_spawner();

    let mut tx_runner = TransactionRunner::new(tx, true);

    let mut res = None;
    test_loop.run_until(
        |tl_data| {
            let client = &tl_data.get(&node_datas[rpc_node_id].client_sender.actor_handle()).client;
            match tx_runner.poll(client_sender, client, &future_spawner) {
                Poll::Pending => false,
                Poll::Ready(tx_res) => {
                    res = Some(tx_res);
                    true
                }
            }
        },
        maximum_duration,
    );

    res.unwrap()
}

/// Creates account ids for the given number of accounts.
pub fn make_accounts(num_accounts: usize) -> Vec<AccountId> {
    let accounts = (0..num_accounts).map(|i| make_account(i)).collect_vec();
    accounts
}

/// Creates an account id to be contained at the given index.
pub fn make_account(index: usize) -> AccountId {
    format!("account{}", index).parse().unwrap()
}

/// Runs a transaction until completion.
/// Works in a non-blocking way which allows to run multiple transactions in parallel.
/// It is meant to be used with run_until.
pub struct TransactionRunner {
    transaction: SignedTransaction,
    tx_sent: bool,
    process_tx_result: Arc<Mutex<Option<Result<ProcessTxResponse, AsyncSendError>>>>,
    retry_when_congested: bool,
    final_result: Option<Result<FinalExecutionOutcomeView, InvalidTxError>>,
}

impl TransactionRunner {
    /// Create a runner which will run this transaction. Doesn't do anything yet,
    /// the transaction will be sent on the first call to `poll`.
    /// If `retry_when_congested` is true, the runner will retry the transaction if it's rejected
    /// because of shard congestion.
    pub fn new(transaction: SignedTransaction, retry_when_congested: bool) -> Self {
        Self {
            transaction: transaction,
            tx_sent: false,
            process_tx_result: Arc::new(Mutex::new(None)),
            retry_when_congested,
            final_result: None,
        }
    }

    /// Make progress on running the transaction.
    /// Returns `Poll::Pending` if the transaction is still running.
    /// Returns `Poll::Ready(_)` with the result if the transaction execution is finished.
    /// The result can be:
    /// Err(InvalidTxError) - the transaction is invalid, rejected before execution.
    /// Ok(FinalExecutionOutcomeView) - transaction was executed, result could be success or an error.
    /// It's meant to be called in `run_until`.
    pub fn poll(
        &mut self,
        client_sender: &TestLoopSender<ClientActorInner>,
        client: &Client,
        future_spawner: &TestLoopFutureSpawner,
    ) -> Poll<Result<FinalExecutionOutcomeView, InvalidTxError>> {
        if let Some(final_result) = &self.final_result {
            // Execution has finished, return the saved result.
            return Poll::Ready(final_result.clone());
        }

        if !self.tx_sent {
            // First call to `poll` - send out the transaction.
            self.send_tx(client_sender, future_spawner);
        }

        if let Some(tx_processing_res) = self.get_tx_processing_res() {
            match tx_processing_res {
                TxProcessingResult::Ok => {} // Initial processing was successful.
                TxProcessingResult::Congested(invalid_tx_err) => {
                    if self.retry_when_congested {
                        // Transaction was rejected because of congestion, retry it.
                        self.send_tx(client_sender, future_spawner);
                    } else {
                        self.final_result = Some(Err(invalid_tx_err.clone()));
                        return Poll::Ready(Err(invalid_tx_err));
                    }
                }
                TxProcessingResult::Invalid(invalid_tx_err) => {
                    // Invalid transaction.
                    self.final_result = Some(Err(invalid_tx_err.clone()));
                    return Poll::Ready(Err(invalid_tx_err));
                }
            }
        }

        if let Ok(final_res) =
            client.chain.get_final_transaction_result(&self.transaction.get_hash())
        {
            // Transaction execution is finished, save and return the final result.
            self.final_result = Some(Ok(final_res.clone()));
            return Poll::Ready(Ok(final_res));
        }

        Poll::Pending
    }

    /// Same as `poll`, but asserts that the transaction was executed successfully.
    /// Useful for tests where the transaction is expected to be executed successfully.
    pub fn poll_assert_success(
        &mut self,
        client_sender: &TestLoopSender<ClientActorInner>,
        client: &Client,
        future_spawner: &TestLoopFutureSpawner,
    ) -> Poll<Vec<u8>> {
        let final_res = match self.poll(client_sender, client, future_spawner) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(final_res) => final_res,
        };
        assert_matches!(final_res, Ok(_));
        let status = final_res.unwrap().status;
        match status {
            FinalExecutionStatus::SuccessValue(res) => Poll::Ready(res),
            _ => panic!("Transaction failed: {:?}", status),
        }
    }

    /// Send the transaction to the network.
    fn send_tx(
        &mut self,
        client_sender: &TestLoopSender<ClientActorInner>,
        future_spawner: &TestLoopFutureSpawner,
    ) {
        let process_tx_request = ProcessTxRequest {
            transaction: self.transaction.clone(),
            is_forwarded: false,
            check_only: false,
        };
        let process_tx_future = client_sender.send_async(process_tx_request);

        self.process_tx_result = Arc::new(Mutex::new(None));
        let process_tx_result_clone = self.process_tx_result.clone();
        future_spawner.spawn("TransactionRunner::send_tx", async move {
            let process_res = process_tx_future.await;
            *process_tx_result_clone.lock().unwrap() = Some(process_res);
        });
        self.tx_sent = true;
    }

    /// Get result of initial processing, if the result is already available.
    fn get_tx_processing_res(&mut self) -> Option<TxProcessingResult> {
        let processing_response_res = self.process_tx_result.lock().unwrap().take()?;
        let process_tx_response = match processing_response_res {
            Ok(process_tx_response) => process_tx_response,
            Err(AsyncSendError::Closed)
            | Err(AsyncSendError::Timeout)
            | Err(AsyncSendError::Dropped) => {
                tracing::warn!(
                    "TransactionRunner::get_tx_processing_res - got error: {:?}",
                    processing_response_res
                );
                return None;
            }
        };
        let res = match process_tx_response {
            ProcessTxResponse::NoResponse => panic!("NoResponse indicates an error"),
            ProcessTxResponse::RequestRouted | // Ok, transaction forwarded to a validator node
            ProcessTxResponse::ValidTx => TxProcessingResult::Ok,
            ProcessTxResponse::InvalidTx(err) => match err {
                InvalidTxError::ShardCongested { .. } | InvalidTxError::ShardStuck { .. } => {
                    TxProcessingResult::Congested(err)
                }
                _ => TxProcessingResult::Invalid(err),
            },
            ProcessTxResponse::DoesNotTrackShard => {
                panic!("Transaction submitted to a node that doesn't track the shard")
            }
        };
        Some(res)
    }
}

enum TxProcessingResult {
    Ok,
    Congested(InvalidTxError),
    Invalid(InvalidTxError),
}
