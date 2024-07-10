use crate::test_loop::env::TestData;
use itertools::Itertools;
use near_async::messaging::SendAsync;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_client::test_utils::test_loop::ClientQueries;
use near_network::client::ProcessTxRequest;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use std::collections::HashMap;

use super::{ONE_NEAR, TGAS};

/// Execute money transfers within given `TestLoop` between given accounts.
/// Runs chain long enough for the transfers to be optimistically executed.
/// Used to generate state changes and check that chain is able to update
/// balances correctly.
/// TODO: consider resending transactions which may be dropped because of
/// missing chunks.
pub(crate) fn execute_money_transfers(
    test_loop: &mut TestLoopV2,
    node_data: &[TestData],
    accounts: &[AccountId],
) {
    let clients = node_data
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    let mut balances = accounts
        .iter()
        .map(|account| (account.clone(), clients.query_balance(&account)))
        .collect::<HashMap<_, _>>();
    let num_clients = clients.len();

    // Transactions have to be built on top of some block in chain. To make
    // sure all clients accept them, we select the head of the client with
    // the smallest height.
    let (_, anchor_hash) = clients
        .iter()
        .map(|client| {
            let head = client.chain.head().unwrap();
            (head.height, head.last_block_hash)
        })
        .min_by_key(|&(height, _)| height)
        .unwrap();
    drop(clients);

    for i in 0..accounts.len() {
        let amount = ONE_NEAR * (i as u128 + 1);
        let sender = &accounts[i];
        let receiver = &accounts[(i + 1) % accounts.len()];
        let tx = SignedTransaction::send_money(
            // TODO: set correct nonce.
            1,
            sender.clone(),
            receiver.clone(),
            &create_user_test_signer(sender).into(),
            amount,
            anchor_hash,
        );
        *balances.get_mut(sender).unwrap() -= amount;
        *balances.get_mut(receiver).unwrap() += amount;
        let future = node_data[i % num_clients]
            .client_sender
            .clone()
            .with_delay(Duration::milliseconds(300 * i as i64))
            .send_async(ProcessTxRequest {
                transaction: tx,
                is_forwarded: false,
                check_only: false,
            });
        drop(future);
    }

    // Give plenty of time for these transactions to complete.
    // TODO: consider explicitly waiting for all execution outcomes.
    test_loop.run_for(Duration::milliseconds(300 * accounts.len() as i64 + 20_000));

    let clients = node_data
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    for account in accounts {
        assert_eq!(
            clients.query_balance(account),
            *balances.get(account).unwrap(),
            "Account balance mismatch for account {}",
            account
        );
    }
}

/// Deploy the test contract to the provided contract_id account. The contract
/// account should already exits. The contract will be deployed from the contract
/// account itself.
///
/// This function does not wait until the transactions is executed.
pub fn deploy_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &[TestData],
    rpc_id: &AccountId,
    contract_id: &AccountId,
) -> CryptoHash {
    let block_hash = get_shared_block_hash(node_datas, test_loop);

    // TOOD make nonce an argument
    let nonce = 1;
    let signer = create_user_test_signer(&contract_id).into();

    let code = near_test_contracts::rs_contract();
    let code = code.to_vec();

    let tx = SignedTransaction::deploy_contract(nonce, contract_id, code, &signer, block_hash);
    let tx_hash = tx.get_hash();
    let process_tx_request =
        ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };

    let rpc_node_data = get_node_data(node_datas, rpc_id);
    let rpc_node_data_sender = &rpc_node_data.client_sender.clone();

    let future = rpc_node_data_sender.send_async(process_tx_request);
    drop(future);

    tracing::debug!(target: "test", ?contract_id, ?tx_hash, "deployed contract");
    tx_hash
}

/// Call the contract deployed at contract id from the sender id.
///
/// This function does not wait until the transactions is executed.
pub fn call_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &[TestData],
    sender_id: &AccountId,
    contract_id: &AccountId,
) -> CryptoHash {
    let block_hash = get_shared_block_hash(node_datas, test_loop);

    // TOOD make nonce an argument
    let nonce = 2;
    let signer = create_user_test_signer(sender_id);

    let burn_gas = 250 * TGAS;
    let attach_gas = 300 * TGAS;

    let deposit = 0;

    // TODO make method and args arguments
    let method_name = "burn_gas_raw".to_owned();
    let args = burn_gas.to_le_bytes().to_vec();

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

    let process_tx_request =
        ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };
    let future = node_datas[0].client_sender.clone().send_async(process_tx_request);
    drop(future);

    tracing::debug!(target: "test", ?sender_id, ?contract_id, ?tx_hash, "called contract");
    tx_hash
}

/// Finds a block that all clients have on their chain and return its hash.
fn get_shared_block_hash(node_datas: &[TestData], test_loop: &mut TestLoopV2) -> CryptoHash {
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
