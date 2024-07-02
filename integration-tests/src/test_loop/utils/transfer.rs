use crate::test_loop::env::TestData;
use itertools::Itertools;
use near_async::messaging::SendAsync;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_client::test_utils::test_loop::ClientQueries;
use near_crypto::{KeyType, PublicKey};
use near_network::client::ProcessTxRequest;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use std::collections::HashMap;

use super::ONE_NEAR;

const TGAS: u64 = 1_000_000_000_000;

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

/// Deploy the test contracts to all of the provided accounts.
pub(crate) fn deploy_contracts(
    test_loop: &mut TestLoopV2,
    node_datas: &[TestData],
) -> Vec<CryptoHash> {
    let block_hash = get_shared_block_hash(node_datas, test_loop);

    let mut txs = vec![];
    for node_data in node_datas {
        let account = node_data.account_id.clone();

        let contract = near_test_contracts::rs_contract();
        let contract_id = format!("contract.{}", account);
        let signer = create_user_test_signer(&account).into();
        let public_key = PublicKey::from_seed(KeyType::ED25519, &contract_id);
        let nonce = 1;

        let transaction = SignedTransaction::create_contract(
            nonce,
            account,
            contract_id.parse().unwrap(),
            contract.to_vec(),
            10 * ONE_NEAR,
            public_key,
            &signer,
            block_hash,
        );

        txs.push(transaction.get_hash());

        let process_tx_request =
            ProcessTxRequest { transaction, is_forwarded: false, check_only: false };
        let future = node_data.client_sender.clone().send_async(process_tx_request);
        drop(future);

        tracing::info!(target: "test", ?contract_id, "deployed contract");
    }
    txs
}

pub fn call_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &[TestData],
    account: &AccountId,
) -> CryptoHash {
    let block_hash = get_shared_block_hash(node_datas, test_loop);

    let nonce = 2;
    let signer = create_user_test_signer(&account);
    let contract_id = format!("contract.{}", account).parse().unwrap();

    let burn_gas = 250 * TGAS;
    let attach_gas = 300 * TGAS;

    let deposit = 0;
    let method_name = "burn_gas_raw".to_owned();
    let args = burn_gas.to_le_bytes().to_vec();

    let transaction = SignedTransaction::call(
        nonce,
        signer.account_id.clone(),
        contract_id,
        &signer.into(),
        deposit,
        method_name,
        args,
        attach_gas,
        block_hash,
    );

    let tx_hash = transaction.get_hash();

    let process_tx_request =
        ProcessTxRequest { transaction, is_forwarded: false, check_only: false };
    let future = node_datas[0].client_sender.clone().send_async(process_tx_request);
    drop(future);

    tx_hash
}

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
