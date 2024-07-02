use crate::test_loop::env::TestData;
use itertools::Itertools;
use near_async::messaging::SendAsync;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_client::test_utils::test_loop::ClientQueries;
use near_network::client::ProcessTxRequest;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use std::collections::HashMap;

use super::ONE_NEAR;

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
