use crate::test_loop::env::TestData;
use itertools::Itertools;
use near_async::messaging::SendAsync;
use near_async::time::Duration;
use near_client::test_utils::test_loop::ClientQueries;
use near_network::client::ProcessTxRequest;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use std::collections::HashMap;

pub(crate) const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

/// Execute money transfers within given `TestLoop` between given accounts.
/// Used to generate state changes and check that chain is able to update
/// balances correctly.
pub(crate) fn execute_money_transfers(
    test_loop: &mut near_async::test_loop::TestLoopV2,
    node_datas: &[TestData],
    accounts: &[AccountId],
) {
    let clients = node_datas
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    let mut balances = accounts
        .iter()
        .cloned()
        .map(|account| (account.clone(), clients.query_balance(&account)))
        .collect::<HashMap<_, _>>();
    let num_clients = clients.len();

    // Assume that all clients are in sync up to this block.
    let anchor_hash = clients[0].chain.head().unwrap().prev_block_hash;
    drop(clients);

    for i in 0..accounts.len() {
        let amount = ONE_NEAR * (i as u128 + 1);
        let tx = SignedTransaction::send_money(
            1,
            accounts[i].clone(),
            accounts[(i + 1) % accounts.len()].clone(),
            &create_user_test_signer(&accounts[i]).into(),
            amount,
            anchor_hash,
        );
        *balances.get_mut(&accounts[i]).unwrap() -= amount;
        *balances.get_mut(&accounts[(i + 1) % accounts.len()]).unwrap() += amount;
        let future = node_datas[i % num_clients]
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
    test_loop.run_for(Duration::seconds(40));

    let clients = node_datas
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
