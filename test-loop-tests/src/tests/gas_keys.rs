use itertools::Itertools;
use near_async::messaging::CanSend;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::ProcessTxRequest;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::{AccessKey, FunctionCallPermission};
use near_primitives::action::{
    Action, AddKeyAction, TransferAction, TransferFromGasKeyAction, TransferToGasKeyAction,
};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, Nonce};
use near_primitives::version::ProtocolFeature;
use near_primitives::views::AccessKeyPermissionView;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;
use crate::utils::client_queries::ClientQueries;
use crate::utils::transactions::get_anchor_hash;

#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_gas_keys() {
    //init_test_logger();
    let builder = TestLoopBuilder::new();

    let num_accounts = 3;
    let num_clients = 2;
    let epoch_length = 10;
    let shard_layout = ShardLayout::single_shard();
    let accounts = (num_accounts - num_clients..num_accounts)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();
    let client_accounts = accounts.iter().take(num_clients).cloned().collect_vec();
    let validators_spec = ValidatorsSpec::desired_roles(
        &client_accounts.iter().map(|t| t.as_str()).collect_vec(),
        &[],
    );

    let gas_price = Balance::from_yoctonear(100_000_000_000);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .genesis_height(10000)
        .protocol_version(ProtocolFeature::GasKeys.protocol_version())
        .gas_prices(gas_price, gas_price)
        .build();

    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let TestLoopEnv { mut test_loop, node_datas, .. } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(client_accounts)
        .build()
        .warmup();

    let mut gas_keys = create_gas_keys(&mut test_loop, &node_datas, &accounts);
    send_transfers_as_gas_keys(&mut test_loop, &node_datas, &mut gas_keys, &accounts);
    test_loop.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

struct GasKeySigner {
    signer: Signer,
    is_full_access: bool,
    nonce: Nonce,
}

fn create_gas_keys(
    test_loop: &mut TestLoopV2,
    node_datas: &[NodeExecutionData],
    accounts: &[AccountId],
) -> Vec<Vec<GasKeySigner>> {
    let clients = node_datas
        .iter()
        .map(|test_data| &test_loop.data.get(&test_data.client_sender.actor_handle()).client)
        .collect_vec();

    let anchor_hash = get_anchor_hash(&clients);

    let mut signers: Vec<Vec<Signer>> = Vec::new();
    for (i, account) in accounts.iter().enumerate() {
        let key1 = InMemorySigner::from_random(account.clone(), KeyType::ED25519);
        let key2 = InMemorySigner::from_random(account.clone(), KeyType::SECP256K1);
        signers.push(vec![Signer::InMemory(key1.clone()), Signer::InMemory(key2.clone())]);
        let action1 = Action::AddKey(Box::new(AddKeyAction {
            public_key: key1.public_key(),
            access_key: AccessKey::gas_key_full_access(),
        }));
        let action2 = Action::AddKey(Box::new(AddKeyAction {
            public_key: key2.public_key(),
            access_key: AccessKey::gas_key_function_call(FunctionCallPermission {
                allowance: None,
                method_names: vec!["allowed".to_string()],
                receiver_id: account.to_string(),
            }),
        }));
        let action3 = Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key: key1.public_key(),
            deposit: Balance::from_near(200000),
        }));
        let action4 = Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key: key2.public_key(),
            deposit: Balance::from_near(200000),
        }));
        let action5 = Action::TransferFromGasKey(Box::new(TransferFromGasKeyAction {
            public_key: key1.public_key(),
            amount: Balance::from_near(100000),
        }));
        let action6 = Action::TransferFromGasKey(Box::new(TransferFromGasKeyAction {
            public_key: key2.public_key(),
            amount: Balance::from_near(100000),
        }));
        let tx = SignedTransaction::from_actions(
            1,
            account.clone(),
            account.clone(),
            &InMemorySigner::test_signer(account),
            vec![action1, action2, action3, action4, action5, action6],
            anchor_hash,
            0,
        );
        let process_tx_request =
            ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };
        node_datas[i % clients.len()].rpc_handler_sender.send(process_tx_request);
    }
    // Give plenty of time for these transactions to complete.
    test_loop.run_for(Duration::seconds(20));

    let clients = node_datas
        .iter()
        .map(|test_data| &test_loop.data.get(&test_data.client_sender.actor_handle()).client)
        .collect_vec();

    let mut gas_key_signers: Vec<Vec<GasKeySigner>> = Vec::new();

    for (_i, (account, keys)) in accounts.iter().zip(&signers).enumerate() {
        eprintln!("Balance for account {}: {}", account, clients.query_balance(account));
        // assert_eq!(clients.query_balance(account), Balance::from_near(800000));
        let gas_key_view1 = clients.query_access_key(account, &keys[0].public_key());
        assert_eq!(gas_key_view1.permission.gas_balance().unwrap(), Balance::from_near(100000));
        assert!(matches!(
            gas_key_view1.permission,
            AccessKeyPermissionView::GasKeyFullAccess { .. }
        ));
        let gas_key_view2 = clients.query_access_key(account, &keys[1].public_key());
        assert_eq!(gas_key_view2.permission.gas_balance().unwrap(), Balance::from_near(100000));
        assert!(matches!(
            gas_key_view2.permission,
            AccessKeyPermissionView::GasKeyFunctionCall{balance: _, allowance: None, method_names, receiver_id } if method_names == vec!["allowed".to_string()] && receiver_id.as_bytes() == account.as_bytes()
        ));
        gas_key_signers.push(vec![
            GasKeySigner {
                signer: keys[0].clone(),
                is_full_access: true,
                nonce: gas_key_view1.nonce,
            },
            GasKeySigner {
                signer: keys[1].clone(),
                is_full_access: false,
                nonce: gas_key_view2.nonce,
            },
        ]);
    }
    gas_key_signers
}

fn send_transfers_as_gas_keys(
    test_loop: &mut TestLoopV2,
    node_datas: &[NodeExecutionData],
    gas_keys: &mut [Vec<GasKeySigner>],
    accounts: &[AccountId],
) {
    let clients = node_datas
        .iter()
        .map(|test_data| &test_loop.data.get(&test_data.client_sender.actor_handle()).client)
        .collect_vec();

    let anchor_hash = get_anchor_hash(&clients);

    let mut expected_account_balances =
        accounts.iter().map(|_| Balance::from_near(800000)).collect_vec();
    let mut expected_gas_key_balances = gas_keys
        .iter()
        .map(|keys| keys.iter().map(|_| Balance::from_near(100000)).collect_vec())
        .collect_vec();

    let gas_key_public_keys = gas_keys
        .iter()
        .map(|keys| keys.iter().map(|key| key.signer.public_key()).collect_vec())
        .collect_vec();

    let deposit1 = Balance::from_near(20);
    let deposit2 = Balance::from_near(50);
    for (from_index, account) in accounts.iter().enumerate() {
        // The second key should fail, because it is a FunctionCall key only.
        for (from_key_index, key) in gas_keys[from_index].iter_mut().enumerate() {
            for (to_index, other_account) in accounts.iter().enumerate() {
                // Let's just transfer to the other account.
                let tx = SignedTransaction::from_actions(
                    key.nonce + 1,
                    account.clone(),
                    other_account.clone(),
                    &key.signer,
                    vec![Action::Transfer(TransferAction { deposit: deposit1 })],
                    anchor_hash,
                    0,
                );
                eprintln!(
                    "Sent transfer of {} from {} of {} to {}, {}",
                    deposit1, from_key_index, account, to_index, other_account
                );
                let process_tx_request =
                    ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };
                node_datas[from_index % clients.len()].rpc_handler_sender.send(process_tx_request);
                key.nonce += 1;
            }
        }
    }

    // Give plenty of time for these transactions to complete.
    test_loop.run_for(Duration::seconds(20));

    let clients = node_datas
        .iter()
        .map(|test_data| &test_loop.data.get(&test_data.client_sender.actor_handle()).client)
        .collect_vec();
    for (i, account) in accounts.iter().enumerate() {
        eprintln!("Account {} has balance {}", account, clients.query_balance(account));

        for (j, key) in gas_keys[i].iter().enumerate() {
            let gas_key_view = clients.query_access_key(account, &key.signer.public_key());

            eprintln!(
                "Gas key {} of account {} has balance {}",
                j,
                account,
                gas_key_view.permission.gas_balance().unwrap()
            );
        }
    }
}
