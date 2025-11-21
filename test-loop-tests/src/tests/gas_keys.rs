use itertools::Itertools;
use near_async::messaging::CanSend;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::ProcessTxRequest;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::{AccessKeyPermission, FunctionCallPermission};
use near_primitives::action::{Action, AddGasKeyAction, TransferAction, TransferToGasKeyAction};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, Nonce, NonceIndex};
use near_primitives::version::ProtocolFeature;
use near_primitives::views::AccessKeyPermissionView;
use rand::Rng;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;
use crate::utils::client_queries::ClientQueries;
use crate::utils::transactions::get_anchor_hash;

#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_gas_keys() {
    init_test_logger();
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

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .genesis_height(10000)
        .protocol_version(ProtocolFeature::GasKeys.protocol_version())
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
    nonces: Vec<Nonce>,
    is_full_access: bool,
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
        let action1 = Action::AddGasKey(Box::new(AddGasKeyAction {
            num_nonces: (i + 1) as u32,
            public_key: key1.public_key(),
            permission: AccessKeyPermission::FullAccess,
        }));
        let action2 = Action::AddGasKey(Box::new(AddGasKeyAction {
            num_nonces: (i + 1) as u32,
            public_key: key2.public_key(),
            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: None,
                method_names: vec!["allowed".to_string()],
                receiver_id: account.to_string(),
            }),
        }));
        let action3 = Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key: key1.public_key(),
            deposit: Balance::from_near(100000),
        }));
        let action4 = Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key: key2.public_key(),
            deposit: Balance::from_near(100000),
        }));
        let tx = SignedTransaction::from_actions_v1(
            1,
            account.clone(),
            account.clone(),
            &InMemorySigner::test_signer(account),
            None,
            vec![action1, action2, action3, action4],
            anchor_hash,
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

    for (i, (account, keys)) in accounts.iter().zip(&signers).enumerate() {
        assert_eq!(clients.query_balance(account), Balance::from_near(800000));
        let gas_key_view1 = clients.query_gas_key(account, &keys[0].public_key());
        assert_eq!(gas_key_view1.balance, Balance::from_near(100000));
        assert_eq!(gas_key_view1.nonces.len(), (i + 1) as usize);
        assert!(matches!(gas_key_view1.permission, AccessKeyPermissionView::FullAccess));
        let gas_key_view2 = clients.query_gas_key(account, &keys[1].public_key());
        assert_eq!(gas_key_view2.balance, Balance::from_near(100000));
        assert_eq!(gas_key_view2.nonces.len(), (i + 1) as usize);
        assert!(matches!(
            gas_key_view2.permission,
            AccessKeyPermissionView::FunctionCall{allowance: None, method_names, receiver_id } if method_names == vec!["allowed".to_string()] && receiver_id.as_bytes() == account.as_bytes()
        ));
        gas_key_signers.push(vec![
            GasKeySigner {
                signer: keys[0].clone(),
                nonces: gas_key_view1.nonces,
                is_full_access: true,
            },
            GasKeySigner {
                signer: keys[1].clone(),
                nonces: gas_key_view2.nonces,
                is_full_access: false,
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

    // Arbitrary deposit amounts for testing
    let deposit1 = Balance::from_near(20);
    let deposit2 = Balance::from_near(50);
    for (from_index, account) in accounts.iter().enumerate() {
        // The second key should fail, because it is a FunctionCall key only.
        for (from_key_index, key) in gas_keys[from_index].iter_mut().enumerate() {
            // Each nonce index can independently support a separate transaction sequence.
            for (nonce_index, nonce) in key.nonces.iter_mut().enumerate() {
                for (to_index, other_account) in accounts.iter().enumerate() {
                    let to_key_index = rand::thread_rng().gen_range(0..2);
                    let use_old_nonce = rand::thread_rng().gen_bool(0.2);
                    let tx = SignedTransaction::from_actions_v1(
                        if use_old_nonce { *nonce } else { *nonce + 1 },
                        account.clone(),
                        other_account.clone(),
                        &key.signer,
                        Some(nonce_index as NonceIndex),
                        vec![
                            // Transfer from gas key to an account.
                            Action::Transfer(TransferAction { deposit: deposit1 }),
                            // Transfer from gas key to gas key.
                            Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
                                public_key: gas_key_public_keys[to_index][to_key_index].clone(),
                                deposit: deposit2,
                            })),
                        ],
                        anchor_hash,
                    );
                    let process_tx_request = ProcessTxRequest {
                        transaction: tx,
                        is_forwarded: false,
                        check_only: false,
                    };
                    if use_old_nonce {
                        // If we use an old nonce, we're testing that reusing a nonce doesn't work.
                        // So send it in later, so that we know this transaction is the one arriving
                        // later for this nonce (and not the intended transaction).
                        let rpc_handler_sender =
                            node_datas[from_index % clients.len()].rpc_handler_sender.clone();
                        test_loop.send_adhoc_event_with_delay(
                            "send old nonce tx".to_string(),
                            Duration::seconds(2),
                            move |_| {
                                rpc_handler_sender.send(process_tx_request);
                            },
                        );
                    } else {
                        node_datas[from_index % clients.len()]
                            .rpc_handler_sender
                            .send(process_tx_request);
                    }

                    // Update expected balances only for the transactions that are expected to succeed.
                    if !use_old_nonce && key.is_full_access {
                        let add = |balance: &mut Balance, amount: Balance| {
                            *balance = balance.checked_add(amount).unwrap();
                        };
                        let sub = |balance: &mut Balance, amount: Balance| {
                            *balance = balance.checked_sub(amount).unwrap();
                        };
                        add(&mut expected_account_balances[to_index], deposit1);
                        sub(&mut expected_gas_key_balances[from_index][from_key_index], deposit1);

                        add(&mut expected_gas_key_balances[to_index][to_key_index], deposit2);
                        sub(&mut expected_gas_key_balances[from_index][from_key_index], deposit2);

                        *nonce += 1;
                    }
                }
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
        assert_eq!(clients.query_balance(account), expected_account_balances[i]);
        for (j, key) in gas_keys[i].iter().enumerate() {
            let gas_key_view = clients.query_gas_key(account, &key.signer.public_key());
            assert_eq!(
                gas_key_view.balance, expected_gas_key_balances[i][j],
                "Gas key balance mismatch for account: {}, key index: {}",
                account, j
            );
            assert_eq!(
                gas_key_view.nonces, key.nonces,
                "Gas key nonces mismatch for account: {}, key index: {}",
                account, j
            );
        }
    }
}
