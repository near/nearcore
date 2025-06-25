use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;
use crate::utils::ONE_NEAR;
use crate::utils::client_queries::ClientQueries;
use crate::utils::transactions::get_anchor_hash;
use itertools::Itertools;
use near_async::messaging::CanSend;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::ProcessTxRequest;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::{AccessKeyPermission, FunctionCallPermission};
use near_primitives::action::{Action, AddGasKeyAction, FundGasKeyAction, TransferAction};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::{SignedTransaction, TransactionNonce};
use near_primitives::types::{AccountId, Nonce, NonceIndex};
use near_primitives::views::AccessKeyPermissionView;
use rand::Rng;

#[test]
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
        .shard_layout(shard_layout.clone())
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
        .genesis_height(10000)
        .transaction_validity_period(1000)
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
        let action3 = Action::FundGasKey(Box::new(FundGasKeyAction {
            public_key: key1.public_key(),
            deposit: 100000 * ONE_NEAR,
        }));
        let action4 = Action::FundGasKey(Box::new(FundGasKeyAction {
            public_key: key2.public_key(),
            deposit: 100000 * ONE_NEAR,
        }));
        let tx = SignedTransaction::from_actions_v2(
            TransactionNonce::AccessKey { nonce: 1 },
            account.clone(),
            account.clone(),
            &InMemorySigner::test_signer(account),
            vec![action1, action2, action3, action4],
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

    for (i, (account, keys)) in accounts.iter().zip(&signers).enumerate() {
        assert_eq!(clients.query_balance(account), 800000 * ONE_NEAR);
        let gas_key_view1 = clients.query_gas_key(account, &keys[0].public_key());
        assert_eq!(gas_key_view1.balance, 100000 * ONE_NEAR);
        assert_eq!(gas_key_view1.nonces.as_ref().unwrap().len(), (i + 1) as usize);
        assert!(matches!(gas_key_view1.permission, AccessKeyPermissionView::FullAccess));
        let gas_key_view2 = clients.query_gas_key(account, &keys[1].public_key());
        assert_eq!(gas_key_view2.balance, 100000 * ONE_NEAR);
        assert_eq!(gas_key_view2.nonces.as_ref().unwrap().len(), (i + 1) as usize);
        assert!(matches!(
            gas_key_view2.permission,
            AccessKeyPermissionView::FunctionCall{allowance: None, method_names, receiver_id } if method_names == vec!["allowed".to_string()] && receiver_id == account.to_string()
        ));
        gas_key_signers.push(vec![
            GasKeySigner {
                signer: keys[0].clone(),
                nonces: gas_key_view1.nonces.unwrap(),
                is_full_access: true,
            },
            GasKeySigner {
                signer: keys[1].clone(),
                nonces: gas_key_view2.nonces.unwrap(),
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

    let mut expected_account_balances = accounts.iter().map(|_| 800000 * ONE_NEAR).collect_vec();
    let mut expected_gas_key_balances = gas_keys
        .iter()
        .map(|keys| keys.iter().map(|_| 100000 * ONE_NEAR).collect_vec())
        .collect_vec();

    let gas_key_public_keys = gas_keys
        .iter()
        .map(|keys| keys.iter().map(|key| key.signer.public_key()).collect_vec())
        .collect_vec();

    for (from_index, account) in accounts.iter().enumerate() {
        // The second key should fail, because it is a FunctionCall key only.
        for (from_key_index, key) in gas_keys[from_index].iter_mut().enumerate() {
            // Each nonce index can independently support a separate transaction sequence.
            for (nonce_index, nonce) in key.nonces.iter_mut().enumerate() {
                for (to_index, other_account) in accounts.iter().enumerate() {
                    let deposit1 = random_deposit();
                    let deposit2 = random_deposit();
                    let deposit3 = random_deposit();
                    let to_key_index = rand::thread_rng().gen_range(0..2);
                    let use_old_nonce = rand::thread_rng().gen_bool(0.2);
                    let tx = SignedTransaction::from_actions_v2(
                        TransactionNonce::GasKey {
                            nonce_index: nonce_index as NonceIndex,
                            nonce: if use_old_nonce { *nonce } else { *nonce + 1 } as Nonce,
                        },
                        account.clone(),
                        other_account.clone(),
                        &key.signer,
                        vec![
                            // Transfer from gas key to an account.
                            Action::Transfer(TransferAction { deposit: deposit1 }),
                            // Transfer from gas key to gas key.
                            Action::FundGasKey(Box::new(FundGasKeyAction {
                                public_key: gas_key_public_keys[to_index][to_key_index].clone(),
                                deposit: deposit2,
                            })),
                            // Transfer from gas key to a non-existent gas key which falls back to the account.
                            Action::FundGasKey(Box::new(FundGasKeyAction {
                                public_key: InMemorySigner::from_random(
                                    other_account.clone(),
                                    KeyType::ED25519,
                                )
                                .public_key(),
                                deposit: deposit3,
                            })),
                        ],
                        anchor_hash,
                        0,
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

                    if !use_old_nonce && key.is_full_access {
                        // Update expected balances only for the transactions that are expected to succeed.
                        expected_account_balances[to_index] += deposit1;
                        expected_gas_key_balances[from_index][from_key_index] -= deposit1;

                        expected_gas_key_balances[to_index][to_key_index] += deposit2;
                        expected_gas_key_balances[from_index][from_key_index] -= deposit2;

                        expected_account_balances[to_index] += deposit3;
                        expected_gas_key_balances[from_index][from_key_index] -= deposit3;

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
                gas_key_view.nonces.as_ref().unwrap(),
                &key.nonces,
                "Gas key nonces mismatch for account: {}, key index: {}",
                account,
                j
            );
        }
    }
}

fn random_deposit() -> u128 {
    // Random deposit between 1 and 100 NEAR
    let mut rng = rand::thread_rng();
    let deposit = rng.gen_range(1..=100) * ONE_NEAR;
    deposit
}
