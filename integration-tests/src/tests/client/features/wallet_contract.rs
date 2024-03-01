use assert_matches::assert_matches;
use near_chain_configs::{Genesis, NEAR_BASE};
use near_client::{test_utils::TestEnv, ProcessTxResponse};
use near_crypto::{InMemorySigner, KeyType, SecretKey};
use near_primitives::errors::{
    ActionError, ActionErrorKind, FunctionCallError, InvalidAccessKeyError, InvalidTxError,
    TxExecutionError,
};
use near_primitives::test_utils::eth_implicit_test_account;
use near_primitives::transaction::{
    Action, AddKeyAction, DeployContractAction, FunctionCallAction, SignedTransaction,
    TransferAction,
};
use near_primitives::utils::derive_eth_implicit_account_id;
use near_primitives::views::{
    FinalExecutionStatus, QueryRequest, QueryResponse, QueryResponseKind,
};
use near_primitives_core::{
    account::AccessKey, checked_feature, types::BlockHeight, version::PROTOCOL_VERSION,
};
use near_store::ShardUId;
use near_vm_runner::ContractCode;
use near_wallet_contract::{wallet_contract, wallet_contract_magic_bytes};
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use node_runtime::ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT;
use rlp::RlpStream;
use testlib::runtime_utils::{alice_account, bob_account, carol_account};

use crate::{
    node::{Node, RuntimeNode},
    tests::client::process_blocks::produce_blocks_from_height,
};

/// Try to process tx in the next blocks, check that tx and all generated receipts succeed.
/// Return height of the next block.
fn check_tx_processing(
    env: &mut TestEnv,
    tx: SignedTransaction,
    height: BlockHeight,
    blocks_number: u64,
) -> BlockHeight {
    let tx_hash = tx.get_hash();
    assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    let next_height = produce_blocks_from_height(env, blocks_number, height);
    let final_outcome = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
    assert_matches!(final_outcome.status, FinalExecutionStatus::SuccessValue(_));
    next_height
}

fn view_request(env: &TestEnv, request: QueryRequest) -> QueryResponse {
    let head = env.clients[0].chain.head().unwrap();
    let head_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
    env.clients[0]
        .runtime_adapter
        .query(
            ShardUId::single_shard(),
            &head_block.chunks()[0].prev_state_root(),
            head.height,
            0,
            &head.prev_block_hash,
            &head.last_block_hash,
            head_block.header().epoch_id(),
            &request,
        )
        .unwrap()
}

/// Tests that ETH-implicit account is created correctly, with Wallet Contract hash.
#[test]
fn test_eth_implicit_account_creation() {
    if !checked_feature!("stable", EthImplicitAccounts, PROTOCOL_VERSION) {
        return;
    }
    let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let eth_implicit_account_id = eth_implicit_test_account();

    // Make zero-transfer to ETH-implicit account, invoking its creation.
    let transfer_tx = SignedTransaction::send_money(
        1,
        signer.account_id.clone(),
        eth_implicit_account_id.clone(),
        &signer,
        0,
        *genesis_block.hash(),
    );
    assert_eq!(env.clients[0].process_tx(transfer_tx, false, false), ProcessTxResponse::ValidTx);
    for i in 1..5 {
        env.produce_block(0, i);
    }

    let magic_bytes = wallet_contract_magic_bytes();

    // Verify the ETH-implicit account has zero balance and appropriate code hash.
    // Check that the account storage fits within zero balance account limit.
    let request = QueryRequest::ViewAccount { account_id: eth_implicit_account_id.clone() };
    match view_request(&env, request).kind {
        QueryResponseKind::ViewAccount(view) => {
            assert_eq!(view.amount, 0);
            assert_eq!(view.code_hash, *magic_bytes.hash());
            assert!(view.storage_usage <= ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT)
        }
        _ => panic!("wrong query response"),
    }

    // Verify that contract code deployed to the ETH-implicit account is near[wallet contract hash].
    let request = QueryRequest::ViewCode { account_id: eth_implicit_account_id };
    match view_request(&env, request).kind {
        QueryResponseKind::ViewCode(view) => {
            let contract_code = ContractCode::new(view.code, None);
            assert_eq!(contract_code.hash(), magic_bytes.hash());
            assert_eq!(contract_code.code(), magic_bytes.code());
        }
        _ => panic!("wrong query response"),
    }
}

/// Test that transactions from ETH-implicit accounts are rejected.
#[test]
fn test_transaction_from_eth_implicit_account_fail() {
    if !checked_feature!("stable", EthImplicitAccounts, PROTOCOL_VERSION) {
        return;
    }
    let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let deposit_for_account_creation = NEAR_BASE;
    let mut height = 1;
    let blocks_number = 5;
    let signer1 = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");

    let secret_key = SecretKey::from_seed(KeyType::SECP256K1, "test");
    let public_key = secret_key.public_key();
    let eth_implicit_account_id = derive_eth_implicit_account_id(public_key.unwrap_as_secp256k1());
    let eth_implicit_account_signer =
        InMemorySigner::from_secret_key(eth_implicit_account_id.clone(), secret_key);

    // Send money to ETH-implicit account, invoking its creation.
    let send_money_tx = SignedTransaction::send_money(
        1,
        "test1".parse().unwrap(),
        eth_implicit_account_id.clone(),
        &signer1,
        deposit_for_account_creation,
        *genesis_block.hash(),
    );
    // Check for tx success status and get new block height.
    height = check_tx_processing(&mut env, send_money_tx, height, blocks_number);
    let block = env.clients[0].chain.get_block_by_height(height - 1).unwrap();

    // Try to send money from ETH-implicit account using `(block_height - 1) * 1e6` as a nonce.
    // That would be a good nonce for any access key, but the transaction should fail nonetheless because there is no access key.
    let nonce = (height - 1) * AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER;
    let send_money_from_eth_implicit_account_tx = SignedTransaction::send_money(
        nonce,
        eth_implicit_account_id.clone(),
        "test0".parse().unwrap(),
        &eth_implicit_account_signer,
        100,
        *block.hash(),
    );
    let response = env.clients[0].process_tx(send_money_from_eth_implicit_account_tx, false, false);
    let expected_tx_error = ProcessTxResponse::InvalidTx(InvalidTxError::InvalidAccessKeyError(
        InvalidAccessKeyError::AccessKeyNotFound {
            account_id: eth_implicit_account_id.clone(),
            public_key: public_key.clone().into(),
        },
    ));
    assert_eq!(response, expected_tx_error);

    // Try to delete ETH-implicit account. Should fail because there is no access key.
    let delete_eth_implicit_account_tx = SignedTransaction::delete_account(
        nonce,
        eth_implicit_account_id.clone(),
        eth_implicit_account_id.clone(),
        "test0".parse().unwrap(),
        &eth_implicit_account_signer,
        *block.hash(),
    );
    let response = env.clients[0].process_tx(delete_eth_implicit_account_tx, false, false);
    assert_eq!(response, expected_tx_error);

    // Try to add an access key to the ETH-implicit account. Should fail because there is no access key.
    let add_access_key_to_eth_implicit_account_tx = SignedTransaction::from_actions(
        nonce,
        eth_implicit_account_id.clone(),
        eth_implicit_account_id.clone(),
        &eth_implicit_account_signer,
        vec![Action::AddKey(Box::new(AddKeyAction {
            public_key,
            access_key: AccessKey::full_access(),
        }))],
        *block.hash(),
    );
    let response =
        env.clients[0].process_tx(add_access_key_to_eth_implicit_account_tx, false, false);
    assert_eq!(response, expected_tx_error);

    // Try to deploy the Wallet Contract again to the ETH-implicit account. Should fail because there is no access key.
    let wallet_contract_code = wallet_contract().code().to_vec();
    let add_access_key_to_eth_implicit_account_tx = SignedTransaction::from_actions(
        nonce,
        eth_implicit_account_id.clone(),
        eth_implicit_account_id,
        &eth_implicit_account_signer,
        vec![Action::DeployContract(DeployContractAction { code: wallet_contract_code })],
        *block.hash(),
    );
    let response =
        env.clients[0].process_tx(add_access_key_to_eth_implicit_account_tx, false, false);
    assert_eq!(response, expected_tx_error);
}

// TODO(eth-implicit) Remove this test and replace it with tests that directly call the `Wallet Contract` when it is ready.
/// Creating an ETH-implicit account with meta-transaction, then attempting to use it with another meta-transaction.
///
/// The `create_account` parameter controls whether we create ETH-implicit account
/// before attempting to use it by making a function call.
/// Depending on `rlp_transaction` blob that is sent to the `Wallet Contract`
/// the transaction is either authorized or unauthorized.
/// The `authorized` parameter controls which case will be tested.
fn meta_tx_call_wallet_contract(create_account: bool, authorized: bool) {
    if !checked_feature!("stable", EthImplicitAccounts, PROTOCOL_VERSION) {
        return;
    }
    let genesis = Genesis::test(vec![alice_account(), bob_account(), carol_account()], 3);
    let relayer = alice_account();
    let node = RuntimeNode::new_from_genesis(&relayer, genesis);
    let sender = bob_account();

    let secret_key = SecretKey::from_seed(KeyType::SECP256K1, "test");
    let public_key = secret_key.public_key();
    let eth_implicit_account = derive_eth_implicit_account_id(public_key.unwrap_as_secp256k1());
    let other_public_key = SecretKey::from_seed(KeyType::SECP256K1, "test2").public_key();

    // Although ETH-implicit account can be zero-balance, we pick 1 here in order to make transfer later from this account.
    let transfer_amount = 1u128;
    let actions = vec![Action::Transfer(TransferAction { deposit: transfer_amount })];

    if create_account {
        // Create ETH-implicit account by funding it.
        node.user()
            .meta_tx(sender.clone(), eth_implicit_account.clone(), relayer.clone(), actions)
            .unwrap()
            .assert_success();
    }

    let target = carol_account();
    let initial_balance = node.view_balance(&target).expect("failed looking up balance");

    // TODO(eth-implicit) Append appropriate values to the RLP stream when proper `Wallet Contract` is implemented.
    let mut stream = RlpStream::new_list(3);
    stream.append(&target.as_str());
    // The RLP trait `Encodable` is not implemented for `u128`. We must encode it as bytes.
    // TODO(eth-implicit) Do not try to encode `u128` values directly, see https://github.com/near/nearcore/pull/10269#discussion_r1425585051.
    stream.append(&transfer_amount.to_be_bytes().as_slice());
    if authorized {
        stream.append(&public_key.key_data());
    } else {
        stream.append(&other_public_key.key_data());
    }
    let rlp_encoded_data = stream.out().to_vec();

    let args = serde_json::json!({
        "target": target.to_string(),
        "rlp_transaction": rlp_encoded_data,
    })
    .to_string()
    .into_bytes();

    let actions = vec![Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "execute_rlp".to_owned(),
        args,
        gas: 30_000_000_000_000,
        deposit: 0,
    }))];
    // Call Wallet Contract with JSON-encoded arguments: `target` and `rlp_transaction`. The `rlp_transaction`'s value is RLP-encoded.
    let tx_result =
        node.user().meta_tx(sender, eth_implicit_account.clone(), relayer, actions).unwrap();
    let wallet_contract_call_result = &tx_result.receipts_outcome[1].outcome.status;

    if create_account && authorized {
        // If the public key recovered from the RLP transaction's signature is valid for this ETH-implicit account,
        // the transaction will succeed. `target`'s balance will increase by `transfer_amount`.
        tx_result.assert_success();
        let final_balance = node.view_balance(&target).expect("failed looking up balance");
        assert_eq!(final_balance, initial_balance + transfer_amount);
        return;
    }

    if create_account {
        // The public key recovered from the RLP transaction's signature isn't valid for this ETH-implicit account.
        // The Wallet Contract will reject this transaction.
        let expected_error = near_primitives::views::ExecutionStatusView::Failure(
            TxExecutionError::ActionError(
                ActionError {
                    index: Some(0),
                    kind: ActionErrorKind::FunctionCallError {
                        0: FunctionCallError::ExecutionError(
                            "Smart contract panicked: Public key does not match the Wallet Contract address."
                            .to_string()
                        )
                    }
                }
            )
        );
        assert_eq!(wallet_contract_call_result, &expected_error);
    } else {
        // The Wallet Contract function call is not executed because the account does not exist.
        let expected_error = near_primitives::views::ExecutionStatusView::Failure(
            TxExecutionError::ActionError(ActionError {
                index: Some(0),
                kind: ActionErrorKind::AccountDoesNotExist { account_id: eth_implicit_account },
            }),
        );
        assert_eq!(wallet_contract_call_result, &expected_error);
    }
}

/// Wallet Contract function call is rejected because the ETH-implicit account does not exist.
#[test]
fn meta_tx_call_wallet_contract_account_does_not_exist() {
    meta_tx_call_wallet_contract(false, true);
}

/// Wallet Contract function call fails because the provided public key does not match the ETH-implicit address.
#[test]
fn meta_tx_call_wallet_contract_unauthorized() {
    meta_tx_call_wallet_contract(true, false);
}

/// Wallet Contract function call is executed succesfully.
#[test]
fn meta_tx_call_wallet_contract_authorized() {
    meta_tx_call_wallet_contract(true, true);
}
