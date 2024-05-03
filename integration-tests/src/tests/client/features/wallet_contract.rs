use assert_matches::assert_matches;
use aurora_engine_transactions::eip_2930::Transaction2930;
use aurora_engine_transactions::EthTransactionKind;
use aurora_engine_types::types::{Address, Wei};
use ethabi::ethereum_types::U256;
use near_chain_configs::{Genesis, NEAR_BASE};
use near_client::{test_utils::TestEnv, ProcessTxResponse};
use near_crypto::{InMemorySigner, KeyType, PublicKey, SecretKey};
use near_primitives::account::id::AccountIdRef;
use near_primitives::account::{AccessKeyPermission, FunctionCallPermission};
use near_primitives::errors::{InvalidAccessKeyError, InvalidTxError};
use near_primitives::test_utils::{create_user_test_signer, eth_implicit_test_account};
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
use testlib::runtime_utils::{alice_account, bob_account};

use crate::tests::client::process_blocks::produce_blocks_from_height;

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
    println!("{final_outcome:?}");
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

fn view_balance(env: &TestEnv, account: &AccountIdRef) -> u128 {
    let request = QueryRequest::ViewAccount { account_id: account.into() };
    match view_request(&env, request).kind {
        QueryResponseKind::ViewAccount(view) => view.amount,
        _ => panic!("wrong query response"),
    }
}

fn view_nonce(env: &TestEnv, account: &AccountIdRef, pk: PublicKey) -> u64 {
    let request = QueryRequest::ViewAccessKey { account_id: account.into(), public_key: pk };
    match view_request(&env, request).kind {
        QueryResponseKind::AccessKey(view) => view.nonce,
        _ => panic!("wrong query response"),
    }
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
    let chain_id = &genesis.config.chain_id;

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

    let magic_bytes = wallet_contract_magic_bytes(chain_id);

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
    let chain_id = &genesis.config.chain_id;
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
        0,
    );
    let response =
        env.clients[0].process_tx(add_access_key_to_eth_implicit_account_tx, false, false);
    assert_eq!(response, expected_tx_error);

    // Try to deploy the Wallet Contract again to the ETH-implicit account. Should fail because there is no access key.
    let wallet_contract_code = wallet_contract(chain_id).code().to_vec();
    let add_access_key_to_eth_implicit_account_tx = SignedTransaction::from_actions(
        nonce,
        eth_implicit_account_id.clone(),
        eth_implicit_account_id,
        &eth_implicit_account_signer,
        vec![Action::DeployContract(DeployContractAction { code: wallet_contract_code })],
        *block.hash(),
        0,
    );
    let response =
        env.clients[0].process_tx(add_access_key_to_eth_implicit_account_tx, false, false);
    assert_eq!(response, expected_tx_error);
}

#[test]
fn test_wallet_contract_interaction() {
    if !checked_feature!("stable", EthImplicitAccounts, PROTOCOL_VERSION) {
        return;
    }

    let genesis = Genesis::test(vec!["test0".parse().unwrap(), alice_account(), bob_account()], 1);
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();

    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let mut height = 1;
    let blocks_number = 10;

    // As the relayer, alice will be sending Near transactions which
    // contain the Ethereum transactions the user signs.
    let relayer = alice_account();
    let mut relayer_signer =
        NearSigner { account_id: &relayer, signer: create_user_test_signer(&relayer) };
    // Bob will receive a $NEAR transfer from the eth implicit account
    let receiver = bob_account();

    // Generate an eth implicit account for the user
    let secret_key = SecretKey::from_seed(KeyType::SECP256K1, "test");
    let public_key = secret_key.public_key();
    let eth_implicit_account = derive_eth_implicit_account_id(public_key.unwrap_as_secp256k1());

    // Create ETH-implicit account by funding it.
    // Although ETH-implicit account can be zero-balance, we pick a non-zero amount
    // here in order to make transfer later from this account.
    let deposit_for_account_creation = NEAR_BASE;
    let actions = vec![Action::Transfer(TransferAction { deposit: deposit_for_account_creation })];
    let nonce =
        view_nonce(&env, relayer_signer.account_id, relayer_signer.signer.public_key.clone()) + 1;
    let block_hash = *genesis_block.hash();
    let signed_transaction = SignedTransaction::from_actions(
        nonce,
        relayer.clone(),
        eth_implicit_account.clone(),
        &relayer_signer.signer,
        actions,
        block_hash,
        0,
    );
    height = check_tx_processing(&mut env, signed_transaction, height, blocks_number);

    // The relayer adds its key to the eth implicit account so that
    // can sign Near transactions for the user.
    let relayer_pk = relayer_signer.signer.public_key.clone();
    let action = Action::AddKey(Box::new(AddKeyAction {
        public_key: relayer_pk,
        access_key: AccessKey {
            nonce: 0,
            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: None,
                receiver_id: eth_implicit_account.to_string(),
                method_names: vec!["rlp_execute".into()],
            }),
        },
    }));
    let signed_transaction = create_rlp_execute_tx(
        &eth_implicit_account,
        action,
        0,
        &eth_implicit_account,
        &secret_key,
        &mut relayer_signer,
        &env,
    );
    height = check_tx_processing(&mut env, signed_transaction, height, blocks_number);

    // Now the relayer can sign transactions for the implicit account directly
    relayer_signer.account_id = &eth_implicit_account;

    let init_wallet_balance = view_balance(&env, &eth_implicit_account);
    let init_receiver_balance = view_balance(&env, &receiver);

    // The user signs a transaction to transfer some $NEAR
    let transfer_amount = NEAR_BASE / 7;
    let action = Action::Transfer(TransferAction { deposit: transfer_amount });
    let signed_transaction = create_rlp_execute_tx(
        &receiver,
        action,
        1,
        &eth_implicit_account,
        &secret_key,
        &mut relayer_signer,
        &env,
    );
    check_tx_processing(&mut env, signed_transaction, height, blocks_number);

    let final_wallet_balance = view_balance(&env, &eth_implicit_account);
    let final_receiver_balance = view_balance(&env, &receiver);

    assert_eq!(final_receiver_balance - init_receiver_balance, transfer_amount);
    let wallet_balance_diff = init_wallet_balance - final_wallet_balance;
    // Wallet balance is a little lower due to gas fees.
    assert!(wallet_balance_diff - transfer_amount < NEAR_BASE / 500);
}

fn create_rlp_execute_tx(
    target: &AccountIdRef,
    mut action: Action,
    nonce: u64,
    eth_implicit_account: &AccountIdRef,
    secret_key: &SecretKey,
    near_signer: &mut NearSigner<'_>,
    env: &TestEnv,
) -> SignedTransaction {
    const CHAIN_ID: u64 = 399;
    // handles 24 vs 18 decimal mismatch between $NEAR and $ETH
    const MAX_YOCTO_NEAR: u128 = 1_000_000;

    // Construct Eth transaction from user's intended action
    let value = match &mut action {
        Action::Transfer(tx) => {
            let raw_amount = tx.deposit;
            tx.deposit = raw_amount % MAX_YOCTO_NEAR;
            Wei::new_u128(raw_amount / MAX_YOCTO_NEAR)
        }
        Action::FunctionCall(fn_call) => {
            let raw_amount = fn_call.deposit;
            fn_call.deposit = raw_amount % MAX_YOCTO_NEAR;
            Wei::new_u128(raw_amount / MAX_YOCTO_NEAR)
        }
        _ => Wei::zero(),
    };
    let tx_data = abi_encode(target.to_string(), action);
    let transaction = Transaction2930 {
        chain_id: CHAIN_ID,
        nonce: nonce.into(),
        gas_price: U256::zero(),
        gas_limit: U256::zero(),
        to: Some(derive_address(target)),
        value,
        data: tx_data,
        access_list: Vec::new(),
    };
    let signed_tx = sign_eth_transaction(transaction, &secret_key);
    let signed_tx_bytes: Vec<u8> = (&signed_tx).into();
    let tx_bytes_b64 = near_primitives::serialize::to_base64(&signed_tx_bytes);
    let args = format!(
        r#"{{
        "target": "{target}",
        "tx_bytes_b64": "{tx_bytes_b64}"
    }}"#
    )
    .into_bytes();

    // Construct Near transaction to `rlp_execute` method
    let actions = vec![Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "rlp_execute".into(),
        args,
        gas: 300_000_000_000_000,
        deposit: 0,
    }))];
    let nonce = view_nonce(env, near_signer.account_id, near_signer.signer.public_key.clone()) + 1;
    let block_hash = *env.clients[0].chain.get_head_block().unwrap().hash();
    SignedTransaction::from_actions(
        nonce,
        near_signer.account_id.into(),
        eth_implicit_account.into(),
        &near_signer.signer,
        actions,
        block_hash,
        0,
    )
}

struct NearSigner<'a> {
    account_id: &'a AccountIdRef,
    signer: InMemorySigner,
}

fn abi_encode(target: String, action: Action) -> Vec<u8> {
    const ADD_KEY_SELECTOR: &[u8] = &[0x75, 0x3c, 0xe5, 0xab];
    const TRANSFER_SELECTOR: &[u8] = &[0x3e, 0xd6, 0x41, 0x24];

    let mut buf = Vec::new();
    match action {
        Action::AddKey(add_key) => {
            buf.extend_from_slice(ADD_KEY_SELECTOR);
            let (public_key_kind, public_key) = match add_key.public_key {
                PublicKey::ED25519(key) => (0, key.as_ref().to_vec()),
                PublicKey::SECP256K1(key) => (1, key.as_ref().to_vec()),
            };
            let nonce = add_key.access_key.nonce;
            let (is_full_access, is_limited_allowance, allowance, receiver_id, method_names) =
                match add_key.access_key.permission {
                    AccessKeyPermission::FullAccess => (true, false, 0, String::new(), Vec::new()),
                    AccessKeyPermission::FunctionCall(permission) => (
                        false,
                        permission.allowance.is_some(),
                        permission.allowance.unwrap_or_default(),
                        permission.receiver_id,
                        permission.method_names,
                    ),
                };
            let tokens = &[
                ethabi::Token::Uint(public_key_kind.into()),
                ethabi::Token::Bytes(public_key),
                ethabi::Token::Uint(nonce.into()),
                ethabi::Token::Bool(is_full_access),
                ethabi::Token::Bool(is_limited_allowance),
                ethabi::Token::Uint(allowance.into()),
                ethabi::Token::String(receiver_id),
                ethabi::Token::Array(method_names.into_iter().map(ethabi::Token::String).collect()),
            ];
            buf.extend_from_slice(&ethabi::encode(tokens));
        }
        Action::Transfer(tx) => {
            buf.extend_from_slice(TRANSFER_SELECTOR);
            let tokens = &[ethabi::Token::String(target), ethabi::Token::Uint(tx.deposit.into())];
            buf.extend_from_slice(&ethabi::encode(tokens));
        }
        _ => unimplemented!(),
    }
    buf
}

fn sign_eth_transaction(transaction: Transaction2930, sk: &SecretKey) -> EthTransactionKind {
    let mut rlp_stream = rlp::RlpStream::new();
    rlp_stream.append(&aurora_engine_transactions::eip_2930::TYPE_BYTE);
    transaction.rlp_append_unsigned(&mut rlp_stream);
    let message_hash = keccak256(rlp_stream.as_raw());
    let signature = sk.sign(&message_hash);
    let bytes: [u8; 65] = match signature {
        near_crypto::Signature::SECP256K1(x) => x.into(),
        _ => panic!("Expected SECP256K1 key"),
    };
    let v = bytes[64];
    let r = U256::from_big_endian(&bytes[0..32]);
    let s = U256::from_big_endian(&bytes[32..64]);
    let signed_transaction = aurora_engine_transactions::eip_2930::SignedTransaction2930 {
        transaction,
        parity: v,
        r,
        s,
    };
    EthTransactionKind::Eip2930(signed_transaction)
}

fn keccak256(bytes: &[u8]) -> [u8; 32] {
    use sha3::{Digest, Keccak256};

    Keccak256::digest(bytes).into()
}

fn derive_address(account_id: &AccountIdRef) -> Address {
    let bytes = if account_id.as_str().starts_with("0x") {
        let buf = hex::decode(&account_id.as_str()[2..42]).expect("account_id is hex encoded");
        return Address::try_from_slice(&buf).expect("slice is correct size");
    } else {
        account_id.as_bytes()
    };
    let hash = keccak256(bytes);
    Address::try_from_slice(&hash[12..32]).expect("slice is correct size")
}
