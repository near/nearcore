use std::collections::BTreeMap;
use std::sync::Arc;

use aurora_engine_transactions::{EthTransactionKind, eip_2930::Transaction2930};
use aurora_engine_types::types::Wei;
use ethabi::ethereum_types::U256;
use near_async::{test_loop::data::TestLoopData, time::Duration};
use near_chain_configs::test_genesis::{
    TestEpochConfigBuilder, TestGenesisBuilder, ValidatorsSpec,
};
use near_crypto::{KeyType, SecretKey};
use near_o11y::testonly::init_test_logger;
use near_primitives::{
    account::id::AccountIdRef,
    action::{Action, FunctionCallAction, GlobalContractDeployMode},
    epoch_manager::EpochConfigStore,
    hash::CryptoHash,
    shard_layout::ShardLayout,
    test_utils::create_user_test_signer,
    transaction::SignedTransaction,
    types::{AccountId, Balance, Gas},
    upgrade_schedule::ProtocolUpgradeVotingSchedule,
    utils::derive_eth_implicit_account_id,
    version::ProtocolFeature,
    views::{QueryRequest, QueryResponseKind},
};
use near_vm_runner::ContractCode;
use near_wallet_contract::eth_wallet_global_contract_hash;
use sha3::{Digest, Keccak256};

use crate::{
    setup::{builder::TestLoopBuilder, env::TestLoopEnv},
    utils::{node::TestLoopNode, transactions},
};

const GLOBAL_CONTRACT_MAINNET_WASM: &[u8] =
    include_bytes!("../../../runtime/near-wallet-contract/res/global_contract_mainnet.wasm");

/// Ethereum chain ID expected by the mainnet wallet contract.
/// After EthImplicitGlobalContract upgrade (PV 83), all ETH implicit accounts
/// (including old magic-bytes ones) resolve to the mainnet global contract.
const MAINNET_ETH_CHAIN_ID: u64 = 397;

/// End-to-end test of the mainnet global contract across a PV 82 -> 83 upgrade.
///
/// Uses chain_id="mocknet" (maps to mainnet global contract hash without requiring
/// the hardcoded mainnet genesis state root).
///
/// Flow:
/// - create old account at PV 82
/// - upgrade to PV 83
/// - deploy global contract
/// - verify old account works via rlp_execute
/// - create new account
/// - verify new account works via rlp_execute
#[test]
fn test_eth_implicit_global_contract_mainnet_upgrade() {
    init_test_logger();

    let contract_code = ContractCode::new(GLOBAL_CONTRACT_MAINNET_WASM.to_vec(), None);
    let expected_hash = eth_wallet_global_contract_hash(near_primitives_core::chains::MOCKNET);
    assert_eq!(*contract_code.hash(), expected_hash, "WASM hash mismatch");

    let (old_pv, new_pv) = (82, 83);
    assert!(!ProtocolFeature::EthImplicitGlobalContract.enabled(old_pv));
    assert!(ProtocolFeature::EthImplicitGlobalContract.enabled(new_pv));

    let epoch_length = 10;
    let validators_spec = ValidatorsSpec::desired_roles(&["validator0"], &[]);
    let relayer: AccountId = "relayer".parse().unwrap();
    let receiver: AccountId = "receiver".parse().unwrap();

    let builder = TestLoopBuilder::new();
    let genesis = TestGenesisBuilder::new()
        .chain_id("mocknet".to_string())
        .protocol_version(old_pv)
        .genesis_time_from_clock(&builder.clock())
        .epoch_length(epoch_length)
        .shard_layout(ShardLayout::single_shard())
        .validators_spec(validators_spec.clone())
        .add_user_account_simple(relayer.clone(), Balance::from_near(1_000_000))
        .add_user_account_simple(receiver.clone(), Balance::from_near(10))
        .build();

    let epoch_config = TestEpochConfigBuilder::new()
        .epoch_length(epoch_length)
        .shard_layout(ShardLayout::single_shard())
        .validators_spec(validators_spec)
        .build();

    let epoch_config_store = EpochConfigStore::test(BTreeMap::from([
        (old_pv, Arc::new(epoch_config.clone())),
        (new_pv, Arc::new(epoch_config)),
    ]));

    let TestLoopEnv { mut test_loop, node_datas, shared_state } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(new_pv))
        .clients(vec!["validator0".parse().unwrap()])
        .build()
        .warmup();

    let node = TestLoopNode::for_account(&node_datas, &"validator0".parse().unwrap());
    let relayer_signer = create_user_test_signer(&relayer);
    let mut relayer_nonce = 0;
    let transfer_amount = Balance::from_near(1).checked_div(7).unwrap();

    // Phase 1: Create ETH implicit account at PV 82 (magic bytes path).
    let secret_key_old = SecretKey::from_seed(KeyType::SECP256K1, "test_old");
    let eth_old = derive_eth_implicit_account_id(secret_key_old.public_key().unwrap_as_secp256k1());

    relayer_nonce += 1;
    let block_hash = transactions::get_shared_block_hash(&node_datas, &test_loop.data);
    node.run_tx(
        &mut test_loop,
        SignedTransaction::send_money(
            relayer_nonce,
            relayer.clone(),
            eth_old.clone(),
            &relayer_signer,
            Balance::from_near(5),
            block_hash,
        ),
        Duration::seconds(5),
    );
    test_loop.run_for(Duration::seconds(2));

    assert_eq!(view_global_contract_hash(&node, &test_loop.data, &eth_old), None);

    // Phase 2: Wait for protocol upgrade to PV 83.
    let client_handle = node_datas[0].client_sender.actor_handle();
    test_loop.run_until(
        |data: &mut TestLoopData| {
            let head = data.get(&client_handle).client.chain.head().unwrap();
            data.get(&client_handle)
                .client
                .epoch_manager
                .get_epoch_info(&head.epoch_id)
                .unwrap()
                .protocol_version()
                == new_pv
        },
        Duration::seconds((4 * epoch_length) as i64),
    );

    // Phase 3: Deploy real mainnet WASM as global contract.
    relayer_nonce += 1;
    let block_hash = transactions::get_shared_block_hash(&node_datas, &test_loop.data);
    node.run_tx(
        &mut test_loop,
        SignedTransaction::deploy_global_contract(
            relayer_nonce,
            relayer.clone(),
            GLOBAL_CONTRACT_MAINNET_WASM.to_vec(),
            &relayer_signer,
            block_hash,
            GlobalContractDeployMode::CodeHash,
        ),
        Duration::seconds(5),
    );
    test_loop.run_for(Duration::seconds(2));

    // Phase 4: Old account still works after upgrade (rlp_execute transfer).
    let before = node.view_account_query(&test_loop.data, &receiver).unwrap().amount;
    relayer_nonce += 1;
    let block_hash = transactions::get_shared_block_hash(&node_datas, &test_loop.data);
    node.run_tx(
        &mut test_loop,
        build_rlp_execute_tx(
            &receiver,
            transfer_amount,
            0,
            &eth_old,
            &secret_key_old,
            &relayer,
            &relayer_signer,
            relayer_nonce,
            block_hash,
        ),
        Duration::seconds(5),
    );
    test_loop.run_for(Duration::seconds(2));
    let after = node.view_account_query(&test_loop.data, &receiver).unwrap().amount;
    assert_eq!(after.checked_sub(before).unwrap(), transfer_amount, "old account transfer failed");

    // Phase 5: Create new ETH implicit account at PV 83 (global contract path).
    let secret_key_new = SecretKey::from_seed(KeyType::SECP256K1, "test_new");
    let eth_new = derive_eth_implicit_account_id(secret_key_new.public_key().unwrap_as_secp256k1());

    relayer_nonce += 1;
    let block_hash = transactions::get_shared_block_hash(&node_datas, &test_loop.data);
    node.run_tx(
        &mut test_loop,
        SignedTransaction::send_money(
            relayer_nonce,
            relayer.clone(),
            eth_new.clone(),
            &relayer_signer,
            Balance::from_near(5),
            block_hash,
        ),
        Duration::seconds(5),
    );
    test_loop.run_for(Duration::seconds(2));

    assert_eq!(view_global_contract_hash(&node, &test_loop.data, &eth_new), Some(expected_hash),);

    // Phase 6: New account works via rlp_execute transfer.
    let before = node.view_account_query(&test_loop.data, &receiver).unwrap().amount;
    relayer_nonce += 1;
    let block_hash = transactions::get_shared_block_hash(&node_datas, &test_loop.data);
    node.run_tx(
        &mut test_loop,
        build_rlp_execute_tx(
            &receiver,
            transfer_amount,
            0,
            &eth_new,
            &secret_key_new,
            &relayer,
            &relayer_signer,
            relayer_nonce,
            block_hash,
        ),
        Duration::seconds(5),
    );
    test_loop.run_for(Duration::seconds(2));
    let after = node.view_account_query(&test_loop.data, &receiver).unwrap().amount;
    assert_eq!(after.checked_sub(before).unwrap(), transfer_amount, "new account transfer failed");

    drop(shared_state);
    test_loop.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

fn view_global_contract_hash(
    node: &TestLoopNode,
    data: &TestLoopData,
    account: &AccountId,
) -> Option<CryptoHash> {
    match node
        .runtime_query(data, QueryRequest::ViewAccount { account_id: account.clone() })
        .unwrap()
        .kind
    {
        QueryResponseKind::ViewAccount(v) => v.global_contract_hash,
        _ => panic!("unexpected query response"),
    }
}

/// Builds an rlp_execute transaction that transfers `amount` to `target`.
/// The Ethereum tx is signed by `eth_secret_key`; the NEAR tx is signed by `near_signer`.
fn build_rlp_execute_tx(
    target: &AccountIdRef,
    amount: Balance,
    eth_nonce: u64,
    eth_implicit_account: &AccountIdRef,
    eth_secret_key: &SecretKey,
    near_signer_account: &AccountIdRef,
    near_signer: &near_crypto::Signer,
    near_nonce: u64,
    block_hash: CryptoHash,
) -> SignedTransaction {
    const MAX_YOCTO_NEAR: u128 = 1_000_000;
    let yocto = amount.as_yoctonear();
    let remainder = Balance::from_yoctonear(yocto % MAX_YOCTO_NEAR);
    let wei_value = Wei::new_u128(yocto / MAX_YOCTO_NEAR);

    // ABI-encode the transfer call: selector + (target_string, yoctonear_remainder)
    let mut tx_data = vec![0x3e, 0xd6, 0x41, 0x24]; // transfer selector
    tx_data.extend_from_slice(&ethabi::encode(&[
        ethabi::Token::String(target.to_string()),
        ethabi::Token::Uint(remainder.as_yoctonear().into()),
    ]));

    let transaction = Transaction2930 {
        chain_id: MAINNET_ETH_CHAIN_ID,
        nonce: eth_nonce.into(),
        gas_price: U256::zero(),
        gas_limit: U256::zero(),
        to: Some(derive_address(target)),
        value: wei_value,
        data: tx_data,
        access_list: Vec::new(),
    };

    let signed_tx = sign_eth_transaction(transaction, eth_secret_key);
    let tx_bytes_b64 = near_primitives::serialize::to_base64(&<Vec<u8>>::from(&signed_tx));
    let args =
        format!(r#"{{"target": "{target}", "tx_bytes_b64": "{tx_bytes_b64}"}}"#).into_bytes();

    SignedTransaction::from_actions(
        near_nonce,
        near_signer_account.into(),
        eth_implicit_account.into(),
        near_signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "rlp_execute".into(),
            args,
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
        block_hash,
    )
}

fn sign_eth_transaction(transaction: Transaction2930, sk: &SecretKey) -> EthTransactionKind {
    let mut rlp_stream = rlp::RlpStream::new();
    rlp_stream.append(&aurora_engine_transactions::eip_2930::TYPE_BYTE);
    transaction.rlp_append_unsigned(&mut rlp_stream);
    let msg_hash: [u8; 32] = Keccak256::digest(rlp_stream.as_raw()).into();
    let sig = sk.sign(&msg_hash);
    let bytes: [u8; 65] = match sig {
        near_crypto::Signature::SECP256K1(x) => x.into(),
        _ => panic!("expected SECP256K1"),
    };
    EthTransactionKind::Eip2930(aurora_engine_transactions::eip_2930::SignedTransaction2930 {
        transaction,
        parity: bytes[64],
        r: U256::from_big_endian(&bytes[0..32]),
        s: U256::from_big_endian(&bytes[32..64]),
    })
}

fn derive_address(account_id: &AccountIdRef) -> aurora_engine_types::types::Address {
    if account_id.as_str().starts_with("0x") {
        let decoded = hex::decode(&account_id.as_str()[2..42]).unwrap();
        return aurora_engine_types::types::Address::try_from_slice(&decoded).unwrap();
    }
    let hash: [u8; 32] = Keccak256::digest(account_id.as_bytes()).into();
    aurora_engine_types::types::Address::try_from_slice(&hash[12..32]).unwrap()
}
