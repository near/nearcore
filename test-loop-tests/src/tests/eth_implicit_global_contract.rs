use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{create_account_id, create_validators_spec, validators_spec_clients};
use aurora_engine_transactions::EthTransactionKind;
use aurora_engine_transactions::eip_2930::Transaction2930;
use aurora_engine_types::types::Wei;
use ethabi::ethereum_types::U256;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_crypto::{KeyType, SecretKey};
use near_jsonrpc_primitives::types::query::{QueryResponseKind, RpcQueryRequest};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::id::AccountIdRef;
use near_primitives::action::{Action, FunctionCallAction, GlobalContractDeployMode};
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, BlockReference, Finality, FunctionArgs, Gas};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::utils::derive_eth_implicit_account_id;
use near_primitives::version::ProtocolFeature;
use near_primitives::views::QueryRequest;
use near_vm_runner::ContractCode;
use near_wallet_contract::{LegacyEthWallet, eth_wallet_global_contract_hash};
use sha3::{Digest, Keccak256};
use std::collections::BTreeMap;
use std::sync::Arc;

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
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[test]
// TODO: re-enable on non-x86_64 once this test runs at protocol version 84+.
// On non-x86_64 (e.g. macOS ARM), Wasmtime is always used, which runs V3
// instrumentation that charges linear gas for memory.grow/table.grow. This test
// runs at a protocol version where linear_op costs are 300 Tgas sentinel values
// (only calibrated in protocol 84+), making any contract with memory.grow
// immediately exceed max_gas_burnt.
#[cfg_attr(any(feature = "protocol_feature_spice", not(target_arch = "x86_64")), ignore)]
fn test_eth_implicit_global_contract_mainnet_upgrade() {
    init_test_logger();

    let contract_code = ContractCode::new(GLOBAL_CONTRACT_MAINNET_WASM.to_vec(), None);
    let expected_hash = eth_wallet_global_contract_hash(near_primitives_core::chains::MOCKNET);
    assert_eq!(*contract_code.hash(), expected_hash, "WASM hash mismatch");

    let (old_pv, new_pv) = (82, 83);
    assert!(!ProtocolFeature::EthImplicitGlobalContract.enabled(old_pv));
    assert!(ProtocolFeature::EthImplicitGlobalContract.enabled(new_pv));

    let epoch_length = 5;
    let chunk_validator_idx = 1;
    let validators_spec = create_validators_spec(1, 1);
    let clients = validators_spec_clients(&validators_spec);
    let relayer = create_account_id("relayer");
    let receiver = create_account_id("receiver");
    let genesis = TestLoopBuilder::new_genesis_builder()
        .chain_id("mocknet".to_string())
        .protocol_version(old_pv)
        .epoch_length(epoch_length)
        .shard_layout(ShardLayout::single_shard())
        .validators_spec(validators_spec)
        .add_user_account_simple(relayer.clone(), Balance::from_near(1_000_000))
        .add_user_account_simple(receiver.clone(), Balance::from_near(10))
        .build();
    let epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from([
        (old_pv, Arc::new(epoch_config.clone())),
        (new_pv, Arc::new(epoch_config)),
    ]));
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(new_pv))
        .clients(clients)
        .build();

    let transfer_amount = Balance::from_near(1).checked_div(7).unwrap();

    // Phase 1: Create ETH implicit account at PV 82 (magic bytes path).
    let secret_key_old = SecretKey::from_seed(KeyType::SECP256K1, "test_old");
    let eth_old = derive_eth_implicit_account_id(secret_key_old.public_key().unwrap_as_secp256k1());

    let create_eth_account_tx =
        env.validator().tx_send_money(&relayer, &eth_old, Balance::from_near(5));
    env.validator_runner().run_tx(create_eth_account_tx, Duration::seconds(5));
    assert_eq!(env.validator().view_account_query(&eth_old).unwrap().global_contract_hash, None);

    // Verify view calls work at PV 82 (legacy wallet contract path).
    // chain_id "mocknet" resolves to the localnet legacy wallet contract.
    let code = rpc_view_code(&mut env, &eth_old);
    let legacy_wasm = LegacyEthWallet::Localnet.contract();
    assert_eq!(
        code,
        legacy_wasm.code(),
        "ViewCode should return legacy localnet wallet contract at PV 82"
    );
    assert_eq!(rpc_get_nonce(&mut env, &eth_old), 0, "get_nonce should return 0 at PV 82");

    // Phase 2: Wait for protocol upgrade to PV 83.
    env.validator_runner().run_until(
        |node| {
            let epoch_id = node.head().epoch_id;
            node.client().epoch_manager.get_epoch_info(&epoch_id).unwrap().protocol_version()
                == new_pv
        },
        Duration::seconds((4 * epoch_length) as i64),
    );

    // Phase 3: Deploy real mainnet WASM as global contract.
    let deploy_tx = env.validator().tx_deploy_global_contract(
        &relayer,
        GLOBAL_CONTRACT_MAINNET_WASM.to_vec(),
        GlobalContractDeployMode::CodeHash,
    );
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));
    // Make sure that global contract propagation finishes
    env.validator_runner().run_for_number_of_blocks(2);

    // Clear the cache here to ensure that eth global contract is properly
    // propagated as part of the witness distribution.
    env.node(chunk_validator_idx).clear_compiled_contract_cache();

    // Verify view calls on old account at PV 83 (global contract path).
    let code = rpc_view_code(&mut env, &eth_old);
    assert_eq!(code, GLOBAL_CONTRACT_MAINNET_WASM, "ViewCode should return global contract WASM");
    assert_eq!(
        rpc_get_nonce(&mut env, &eth_old),
        0,
        "get_nonce should return 0 before any rlp_execute"
    );

    // Phase 4: Old account still works after upgrade (rlp_execute transfer).
    let before = env.validator().view_account_query(&receiver).unwrap().amount;
    let tx = build_rlp_execute_tx(
        &env,
        &receiver,
        transfer_amount,
        0,
        &eth_old,
        &secret_key_old,
        &relayer,
    );
    env.validator_runner().run_tx(tx, Duration::seconds(5));
    let after = env.validator().view_account_query(&receiver).unwrap().amount;
    assert_eq!(after.checked_sub(before).unwrap(), transfer_amount, "old account transfer failed");
    assert_eq!(rpc_get_nonce(&mut env, &eth_old), 1, "get_nonce should return 1 after rlp_execute");

    // Phase 5: Create new ETH implicit account at PV 83 (global contract path).
    let secret_key_new = SecretKey::from_seed(KeyType::SECP256K1, "test_new");
    let eth_new = derive_eth_implicit_account_id(secret_key_new.public_key().unwrap_as_secp256k1());

    let create_eth_new_tx =
        env.validator().tx_send_money(&relayer, &eth_new, Balance::from_near(5));
    env.validator_runner().run_tx(create_eth_new_tx, Duration::seconds(5));
    assert_eq!(
        env.validator().view_account_query(&eth_new).unwrap().global_contract_hash,
        Some(expected_hash)
    );

    // Verify view calls on new account at PV 83.
    let code = rpc_view_code(&mut env, &eth_new);
    assert_eq!(code, GLOBAL_CONTRACT_MAINNET_WASM, "ViewCode should return global contract WASM");
    assert_eq!(rpc_get_nonce(&mut env, &eth_new), 0, "get_nonce should return 0 for new account");

    // Phase 6: New account works via rlp_execute transfer.
    let before = env.validator().view_account_query(&receiver).unwrap().amount;
    let tx = build_rlp_execute_tx(
        &env,
        &receiver,
        transfer_amount,
        0,
        &eth_new,
        &secret_key_new,
        &relayer,
    );
    env.validator_runner().run_tx(tx, Duration::seconds(5));
    let after = env.validator().view_account_query(&receiver).unwrap().amount;
    assert_eq!(after.checked_sub(before).unwrap(), transfer_amount, "new account transfer failed");
    assert_eq!(rpc_get_nonce(&mut env, &eth_new), 1, "get_nonce should return 1 after rlp_execute");
}

fn build_rlp_execute_tx_args(
    target: &AccountIdRef,
    amount: Balance,
    eth_nonce: u64,
    eth_secret_key: &SecretKey,
) -> Vec<u8> {
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
    args
}

/// Builds an rlp_execute transaction that transfers `amount` to `target`.
/// The Ethereum tx is signed by `eth_secret_key`; the NEAR tx is created via
/// `env.validator().tx_from_actions()` which handles nonce and block hash.
fn build_rlp_execute_tx(
    env: &TestLoopEnv,
    target: &AccountIdRef,
    amount: Balance,
    eth_nonce: u64,
    eth_implicit_account: &AccountId,
    eth_secret_key: &SecretKey,
    near_signer_account: &AccountId,
) -> SignedTransaction {
    let args = build_rlp_execute_tx_args(target, amount, eth_nonce, eth_secret_key);
    env.validator().tx_from_actions(
        near_signer_account,
        eth_implicit_account,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "rlp_execute".into(),
            args,
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
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

/// Query ViewCode via JSON-RPC and return the contract code bytes.
fn rpc_view_code(env: &mut TestLoopEnv, account_id: &AccountId) -> Vec<u8> {
    let result = env
        .validator_runner()
        .run_jsonrpc_query(
            RpcQueryRequest {
                block_reference: BlockReference::Finality(Finality::None),
                request: QueryRequest::ViewCode { account_id: account_id.clone() },
            },
            Duration::seconds(5),
        )
        .unwrap();
    match result.kind {
        QueryResponseKind::ViewCode(view) => view.code,
        other => panic!("expected ViewCode, got: {other:?}"),
    }
}

/// Call `get_nonce` view function via JSON-RPC and return the decoded nonce.
fn rpc_get_nonce(env: &mut TestLoopEnv, account_id: &AccountId) -> u64 {
    let result = env
        .validator_runner()
        .run_jsonrpc_query(
            RpcQueryRequest {
                block_reference: BlockReference::Finality(Finality::None),
                request: QueryRequest::CallFunction {
                    account_id: account_id.clone(),
                    method_name: "get_nonce".to_string(),
                    args: FunctionArgs::from(vec![]),
                },
            },
            Duration::seconds(5),
        )
        .unwrap();
    let bytes = match result.kind {
        QueryResponseKind::CallResult(call_result) => call_result.result,
        other => panic!("expected CallResult, got: {other:?}"),
    };
    let nonce: String = serde_json::from_slice(&bytes).expect("get_nonce should return valid JSON");
    nonce.parse().expect("get_nonce should return a numeric string")
}
