use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{create_account_id, create_validators_spec, validators_spec_clients};
use near_async::time::Duration;
use near_client::QueryError;
use near_crypto::{KeyType, SecretKey};
use near_o11y::testonly::init_test_logger;
use near_primitives::action::GlobalContractDeployMode;
use near_primitives::chains::MOCKNET;
use near_primitives::types::{AccountId, Balance};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::utils::derive_eth_implicit_account_id;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature, ProtocolVersion};
use near_primitives::views::{ContractCodeView, QueryRequest, QueryResponseKind};

const FUNDED_BALANCE: Balance = Balance::from_near(5);

/// Create an eth-implicit account before and after the `UpdatedEthWalletContract` protocol feature.
/// After the feature, both accounts should have their contracts resolve to the new version.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_eth_implicit_account_wallet_contract_upgrade() {
    init_test_logger();

    // The test upgrades to PROTOCOL_VERSION, which must have the feature enabled.
    if !ProtocolFeature::UpdatedEthWalletContract.enabled(PROTOCOL_VERSION) {
        return;
    }

    let wallet_contract =
        include_bytes!("../../../runtime/near-wallet-contract/res/global_contract_mainnet.wasm");
    let old_pv = ProtocolFeature::UpdatedEthWalletContract.protocol_version() - 1;
    let epoch_length = 10;
    let relayer = create_account_id("relayer");

    // Create environment at a protocol version before the wallet upgrade.
    // Ensure it uses the MOCKNET chain id to test production contract selection logic.
    let validator_spec = create_validators_spec(2, 0);
    let clients = validators_spec_clients(&validator_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(old_pv)
        .chain_id(MOCKNET.into())
        .epoch_length(epoch_length)
        .validators_spec(validator_spec)
        .shard_layout_single_shard()
        .add_user_account_simple(relayer.clone(), Balance::from_near(100))
        .build();
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .genesis(genesis)
        .clients(clients)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(PROTOCOL_VERSION))
        .build();

    // Deploy wallet contract used by eth-implicit accounts (after the protocol feature is enabled).
    let wallet_deploy = env.validator().tx_deploy_global_contract(
        &relayer,
        wallet_contract.to_vec(),
        GlobalContractDeployMode::CodeHash,
    );
    env.validator_runner().run_tx(wallet_deploy, Duration::seconds(5));

    // Create an eth-implicit account.
    let old_eth_secret_key = SecretKey::from_seed(KeyType::SECP256K1, "old_contract");
    let old_eth_account =
        derive_eth_implicit_account_id(old_eth_secret_key.public_key().unwrap_as_secp256k1());
    let fund_tx = env.validator().tx_send_money(&relayer, &old_eth_account, FUNDED_BALANCE);
    env.validator_runner().run_tx(fund_tx, Duration::seconds(5));

    // Check the account is created with the old hash.
    check_eth_implicit_contract_hash(&env, old_pv, &old_eth_account);

    // Check the hash resolves to the old contract (not actually deployed in this test).
    let query_result = env
        .validator()
        .runtime_query(QueryRequest::ViewCode { account_id: old_eth_account.clone() });
    match &query_result {
        Err(QueryError::NoContractCode { contract_account_id, .. }) => {
            assert_eq!(contract_account_id, &old_eth_account);
        }
        other => panic!("Unexpected view code response: {other:?}"),
    }

    // Wait for protocol upgrade to happen.
    env.validator_runner().run_for_number_of_blocks((2 * epoch_length) as usize);
    assert_eq!(
        env.rpc_node().protocol_version_at_head(),
        PROTOCOL_VERSION,
        "Expected protocol version upgrade"
    );

    // Create a new eth-implicit account
    let new_eth_secret_key = SecretKey::from_seed(KeyType::SECP256K1, "new_contract");
    let new_eth_account =
        derive_eth_implicit_account_id(new_eth_secret_key.public_key().unwrap_as_secp256k1());
    let fund_tx = env.validator().tx_send_money(&relayer, &new_eth_account, FUNDED_BALANCE);
    env.validator_runner().run_tx(fund_tx, Duration::seconds(5));

    // Check the account is created with the new hash.
    check_eth_implicit_contract_hash(&env, PROTOCOL_VERSION, &new_eth_account);

    // Check both accounts resolve to the new contract.
    check_eth_implicit_view_code(&env, old_eth_account);
    check_eth_implicit_view_code(&env, new_eth_account);
}

fn check_eth_implicit_contract_hash(
    env: &TestLoopEnv,
    pv: ProtocolVersion,
    eth_account: &AccountId,
) {
    let account = env.validator().view_account_query(&eth_account).unwrap();
    assert_eq!(account.amount, FUNDED_BALANCE);
    assert!(account.global_contract_hash.is_some(), "eth-implicit accounts have a global contract");

    let global_contract_hash = account.global_contract_hash.unwrap();
    let expected_global_contract_hash =
        near_wallet_contract::eth_wallet_global_contract_hash(MOCKNET, pv);
    assert_eq!(global_contract_hash, expected_global_contract_hash);
}

fn check_eth_implicit_view_code(env: &TestLoopEnv, eth_account: AccountId) {
    let expected_hash =
        near_wallet_contract::eth_wallet_global_contract_hash(MOCKNET, PROTOCOL_VERSION);
    let query_result =
        env.validator().runtime_query(QueryRequest::ViewCode { account_id: eth_account });
    match query_result.expect("Wallet contract exists").kind {
        QueryResponseKind::ViewCode(ContractCodeView { hash, .. }) => {
            assert_eq!(hash, expected_hash);
        }
        other => panic!("Unexpected view code response: {other:?}"),
    }
}
