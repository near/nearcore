use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_crypto::{InMemorySigner, KeyType};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::AccessKey;
use near_primitives::action::AddKeyAction;
use near_primitives::action::delegate::{DelegateAction, NonDelegateAction, SignedDelegateAction};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::Action;
use near_primitives::types::Balance;

/// Tests the classic meta transaction (NEP-366) flow: an account signs an action
/// but a different account (the relayer) pays the gas.
///
/// A candidate account is created with a fixed balance and a single full-access
/// key. A `DelegateAction` signed by the candidate adds a second full-access key
/// to it, wrapped in a transaction signed and submitted by the relayer. We verify
/// the new key was added, that the candidate's balance is unchanged, and that the
/// relayer's balance decreased — proving the relayer, not the candidate, paid the
/// gas. A non-zero gas price is required, otherwise gas is free and no balance moves.
#[test]
fn test_meta_tx() {
    init_test_logger();

    let relayer = create_account_id("relayer");
    let candidate = create_account_id("candidate.relayer");
    let candidate_amount = Balance::from_near(123);

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&relayer, Balance::from_near(1_000_000))
        .gas_prices(Balance::from_yoctonear(1), Balance::from_yoctonear(1))
        .build();

    let create_tx = env.rpc_node().tx_create_account(&relayer, &candidate, candidate_amount);
    env.rpc_runner().run_tx(create_tx, Duration::seconds(5));

    // The candidate starts with exactly its own (full-access) key and the given balance.
    let candidate_signer = create_user_test_signer(&candidate);
    assert!(
        env.rpc_node().view_access_key_query(&candidate, &candidate_signer.public_key()).is_ok()
    );
    assert_eq!(env.rpc_node().query_balance(&candidate), candidate_amount);

    let new_key =
        InMemorySigner::from_seed(candidate.clone(), KeyType::ED25519, "new_key").public_key();
    assert!(env.rpc_node().view_access_key_query(&candidate, &new_key).is_err());

    // Build the meta transaction: the candidate signs a DelegateAction that adds
    // the new key to itself.
    let add_key_action = Action::AddKey(Box::new(AddKeyAction {
        public_key: new_key.clone(),
        access_key: AccessKey::full_access(),
    }));
    let candidate_nonce = env.rpc_node().get_next_nonce(&candidate);
    let delegate_action = DelegateAction {
        sender_id: candidate.clone(),
        receiver_id: candidate.clone(),
        actions: vec![NonDelegateAction::try_from(add_key_action).unwrap()],
        nonce: candidate_nonce,
        max_block_height: env.rpc_node().head().height + 100,
        public_key: candidate_signer.public_key(),
    };
    let signed_delegate_action = SignedDelegateAction::sign(&candidate_signer, delegate_action);

    // The relayer wraps and submits it, paying the gas.
    let relayer_balance_before = env.rpc_node().query_balance(&relayer);
    let meta_tx =
        env.rpc_node().tx_from_actions(&relayer, &candidate, vec![signed_delegate_action.into()]);
    env.rpc_runner().run_tx(meta_tx, Duration::seconds(5));

    // Both keys now exist on the candidate account, and its balance is unchanged:
    // the relayer, not the candidate, paid the gas.
    assert!(
        env.rpc_node().view_access_key_query(&candidate, &candidate_signer.public_key()).is_ok()
    );
    assert!(env.rpc_node().view_access_key_query(&candidate, &new_key).is_ok());
    assert_eq!(env.rpc_node().query_balance(&candidate), candidate_amount);

    // The relayer's balance dropped: it paid the gas for the meta transaction.
    let relayer_balance_after = env.rpc_node().query_balance(&relayer);
    assert!(
        relayer_balance_after < relayer_balance_before,
        "relayer balance should decrease (it paid the gas): before={relayer_balance_before}, after={relayer_balance_after}"
    );
}
