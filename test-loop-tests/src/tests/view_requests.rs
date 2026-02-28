use near_async::messaging::Handler;
use near_async::time::Duration;
use near_client::GetStateChanges;
use near_crypto::{KeyType, SecretKey};
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, AddKeyAction};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, AccountWithPublicKey, Balance, NonceIndex};
use near_primitives::views::{StateChangeValueView, StateChangesRequestView};
use near_primitives_core::account::AccessKey;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::transactions::get_shared_block_hash;

#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_access_key_changes_includes_gas_key_nonces() {
    init_test_logger();

    let epoch_length = 10;
    let submitter: AccountId = "account2".parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .validators(2, 0)
        .enable_rpc()
        .epoch_length(epoch_length)
        .add_user_account(&submitter, Balance::from_near(1_000_000))
        .build()
        .warmup();

    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    // TODO(spice): Replace with get_next_nonce once it works with spice.
    // let nonce = get_next_nonce(&env.test_loop.data, &env.node_datas, &submitter);
    let nonce = 1;

    let num_nonces: NonceIndex = 2;
    let gas_key_secret = SecretKey::from_seed(KeyType::ED25519, "gas_key");
    let gas_key_public = gas_key_secret.public_key();
    let tx = SignedTransaction::from_actions(
        nonce,
        submitter.clone(),
        submitter.clone(),
        &create_user_test_signer(&submitter),
        vec![Action::AddKey(Box::new(AddKeyAction {
            public_key: gas_key_public.clone(),
            access_key: AccessKey::gas_key_full_access(num_nonces),
        }))],
        block_hash,
    );
    let outcome = env.rpc_runner().execute_tx(tx, Duration::seconds(5)).unwrap();
    let tx_block_hash = outcome.transaction_outcome.block_hash;
    let tx_block_header = env.rpc_node().client().chain.get_block_header(&tx_block_hash).unwrap();
    env.rpc_runner().run_until_block_executed(&tx_block_header, Duration::seconds(10));
    let mut rpc = env.rpc_node_mut();
    let view_client = rpc.view_client_actor();

    // Test AllAccessKey changes request
    let state_changes = view_client
        .handle(GetStateChanges {
            block_hash: tx_block_hash,
            state_changes_request: StateChangesRequestView::AllAccessKeyChanges {
                account_ids: vec![submitter.clone()],
            },
        })
        .unwrap();

    let access_key_updates = state_changes
        .iter()
        .filter(|sc| matches!(&sc.value, StateChangeValueView::AccessKeyUpdate { .. }))
        .count();
    let gas_key_nonce_updates = state_changes
        .iter()
        .filter(|sc| matches!(&sc.value, StateChangeValueView::GasKeyNonceUpdate { .. }))
        .count();
    assert_eq!(access_key_updates, 2); // One for the new gas key, one for the access key used to submit the tx.
    assert_eq!(gas_key_nonce_updates, num_nonces as usize);

    // Test SingleAccessKey changes request
    let state_changes = view_client
        .handle(GetStateChanges {
            block_hash: tx_block_hash,
            state_changes_request: StateChangesRequestView::SingleAccessKeyChanges {
                keys: vec![AccountWithPublicKey {
                    account_id: submitter,
                    public_key: gas_key_public,
                }],
            },
        })
        .unwrap();
    let access_key_updates = state_changes
        .iter()
        .filter(|sc| matches!(&sc.value, StateChangeValueView::AccessKeyUpdate { .. }))
        .count();
    let gas_key_nonce_updates = state_changes
        .iter()
        .filter(|sc| matches!(&sc.value, StateChangeValueView::GasKeyNonceUpdate { .. }))
        .count();
    assert_eq!(access_key_updates, 1); // Only for the new gas key.
    assert_eq!(gas_key_nonce_updates, num_nonces as usize);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
