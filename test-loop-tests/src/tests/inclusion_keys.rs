use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use crate::utils::node::TestLoopNode;
use near_async::time::Duration;
use near_crypto::{PublicKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::AccessKey;
use near_primitives::action::FundInclusionKeyAction;
use near_primitives::action::delegate::{DelegateAction, SignedDelegateAction};
use near_primitives::errors::{InvalidTxError, TxExecutionError};
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{Action, SignedTransaction, TransferAction};
use near_primitives::types::{AccountId, Balance};
use near_store::get_access_key;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_inclusion_key_tx_nonce_used_by_delegate_action_charged_at_execution() {
    init_test_logger();

    let alice = create_account_id("alice");
    let relayer = create_account_id("relayer");
    let receiver = create_account_id("receiver");
    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let max_gas_price = Balance::from_yoctonear(10_000_000_000_000_000_000_000);
    let mut env = TestLoopBuilder::new()
        .validators(1, 1)
        .gas_prices(min_gas_price, max_gas_price)
        .add_user_account(&alice, Balance::from_near(10))
        .add_user_account(&relayer, Balance::from_near(10))
        .add_user_account(&receiver, Balance::from_near(0))
        .delay_warmup()
        .config_modifier(|c, _| {
            c.set_spice_pending_transaction_queue_enabled(true);
        })
        .build();
    let execution_delay = 4;
    env.delay_endorsements_propagation(execution_delay);
    let mut env = env.warmup();
    let alice_signer: Signer = create_user_test_signer(&alice);

    let key_balance = Balance::from_millinear(10);
    let fund_nonce = env.validator().get_next_nonce(&alice);
    let fund_tx = SignedTransaction::from_actions(
        fund_nonce,
        alice.clone(),
        alice.clone(),
        &alice_signer,
        vec![Action::FundInclusionKey(Box::new(FundInclusionKeyAction {
            public_key: alice_signer.public_key(),
            target_balance: key_balance,
        }))],
        env.validator().head().last_block_hash,
    );
    env.validator_runner().run_tx(fund_tx, Duration::seconds(20));

    let delegate_action_nonce = fund_nonce + 5;
    let delegate_action = DelegateAction {
        sender_id: alice.clone(),
        receiver_id: receiver.clone(),
        actions: vec![
            Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })
                .try_into()
                .unwrap(),
        ],
        nonce: delegate_action_nonce,
        max_block_height: 1_000_000,
        public_key: alice_signer.public_key(),
    };
    let signed_delegate_action = SignedDelegateAction::sign(&alice_signer, delegate_action);
    let delegate_tx = env.validator().tx_from_actions(
        &relayer,
        &alice,
        vec![Action::Delegate(Box::new(signed_delegate_action))],
    );
    let delegate_tx_hash = env.validator().submit_tx(delegate_tx);
    let delegate_tx_height = env.validator_runner().run_until_included(&[delegate_tx_hash]);

    // The certified state and the pending transaction queue do not see the delegate action, so
    // this nonce is still admitted. The delegate action uses it before this transaction executes.
    let stale_nonce = fund_nonce + 1;
    let stale_tx = SignedTransaction::send_money(
        stale_nonce,
        alice.clone(),
        receiver,
        &alice_signer,
        Balance::from_yoctonear(1),
        env.validator().head().last_block_hash,
    );
    let stale_tx_hash = env.validator().submit_tx(stale_tx);
    let stale_tx_height = env.validator_runner().run_until_included(&[stale_tx_hash]);
    let stale_tx_outcome =
        env.validator_runner().run_until_outcome_available(stale_tx_hash, Duration::seconds(20));

    assert!(delegate_tx_height + 1 < stale_tx_height);
    let stale_tx_status = &stale_tx_outcome.outcome_with_id.outcome.status;
    assert!(
        matches!(
            stale_tx_status,
            near_primitives::transaction::ExecutionStatus::Failure(
                TxExecutionError::InvalidTxError(InvalidTxError::InvalidNonce { .. })
            )
        ),
        "{stale_tx_status:?}"
    );
    let charge = stale_tx_outcome.outcome_with_id.outcome.tokens_burnt;
    assert!(charge > Balance::ZERO);
    let inclusion_key = access_key_at_block(
        &env.validator(),
        &alice,
        &alice_signer.public_key(),
        stale_tx_outcome.block_hash,
    );
    let inclusion_key_info = inclusion_key.inclusion_key_info().unwrap();
    assert_eq!(inclusion_key.nonce, delegate_action_nonce);
    assert_eq!(inclusion_key_info.last_transaction_nonce, stale_nonce);
    assert_eq!(inclusion_key_info.balance, key_balance.checked_sub(charge).unwrap());
}

fn access_key_at_block(
    node: &TestLoopNode<'_>,
    account_id: &AccountId,
    public_key: &PublicKey,
    block_hash: CryptoHash,
) -> AccessKey {
    let client = node.client();
    let epoch_id = client.epoch_manager.get_epoch_id(&block_hash).unwrap();
    let shard_layout = client.epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_uid = shard_layout.account_id_to_shard_uid(account_id);
    let chunk_extra = client.chain.get_chunk_extra(&block_hash, &shard_uid).unwrap();
    let trie = client
        .runtime_adapter
        .get_trie_for_shard(shard_uid.shard_id(), &block_hash, *chunk_extra.state_root(), false)
        .unwrap();
    get_access_key(&trie, account_id, public_key).unwrap().unwrap()
}
