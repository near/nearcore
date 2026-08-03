//! `prepare_transactions` must tolerate a missing signer per-tx instead of aborting the whole chunk.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::messaging::CanSend;
use near_async::time::Duration;
use near_client::ProcessTxRequest;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::AccessKey;
use near_primitives::action::{AddKeyAction, DeleteAccountAction, DeleteKeyAction};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{Action, SignedTransaction};
use near_primitives::types::{AccountId, Balance, BlockHeightDelta, ShardId};

const NUM_VALIDATORS: usize = 4;
const SHARD_INDEX: usize = 0;
const EPOCH_LENGTH: BlockHeightDelta = 100;
const TRANSACTION_VALIDITY_PERIOD: BlockHeightDelta = 200;

#[derive(Clone, Copy)]
enum RemovalKind {
    AccessKey,
    Account,
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn pooled_tx_skipped_per_tx_when_signer_access_key_deleted() {
    run_stale_signer_scenario(RemovalKind::AccessKey);
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn pooled_tx_skipped_per_tx_when_signer_account_deleted() {
    run_stale_signer_scenario(RemovalKind::Account);
}

fn run_stale_signer_scenario(removal: RemovalKind) {
    init_test_logger();

    let victim = create_account_id("victim");
    let receiver = create_account_id("receiver");

    let mut env = TestLoopBuilder::new()
        .validators(NUM_VALIDATORS, 0)
        .epoch_length(EPOCH_LENGTH)
        .transaction_validity_period(TRANSACTION_VALIDITY_PERIOD)
        .enable_rpc()
        .add_user_accounts([&victim, &receiver], Balance::from_near(1_000))
        .build();

    let stale_signer: Signer = create_user_test_signer(&victim).into();
    let aux_signer: Signer =
        InMemorySigner::from_seed(victim.clone(), KeyType::ED25519, "aux-key").into();

    let add_key_tx = env.rpc_node().tx_from_actions(
        &victim,
        &victim,
        vec![Action::AddKey(Box::new(AddKeyAction {
            public_key: aux_signer.public_key(),
            access_key: AccessKey::full_access(),
        }))],
    );
    env.rpc_runner().run_tx(add_key_tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    let attack_head = env.rpc_node().head();
    let shard_id = env.rpc_node().client().epoch_manager.shard_ids(&attack_head.epoch_id).unwrap()
        [SHARD_INDEX];

    let (target_producer, first_stale_offset, second_stale_offset) =
        find_stale_producer_slots(&env, shard_id);
    let first_stale_height = attack_head.height + first_stale_offset;
    let second_stale_height = attack_head.height + second_stale_offset;

    // is_forwarded=true keeps the seed in target_producer's pool only;
    // without it, an active validator re-broadcasts to upcoming chunk producers.
    let stale_tx = SignedTransaction::send_money(
        env.rpc_node().get_next_nonce(&victim),
        victim.clone(),
        receiver.clone(),
        &stale_signer,
        Balance::from_yoctonear(1),
        attack_head.last_block_hash,
    );
    env.node_for_account(&target_producer).node_data.rpc_handler_sender.send(ProcessTxRequest {
        transaction: stale_tx,
        is_forwarded: true,
        check_only: false,
    });

    let aux_nonce =
        env.rpc_node().view_access_key_query(&victim, &aux_signer.public_key()).unwrap().nonce + 1;
    let removal_action = match removal {
        RemovalKind::AccessKey => {
            Action::DeleteKey(Box::new(DeleteKeyAction { public_key: stale_signer.public_key() }))
        }
        RemovalKind::Account => {
            Action::DeleteAccount(DeleteAccountAction { beneficiary_id: receiver })
        }
    };
    let removal_tx = SignedTransaction::from_actions(
        aux_nonce,
        victim.clone(),
        victim.clone(),
        &aux_signer,
        vec![removal_action],
        attack_head.last_block_hash,
    );
    env.rpc_runner().run_tx(removal_tx, Duration::seconds(5));
    // Advance one block past the slot we want to inspect so the rpc node has
    // caught up to validator 0's view by the time we read the block.
    env.validator_runner().run_until_head_height(second_stale_height + 1);

    assert!(
        env.rpc_node().view_access_key_query(&victim, &stale_signer.public_key()).is_err(),
        "stale signer state should have been removed",
    );

    let rpc_node = env.rpc_node();
    let chain = &rpc_node.client().chain;
    for height in [first_stale_height, second_stale_height] {
        let block = chain.get_block_by_height(height).unwrap();
        let chunk_mask = block.header().chunk_mask();
        assert!(
            chunk_mask[SHARD_INDEX],
            "height {height}: shard {SHARD_INDEX} chunk should have been produced \
             (a stale pooled tx must be skipped per-tx, not abort the whole chunk); \
             got chunk_mask={chunk_mask:?}",
        );
    }
}

/// Picks an upcoming chunk producer whose first slot lies past the tx
/// routing horizon, plus a later slot for the same producer.
fn find_stale_producer_slots(
    env: &TestLoopEnv,
    shard_id: ShardId,
) -> (AccountId, BlockHeightDelta, BlockHeightDelta) {
    const MAX_FIRST_OFFSET: BlockHeightDelta = 40;
    const MAX_SECOND_OFFSET: BlockHeightDelta = 80;

    let rpc_node = env.rpc_node();
    let head = rpc_node.head();
    let epoch_manager = rpc_node.client().epoch_manager.clone();
    // Routing reaches both the [2, horizon] range and head + 2*horizon - 1, so
    // the first stale slot must lie past 2*horizon to keep the seeded producer
    // out of the horizon at submission time.
    let min_first_offset = 2 * rpc_node.client().config.tx_routing_height_horizon;

    let producer_at = |offset: BlockHeightDelta| -> AccountId {
        epoch_manager
            .get_chunk_producer_info(&ChunkProductionKey {
                epoch_id: head.epoch_id,
                height_created: head.height + offset,
                shard_id,
            })
            .unwrap()
            .take_account_id()
    };

    for first_offset in min_first_offset..=MAX_FIRST_OFFSET {
        let candidate = producer_at(first_offset);
        if (1..first_offset).any(|offset| producer_at(offset) == candidate) {
            continue;
        }
        if let Some(second_offset) =
            (first_offset + 1..=MAX_SECOND_OFFSET).find(|&o| producer_at(o) == candidate)
        {
            return (candidate, first_offset, second_offset);
        }
    }
    panic!(
        "could not find a chunk producer for shard {shard_id} with first slot in \
         [{min_first_offset}, {MAX_FIRST_OFFSET}] reappearing within {MAX_SECOND_OFFSET}"
    );
}
