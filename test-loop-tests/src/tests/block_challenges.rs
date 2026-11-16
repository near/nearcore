use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::HandlerResult;
use crate::utils::node::TestLoopNode;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_chain::Error;
use near_crypto::{KeyType, Signature};
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Block;
use near_primitives::challenge::{BlockDoubleSign, Challenge, ChallengeBody};
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::utils::MaybeValidated;
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use std::cell::OnceCell;
use std::rc::Rc;
use std::sync::Arc;

struct ChallengedBlockBroadcast {
    challenged_height: BlockHeight,
    challenged_producer: AccountId,
    /// The broadcast copy carrying the challenge, once it has been sent.
    challenged_block: Rc<OnceCell<Arc<Block>>>,
}

/// Builds a 4-validator chain at `protocol_version`. The producer of `challenged_height` keeps its
/// block and broadcasts a copy whose body carries one challenge. That producer builds on its own
/// block, so `challenged_height` is picked such that another validator produces the next height.
fn setup_challenged_block_broadcast(
    protocol_version: ProtocolVersion,
) -> (TestLoopEnv, ChallengedBlockBroadcast) {
    let mut env =
        TestLoopBuilder::new().validators(4, 0).protocol_version(protocol_version).build();
    let head = env.node(0).head();
    let epoch_manager = env.node(0).client().epoch_manager.clone();
    let block_producer = |height| epoch_manager.get_block_producer(&head.epoch_id, height).unwrap();
    let challenged_height = (head.height + 3..)
        .find(|&height| block_producer(height) != block_producer(height + 1))
        .unwrap();
    let challenged_producer = block_producer(challenged_height);
    let challenged_block = Rc::new(OnceCell::new());

    let signer = create_test_signer(challenged_producer.as_str());
    let account_id = challenged_producer.clone();
    let sent_block = challenged_block.clone();
    let producer_data = &env.node_datas[env.account_data_idx(&challenged_producer)];
    let peer_actor = env.test_loop.data.get_mut(&producer_data.peer_manager_sender.actor_handle());
    peer_actor.register_override_handler(Box::new(move |request| match request {
        NetworkRequests::Block { block } if block.header().height() == challenged_height => {
            // The producer may broadcast its block more than once; every send carries the same copy.
            let challenged_block = sent_block.get_or_init(|| {
                let mut challenge = Challenge {
                    body: ChallengeBody::BlockDoubleSign(BlockDoubleSign {
                        left_block_header: vec![1],
                        right_block_header: vec![2],
                    }),
                    account_id: account_id.clone(),
                    signature: Signature::empty(KeyType::ED25519),
                    hash: CryptoHash::default(),
                };
                challenge.init();
                let mut challenged_block = (*block).clone();
                challenged_block.set_challenges(vec![challenge]);
                let body_hash = challenged_block.compute_block_body_hash().unwrap();
                challenged_block.mut_header().set_block_body_hash(body_hash);
                challenged_block.mut_header().resign(&signer);
                Arc::new(challenged_block)
            });
            HandlerResult::Unhandled(NetworkRequests::Block { block: challenged_block.clone() })
        }
        other => HandlerResult::Unhandled(other),
    }));

    (env, ChallengedBlockBroadcast { challenged_height, challenged_producer, challenged_block })
}

/// Runs until every node other than `excluded` has finalized a block above `height`.
fn run_until_finalized_above(
    env: &mut TestLoopEnv,
    height: BlockHeight,
    excluded: Option<&AccountId>,
) {
    let node_datas = env.node_datas.clone();
    env.test_loop.run_until(
        |data| {
            node_datas
                .iter()
                .filter(|node_data| Some(&node_data.account_id) != excluded)
                .all(|node_data| TestLoopNode { data, node_data }.final_head().height > height)
        },
        Duration::seconds(15),
    );
}

#[test]
fn block_with_challenges_accepted_before_reject_feature() {
    init_test_logger();
    let protocol_version = ProtocolFeature::RejectBlockChallenges.protocol_version() - 1;
    let (mut env, broadcast) = setup_challenged_block_broadcast(protocol_version);
    let challenged_height = broadcast.challenged_height;

    run_until_finalized_above(&mut env, challenged_height, None);

    let challenged_block =
        broadcast.challenged_block.get().expect("challenged block was not sent").clone();
    let challenged_hash = *challenged_block.hash();
    for node_data in &env.node_datas {
        let node = env.node_for_account(&node_data.account_id);
        let chain = &node.client().chain;
        let canonical_hash = chain.chain_store.get_block_hash_by_height(challenged_height).unwrap();
        assert_eq!(canonical_hash, challenged_hash);
        assert!(chain.get_block(&challenged_hash).unwrap().challenges_present());
        assert_matches!(
            chain.validate_block(&MaybeValidated::from(challenged_block.clone())),
            Ok(())
        );
    }
}

#[test]
fn block_with_challenges_rejected_with_reject_feature() {
    init_test_logger();
    let protocol_version = ProtocolFeature::RejectBlockChallenges.protocol_version();
    let (mut env, broadcast) = setup_challenged_block_broadcast(protocol_version);
    let challenged_height = broadcast.challenged_height;

    run_until_finalized_above(&mut env, challenged_height, Some(&broadcast.challenged_producer));

    let challenged_block =
        broadcast.challenged_block.get().expect("challenged block was not sent").clone();
    let challenged_hash = *challenged_block.hash();
    for node_data in &env.node_datas {
        let node = env.node_for_account(&node_data.account_id);
        let chain = &node.client().chain;
        if node_data.account_id == broadcast.challenged_producer {
            // The other validators ban the producer for the invalid block, which cuts it off.
            assert!(node.final_head().height < challenged_height);
            continue;
        }
        assert_matches!(
            chain.validate_block(&MaybeValidated::from(challenged_block.clone())),
            Err(Error::InvalidChallenge)
        );
        assert!(chain.get_block(&challenged_hash).is_err());
        let hashes = chain.chain_store.get_all_block_hashes_by_height(challenged_height);
        assert!(!hashes.values().flatten().any(|hash| hash == &challenged_hash));
    }
}
