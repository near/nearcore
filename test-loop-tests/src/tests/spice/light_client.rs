use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_account_id, create_validators_spec};
use near_async::messaging::Handler as _;
use near_async::time::Duration;
use near_client::GetBlock;
use near_client_primitives::types::{GetBlockProof, GetExecutionOutcome, GetNextLightClientBlock};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::compute_root_from_path;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, BlockReference, Finality, TransactionOrReceiptId};
use near_primitives::views::LightClientBlockLiteView;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_light_client_proof() {
    init_test_logger();

    let sender = create_account_id("sender");
    let receiver = create_account_id("receiver");

    let mut env = TestLoopBuilder::new()
        .validators_spec(create_validators_spec(2, 1))
        .enable_rpc()
        .add_user_account(&sender, Balance::from_near(10))
        .add_user_account(&receiver, Balance::from_near(0))
        .build();

    let tx = SignedTransaction::send_money(
        1,
        sender.clone(),
        receiver,
        &create_user_test_signer(&sender),
        Balance::from_near(1),
        env.rpc_node().head().last_block_hash,
    );
    let tx_hash = tx.get_hash();
    let outcome = env.rpc_runner().execute_tx(tx, Duration::seconds(20)).unwrap();
    let tx_block_hash = outcome.transaction_outcome.block_hash;
    let tx_height =
        env.rpc_node().client().chain.get_block_header(&tx_block_hash).unwrap().height();
    // The proof anchors to the final head's prev, so run one block past the consensus
    // head that certified the tx -- making that head the final head's prev.
    env.rpc_runner().run_until_certified(tx_height);
    let certified_head_height = env.rpc_node().head().height;
    env.rpc_runner().run_until_final_head_height(certified_head_height + 1);

    // The trusted head + its certified anchor root, from next_light_client_block (which
    // returns a final block). Pass the head's prev as the client's last tracked head.
    let final_head = env
        .rpc_node_mut()
        .view_client_actor()
        .handle(GetBlock(BlockReference::Finality(Finality::Final)))
        .unwrap();
    let light_client_block = env
        .rpc_node_mut()
        .view_client_actor()
        .handle(GetNextLightClientBlock { last_block_hash: final_head.header.prev_hash })
        .unwrap()
        .expect("next_light_client_block must return the certified head");
    let head_certified_root = light_client_block
        .inner_lite
        .certified_block_merkle_root
        .expect("light-client head must commit certified_block_merkle_root");
    let light_client_head = LightClientBlockLiteView {
        prev_block_hash: light_client_block.prev_block_hash,
        inner_rest_hash: light_client_block.inner_rest_hash,
        inner_lite: light_client_block.inner_lite.clone(),
    }
    .hash();

    let outcome_response = env
        .rpc_node_mut()
        .view_client_actor()
        .handle(GetExecutionOutcome {
            id: TransactionOrReceiptId::Transaction {
                transaction_hash: tx_hash,
                sender_id: sender,
            },
        })
        .unwrap();
    let block_proof_response = env
        .rpc_node_mut()
        .view_client_actor()
        .handle(GetBlockProof {
            block_hash: outcome_response.outcome_proof.block_hash,
            head_block_hash: light_client_head,
        })
        .unwrap();
    let outcome_proof = outcome_response.outcome_proof;
    let outcome_root_proof = outcome_response.outcome_root_proof;
    let block_header_lite = block_proof_response.block_header_lite;
    let block_proof = block_proof_response.proof;

    // Verify like a light client: two merkle proofs chained through block B, the
    // certified accumulator standing in for block_merkle_root:
    //
    //   tx outcome --outcome_proof--> B's certified outcome_root   (read from B's lite view)
    //   B's leaf   --block_proof----> head's certified accumulator  (trusted via the head)

    // 1. The tx's outcome is committed in B's certified outcome_root.
    let outcome_hash = CryptoHash::hash_borsh(&outcome_proof.to_hashes());
    let shard_outcome_root = compute_root_from_path(&outcome_proof.proof, outcome_hash);
    let b_outcome_root =
        compute_root_from_path(&outcome_root_proof, CryptoHash::hash_borsh(shard_outcome_root));
    assert_eq!(b_outcome_root, block_header_lite.inner_lite.outcome_root);

    // 2. B is included in the certified accumulator the trusted head commits to.
    // (B's whole reconstructed lite view is hashed into the leaf, so it's verified too.)
    let b_leaf = block_header_lite.hash();
    assert_eq!(compute_root_from_path(&block_proof, b_leaf), head_certified_root);
}
