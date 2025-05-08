use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;
use near_chain::{Chain, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::create_chunk;
use near_client::{ProcessTxResponse, ProduceChunkResult};
use near_primitives::account::id::AccountIdRef;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{SignedTransaction, ValidatedTransaction};
use near_primitives::types::{AccountId, ShardId};

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

/// Test that processing chunks with invalid transactions does not lead to panics
#[test]
fn test_invalid_transactions_no_panic() {
    let accounts =
        vec!["test0".parse().unwrap(), "test1".parse().unwrap(), "test2".parse().unwrap()];
    let signers: Vec<_> = accounts
        .iter()
        .map(|account_id: &AccountId| {
            create_user_test_signer(AccountIdRef::new(account_id.as_str()).unwrap())
        })
        .collect();
    let genesis = Genesis::test(accounts.clone(), 2);
    let mut env = TestEnv::builder(&genesis.config)
        .validators(accounts.clone())
        .clients(accounts.clone())
        .nightshade_runtimes(&genesis)
        .build();
    let new_signer = create_user_test_signer(AccountIdRef::new("test3").unwrap());

    let tip = env.clients[0].chain.head().unwrap();
    let sender_account = accounts[0].clone();
    let receiver_account = accounts[1].clone();
    let invalid_txs = vec![
        // transaction with invalid balance
        SignedTransaction::send_money(
            1,
            sender_account.clone(),
            receiver_account.clone(),
            &signers[0],
            u128::MAX,
            tip.last_block_hash,
        ),
        // transaction with invalid nonce
        SignedTransaction::send_money(
            0,
            sender_account.clone(),
            receiver_account.clone(),
            &signers[0],
            ONE_NEAR,
            tip.last_block_hash,
        ),
        // transaction with invalid sender account
        SignedTransaction::send_money(
            2,
            "test3".parse().unwrap(),
            receiver_account.clone(),
            &new_signer,
            ONE_NEAR,
            tip.last_block_hash,
        ),
    ];
    let invalid_txs = invalid_txs
        .into_iter()
        .map(|signed_tx| ValidatedTransaction::new_for_test(signed_tx))
        .collect::<Vec<_>>();
    // Need to create a valid transaction with the same accounts touched in order to have some state witness generated
    let valid_tx = SignedTransaction::send_money(
        1,
        sender_account,
        receiver_account,
        &signers[0],
        ONE_NEAR,
        tip.last_block_hash,
    );
    let mut start_height = 1;
    for tx in invalid_txs {
        for height in start_height..start_height + 3 {
            let tip = env.clients[0].chain.head().unwrap();
            let chunk_producer = env.get_chunk_producer_at_offset(&tip, 1, ShardId::new(0));
            let block_producer = env.get_block_producer_at_offset(&tip, 1);

            let tx_processor = env.rpc_handler(&chunk_producer).clone();
            let client = env.client(&chunk_producer);
            let transactions = if height == start_height { vec![tx.clone()] } else { vec![] };
            if height == start_height {
                let res = tx_processor.process_tx(valid_tx.clone(), false, false);
                assert!(matches!(res, ProcessTxResponse::ValidTx))
            }

            let (ProduceChunkResult { chunk, encoded_chunk_parts_paths, receipts }, _) =
                create_chunk(client, transactions);
            let shard_chunk = chunk.to_shard_chunk().clone();
            client
                .persist_and_distribute_encoded_chunk(
                    chunk,
                    encoded_chunk_parts_paths,
                    receipts,
                    client.validator_signer.get().unwrap().validator_id().clone(),
                )
                .unwrap();
            let prev_block = client.chain.get_block(shard_chunk.prev_block()).unwrap();
            let prev_chunk_header = Chain::get_prev_chunk_header(
                client.epoch_manager.as_ref(),
                &prev_block,
                shard_chunk.shard_id(),
            )
            .unwrap();
            client
                .send_chunk_state_witness_to_chunk_validators(
                    &client
                        .epoch_manager
                        .get_epoch_id_from_prev_block(shard_chunk.prev_block())
                        .unwrap(),
                    prev_block.header(),
                    &prev_chunk_header,
                    &shard_chunk,
                    &client.validator_signer.get(),
                )
                .unwrap();

            env.process_partial_encoded_chunks();
            for i in 0..env.clients.len() {
                env.process_shards_manager_responses(i);
            }
            env.propagate_chunk_state_witnesses_and_endorsements(true);
            let block = env.client(&block_producer).produce_block(height).unwrap().unwrap();
            for client in &mut env.clients {
                client
                    .process_block_test_no_produce_chunk_allow_errors(
                        block.clone().into(),
                        Provenance::NONE,
                    )
                    .unwrap();
            }
        }
        start_height += 3;
    }
}

/// Test that processing a chunk with invalid transactions within it does not invalidate the entire
/// chunk and discards just the invalid transactions within.
///
/// Tests the `RelaxedChunkValidation` feature.
#[test]
#[cfg(feature = "nightly")]
fn test_invalid_transactions_dont_invalidate_chunk() {
    near_o11y::testonly::init_test_logger();
    let accounts =
        vec!["test0".parse().unwrap(), "test1".parse().unwrap(), "test2".parse().unwrap()];
    let signers: Vec<_> = accounts
        .iter()
        .map(|account_id: &AccountId| {
            create_user_test_signer(AccountIdRef::new(account_id.as_str()).unwrap())
        })
        .collect();
    let genesis = Genesis::test(accounts.clone(), 2);
    let mut env = TestEnv::builder(&genesis.config)
        .validators(accounts.clone())
        .clients(accounts.clone())
        .nightshade_runtimes(&genesis)
        .build();
    let new_signer = create_user_test_signer(AccountIdRef::new("test3").unwrap());

    let tip = env.clients[0].chain.head().unwrap();
    let sender_account = accounts[0].clone();
    let receiver_account = accounts[1].clone();
    let chunk_transactions = vec![
        SignedTransaction::send_money(
            1,
            sender_account.clone(),
            receiver_account.clone(),
            &signers[0],
            ONE_NEAR,
            tip.last_block_hash,
        ),
        // transaction with invalid balance
        SignedTransaction::send_money(
            2,
            sender_account.clone(),
            receiver_account.clone(),
            &signers[0],
            u128::MAX,
            tip.last_block_hash,
        ),
        // transaction with invalid nonce
        SignedTransaction::send_money(
            0,
            sender_account,
            receiver_account.clone(),
            &signers[0],
            ONE_NEAR,
            tip.last_block_hash,
        ),
        // transaction with invalid sender account
        SignedTransaction::send_money(
            3,
            "test3".parse().unwrap(),
            receiver_account,
            &new_signer,
            ONE_NEAR,
            tip.last_block_hash,
        ),
    ];
    let chunk_transactions = chunk_transactions
        .into_iter()
        .map(|signed_tx| ValidatedTransaction::new_for_test(signed_tx))
        .collect();

    let tip = env.clients[0].chain.head().unwrap();
    let chunk_producer = env.get_chunk_producer_at_offset(&tip, 1, ShardId::new(0));
    let block_producer = env.get_block_producer_at_offset(&tip, 1);
    let client = env.client(&chunk_producer);
    let (ProduceChunkResult { chunk, encoded_chunk_parts_paths, receipts }, _) =
        create_chunk(client, chunk_transactions);
    let shard_chunk = chunk.to_shard_chunk().clone();

    client
        .persist_and_distribute_encoded_chunk(
            chunk,
            encoded_chunk_parts_paths,
            receipts,
            client.validator_signer.get().unwrap().validator_id().clone(),
        )
        .unwrap();

    let prev_block = client.chain.get_block(shard_chunk.prev_block()).unwrap();
    let prev_chunk_header = Chain::get_prev_chunk_header(
        client.epoch_manager.as_ref(),
        &prev_block,
        shard_chunk.shard_id(),
    )
    .unwrap();
    client
        .send_chunk_state_witness_to_chunk_validators(
            &client.epoch_manager.get_epoch_id_from_prev_block(shard_chunk.prev_block()).unwrap(),
            prev_block.header(),
            &prev_chunk_header,
            &shard_chunk,
            &client.validator_signer.get(),
        )
        .unwrap();

    env.process_partial_encoded_chunks();
    for i in 0..env.clients.len() {
        env.process_shards_manager_responses(i);
    }
    env.propagate_chunk_state_witnesses_and_endorsements(true);
    let block = env.client(&block_producer).produce_block(1).unwrap().unwrap();
    for client in &mut env.clients {
        let signer = client.validator_signer.get();
        client.start_process_block(block.clone().into(), Provenance::NONE, None, &signer).unwrap();
        near_chain::test_utils::wait_for_all_blocks_in_processing(&mut client.chain);
        let (accepted_blocks, _errors) = client.postprocess_ready_blocks(None, true, &signer);
        assert_eq!(accepted_blocks.len(), 1);
    }

    env.process_partial_encoded_chunks();
    for i in 0..env.clients.len() {
        env.process_shards_manager_responses(i);
    }
    env.propagate_chunk_state_witnesses_and_endorsements(true);
    let block = env.client(&block_producer).produce_block(2).unwrap().unwrap();
    for client in &mut env.clients {
        let signer = client.validator_signer.get();
        client.start_process_block(block.clone().into(), Provenance::NONE, None, &signer).unwrap();
        near_chain::test_utils::wait_for_all_blocks_in_processing(&mut client.chain);
        let (accepted_blocks, _errors) = client.postprocess_ready_blocks(None, true, &signer);
        assert_eq!(accepted_blocks.len(), 1);
    }
    env.propagate_chunk_state_witnesses_and_endorsements(true);

    let mut receipts = std::collections::BTreeSet::<near_primitives::hash::CryptoHash>::new();
    for client in &mut env.clients {
        let head = client.chain.get_head_block().unwrap();
        let chunk_hash = head.chunks().iter_raw().next().unwrap().chunk_hash();
        let Ok(chunk) = client.chain.mut_chain_store().get_chunk(&chunk_hash) else {
            continue;
        };
        receipts.extend(chunk.prev_outgoing_receipts().into_iter().map(|r| *r.receipt_id()));
    }
    assert_eq!(receipts.len(), 1, "only one receipt for the only valid transaction is expected");
}
