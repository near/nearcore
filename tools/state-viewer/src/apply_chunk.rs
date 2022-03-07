use anyhow::{anyhow, Context};
use near_chain::chain::collect_receipts_from_response;
use near_chain::migrations::check_if_block_is_first_with_chunk_of_version;
use near_chain::types::ApplyTransactionResult;
use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::combine_hash;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ChunkHash, ReceiptProof};
use near_primitives::syncing::ReceiptProofResponse;
use near_primitives_core::hash::hash;
use near_primitives_core::types::Gas;
use nearcore::NightshadeRuntime;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use std::cmp::Ord;
use std::collections::HashSet;
use std::sync::Arc;

// like ChainStoreUpdate::get_incoming_receipts_for_shard(), but for the case when we don't
// know of a block containing the target chunk
fn get_incoming_receipts(
    chain_store: &mut ChainStore,
    chunk_hash: &ChunkHash,
    shard_id: u64,
    target_height: u64,
    prev_hash: &CryptoHash,
    prev_height_included: u64,
    rng: Option<StdRng>,
) -> anyhow::Result<Vec<Receipt>> {
    let mut receipt_proofs = vec![];

    let chunk_hashes = chain_store.get_all_chunk_hashes_by_height(target_height)?;
    if !chunk_hashes.contains(chunk_hash) {
        return Err(anyhow!(
            "given chunk hash is not listed in ColChunkHashesByHeight[{}]",
            target_height
        ));
    }

    let mut chunks =
        chunk_hashes.iter().map(|h| chain_store.get_chunk(h).unwrap().clone()).collect::<Vec<_>>();
    chunks.sort_by(|left, right| left.shard_id().cmp(&right.shard_id()));

    for chunk in chunks {
        let partial_encoded_chunk = chain_store.get_partial_chunk(&chunk.chunk_hash()).unwrap();
        for receipt in partial_encoded_chunk.receipts().iter() {
            let ReceiptProof(_, shard_proof) = receipt;
            if shard_proof.to_shard_id == shard_id {
                receipt_proofs.push(receipt.clone());
            }
        }
    }

    if let Some(mut rng) = rng {
        // for testing purposes, shuffle the receipts the same way it's done normally so we can compare the state roots
        receipt_proofs.shuffle(&mut rng);
    }
    let mut responses = vec![ReceiptProofResponse(CryptoHash::default(), receipt_proofs)];
    responses.extend_from_slice(&chain_store.store_update().get_incoming_receipts_for_shard(
        shard_id,
        *prev_hash,
        prev_height_included,
    )?);
    Ok(collect_receipts_from_response(&responses))
}

fn check_hashes_exist<T, F: Fn(&T) -> CryptoHash>(
    hashes: &Option<Vec<CryptoHash>>,
    items: &[T],
    item_hash: F,
) -> Result<(), CryptoHash> {
    match hashes {
        Some(hashes) => {
            let hashes_seen = items.iter().map(item_hash).collect::<HashSet<_>>();
            for hash in hashes.iter() {
                if !hashes_seen.contains(hash) {
                    return Err(*hash);
                }
            }
            Ok(())
        }
        None => Ok(()),
    }
}

// returns (apply_result, gas limit)
pub(crate) fn apply_chunk(
    runtime: Arc<NightshadeRuntime>,
    chain_store: &mut ChainStore,
    chunk_hash: ChunkHash,
    target_height: Option<u64>,
    tx_hashes: &Option<Vec<CryptoHash>>,
    receipt_hashes: &Option<Vec<CryptoHash>>,
    rng: Option<StdRng>,
) -> anyhow::Result<(ApplyTransactionResult, Gas)> {
    let chunk = chain_store.get_chunk(&chunk_hash)?;
    let chunk_header = chunk.cloned_header();

    let prev_block_hash = chunk_header.prev_block_hash();
    let shard_id = chunk.shard_id();
    let prev_state_root = chunk.prev_state_root();

    let transactions = chunk.transactions().clone();
    check_hashes_exist(&tx_hashes, &transactions, |tx| tx.get_hash()).map_err(|hash| {
        anyhow!("transaction with hash {} not found in chunk {:?}", hash, &chunk_hash)
    })?;

    let prev_block =
        chain_store.get_block(&prev_block_hash).context("Failed getting chunk's prev block")?;
    let prev_height_included = prev_block.chunks()[shard_id as usize].height_included();
    let prev_height = prev_block.header().height();
    let target_height = match target_height {
        Some(h) => h,
        None => prev_height + 1,
    };
    let prev_timestamp = prev_block.header().raw_timestamp();
    let gas_price = prev_block.header().gas_price();
    let receipts = get_incoming_receipts(
        chain_store,
        &chunk_hash,
        shard_id,
        target_height,
        &prev_block_hash,
        prev_height_included,
        rng,
    )
    .context("Failed collecting incoming receipts")?;
    check_hashes_exist(&receipt_hashes, &receipts, |r| r.receipt_id).map_err(|hash| {
        anyhow!("receipt with ID {} not found in any incoming receipt for shard {}", hash, shard_id)
    })?;
    let is_first_block_with_chunk_of_version = check_if_block_is_first_with_chunk_of_version(
        chain_store,
        runtime.as_ref(),
        &prev_block_hash,
        shard_id,
    )?;

    Ok((
        runtime.apply_transactions(
            shard_id,
            &prev_state_root,
            target_height,
            prev_timestamp + 1_000_000_000,
            &prev_block_hash,
            &combine_hash(
                &prev_block_hash,
                &hash("nonsense block hash for testing purposes".as_ref()),
            ),
            &receipts,
            &transactions,
            chunk_header.validator_proposals(),
            gas_price,
            chunk_header.gas_limit(),
            &vec![],
            hash("random seed".as_ref()),
            true,
            is_first_block_with_chunk_of_version,
            None,
        )?,
        chunk_header.gas_limit(),
    ))
}

#[cfg(test)]
mod test {
    use near_chain::{ChainGenesis, ChainStore, ChainStoreAccess, Provenance, RuntimeAdapter};
    use near_chain_configs::Genesis;
    use near_client::test_utils::TestEnv;
    use near_crypto::{InMemorySigner, KeyType};
    use near_network::types::NetworkClientResponses;
    use near_primitives::hash::CryptoHash;
    use near_primitives::runtime::config_store::RuntimeConfigStore;
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::utils::get_num_seats_per_shard;
    use near_store::test_utils::create_test_store;
    use nearcore::config::GenesisExt;
    use nearcore::NightshadeRuntime;
    use nearcore::TrackedConfig;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::path::Path;
    use std::sync::Arc;

    fn send_txs(env: &mut TestEnv, signers: &[InMemorySigner], height: u64, hash: CryptoHash) {
        for (i, signer) in signers.iter().enumerate() {
            let from = format!("test{}", i);
            let to = format!("test{}", (i + 1) % signers.len());
            let tx = SignedTransaction::send_money(
                height,
                from.parse().unwrap(),
                to.parse().unwrap(),
                signer,
                100,
                hash,
            );
            let response = env.clients[0].process_tx(tx, false, false);
            assert_eq!(response, NetworkClientResponses::ValidTx);
        }
    }

    #[test]
    fn test_apply_chunk() {
        let genesis = Genesis::test_sharded(
            vec![
                "test0".parse().unwrap(),
                "test1".parse().unwrap(),
                "test2".parse().unwrap(),
                "test3".parse().unwrap(),
            ],
            1,
            get_num_seats_per_shard(4, 1),
        );

        let store = create_test_store();
        let mut chain_store = ChainStore::new(store.clone(), genesis.config.genesis_height);
        let runtime = Arc::new(NightshadeRuntime::test_with_runtime_config_store(
            Path::new("."),
            store,
            &genesis,
            TrackedConfig::AllShards,
            RuntimeConfigStore::test(),
        ));
        let chain_genesis = ChainGenesis::test();

        let signers = (0..4)
            .map(|i| {
                let acc = format!("test{}", i);
                InMemorySigner::from_seed(acc.parse().unwrap(), KeyType::ED25519, &acc)
            })
            .collect::<Vec<_>>();

        let mut env =
            TestEnv::builder(chain_genesis).runtime_adapters(vec![runtime.clone()]).build();
        let genesis_hash = *env.clients[0].chain.genesis().hash();

        for height in 1..10 {
            send_txs(&mut env, &signers, height, genesis_hash);

            // assert!(response == NetworkClientResponses::RequestRouted || response == NetworkClientResponses::ValidTx);
            // let (e, _, _) = create_chunk_on_height_for_shard(&mut env.clients[0], height, 0);
            // let chunk = e.decode_chunk(runtime.num_data_parts()).unwrap();
            let block = env.clients[0].produce_block(height).unwrap().unwrap();

            let hash = *block.hash();
            let chunk_hashes = block.chunks().iter().map(|c| c.chunk_hash()).collect::<Vec<_>>();
            let epoch_id = block.header().epoch_id().clone();

            env.process_block(0, block, Provenance::PRODUCED);

            let new_roots = (0..4)
                .map(|i| {
                    let shard_uid = runtime.shard_id_to_uid(i, &epoch_id).unwrap();
                    chain_store.get_chunk_extra(&hash, &shard_uid).unwrap().state_root().clone()
                })
                .collect::<Vec<_>>();

            if height >= 2 {
                for shard in 0..4 {
                    // we will shuffle receipts the same as in production, otherwise the state roots don't match
                    let mut slice = [0u8; 32];
                    slice.copy_from_slice(hash.as_ref());
                    let rng: StdRng = SeedableRng::from_seed(slice);

                    let chunk_hash = &chunk_hashes[shard];
                    let new_root = new_roots[shard];

                    let (apply_result, _) = crate::apply_chunk::apply_chunk(
                        runtime.clone(),
                        &mut chain_store,
                        chunk_hash.clone(),
                        None,
                        &None,
                        &None,
                        Some(rng),
                    )
                    .unwrap();
                    assert_eq!(apply_result.new_root, new_root);
                }
            }
        }
    }
}
