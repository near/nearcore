use crate::EpochManager;
use crate::epoch_info_aggregator::EpochInfoAggregator;
use crate::reward_calculator::NUM_NS_IN_SECOND;
use crate::test_utils::{DEFAULT_TOTAL_SUPPLY, record_block, setup_default_epoch_manager, stake};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
use near_primitives::types::{Balance, BlockHeight};
use near_primitives::version::PROTOCOL_VERSION;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

/// Checks that the epoch info aggregator of every block equals its stats recomputed from the
/// block's epoch start. The blocks form random fork trees with random chunk masks and
/// endorsements.
#[test]
fn test_epoch_info_aggregator_matches_recomputation() {
    for seed in 0..200 {
        let mut rng = StdRng::seed_from_u64(seed);
        let validators = vec![
            ("test1".parse().unwrap(), Balance::from_yoctonear(1_000)),
            ("test2".parse().unwrap(), Balance::from_yoctonear(1_000)),
            ("test3".parse().unwrap(), Balance::from_yoctonear(1_000)),
        ];
        // Epoch length 5 and 30 heights, so the trees cross several epoch boundaries.
        let mut em = setup_default_epoch_manager(validators, 5, 2, 3, 90, 60);
        let blocks = record_random_fork_tree(&mut em, &mut rng, seed, 30);

        for (hash, height) in blocks {
            let aggregator = em.get_epoch_info_aggregator_upto_last(&hash).unwrap();
            let recomputed = recompute_aggregator(&em, &hash);
            // `EpochInfoAggregator` has no `PartialEq`. Borsh writes its maps in key order.
            assert_eq!(
                borsh::to_vec(&aggregator).unwrap(),
                borsh::to_vec(&recomputed).unwrap(),
                "seed {seed}, height {height}\naggregator: {aggregator:?}\nrecomputed: {recomputed:?}"
            );
        }
    }
}

/// Records a genesis block and then `num_heights - 1` blocks at increasing heights. Each
/// block extends the latest one with probability 0.8, and otherwise a random earlier block,
/// which forks and skips heights. Returns every recorded block with its height.
fn record_random_fork_tree(
    em: &mut EpochManager,
    rng: &mut StdRng,
    seed: u64,
    num_heights: BlockHeight,
) -> Vec<(CryptoHash, BlockHeight)> {
    let genesis = CryptoHash::hash_borsh((seed, 0u64));
    record_block(em, CryptoHash::default(), genesis, 0, vec![]);
    let mut blocks = vec![(genesis, 0)];
    for height in 1..num_heights {
        let prev = if rng.gen_bool(0.8) {
            *blocks.last().unwrap()
        } else {
            blocks[rng.gen_range(0..blocks.len())]
        };
        let hash = CryptoHash::hash_borsh((seed, height));
        record_block_with_random_chunks(em, rng, prev, hash, height);
        blocks.push((hash, height));
    }
    blocks
}

/// Records `hash` on `prev` with a random chunk mask, random endorsement bits for the
/// produced chunks, and sometimes a stake proposal.
fn record_block_with_random_chunks(
    em: &mut EpochManager,
    rng: &mut StdRng,
    (prev_hash, prev_height): (CryptoHash, BlockHeight),
    hash: CryptoHash,
    height: BlockHeight,
) {
    let epoch_id = em.get_epoch_id(&prev_hash).unwrap();
    let shard_layout = em.get_shard_layout(&epoch_id).unwrap();
    let chunk_mask: Vec<bool> = shard_layout.shard_ids().map(|_| rng.gen_bool(0.7)).collect();
    let endorsements = shard_layout
        .shard_ids()
        .zip(&chunk_mask)
        .map(|(shard_id, produced)| {
            if !produced {
                return vec![];
            }
            let assignments =
                em.get_chunk_validator_assignments(&epoch_id, shard_id, prev_height + 1).unwrap();
            assignments.assignments().iter().map(|_| rng.gen_bool(0.8)).collect()
        })
        .collect();
    let proposals = if rng.gen_bool(0.2) {
        let account_id = format!("test{}", rng.gen_range(1..6)).parse().unwrap();
        vec![stake(account_id, Balance::from_yoctonear(rng.gen_range(100..2000)))]
    } else {
        vec![]
    };
    let block_info = BlockInfo::new(
        hash,
        height,
        prev_height,
        prev_hash,
        prev_hash,
        proposals,
        chunk_mask,
        DEFAULT_TOTAL_SUPPLY,
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        height * NUM_NS_IN_SECOND,
        ChunkEndorsementsBitmap::from_endorsements(endorsements),
        None,
    );
    em.record_block_info(block_info, [0; 32]).unwrap().commit();
}

/// Recomputes the aggregator of `block_hash` from its epoch start with `update_tail`,
/// independent of `get_epoch_info_aggregator_upto_last`. It uses the same chunk producer
/// lookup, so the test covers how stats are combined, not which producer is credited.
fn recompute_aggregator(em: &EpochManager, block_hash: &CryptoHash) -> EpochInfoAggregator {
    let mut block_info = em.get_block_info(block_hash).unwrap();
    let epoch_id = *block_info.epoch_id();
    let epoch_info = em.get_epoch_info(&epoch_id).unwrap();
    let shard_layout = em.get_shard_layout(&epoch_id).unwrap();
    let mut aggregator = EpochInfoAggregator::new(epoch_id, *block_hash);
    while !block_info.is_genesis() && *block_info.epoch_id() == epoch_id {
        let prev_block_info = em.get_block_info(block_info.prev_hash()).unwrap();
        let chunk_producers = em.anchored_chunk_producers_for_aggregator(
            &epoch_id,
            &epoch_info,
            &shard_layout,
            &prev_block_info,
        );
        aggregator.update_tail(
            &block_info,
            &epoch_info,
            &shard_layout,
            prev_block_info.height(),
            chunk_producers.as_ref(),
        );
        block_info = prev_block_info;
    }
    aggregator
}
