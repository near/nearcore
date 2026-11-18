use borsh::{BorshDeserialize, BorshSerialize};
use near_epoch_manager::epoch_info_aggregator::EpochInfoAggregator;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::hash::CryptoHash;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, ChunkStats, EpochId, ShardId, ValidatorId, ValidatorStats,
};
use near_primitives::version::ProtocolVersion;
use near_store::{DBCol, Store};
use std::collections::{BTreeMap, HashMap};

/// Layout of [`EpochInfoAggregator`] before DB version 54.
#[derive(BorshSerialize, BorshDeserialize)]
struct EpochInfoAggregatorV53 {
    block_tracker: HashMap<ValidatorId, ValidatorStats>,
    shard_tracker: HashMap<ShardId, HashMap<ValidatorId, ChunkStats>>,
    version_tracker: HashMap<ValidatorId, ProtocolVersion>,
    all_proposals: BTreeMap<AccountId, ValidatorStake>,
    epoch_id: EpochId,
    last_block_hash: CryptoHash,
}

/// Rewrites the stored [`EpochInfoAggregator`] to the layout that splits `shard_tracker`
/// into per-shard chunk production stats, chunk endorsement stats summed over shards, and
/// chunk endorsement stats summed over the validators of each shard.
///
/// Hot store only: the column is not copied to cold storage.
///
/// Not safe to run twice. If the node stops before the version bump, restore the store from
/// the migration snapshot (`store.migration_snapshot`, enabled by default).
pub(super) fn migrate_53_to_54(hot_store: &Store) -> anyhow::Result<()> {
    tracing::info!(target: "migrations", "starting migration from DB version 53 to 54");

    let Some(old) = hot_store.get_ser::<EpochInfoAggregatorV53>(DBCol::EpochInfo, AGGREGATOR_KEY)
    else {
        tracing::info!(target: "migrations", "no epoch info aggregator stored, nothing to migrate");
        return Ok(());
    };

    let mut chunk_production: HashMap<ShardId, HashMap<ValidatorId, ValidatorStats>> =
        HashMap::new();
    let mut chunk_endorsement: HashMap<ValidatorId, ValidatorStats> = HashMap::new();
    let mut chunk_endorsement_by_shard: HashMap<ShardId, ValidatorStats> = HashMap::new();
    let mut old_entries = 0;
    for (shard_id, validators) in old.shard_tracker {
        for (validator_id, stats) in validators {
            old_entries += 1;
            // The old layout counted a chunk or an endorsement only together with its
            // `expected`, so `produced <= expected` and a side with `expected == 0` holds no
            // stats. Dropping it loses nothing.
            if stats.production.expected > 0 {
                chunk_production
                    .entry(shard_id)
                    .or_default()
                    .insert(validator_id, stats.production);
            }
            if stats.endorsement.expected > 0 {
                let endorsement = chunk_endorsement.entry(validator_id).or_default();
                endorsement.produced += stats.endorsement.produced;
                endorsement.expected += stats.endorsement.expected;
                let shard_endorsement = chunk_endorsement_by_shard.entry(shard_id).or_default();
                shard_endorsement.produced += stats.endorsement.produced;
                shard_endorsement.expected += stats.endorsement.expected;
            }
        }
    }
    let new = EpochInfoAggregator {
        block_tracker: old.block_tracker,
        chunk_production,
        chunk_endorsement,
        chunk_endorsement_by_shard,
        version_tracker: old.version_tracker,
        all_proposals: old.all_proposals,
        epoch_id: old.epoch_id,
        last_block_hash: old.last_block_hash,
    };

    let mut store_update = hot_store.store_update();
    store_update.set_ser(DBCol::EpochInfo, AGGREGATOR_KEY, &new);
    store_update.commit();

    tracing::info!(
        target: "migrations",
        old_entries,
        chunk_production_entries = new.chunk_production.values().map(HashMap::len).sum::<usize>(),
        chunk_endorsement_entries = new.chunk_endorsement.len(),
        chunk_endorsement_by_shard_entries = new.chunk_endorsement_by_shard.len(),
        "completed migration from DB version 53 to 54"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{EpochInfoAggregatorV53, migrate_53_to_54};
    use near_crypto::{KeyType, PublicKey};
    use near_epoch_manager::epoch_info_aggregator::EpochInfoAggregator;
    use near_primitives::epoch_manager::AGGREGATOR_KEY;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::types::{Balance, ChunkStats, EpochId, ShardId, ValidatorStats};
    use near_store::DBCol;
    use near_store::test_utils::create_test_store;
    use std::collections::{BTreeMap, HashMap};

    #[test]
    fn splits_shard_tracker_into_production_and_endorsement() {
        let store = create_test_store();
        let shard0 = ShardId::new(0);
        let shard1 = ShardId::new(1);
        let old = EpochInfoAggregatorV53 {
            block_tracker: HashMap::from([(0, ValidatorStats { produced: 9, expected: 10 })]),
            shard_tracker: HashMap::from([
                (
                    shard0,
                    HashMap::from([
                        (0, ChunkStats::new(8, 10, 3, 4)),
                        (1, ChunkStats::new_with_endorsement(5, 6)),
                    ]),
                ),
                (
                    shard1,
                    HashMap::from([
                        (0, ChunkStats::new_with_endorsement(1, 2)),
                        (1, ChunkStats::new(7, 7, 2, 3)),
                    ]),
                ),
            ]),
            version_tracker: HashMap::from([(0, 80)]),
            all_proposals: BTreeMap::from([(
                "test0".parse().unwrap(),
                ValidatorStake::new(
                    "test0".parse().unwrap(),
                    PublicKey::empty(KeyType::ED25519),
                    Balance::from_yoctonear(1_000),
                ),
            )]),
            epoch_id: EpochId(CryptoHash::hash_bytes(b"epoch")),
            last_block_hash: CryptoHash::hash_bytes(b"block"),
        };
        let mut store_update = store.store_update();
        store_update.set_ser(DBCol::EpochInfo, AGGREGATOR_KEY, &old);
        store_update.commit();

        migrate_53_to_54(&store).unwrap();

        let new: EpochInfoAggregator = store.get_ser(DBCol::EpochInfo, AGGREGATOR_KEY).unwrap();
        assert_eq!(new.block_tracker, old.block_tracker);
        assert_eq!(
            new.chunk_production,
            HashMap::from([
                (shard0, HashMap::from([(0, ValidatorStats { produced: 8, expected: 10 })])),
                (shard1, HashMap::from([(1, ValidatorStats { produced: 7, expected: 7 })])),
            ])
        );
        assert_eq!(
            new.chunk_endorsement,
            HashMap::from([
                (0, ValidatorStats { produced: 4, expected: 6 }),
                (1, ValidatorStats { produced: 7, expected: 9 }),
            ])
        );
        assert_eq!(
            new.chunk_endorsement_by_shard,
            HashMap::from([
                (shard0, ValidatorStats { produced: 8, expected: 10 }),
                (shard1, ValidatorStats { produced: 3, expected: 5 }),
            ])
        );
        assert_eq!(new.version_tracker, old.version_tracker);
        assert_eq!(new.all_proposals, old.all_proposals);
        assert_eq!(new.epoch_id, old.epoch_id);
        assert_eq!(new.last_block_hash, old.last_block_hash);
    }

    #[test]
    fn migrates_store_without_aggregator() {
        let store = create_test_store();
        migrate_53_to_54(&store).unwrap();
        assert!(store.get(DBCol::EpochInfo, AGGREGATOR_KEY).is_none());
    }
}
