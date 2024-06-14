use crate::metadata::DbKind;
use crate::{DBCol, Store, StoreUpdate};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::epoch_manager::epoch_info::EpochSummary;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::hash::CryptoHash;
use near_primitives::state::FlatStateValue;
use near_primitives::transaction::{ExecutionOutcomeWithIdAndProof, ExecutionOutcomeWithProof};
use near_primitives::types::{
    validator_stake::ValidatorStake, AccountId, Balance, EpochId, NumBlocks, ShardId, ValidatorId,
    ValidatorKickoutReason, ValidatorStats,
};
use near_primitives::types::{BlockChunkValidatorStats, ChunkStats};
use near_primitives::utils::get_outcome_id_block_hash;
use near_primitives::version::ProtocolVersion;
use std::collections::{BTreeMap, HashMap};
use tracing::info;

pub struct BatchedStoreUpdate<'a> {
    batch_size_limit: usize,
    batch_size: usize,
    store: &'a Store,
    store_update: Option<StoreUpdate>,
    total_size_written: u64,
    printed_total_size_written: u64,
}

const PRINT_PROGRESS_EVERY_BYTES: u64 = bytesize::GIB;

impl<'a> BatchedStoreUpdate<'a> {
    pub fn new(store: &'a Store, batch_size_limit: usize) -> Self {
        Self {
            batch_size_limit,
            batch_size: 0,
            store,
            store_update: Some(store.store_update()),
            total_size_written: 0,
            printed_total_size_written: 0,
        }
    }

    fn commit(&mut self) -> std::io::Result<()> {
        let store_update = self.store_update.take().unwrap();
        store_update.commit()?;
        self.store_update = Some(self.store.store_update());
        self.batch_size = 0;
        Ok(())
    }

    fn set_or_insert_ser<T: BorshSerialize>(
        &mut self,
        col: DBCol,
        key: &[u8],
        value: &T,
        insert: bool,
    ) -> std::io::Result<()> {
        let value_bytes = borsh::to_vec(&value)?;
        let entry_size = key.as_ref().len() + value_bytes.len() + 8;
        self.batch_size += entry_size;
        self.total_size_written += entry_size as u64;
        let update = self.store_update.as_mut().unwrap();
        if insert {
            update.insert(col, key.to_vec(), value_bytes);
        } else {
            update.set(col, key.as_ref(), &value_bytes);
        }

        if self.batch_size > self.batch_size_limit {
            self.commit()?;
        }
        if self.total_size_written - self.printed_total_size_written > PRINT_PROGRESS_EVERY_BYTES {
            info!(
                target: "migrations",
                "Migrations: {} written",
                bytesize::to_string(self.total_size_written, true)
            );
            self.printed_total_size_written = self.total_size_written;
        }

        Ok(())
    }

    pub fn set_ser<T: BorshSerialize>(
        &mut self,
        col: DBCol,
        key: &[u8],
        value: &T,
    ) -> std::io::Result<()> {
        self.set_or_insert_ser(col, key, value, false)
    }

    pub fn insert_ser<T: BorshSerialize>(
        &mut self,
        col: DBCol,
        key: &[u8],
        value: &T,
    ) -> std::io::Result<()> {
        self.set_or_insert_ser(col, key, value, true)
    }

    pub fn finish(mut self) -> std::io::Result<()> {
        if self.batch_size > 0 {
            self.commit()?;
        }

        Ok(())
    }
}

/// Migrates the database from version 32 to 33.
///
/// This removes the TransactionResult column and moves it to TransactionResultForBlock.
/// The new column removes the need for high-latency read-modify-write operations when committing
/// new blocks.
pub fn migrate_32_to_33(store: &Store) -> anyhow::Result<()> {
    let mut update = BatchedStoreUpdate::new(&store, 10_000_000);
    for row in
        store.iter_prefix_ser::<Vec<ExecutionOutcomeWithIdAndProof>>(DBCol::_TransactionResult, &[])
    {
        let (_, mut outcomes) = row?;
        // It appears that it was possible that the same entry in the original column contained
        // duplicate outcomes. We remove them here to avoid panicing due to issuing a
        // self-overwriting transaction.
        outcomes.sort_by_key(|outcome| (*outcome.id(), outcome.block_hash));
        outcomes.dedup_by_key(|outcome| (*outcome.id(), outcome.block_hash));
        for outcome in outcomes {
            update.insert_ser(
                DBCol::TransactionResultForBlock,
                &get_outcome_id_block_hash(outcome.id(), &outcome.block_hash),
                &ExecutionOutcomeWithProof {
                    proof: outcome.proof,
                    outcome: outcome.outcome_with_id.outcome,
                },
            )?;
        }
    }
    update.finish()?;
    let mut delete_old_update = store.store_update();
    delete_old_update.delete_all(DBCol::_TransactionResult);
    delete_old_update.commit()?;
    Ok(())
}

/// Migrates the database from version 33 to 34.
///
/// Most importantly, this involves adding KIND entry to DbVersion column,
/// removing IS_ARCHIVAL from BlockMisc column.  Furthermore, migration deletes
/// GCCount column which is no longer used.
///
/// If the database has IS_ARCHIVAL key in BlockMisc column set to true, this
/// overrides value of is_node_archival argument.  Otherwise, the kind of the
/// resulting database is determined based on that argument.
pub fn migrate_33_to_34(store: &Store, mut is_node_archival: bool) -> anyhow::Result<()> {
    const IS_ARCHIVE_KEY: &[u8; 10] = b"IS_ARCHIVE";

    let is_store_archival =
        store.get_ser::<bool>(DBCol::BlockMisc, IS_ARCHIVE_KEY)?.unwrap_or_default();

    if is_store_archival != is_node_archival {
        if is_store_archival {
            tracing::info!(target: "migrations", "Opening an archival database.");
            tracing::warn!(target: "migrations", "Ignoring `archive` client configuration and setting database kind to Archive.");
        } else {
            tracing::info!(target: "migrations", "Running node in archival mode (as per `archive` client configuration).");
            tracing::info!(target: "migrations", "Setting database kind to Archive.");
            tracing::warn!(target: "migrations", "Starting node in non-archival mode will no longer be possible with this database.");
        }
        is_node_archival = true;
    }

    let mut update = store.store_update();
    if is_store_archival {
        update.delete(DBCol::BlockMisc, IS_ARCHIVE_KEY);
    }
    let kind = if is_node_archival { DbKind::Archive } else { DbKind::RPC };
    update.set(DBCol::DbVersion, crate::metadata::KIND_KEY, <&str>::from(kind).as_bytes());
    update.delete_all(DBCol::_GCCount);
    update.commit()?;
    Ok(())
}

/// Migrates the database from version 34 to 35.
///
/// This involves deleting contents of Peers column which is now
/// deprecated and no longer used.
pub fn migrate_34_to_35(store: &Store) -> anyhow::Result<()> {
    let mut update = store.store_update();
    update.delete_all(DBCol::_Peers);
    update.commit()?;
    Ok(())
}

/// Migrates the database from version 36 to 37.
///
/// This involves rewriting all FlatStateChanges entries in the new format.
/// The size of that column should not exceed several dozens of entries.
pub fn migrate_36_to_37(store: &Store) -> anyhow::Result<()> {
    #[derive(borsh::BorshDeserialize)]
    struct LegacyFlatStateChanges(HashMap<Vec<u8>, Option<near_primitives::state::ValueRef>>);

    let mut update = store.store_update();
    update.delete_all(DBCol::FlatStateChanges);
    for result in store.iter(DBCol::FlatStateChanges) {
        let (key, old_value) = result?;
        let new_value = borsh::to_vec(&crate::flat::FlatStateChanges(
            LegacyFlatStateChanges::try_from_slice(&old_value)?
                .0
                .into_iter()
                .map(|(key, value_ref)| (key, value_ref.map(|v| FlatStateValue::Ref(v))))
                .collect(),
        ))?;
        update.set(DBCol::FlatStateChanges, &key, &new_value);
    }
    update.commit()?;
    Ok(())
}

/// Migrates the database from version 37 to 38.
///
/// Rewrites FlatStateDeltaMetadata to add a bit to Metadata, `prev_block_with_changes`.
/// That bit is initialized with a `None` regardless of the corresponding flat state changes.
pub fn migrate_37_to_38(store: &Store) -> anyhow::Result<()> {
    #[derive(borsh::BorshDeserialize)]
    struct LegacyFlatStateDeltaMetadata {
        block: crate::flat::BlockInfo,
    }

    let mut update = store.store_update();
    update.delete_all(DBCol::FlatStateDeltaMetadata);
    for result in store.iter(DBCol::FlatStateDeltaMetadata) {
        let (key, old_value) = result?;
        let LegacyFlatStateDeltaMetadata { block } =
            LegacyFlatStateDeltaMetadata::try_from_slice(&old_value)?;
        let new_value =
            crate::flat::FlatStateDeltaMetadata { block, prev_block_with_changes: None };
        update.set(DBCol::FlatStateDeltaMetadata, &key, &borsh::to_vec(&new_value)?);
    }
    update.commit()?;
    Ok(())
}

use near_primitives::serialize::dec_format;
#[derive(BorshSerialize, BorshDeserialize, serde::Deserialize)]
pub enum LegacyValidatorKickoutReason {
    /// Slashed validators are kicked out.
    Slashed,
    /// Validator didn't produce enough blocks.
    NotEnoughBlocks { produced: NumBlocks, expected: NumBlocks },
    /// Validator didn't produce enough chunks.
    NotEnoughChunks { produced: NumBlocks, expected: NumBlocks },
    /// Validator unstaked themselves.
    Unstaked,
    /// Validator stake is now below threshold
    NotEnoughStake {
        #[serde(with = "dec_format", rename = "stake_u128")]
        stake: Balance,
        #[serde(with = "dec_format", rename = "threshold_u128")]
        threshold: Balance,
    },
    /// Enough stake but is not chosen because of seat limits.
    DidNotGetASeat,
    /// Validator didn't produce enough chunk endorsements.
    NotEnoughChunkEndorsements { produced: NumBlocks, expected: NumBlocks },
}

#[derive(BorshSerialize, BorshDeserialize)]
struct LegacyEpochSummaryV39 {
    pub prev_epoch_last_block_hash: CryptoHash,
    /// Proposals from the epoch, only the latest one per account
    pub all_proposals: Vec<ValidatorStake>,
    /// Kickout set, includes slashed
    pub validator_kickout: HashMap<AccountId, LegacyValidatorKickoutReason>,
    /// Only for validators who met the threshold and didn't get slashed
    pub validator_block_chunk_stats: HashMap<AccountId, BlockChunkValidatorStats>,
    /// Protocol version for next epoch.
    pub next_next_epoch_version: ProtocolVersion,
}

/// Migrates the database from version 38 to 39.
///
/// Rewrites Epoch summary to include endorsement stats.
pub fn migrate_38_to_39(store: &Store) -> anyhow::Result<()> {
    #[derive(BorshSerialize, BorshDeserialize)]
    struct EpochInfoAggregator<T> {
        /// Map from validator index to (num_blocks_produced, num_blocks_expected) so far in the given epoch.
        pub block_tracker: HashMap<ValidatorId, ValidatorStats>,
        /// For each shard, a map of validator id to (num_chunks_produced, num_chunks_expected) so far in the given epoch.
        pub shard_tracker: HashMap<ShardId, HashMap<ValidatorId, T>>,
        /// Latest protocol version that each validator supports.
        pub version_tracker: HashMap<ValidatorId, ProtocolVersion>,
        /// All proposals in this epoch up to this block.
        pub all_proposals: BTreeMap<AccountId, ValidatorStake>,
        /// Id of the epoch that this aggregator is in.
        pub epoch_id: EpochId,
        /// Last block hash recorded.
        pub last_block_hash: CryptoHash,
    }

    type LegacyEpochInfoAggregator = EpochInfoAggregator<ValidatorStats>;
    type NewEpochInfoAggregator = EpochInfoAggregator<ChunkStats>;

    #[derive(BorshDeserialize)]
    struct LegacyBlockChunkValidatorStats {
        pub block_stats: ValidatorStats,
        pub chunk_stats: ValidatorStats,
    }

    #[derive(BorshDeserialize)]
    struct LegacyEpochSummaryV38 {
        pub prev_epoch_last_block_hash: CryptoHash,
        /// Proposals from the epoch, only the latest one per account
        pub all_proposals: Vec<ValidatorStake>,
        /// Kickout set, includes slashed
        pub validator_kickout: HashMap<AccountId, LegacyValidatorKickoutReason>,
        /// Only for validators who met the threshold and didn't get slashed
        pub validator_block_chunk_stats: HashMap<AccountId, LegacyBlockChunkValidatorStats>,
        /// Protocol version for next epoch.
        pub next_version: ProtocolVersion,
    }

    let mut update = store.store_update();

    // Update EpochInfoAggregator
    let maybe_legacy_aggregator: Option<LegacyEpochInfoAggregator> =
        store.get_ser(DBCol::EpochInfo, AGGREGATOR_KEY)?;
    if let Some(legacy_aggregator) = maybe_legacy_aggregator {
        let new_aggregator = NewEpochInfoAggregator {
            block_tracker: legacy_aggregator.block_tracker,
            shard_tracker: legacy_aggregator
                .shard_tracker
                .into_iter()
                .map(|(shard_id, legacy_stats)| {
                    let new_stats = legacy_stats
                        .into_iter()
                        .map(|(validator_id, stats)| {
                            (
                                validator_id,
                                ChunkStats::new_with_production(stats.produced, stats.expected),
                            )
                        })
                        .collect();
                    (shard_id, new_stats)
                })
                .collect(),
            version_tracker: legacy_aggregator.version_tracker,
            all_proposals: legacy_aggregator.all_proposals,
            epoch_id: legacy_aggregator.epoch_id,
            last_block_hash: legacy_aggregator.last_block_hash,
        };
        update.set_ser(DBCol::EpochInfo, AGGREGATOR_KEY, &new_aggregator)?;
    }

    // Update EpochSummary
    for result in store.iter(DBCol::EpochValidatorInfo) {
        let (key, old_value) = result?;
        let legacy_summary = LegacyEpochSummaryV38::try_from_slice(&old_value)?;
        let new_value = LegacyEpochSummaryV39 {
            prev_epoch_last_block_hash: legacy_summary.prev_epoch_last_block_hash,
            all_proposals: legacy_summary.all_proposals,
            validator_kickout: legacy_summary.validator_kickout,
            validator_block_chunk_stats: legacy_summary
                .validator_block_chunk_stats
                .into_iter()
                .map(|(account_id, stats)| {
                    let new_stats = BlockChunkValidatorStats {
                        block_stats: stats.block_stats,
                        chunk_stats: ChunkStats::new_with_production(
                            stats.chunk_stats.produced,
                            stats.chunk_stats.expected,
                        ),
                    };
                    (account_id, new_stats)
                })
                .collect(),
            next_next_epoch_version: legacy_summary.next_version,
        };
        update.set(DBCol::EpochValidatorInfo, &key, &borsh::to_vec(&new_value)?);
    }

    update.commit()?;
    Ok(())
}

/// Migrates the database from version 39 to 40.
///
/// Rewrites ValidatorKickoutReason to introduce NotEnoughChunkEndorsements variant
pub fn migrate_39_to_40(store: &Store) -> anyhow::Result<()> {
    if cfg!(feature = "statelessnet_protocol") {
        tracing::info!(
            target: "migrations",
            "For statelessnet, ValidatorKickoutReason is already at correct format. \
            Skipping migration from 39 to 40."
        );
        return Ok(());
    }

    impl From<LegacyValidatorKickoutReason> for ValidatorKickoutReason {
        fn from(reason: LegacyValidatorKickoutReason) -> Self {
            match reason {
                LegacyValidatorKickoutReason::Slashed => ValidatorKickoutReason::Slashed,
                LegacyValidatorKickoutReason::NotEnoughBlocks { produced, expected } => {
                    ValidatorKickoutReason::NotEnoughBlocks { produced, expected }
                }
                LegacyValidatorKickoutReason::NotEnoughChunks { produced, expected } => {
                    ValidatorKickoutReason::NotEnoughChunks { produced, expected }
                }
                LegacyValidatorKickoutReason::Unstaked => ValidatorKickoutReason::Unstaked,
                LegacyValidatorKickoutReason::NotEnoughStake { stake, threshold } => {
                    ValidatorKickoutReason::NotEnoughStake { stake, threshold }
                }
                LegacyValidatorKickoutReason::DidNotGetASeat => {
                    ValidatorKickoutReason::DidNotGetASeat
                }
                LegacyValidatorKickoutReason::NotEnoughChunkEndorsements { produced, expected } => {
                    ValidatorKickoutReason::NotEnoughChunkEndorsements { produced, expected }
                }
            }
        }
    }

    let mut update = store.store_update();
    // Update EpochSummary
    for result in store.iter(DBCol::EpochValidatorInfo) {
        let (key, old_value) = result?;
        let legacy_summary = LegacyEpochSummaryV39::try_from_slice(&old_value)?;
        let legacy_validator_kickout = legacy_summary.validator_kickout;
        let validator_kickout: HashMap<AccountId, ValidatorKickoutReason> =
            legacy_validator_kickout
                .into_iter()
                .map(|(account, kickout)| (account, kickout.into()))
                .collect();
        let new_value = EpochSummary {
            prev_epoch_last_block_hash: legacy_summary.prev_epoch_last_block_hash,
            all_proposals: legacy_summary.all_proposals,
            validator_kickout,
            validator_block_chunk_stats: legacy_summary.validator_block_chunk_stats,
            next_next_epoch_version: legacy_summary.next_next_epoch_version,
        };
        update.set(DBCol::EpochValidatorInfo, &key, &borsh::to_vec(&new_value)?);
    }

    update.commit()?;
    Ok(())
}
