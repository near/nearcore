use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use near_primitives::epoch_manager::epoch_info::{EpochInfo, EpochInfoV1};
use near_primitives::hash::CryptoHash;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::AccountId;

use crate::metadata::DbKind;
use crate::{DBCol, Store, StoreUpdate};

pub struct BatchedStoreUpdate<'a> {
    batch_size_limit: usize,
    batch_size: usize,
    store: &'a Store,
    store_update: Option<StoreUpdate>,
}

impl<'a> BatchedStoreUpdate<'a> {
    pub fn new(store: &'a Store, batch_size_limit: usize) -> Self {
        Self { batch_size_limit, batch_size: 0, store, store_update: Some(store.store_update()) }
    }

    fn commit(&mut self) -> std::io::Result<()> {
        let store_update = self.store_update.take().unwrap();
        store_update.commit()?;
        self.store_update = Some(self.store.store_update());
        self.batch_size = 0;
        Ok(())
    }

    pub fn set_ser<T: BorshSerialize>(
        &mut self,
        col: DBCol,
        key: &[u8],
        value: &T,
    ) -> std::io::Result<()> {
        let value_bytes = value.try_to_vec()?;
        self.batch_size += key.as_ref().len() + value_bytes.len() + 8;
        self.store_update.as_mut().unwrap().set(col, key.as_ref(), &value_bytes);

        if self.batch_size > self.batch_size_limit {
            self.commit()?;
        }

        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<()> {
        if self.batch_size > 0 {
            self.commit()?;
        }

        Ok(())
    }
}

fn map_col<T, U, F>(store: &Store, col: DBCol, f: F) -> std::io::Result<()>
where
    T: BorshDeserialize,
    U: BorshSerialize,
    F: Fn(T) -> U,
{
    let mut store_update = BatchedStoreUpdate::new(store, 10_000_000);
    for pair in store.iter(col) {
        let (key, value) = pair?;
        let new_value = f(T::try_from_slice(&value).unwrap());
        store_update.set_ser(col, &key, &new_value)?;
    }
    store_update.finish()
}

/// Migrates database from version 28 to 29.
///
/// Deletes all data from _NextBlockWithNewChunk and _LastBlockWithNewChunk
/// columns.
pub fn migrate_28_to_29(storage: &crate::NodeStorage) -> anyhow::Result<()> {
    let mut update = storage.get_store(crate::Temperature::Hot).store_update();
    update.delete_all(DBCol::_NextBlockWithNewChunk);
    update.delete_all(DBCol::_LastBlockWithNewChunk);
    update.commit()?;
    Ok(())
}

/// Migrates database from version 29 to 30.
///
/// Migrates all structures that use ValidatorStake to versionized version.
pub fn migrate_29_to_30(storage: &crate::NodeStorage) -> anyhow::Result<()> {
    use near_primitives::epoch_manager::block_info::BlockInfo;
    use near_primitives::epoch_manager::epoch_info::EpochSummary;
    use near_primitives::epoch_manager::AGGREGATOR_KEY;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_primitives::types::validator_stake::ValidatorStakeV1;
    use near_primitives::types::{
        BlockChunkValidatorStats, EpochId, ProtocolVersion, ShardId, ValidatorId,
        ValidatorKickoutReason, ValidatorStats,
    };
    use std::collections::BTreeMap;

    let store = storage.get_store(crate::Temperature::Hot);

    #[derive(BorshDeserialize)]
    pub struct OldEpochSummary {
        pub prev_epoch_last_block_hash: CryptoHash,
        pub all_proposals: Vec<ValidatorStakeV1>,
        pub validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
        pub validator_block_chunk_stats: HashMap<AccountId, BlockChunkValidatorStats>,
        pub next_version: ProtocolVersion,
    }

    #[derive(BorshDeserialize)]
    pub struct OldEpochInfoAggregator {
        pub block_tracker: HashMap<ValidatorId, ValidatorStats>,
        pub shard_tracker: HashMap<ShardId, HashMap<ValidatorId, ValidatorStats>>,
        pub version_tracker: HashMap<ValidatorId, ProtocolVersion>,
        pub all_proposals: BTreeMap<AccountId, ValidatorStakeV1>,
        pub epoch_id: EpochId,
        pub last_block_hash: CryptoHash,
    }
    #[derive(BorshSerialize)]
    pub struct NewEpochInfoAggregator {
        pub block_tracker: HashMap<ValidatorId, ValidatorStats>,
        pub shard_tracker: HashMap<ShardId, HashMap<ValidatorId, ValidatorStats>>,
        pub version_tracker: HashMap<ValidatorId, ProtocolVersion>,
        pub all_proposals: BTreeMap<AccountId, ValidatorStake>,
        pub epoch_id: EpochId,
        pub last_block_hash: CryptoHash,
    }

    map_col(&store, DBCol::ChunkExtra, ChunkExtra::V1)?;

    map_col(&store, DBCol::BlockInfo, BlockInfo::V1)?;

    map_col(&store, DBCol::EpochValidatorInfo, |info: OldEpochSummary| EpochSummary {
        prev_epoch_last_block_hash: info.prev_epoch_last_block_hash,
        all_proposals: info.all_proposals.into_iter().map(ValidatorStake::V1).collect(),
        validator_kickout: info.validator_kickout,
        validator_block_chunk_stats: info.validator_block_chunk_stats,
        next_version: info.next_version,
    })?;

    // DBCol::EpochInfo has a special key which contains a different type than all other
    // values (EpochInfoAggregator), so we cannot use `map_col` on it. We need to handle
    // the AGGREGATOR_KEY differently from all others.
    let col = DBCol::EpochInfo;
    let keys = store
        .iter(col)
        .map(|item| item.map(|(key, _)| key))
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut store_update = BatchedStoreUpdate::new(&store, 10_000_000);
    for key in keys {
        if key.as_ref() == AGGREGATOR_KEY {
            let value: OldEpochInfoAggregator = store.get_ser(col, key.as_ref()).unwrap().unwrap();
            let new_value = NewEpochInfoAggregator {
                block_tracker: value.block_tracker,
                shard_tracker: value.shard_tracker,
                version_tracker: value.version_tracker,
                epoch_id: value.epoch_id,
                last_block_hash: value.last_block_hash,
                all_proposals: value
                    .all_proposals
                    .into_iter()
                    .map(|(account, stake)| (account, ValidatorStake::V1(stake)))
                    .collect(),
            };
            store_update.set_ser(col, key.as_ref(), &new_value)?;
        } else {
            let value: EpochInfoV1 = store.get_ser(col, key.as_ref()).unwrap().unwrap();
            let new_value = EpochInfo::V1(value);
            store_update.set_ser(col, key.as_ref(), &new_value)?;
        }
    }

    store_update.finish()?;
    Ok(())
}

/// Migrates the database from version 31 to 32.
///
/// This involves adding KIND entry to DbVersion column, removing IS_ARCHIVAL
/// from BlockMisc column and deleting GCCount column.
///
/// Note that if the database has IS_ARCHIVAL key in BlockMisc column and it’s
/// value is true, that overrides is_node_archival argument.
pub fn migrate_31_to_32(storage: &crate::NodeStorage, archive: bool) -> anyhow::Result<()> {
    const IS_ARCHIVE_KEY: &[u8; 10] = b"IS_ARCHIVE";

    let hot = storage.get_store(crate::Temperature::Hot);
    let mut update = hot.store_update();
    if let Some(got) = hot.get_ser::<bool>(DBCol::BlockMisc, IS_ARCHIVE_KEY)? {
        anyhow::ensure!(
            archive || !got,
            "Database is an Archive but we’re expecting RPC database; \
             converting from archive to non-archive is not possible"
        );
        update.delete(DBCol::BlockMisc, IS_ARCHIVE_KEY);
    }
    let kind = if archive { DbKind::Archive } else { DbKind::RPC };
    update.set(DBCol::DbVersion, crate::metadata::KIND_KEY, <&str>::from(kind).as_bytes());
    update.delete_all(DBCol::_ChunkPerHeightShard);
    update.delete_all(DBCol::_GCCount);
    update.commit()?;
    Ok(())
}
