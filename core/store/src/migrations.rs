use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use near_primitives::epoch_manager::epoch_info::{EpochInfo, EpochInfoV1};
use near_primitives::hash::CryptoHash;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::AccountId;

use crate::{DBCol, Store, StoreOpener, StoreUpdate};

pub fn set_store_version(store: &Store, db_version: u32) {
    let mut store_update = store.store_update();
    // Contrary to other integers, we’re using textual representation for
    // storing DbVersion in VERSION_KEY thus to_string rather than to_le_bytes.
    store_update.set(DBCol::DbVersion, crate::db::VERSION_KEY, db_version.to_string().as_bytes());
    store_update.commit().expect("Failed to write version to database");
}

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

    fn commit(&mut self) -> Result<(), std::io::Error> {
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
    ) -> Result<(), std::io::Error> {
        let value_bytes = value.try_to_vec()?;
        self.batch_size += key.as_ref().len() + value_bytes.len() + 8;
        self.store_update.as_mut().unwrap().set(col, key.as_ref(), &value_bytes);

        if self.batch_size > self.batch_size_limit {
            self.commit()?;
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<(), std::io::Error> {
        if self.batch_size > 0 {
            self.commit()?;
        }

        Ok(())
    }
}

fn map_col<T, U, F>(store: &Store, col: DBCol, f: F) -> Result<(), std::io::Error>
where
    T: BorshDeserialize,
    U: BorshSerialize,
    F: Fn(T) -> U,
{
    let keys: Vec<_> = store.iter(col).map(Result::unwrap).map(|(key, _)| key).collect();
    let mut store_update = BatchedStoreUpdate::new(store, 10_000_000);

    for key in keys {
        let value: T = store.get_ser(col, key.as_ref())?.unwrap();
        let new_value = f(value);
        store_update.set_ser(col, key.as_ref(), &new_value)?;
    }

    store_update.finish()?;

    Ok(())
}

#[allow(unused)]
fn map_col_from_key<U, F>(store: &Store, col: DBCol, f: F) -> Result<(), std::io::Error>
where
    U: BorshSerialize,
    F: Fn(&[u8]) -> U,
{
    let mut store_update = store.store_update();
    let batch_size_limit = 10_000_000;
    let mut batch_size = 0;
    for (key, _) in store.iter(col).map(Result::unwrap) {
        let new_value = f(&key);
        let new_bytes = new_value.try_to_vec()?;
        batch_size += key.as_ref().len() + new_bytes.len() + 8;
        store_update.set(col, key.as_ref(), &new_bytes);

        if batch_size > batch_size_limit {
            store_update.commit()?;
            store_update = store.store_update();
            batch_size = 0;
        }
    }

    if batch_size > 0 {
        store_update.commit()?;
    }

    Ok(())
}

pub fn migrate_28_to_29(store_opener: &StoreOpener) {
    let store = store_opener.open();
    let mut store_update = store.store_update();
    store_update.delete_all(DBCol::_NextBlockWithNewChunk);
    store_update.delete_all(DBCol::_LastBlockWithNewChunk);
    store_update.commit().unwrap();

    set_store_version(&store, 29);
}

pub fn migrate_29_to_30(store_opener: &StoreOpener) {
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

    let store = store_opener.open();

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

    map_col(&store, DBCol::ChunkExtra, ChunkExtra::V1).unwrap();

    map_col(&store, DBCol::BlockInfo, BlockInfo::V1).unwrap();

    map_col(&store, DBCol::EpochValidatorInfo, |info: OldEpochSummary| EpochSummary {
        prev_epoch_last_block_hash: info.prev_epoch_last_block_hash,
        all_proposals: info.all_proposals.into_iter().map(ValidatorStake::V1).collect(),
        validator_kickout: info.validator_kickout,
        validator_block_chunk_stats: info.validator_block_chunk_stats,
        next_version: info.next_version,
    })
    .unwrap();

    // DBCol::EpochInfo has a special key which contains a different type than all other
    // values (EpochInfoAggregator), so we cannot use `map_col` on it. We need to handle
    // the AGGREGATOR_KEY differently from all others.
    let col = DBCol::EpochInfo;
    let keys: Vec<_> = store.iter(col).map(Result::unwrap).map(|(key, _)| key).collect();
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
            store_update.set_ser(col, key.as_ref(), &new_value).unwrap();
        } else {
            let value: EpochInfoV1 = store.get_ser(col, key.as_ref()).unwrap().unwrap();
            let new_value = EpochInfo::V1(value);
            store_update.set_ser(col, key.as_ref(), &new_value).unwrap();
        }
    }

    store_update.finish().unwrap();

    set_store_version(&store, 30);
}
