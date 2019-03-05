use super::{
    BlockChainStorage, GenericStorage, COL_PROPOSAL, 
    COL_PARTICIPATION, COL_THRESHOLD, COL_ACCEPTED_AUTHORITY,
    COL_PROCESSED_BLOCKS
};
use crate::storages::ChainId;
use crate::KeyValueDB;
use primitives::beacon::{SignedBeaconBlock, SignedBeaconBlockHeader};
use primitives::types::{AuthorityStake, AuthorityMask};
use primitives::serialize::{Encode, Decode};
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

type Epoch = u64;
type Slot = u64;

/// Beacon chain does not require additional behavior besides storing and retrieving blocks. Later,
/// we can add authority info.
pub struct BeaconChainStorage {
    generic_storage: BlockChainStorage<SignedBeaconBlockHeader, SignedBeaconBlock>,
}

impl GenericStorage<SignedBeaconBlockHeader, SignedBeaconBlock> for BeaconChainStorage {
    #[inline]
    fn blockchain_storage_mut(
        &mut self,
    ) -> &mut BlockChainStorage<SignedBeaconBlockHeader, SignedBeaconBlock> {
        &mut self.generic_storage
    }
}

impl BeaconChainStorage {
    pub fn new(storage: Arc<KeyValueDB>) -> Self {
        Self { 
            generic_storage: BlockChainStorage::new(storage, ChainId::BeaconChain),
        }
    }

    pub fn get_proposal(&mut self) -> HashMap<Slot, Vec<AuthorityStake>> {
        read_one_column(self.generic_storage.storage.as_ref(), COL_PROPOSAL)
    }

    pub fn set_proposal(&mut self, proposal: &HashMap<Slot, Vec<AuthorityStake>>) {
        write_one_column(self.generic_storage.storage.as_ref(), COL_PROPOSAL, proposal);
    }

    pub fn get_participation(&mut self) -> HashMap<Slot, AuthorityMask> {
        read_one_column(self.generic_storage.storage.as_ref(), COL_PARTICIPATION)
    }

    pub fn set_participation(&mut self, participation: &HashMap<Slot, AuthorityMask>) {
        write_one_column(self.generic_storage.storage.as_ref(), COL_PARTICIPATION, participation);
    }

    pub fn get_processed_blocks(&mut self) -> HashMap<Epoch, HashSet<Slot>> {
        read_one_column(self.generic_storage.storage.as_ref(), COL_PROCESSED_BLOCKS)
    }

    pub fn set_processed_blocks(&mut self, processed_blocks: &HashMap<Epoch, HashSet<Slot>>) {
        write_one_column(self.generic_storage.storage.as_ref(), COL_PROCESSED_BLOCKS, processed_blocks);
    }

    pub fn get_threshold(&mut self) -> HashMap<Epoch, u64> {
        read_one_column(self.generic_storage.storage.as_ref(), COL_THRESHOLD)
    }

    pub fn set_threshold(&mut self, threshold: &HashMap<Epoch, u64>) {
        write_one_column(self.generic_storage.storage.as_ref(), COL_THRESHOLD, threshold);
    }

    pub fn get_accepted_authorities(&mut self) -> HashMap<Slot, Vec<AuthorityStake>> {
        read_one_column(self.generic_storage.storage.as_ref(), COL_ACCEPTED_AUTHORITY)
    }

    pub fn set_accepted_authorities(&mut self, accepted_authorities: &HashMap<Slot, Vec<AuthorityStake>>) {
        write_one_column(self.generic_storage.storage.as_ref(), COL_ACCEPTED_AUTHORITY, accepted_authorities);
    }
}

fn read_one_column<K, V>(storage: &KeyValueDB, col: u32) -> HashMap<K, V>
where 
    K: Encode + Decode + Eq + Hash,
    V: Encode + Decode 
{
    storage.iter(Some(col)).map(|(k, v)| {
        let key: K = Decode::decode(&k).expect("failed to decode key");
        let value: V = Decode::decode(&v).expect("failed to decode value");
        (key, value)
    }).collect()
}

fn write_one_column<K, V>(storage: &KeyValueDB, col: u32, data: &HashMap<K, V>)
where
    K: Encode + Decode + Eq + Hash,
    V: Encode + Decode
{
    let mut db_transaction = storage.transaction();
    for (k, v) in data.iter() {
        let key = Encode::encode(k).expect("failed to encode key");
        let value = Encode::encode(v).expect("failed to encode value");
        db_transaction.put(Some(col), &key, &value);
    }
    storage.write(db_transaction).expect("failed to write to storage");
}