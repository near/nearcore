use crate::single_shard_storage_mutator::SingleShardStorageMutator;
use near_crypto::PublicKey;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::account::{AccessKey, Account};
use near_primitives::types::{AccountId, EpochId, StateRoot};
use nearcore::NightshadeRuntime;
use std::sync::Arc;

/// Object that updates the existing state. Combines all changes, commits them
/// and returns new state roots.
pub(crate) struct StorageMutator {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    epoch_id: EpochId,
    mutators: Vec<SingleShardStorageMutator>,
}

impl StorageMutator {
    pub(crate) fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime: &NightshadeRuntime,
        epoch_id: EpochId,
        state_roots: Vec<StateRoot>,
    ) -> anyhow::Result<Self> {
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
        assert_eq!(shard_layout.shard_ids().count(), state_roots.len());

        let mut mutators = vec![];
        for state_root in state_roots {
            mutators.push(SingleShardStorageMutator::new(runtime, state_root)?);
        }
        Ok(Self { epoch_manager, epoch_id, mutators })
    }

    fn mutator(
        &mut self,
        account_id: &AccountId,
    ) -> anyhow::Result<&mut SingleShardStorageMutator> {
        let shard_id = self.epoch_manager.account_id_to_shard_id(&account_id, &self.epoch_id)?;
        Ok(&mut self.mutators[shard_id as usize])
    }

    pub(crate) fn set_account(
        &mut self,
        account_id: &AccountId,
        value: Account,
    ) -> anyhow::Result<()> {
        self.mutator(account_id)?.set_account(account_id.clone(), value)
    }

    pub(crate) fn set_access_key(
        &mut self,
        account_id: &AccountId,
        public_key: PublicKey,
        access_key: AccessKey,
    ) -> anyhow::Result<()> {
        self.mutator(account_id)?.set_access_key(account_id.clone(), public_key, access_key)
    }

    pub(crate) fn commit(self) -> anyhow::Result<Vec<StateRoot>> {
        let shard_layout = self.epoch_manager.get_shard_layout(&self.epoch_id)?;
        let all_shard_uids = shard_layout.shard_uids();
        let mut state_roots = vec![];
        for (mutator, shard_uid) in self.mutators.into_iter().zip(all_shard_uids.into_iter()) {
            let state_root = mutator.commit(&shard_uid, 0)?;
            state_roots.push(state_root);
        }
        Ok(state_roots)
    }
}
