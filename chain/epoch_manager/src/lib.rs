use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    AccountId, BlockIndex, GasUsage, ShardId, ValidatorId, ValidatorStake,
};
use near_store::{Store, StoreUpdate, COL_BLOCK_INFO, COL_EPOCH_INFO};

use crate::proposals::proposals_to_epoch_info;
pub use crate::types::{BlockInfo, EpochConfig, EpochError, EpochId, EpochInfo, RngSeed};

mod proposals;
#[cfg(test)]
mod test_utils;
mod types;

/// Tracks epoch information across different forks, such as validators.
/// Note: that even after garbage collection, the data about genesis epoch should be in the store.
pub struct EpochManager {
    store: Arc<Store>,
    /// Current epoch config.
    /// TODO: must be dynamically changing over time, so there should be a way to change it.
    config: EpochConfig,

    /// Cache of epoch information.
    epochs_info: HashMap<EpochId, EpochInfo>,
    /// Cache of block information.
    blocks_info: HashMap<CryptoHash, BlockInfo>,
}

impl EpochManager {
    pub fn new(
        store: Arc<Store>,
        config: EpochConfig,
        validators: Vec<ValidatorStake>,
    ) -> Result<Self, EpochError> {
        let mut epoch_manager = EpochManager {
            store,
            config,
            epochs_info: HashMap::default(),
            blocks_info: HashMap::default(),
        };
        let genesis_epoch_id = EpochId(CryptoHash::default());
        if !epoch_manager.has_epoch_info(&genesis_epoch_id)? {
            // Missing genesis epoch, means that there is no validator initialize yet.
            let epoch_info = proposals_to_epoch_info(
                &epoch_manager.config,
                [0; 32],
                &EpochInfo::default(),
                validators,
                HashSet::default(),
            )?;
            let block_info = BlockInfo::default();
            let mut store_update = epoch_manager.store.store_update();
            epoch_manager.save_epoch_info(&mut store_update, &genesis_epoch_id, epoch_info)?;
            epoch_manager.save_block_info(&mut store_update, &CryptoHash::default(), block_info)?;
            store_update.commit()?;
        }
        Ok(epoch_manager)
    }

    fn collect_blocks_info(
        &mut self,
        epoch_id: &EpochId,
        last_block_hash: &CryptoHash,
    ) -> Result<(CryptoHash, Vec<ValidatorStake>, HashSet<AccountId>, GasUsage), EpochError> {
        let mut proposals = vec![];
        let mut validator_kickout = HashSet::new();
        let mut validator_tracker = HashMap::new();
        let mut total_gas_used = 0;

        let epoch_info = self.get_epoch_info(epoch_id)?.clone();

        // Gather slashed validators and add them to kick out first.
        let slashed_validators = self.get_slashed_validators(last_block_hash)?.clone();
        for account_id in slashed_validators.iter() {
            validator_kickout.insert(account_id.clone());
        }

        let mut hash = *last_block_hash;
        loop {
            let info = self.get_block_info(&hash)?.clone();
            if &info.epoch_id != epoch_id || info.prev_hash == hash {
                break;
            }

            for proposal in info.proposals {
                if !slashed_validators.contains(&proposal.account_id) {
                    if proposal.amount == 0 {
                        validator_kickout.insert(proposal.account_id.clone());
                    }
                    proposals.push(proposal);
                }
            }
            let validator_id = self.block_producer_from_info(&epoch_info, info.index);
            validator_tracker.entry(validator_id).and_modify(|e| *e += 1).or_insert(1);
            total_gas_used += info.gas_used;

            hash = info.prev_hash;
        }

        let epoch_info = self.get_epoch_info(epoch_id)?.clone();
        let last_block_info = self.get_block_info(&last_block_hash)?.clone();
        let first_block_info = self.get_block_info(&last_block_info.epoch_first_block)?.clone();
        let num_expected_blocks = self.get_num_expected_blocks(&epoch_info, &first_block_info)?;

        // Compute kick outs for validators who are offline.
        let mut all_kicked_out = true;
        let mut maximum_block_prod_ratio = 0;
        let mut max_validator_id = None;
        let validator_kickout_threshold = self.config.validator_kickout_threshold;

        for (i, _) in epoch_info.validators.iter().enumerate() {
            let num_blocks = validator_tracker.get(&i).unwrap_or(&0).clone();
            // Note, we use * 100 to keep this in integer space.
            let mut cur_ratio = (num_blocks * 100) / num_expected_blocks[&i];
            let account_id = epoch_info.validators[i].account_id.clone();
            // TODO: replace validator_kickout_threshold with an integer
            if cur_ratio < (validator_kickout_threshold * 100.0) as u64 {
                validator_kickout.insert(account_id);
            } else {
                if !validator_kickout.contains(&account_id) {
                    all_kicked_out = false;
                } else {
                    cur_ratio = 0;
                }
            }
            if cur_ratio > maximum_block_prod_ratio {
                maximum_block_prod_ratio = cur_ratio;
                max_validator_id = Some(i);
            }
        }
        // If all validators kicked out, keep the one that produce most of the blocks.
        if all_kicked_out {
            if let Some(validator_id) = max_validator_id {
                validator_kickout.remove(&epoch_info.validators[validator_id].account_id);
            }
        }

        Ok((hash, proposals, validator_kickout, total_gas_used))
    }

    // Finalizes epoch (T), where given last block hash is given, and returns next next epoch id (T + 2).
    fn finalize_epoch(
        &mut self,
        store_update: &mut StoreUpdate,
        epoch_id: &EpochId,
        last_block_hash: &CryptoHash,
        rng_seed: RngSeed,
    ) -> Result<EpochId, EpochError> {
        let (last_block_hash_prev_epoch, proposals, validator_kickout, total_gas_used) =
            self.collect_blocks_info(epoch_id, last_block_hash)?;
        println!("EpochId: {:?}, LBH: {:?}", epoch_id, last_block_hash);
        let current_epoch_info = self.get_epoch_info(epoch_id)?.clone();
        let next_next_epoch_info = proposals_to_epoch_info(
            &self.config,
            rng_seed,
            &current_epoch_info,
            proposals,
            validator_kickout,
        )?;
        // This epoch info is computed for the epoch after next (T+2),
        // where epoch_id of it is the hash of last block in this epoch )T).
        self.save_epoch_info(store_update, &EpochId(*last_block_hash), next_next_epoch_info)?;
        // Return next epoch (T+1) id as hash of last block in previous epoch (T-1).
        Ok(EpochId(last_block_hash_prev_epoch))
    }

    pub fn record_block_info(
        &mut self,
        current_hash: &CryptoHash,
        mut block_info: BlockInfo,
        rng_seed: RngSeed,
    ) -> Result<StoreUpdate, EpochError> {
        let mut store_update = self.store.store_update();
        // Check that we didn't record this block yet.
        if !self.has_block_info(current_hash)? {
            println!("{:?}", block_info);
            if block_info.prev_hash == CryptoHash::default() {
                // This is genesis block, we special case as new epoch.
            } else {
                let prev_block_info = self.get_block_info(&block_info.prev_hash)?.clone();
                println!(
                    "Prev block info: {:?}: {}",
                    prev_block_info,
                    self.is_next_block_in_next_epoch(&prev_block_info).unwrap()
                );
                if self.is_next_block_in_next_epoch(&prev_block_info)? {
                    // Current block is in the new epoch, finalize this one.
                    self.finalize_epoch(
                        &mut store_update,
                        &block_info.epoch_id,
                        &block_info.prev_hash,
                        rng_seed,
                    )?;
                    block_info.epoch_id = self.get_next_epoch_id(&prev_block_info)?;
                    block_info.epoch_first_block = current_hash.clone();
                } else {
                    // Same epoch as parent, copy epoch_id and epoch_start_index.
                    block_info.epoch_id = prev_block_info.epoch_id.clone();
                    block_info.epoch_first_block = prev_block_info.epoch_first_block;
                }
            }
            self.save_block_info(&mut store_update, current_hash, block_info)?;
        }
        Ok(store_update)
    }

    fn get_num_expected_blocks(
        &mut self,
        epoch_info: &EpochInfo,
        epoch_first_block_info: &BlockInfo,
    ) -> Result<HashMap<ValidatorId, u64>, EpochError> {
        let mut num_expected = HashMap::default();
        let prev_epoch_last_block = self.get_block_info(&epoch_first_block_info.prev_hash)?;
        // We iterate from next index after previous epoch's last block, for epoch_length blocks.
        for index in (prev_epoch_last_block.index + 1)
            ..=(prev_epoch_last_block.index + self.config.epoch_length)
        {
            num_expected
                .entry(self.block_producer_from_info(epoch_info, index))
                .and_modify(|e| *e += 1)
                .or_insert(1);
        }
        Ok(num_expected)
    }

    fn block_producer_from_info(&self, epoch_info: &EpochInfo, index: BlockIndex) -> ValidatorId {
        epoch_info.block_producers[(index % (epoch_info.block_producers.len() as u64)) as usize]
    }

    fn get_slashed_validators(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<&HashSet<AccountId>, EpochError> {
        Ok(&self.get_block_info(block_hash)?.slashed)
    }

    /// Returns true, if given current block info, next block suppose to be in the next epoch.
    fn is_next_block_in_next_epoch(&mut self, block_info: &BlockInfo) -> Result<bool, EpochError> {
        Ok(block_info.index + 1
            >= self.get_block_info(&block_info.epoch_first_block)?.index + self.config.epoch_length)
    }

    /// Returns epoch id for the next epoch (T+1), given an block info in current epoch (T).
    fn get_next_epoch_id(&mut self, block_info: &BlockInfo) -> Result<EpochId, EpochError> {
        let first_block_info = self.get_block_info(&block_info.epoch_first_block)?;
        Ok(EpochId(first_block_info.prev_hash))
    }

    fn has_epoch_info(&mut self, epoch_id: &EpochId) -> Result<bool, EpochError> {
        match self.get_epoch_info(epoch_id) {
            Ok(_) => Ok(true),
            Err(EpochError::EpochOutOfBounds) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn get_epoch_info(&mut self, epoch_id: &EpochId) -> Result<&EpochInfo, EpochError> {
        if !self.epochs_info.contains_key(epoch_id) {
            let epoch_info = self
                .store
                .get_ser(COL_EPOCH_INFO, epoch_id.as_ref())
                .map_err(|err| err.into())
                .and_then(|value| value.ok_or_else(|| EpochError::EpochOutOfBounds))?;
            self.epochs_info.insert(epoch_id.clone(), epoch_info);
        }
        self.epochs_info.get(epoch_id).ok_or(EpochError::EpochOutOfBounds)
    }

    fn save_epoch_info(
        &mut self,
        store_update: &mut StoreUpdate,
        epoch_id: &EpochId,
        epoch_info: EpochInfo,
    ) -> Result<(), EpochError> {
        store_update
            .set_ser(COL_EPOCH_INFO, epoch_id.as_ref(), &epoch_info)
            .map_err(|err| EpochError::from(err))?;
        self.epochs_info.insert(epoch_id.clone(), epoch_info);
        Ok(())
    }

    fn has_block_info(&mut self, hash: &CryptoHash) -> Result<bool, EpochError> {
        match self.get_block_info(hash) {
            Ok(_) => Ok(true),
            Err(EpochError::MissingBlock(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn get_block_info(&mut self, hash: &CryptoHash) -> Result<&BlockInfo, EpochError> {
        if !self.blocks_info.contains_key(hash) {
            let block_info = self
                .store
                .get_ser(COL_BLOCK_INFO, hash.as_ref())
                .map_err(|err| EpochError::from(err))
                .and_then(|value| value.ok_or_else(|| EpochError::MissingBlock(*hash)))?;
            self.blocks_info.insert(*hash, block_info);
        }
        self.blocks_info.get(hash).ok_or(EpochError::MissingBlock(*hash))
    }

    fn save_block_info(
        &mut self,
        store_update: &mut StoreUpdate,
        block_hash: &CryptoHash,
        block_info: BlockInfo,
    ) -> Result<(), EpochError> {
        store_update
            .set_ser(COL_BLOCK_INFO, block_hash.as_ref(), &block_info)
            .map_err(|err| EpochError::from(err))?;
        self.blocks_info.insert(*block_hash, block_info);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::hash::hash;
    use near_store::test_utils::create_test_store;

    use crate::test_utils::{change_stake, epoch_config, epoch_info, stake};

    fn hash_range(num: usize) -> Vec<CryptoHash> {
        let mut result = vec![];
        for i in 0..num {
            result.push(hash(&vec![i as u8]));
        }
        result
    }

    fn record_block(
        epoch_manager: &mut EpochManager,
        prev_h: CryptoHash,
        cur_h: CryptoHash,
        index: BlockIndex,
        proposals: Vec<ValidatorStake>,
    ) {
        epoch_manager
            .record_block_info(
                &cur_h,
                BlockInfo::new(index, prev_h, proposals, vec![], HashSet::default(), 0),
                [0; 32],
            )
            .unwrap()
            .commit()
            .unwrap();
    }

    #[test]
    fn test_validator_change_of_stake() {
        let store = create_test_store();
        let config = epoch_config(2, 1, 2, 0, 0.9);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut epoch_manager = EpochManager::new(store, config, validators.clone()).unwrap();
        let h = hash_range(4);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1", 10)]);
        // New epoch starts here.
        record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[1])).unwrap(),
            &epoch_info(
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                change_stake(vec![("test1", 0), ("test2", amount_staked)])
            )
        );
    }
}
