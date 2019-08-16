use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::iter;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};

use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    AccountId, Balance, BlockIndex, ShardId, ValidatorId, ValidatorStake,
};
use near_store::{Store, StoreUpdate, COL_LAST_EPOCH_PROPOSALS, COL_PROPOSALS, COL_VALIDATORS};

const LAST_EPOCH_KEY: &[u8] = b"LAST_EPOCH";

#[derive(Eq, PartialEq)]
pub enum ValidatorError {
    /// Error calculating threshold from given stakes for given number of seats.
    /// Only should happened if calling code doesn't check for integer value of stake > number of seats.
    ThresholdError(Balance, u64),
    /// Requesting validators for an epoch that wasn't computed yet.
    EpochOutOfBounds,
    /// Number of selected seats doesn't match requested.
    SelectedSeatsMismatch(u64, ValidatorId),
    /// Missing block hash in the storage (means there is some structural issue).
    MissingBlock(CryptoHash),
    /// Other error.
    Other(String),
}

impl std::error::Error for ValidatorError {}

impl fmt::Debug for ValidatorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ValidatorError::ThresholdError(stakes_sum, num_seats) => write!(
                f,
                "Total stake {} must be higher than the number of seats {}",
                stakes_sum, num_seats
            ),
            ValidatorError::EpochOutOfBounds => write!(f, "Epoch out of bounds"),
            ValidatorError::SelectedSeatsMismatch(selected, required) => write!(
                f,
                "Number of selected seats {} < total number of seats {}",
                selected, required
            ),
            ValidatorError::MissingBlock(hash) => write!(f, "Missing block {}", hash),
            ValidatorError::Other(err) => write!(f, "Other: {}", err),
        }
    }
}

impl fmt::Display for ValidatorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ValidatorError::ThresholdError(stake, num_seats) => {
                write!(f, "ThresholdError({}, {})", stake, num_seats)
            }
            ValidatorError::EpochOutOfBounds => write!(f, "EpochOutOfBounds"),
            ValidatorError::SelectedSeatsMismatch(num_seats, validator) => {
                write!(f, "SelectedSeatsMismatch({}, {})", num_seats, validator)
            }
            ValidatorError::MissingBlock(hash) => write!(f, "MissingBlock({})", hash),
            ValidatorError::Other(err) => write!(f, "Other({})", err),
        }
    }
}

impl From<std::io::Error> for ValidatorError {
    fn from(error: std::io::Error) -> ValidatorError {
        ValidatorError::Other(error.to_string())
    }
}

/// Find threshold of stake per seat, given provided stakes and required number of seats.
fn find_threshold(stakes: &[Balance], num_seats: u64) -> Result<Balance, ValidatorError> {
    let stakes_sum: Balance = stakes.iter().sum();
    if stakes_sum < num_seats.into() {
        return Err(ValidatorError::ThresholdError(stakes_sum, num_seats));
    }
    let (mut left, mut right): (Balance, Balance) = (1, stakes_sum + 1);
    'outer: loop {
        if left == right - 1 {
            break Ok(left);
        }
        let mid = (left + right) / 2;
        let mut current_sum: Balance = 0;
        for item in stakes.iter() {
            current_sum += item / mid;
            if current_sum >= num_seats as u128 {
                left = mid;
                continue 'outer;
            }
        }
        right = mid;
    }
}

/// Calculates new seat assignments based on current seat assignments and proposals.
fn proposals_to_assignments(
    epoch_config: ValidatorEpochConfig,
    current_assignments: &ValidatorAssignment,
    proposals: Vec<ValidatorStake>,
    validator_kickout: HashMap<AccountId, bool>,
) -> Result<ValidatorAssignment, ValidatorError> {
    // Combine proposals with rollovers.
    let mut ordered_proposals = BTreeMap::new();
    let mut stake_change = BTreeMap::new();
    for p in proposals {
        if *validator_kickout.get(&p.account_id).unwrap_or(&false) {
            stake_change.insert(p.account_id, (0, p.amount));
        } else {
            // since proposals is ordered by nonce, we always overwrite the
            // entry with the latest proposal within the epoch
            ordered_proposals.insert(p.account_id.clone(), p);
        }
    }
    for r in current_assignments.validators.iter() {
        match ordered_proposals.entry(r.account_id.clone()) {
            Entry::Occupied(e) => {
                let p = &*e.get();
                let return_stake = if r.amount > p.amount { r.amount - p.amount } else { 0 };
                stake_change.insert(r.account_id.clone(), (p.amount, return_stake));
            }
            Entry::Vacant(e) => {
                if !*validator_kickout.get(&r.account_id).unwrap_or(&true) {
                    e.insert(r.clone());
                } else {
                    stake_change.insert(r.account_id.clone(), (0, r.amount));
                }
            }
        }
    }

    // Get the threshold given current number of seats and stakes.
    let num_fisherman_seats: usize = epoch_config.avg_fisherman_per_shard.iter().sum();
    let num_seats = epoch_config.num_block_producers + num_fisherman_seats;
    let stakes = ordered_proposals.iter().map(|(_, p)| p.amount).collect::<Vec<_>>();
    let threshold = find_threshold(&stakes, num_seats as u64)?;
    // Remove proposals under threshold.
    let mut final_proposals = BTreeMap::new();
    for (account_id, p) in ordered_proposals {
        if p.amount >= threshold {
            if !stake_change.contains_key(&p.account_id) {
                stake_change.insert(p.account_id.clone(), (p.amount, 0));
            }
            final_proposals.insert(account_id, p);
        } else {
            stake_change
                .entry(p.account_id)
                .and_modify(|(new_stake, return_stake)| {
                    if *new_stake != 0 {
                        *return_stake += *new_stake;
                        *new_stake = 0;
                    }
                })
                .or_insert((0, p.amount));
        }
    }

    let (final_proposals, validator_to_index) = final_proposals.into_iter().enumerate().fold(
        (vec![], HashMap::new()),
        |(mut proposals, mut validator_to_index), (i, (account_id, p))| {
            validator_to_index.insert(account_id, i);
            proposals.push(p);
            (proposals, validator_to_index)
        },
    );

    // Duplicate each proposal for number of seats it has.
    let mut dup_proposals = final_proposals
        .iter()
        .enumerate()
        .flat_map(|(i, p)| iter::repeat(i).take((p.amount / threshold) as usize))
        .collect::<Vec<_>>();
    if dup_proposals.len() < num_seats as usize {
        return Err(ValidatorError::SelectedSeatsMismatch(dup_proposals.len() as u64, num_seats));
    }

    // Shuffle duplicate proposals.
    let mut rng: StdRng = SeedableRng::from_seed(epoch_config.rng_seed);
    dup_proposals.shuffle(&mut rng);

    // Block producers are first `num_block_producers` proposals.
    let block_producers = dup_proposals[..epoch_config.num_block_producers].to_vec();

    // Collect proposals into block producer assignments.
    let mut chunk_producers: Vec<Vec<ValidatorId>> = vec![];
    let mut last_index: usize = 0;
    for num_seats in epoch_config.block_producers_per_shard.iter() {
        let mut cp: Vec<ValidatorId> = vec![];
        for i in 0..*num_seats {
            let proposal_index = dup_proposals[(i + last_index) % epoch_config.num_block_producers];
            cp.push(proposal_index);
        }
        chunk_producers.push(cp);
        last_index = (last_index + num_seats) % epoch_config.num_block_producers;
    }

    // TODO(1050): implement fishermen allocation.
    let expected_epoch_start = if current_assignments.expected_epoch_start == 0
        && current_assignments.validators.is_empty()
    {
        // genesis block
        0
    } else {
        // Since the current assignment is the one stored at epoch X - 2, when
        // calculating the expected start for this epoch, we need to add twice
        // the epoch length
        current_assignments.expected_epoch_start + 2 * epoch_config.epoch_length
    };

    let final_stake_change = stake_change.into_iter().map(|(k, (v, _))| (k, v)).collect();

    Ok(ValidatorAssignment {
        validators: final_proposals,
        validator_to_index,
        block_producers,
        chunk_producers,
        fishermen: HashMap::default(),
        expected_epoch_start,
        stake_change: final_stake_change,
    })
}

fn get_epoch_block_proposer_info(
    validator_assignment: &ValidatorAssignment,
    epoch_start_index: BlockIndex,
    epoch_end_index: BlockIndex,
) -> (HashMap<BlockIndex, usize>, HashMap<usize, u32>) {
    let mut block_index_to_validator = HashMap::new();
    let mut validator_to_num_blocks = HashMap::new();
    let num_seats = validator_assignment.block_producers.len() as u64;
    for block_index in epoch_start_index..=epoch_end_index {
        let validator_idx =
            validator_assignment.block_producers[(block_index % num_seats) as usize];
        validator_to_num_blocks.entry(validator_idx).and_modify(|e| *e += 1).or_insert(1);
        block_index_to_validator.insert(block_index, validator_idx);
    }
    (block_index_to_validator, validator_to_num_blocks)
}

/// Epoch config, determines validator assignment for given epoch.
/// Can change from epoch to epoch depending on the sharding and other parameters, etc.
#[derive(Clone)]
pub struct ValidatorEpochConfig {
    /// Epoch length in blocks.
    pub epoch_length: BlockIndex,
    /// Source of randomness.
    pub rng_seed: [u8; 32],
    /// Number of shards currently.
    pub num_shards: ShardId,
    /// Number of block producers.
    pub num_block_producers: ValidatorId,
    /// Number of block producers per each shard.
    pub block_producers_per_shard: Vec<ValidatorId>,
    /// Expected number of fisherman per each shard.
    pub avg_fisherman_per_shard: Vec<ValidatorId>,
    /// Criterion for kicking out validators
    pub validator_kickout_threshold: f64,
}

/// Information about validator seat assignments.
#[derive(BorshSerialize, BorshDeserialize, Default, Clone, Debug)]
pub struct ValidatorAssignment {
    /// List of current validators.
    pub validators: Vec<ValidatorStake>,
    /// Validator account id to index in proposals.
    pub validator_to_index: HashMap<AccountId, ValidatorId>,
    /// Weights for each of the validators responsible for block production.
    pub block_producers: Vec<ValidatorId>,
    /// Per each shard, ids and seats of validators that are responsible.
    pub chunk_producers: Vec<Vec<ValidatorId>>,
    /// Weight of given validator used to determine how many shards they will validate.
    pub fishermen: HashMap<ValidatorId, u64>,
    /// Expected epoch start index: previous expected epoch start + epoch_length
    pub expected_epoch_start: BlockIndex,
    /// New stake for validators
    pub stake_change: BTreeMap<AccountId, Balance>,
}

impl PartialEq for ValidatorAssignment {
    fn eq(&self, other: &ValidatorAssignment) -> bool {
        let normal_eq = self.validators == other.validators
            && self.block_producers == other.block_producers
            && self.chunk_producers == other.chunk_producers
            && self.expected_epoch_start == other.expected_epoch_start
            && self.stake_change == other.stake_change;
        if !normal_eq {
            return false;
        }
        for (k, v) in self.validator_to_index.iter() {
            if let Some(v1) = other.validator_to_index.get(k) {
                if *v1 != *v {
                    return false;
                }
            } else {
                return false;
            }
        }
        for (k, v) in other.validator_to_index.iter() {
            if let Some(v1) = self.validator_to_index.get(k) {
                if *v1 != *v {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }
}

impl Eq for ValidatorAssignment {}

/// Information per each index about validators.
#[derive(BorshSerialize, BorshDeserialize, Default, Clone, Debug)]
pub struct ValidatorIndexInfo {
    pub index: BlockIndex,
    pub prev_hash: CryptoHash,
    pub epoch_start_hash: CryptoHash,
    pub proposals: Vec<ValidatorStake>,
    pub validator_mask: Vec<bool>,
    pub slashed: HashSet<AccountId>,
}

/// Manages current validators and validator proposals in the current epoch across different forks.
pub struct ValidatorManager {
    store: Arc<Store>,
    /// Current epoch config.
    /// TODO: must be dynamically changing over time, so there should be a way to change it.
    config: ValidatorEpochConfig,

    last_epoch: CryptoHash,
    epoch_validators: HashMap<CryptoHash, ValidatorAssignment>,
    validator_info: HashMap<CryptoHash, ValidatorIndexInfo>,
}

impl ValidatorManager {
    pub fn new(
        initial_epoch_config: ValidatorEpochConfig,
        initial_validators: Vec<ValidatorStake>,
        store: Arc<Store>,
    ) -> Result<Self, ValidatorError> {
        let mut epoch_validators = HashMap::default();
        let mut validator_info = HashMap::default();
        let last_epoch = match store.get_ser(COL_PROPOSALS, LAST_EPOCH_KEY) {
            // TODO: check consistency of the db by querying it here?
            Ok(Some(value)) => value,
            Ok(None) => {
                let pre_gensis_hash = CryptoHash::default();
                let initial_assigment = proposals_to_assignments(
                    initial_epoch_config.clone(),
                    &ValidatorAssignment::default(),
                    initial_validators,
                    HashMap::new(),
                )?;
                let validator_index_info = ValidatorIndexInfo {
                    index: 0,
                    prev_hash: pre_gensis_hash,
                    epoch_start_hash: pre_gensis_hash,
                    proposals: vec![],
                    validator_mask: vec![],
                    slashed: HashSet::new(),
                };

                let mut store_update = store.store_update();
                store_update.set_ser(
                    COL_PROPOSALS,
                    pre_gensis_hash.as_ref(),
                    &validator_index_info,
                )?;
                store_update.set_ser(
                    COL_VALIDATORS,
                    pre_gensis_hash.as_ref(),
                    &initial_assigment,
                )?;
                store_update.commit()?;

                epoch_validators.insert(pre_gensis_hash, initial_assigment);
                validator_info.insert(pre_gensis_hash, validator_index_info);
                pre_gensis_hash
            }
            Err(err) => return Err(ValidatorError::Other(err.to_string())),
        };
        Ok(ValidatorManager {
            store,
            config: initial_epoch_config,
            last_epoch,
            epoch_validators,
            validator_info,
        })
    }

    fn get_index_info(&mut self, hash: &CryptoHash) -> Result<&ValidatorIndexInfo, ValidatorError> {
        if !self.validator_info.contains_key(hash) {
            match self
                .store
                .get_ser(COL_PROPOSALS, hash.as_ref())
                .map_err(|err| ValidatorError::Other(err.to_string()))?
            {
                Some(validators) => self.validator_info.insert(*hash, validators),
                None => return Err(ValidatorError::MissingBlock(*hash)),
            };
        }
        Ok(self.validator_info.get(hash).unwrap())
    }

    pub fn get_epoch_offset(
        &mut self,
        parent_hash: CryptoHash,
        index: BlockIndex,
    ) -> Result<(CryptoHash, BlockIndex), ValidatorError> {
        // TODO(1049): handle that config epoch length can change over time from runtime.
        let parent_info = self
            .get_index_info(&parent_hash)
            .map_err(|_| ValidatorError::EpochOutOfBounds)?
            .clone();
        let (epoch_start_index, epoch_start_parent_hash) =
            if parent_hash == parent_info.epoch_start_hash {
                (parent_info.index, parent_info.prev_hash)
            } else {
                let epoch_start_info = self.get_index_info(&parent_info.epoch_start_hash)?;
                (epoch_start_info.index, epoch_start_info.prev_hash)
            };

        if epoch_start_index + self.config.epoch_length <= index {
            // If this is next epoch index, return parent's epoch hash and 0 as offset.
            Ok((parent_info.epoch_start_hash, 0))
        } else {
            // If index is within the same epoch as its parent, return its epoch parent and current offset from this epoch start.
            let prev_epoch_info = self.get_index_info(&epoch_start_parent_hash)?;
            Ok((prev_epoch_info.epoch_start_hash, index - epoch_start_index))
        }
    }

    /// Get previous epoch hash given current epoch hash
    pub fn get_prev_epoch_hash(
        &mut self,
        epoch_hash: &CryptoHash,
    ) -> Result<CryptoHash, ValidatorError> {
        let parent_hash = self.get_index_info(&epoch_hash)?.prev_hash;
        self.get_index_info(&parent_hash).map(|info| info.epoch_start_hash)
    }

    pub fn get_validators(
        &mut self,
        epoch_hash: CryptoHash,
    ) -> Result<&ValidatorAssignment, ValidatorError> {
        if !self.epoch_validators.contains_key(&epoch_hash) {
            match self
                .store
                .get_ser(COL_VALIDATORS, epoch_hash.as_ref())
                .map_err(|err| ValidatorError::Other(err.to_string()))?
            {
                Some(validators) => self.epoch_validators.insert(epoch_hash, validators),
                None => return Err(ValidatorError::EpochOutOfBounds),
            };
        }
        Ok(self.epoch_validators.get(&epoch_hash).unwrap())
    }

    pub fn get_slashed_validators(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<&HashSet<AccountId>, ValidatorError> {
        Ok(&self.get_index_info(block_hash)?.slashed)
    }

    fn set_validators(
        &mut self,
        epoch_hash: &CryptoHash,
        assignment: ValidatorAssignment,
        store_update: &mut StoreUpdate,
    ) -> Result<(), ValidatorError> {
        store_update.set_ser(COL_VALIDATORS, epoch_hash.as_ref(), &assignment)?;
        self.epoch_validators.insert(*epoch_hash, assignment);
        Ok(())
    }

    pub fn finalize_epoch(
        &mut self,
        epoch_hash: &CryptoHash,
        last_hash: &CryptoHash,
        new_hash: &CryptoHash,
    ) -> Result<(), ValidatorError> {
        let mut proposals = vec![];
        let mut validator_kickout = HashMap::new();
        let mut validator_tracker = HashMap::new();
        let mut hash = *last_hash;
        let last_block_info = self.get_index_info(&last_hash)?.clone();
        let prev_epoch_hash = self.get_prev_epoch_hash(&epoch_hash)?;
        let (block_index_to_validator, validator_to_num_blocks) = {
            let validator_assignment = self.get_validators(prev_epoch_hash)?;
            get_epoch_block_proposer_info(
                validator_assignment,
                validator_assignment.expected_epoch_start,
                last_block_info.index,
            )
        };

        let slashed = self.get_slashed_validators(last_hash)?.clone();
        for account_id in slashed.iter() {
            validator_kickout.insert(account_id.clone(), true);
        }

        loop {
            let info = self.get_index_info(&hash)?.clone();
            if info.epoch_start_hash != *epoch_hash || info.prev_hash == hash {
                break;
            }
            for proposal in info.proposals {
                if !slashed.contains(&proposal.account_id) {
                    if proposal.amount == 0 {
                        validator_kickout.insert(proposal.account_id.clone(), true);
                    }
                    proposals.push(proposal);
                }
            }
            // safe to unwrap because block_index_to_validator is computed from indices in this epoch
            let validator = *block_index_to_validator.get(&info.index).unwrap();
            validator_tracker.entry(validator).and_modify(|e| *e += 1).or_insert(1);
            hash = info.prev_hash;
        }
        let mut store_update = self.store.store_update();

        let mut last_epoch_proposals = self
            .store
            .get_ser(COL_LAST_EPOCH_PROPOSALS, epoch_hash.as_ref())?
            .unwrap_or_else(|| vec![]);
        let cur_proposals = proposals.clone();
        last_epoch_proposals.append(&mut proposals);
        let proposals = last_epoch_proposals;

        {
            let validator_kickout_threshold = self.config.validator_kickout_threshold;
            let validator_assignment = self.get_validators(prev_epoch_hash)?;
            let mut all_kicked_out = true;
            let mut maximum_block_prod_ratio: f64 = 0.0;
            let mut max_account_id = None;
            for (i, num_blocks) in validator_tracker.into_iter() {
                let num_blocks_expected = *validator_to_num_blocks.get(&i).unwrap();
                let mut cur_ratio = (num_blocks as f64) / num_blocks_expected as f64;
                let account_id = validator_assignment.validators[i].account_id.clone();
                if cur_ratio < validator_kickout_threshold {
                    validator_kickout.insert(account_id, true);
                } else {
                    if !validator_kickout.contains_key(&account_id) {
                        validator_kickout.insert(account_id, false);
                        all_kicked_out = false;
                    } else {
                        cur_ratio = 0.0;
                    }
                }
                if cur_ratio > maximum_block_prod_ratio {
                    maximum_block_prod_ratio = cur_ratio;
                    max_account_id = Some(i);
                }
            }
            if all_kicked_out {
                if let Some(i) = max_account_id {
                    let account_id = validator_assignment.validators[i].account_id.clone();
                    validator_kickout.insert(account_id, false);
                }
            }
        }

        let assignment = proposals_to_assignments(
            self.config.clone(),
            self.get_validators(prev_epoch_hash)?,
            proposals,
            validator_kickout,
        )?;

        self.last_epoch = *new_hash;
        self.set_validators(new_hash, assignment, &mut store_update)?;
        store_update.set_ser(COL_PROPOSALS, LAST_EPOCH_KEY, epoch_hash)?;
        store_update.set_ser(COL_LAST_EPOCH_PROPOSALS, new_hash.as_ref(), &cur_proposals)?;
        store_update.commit().map_err(|err| ValidatorError::Other(err.to_string()))?;
        Ok(())
    }

    /// Add proposals from given header into validators.
    pub fn add_proposals(
        &mut self,
        prev_hash: CryptoHash,
        current_hash: CryptoHash,
        index: BlockIndex,
        proposals: Vec<ValidatorStake>,
        slashed_validators: Vec<AccountId>,
        validator_mask: Vec<bool>,
    ) -> Result<StoreUpdate, ValidatorError> {
        let mut store_update = self.store.store_update();
        if self.store.get(COL_PROPOSALS, current_hash.as_ref())?.is_none() {
            // TODO: keep track of size here to make sure we can't be spammed storing non interesting forks.
            let parent_info = self.get_index_info(&prev_hash)?.clone();
            let epoch_start_hash = if prev_hash == CryptoHash::default() {
                // If this genesis block, we save genesis validators for it.
                let mut store_update = self.store.store_update();
                let mut genesis_validators = self.get_validators(CryptoHash::default())?.clone();
                genesis_validators.expected_epoch_start = self.config.epoch_length;
                store_update.set_ser(COL_VALIDATORS, current_hash.as_ref(), &genesis_validators)?;
                store_update.set_ser::<Vec<ValidatorStake>>(
                    COL_LAST_EPOCH_PROPOSALS,
                    current_hash.as_ref(),
                    &vec![],
                )?;
                store_update.commit().map_err(|err| ValidatorError::Other(err.to_string()))?;

                current_hash
            } else {
                let epoch_start_info = self.get_index_info(&parent_info.epoch_start_hash)?;
                if epoch_start_info.index + self.config.epoch_length <= index {
                    // This is first block of the next epoch, finalize it and return current hash and index as epoch hash/start.
                    // TODO: remove this clutch
                    if self.get_validators(current_hash).is_err() {
                        self.finalize_epoch(
                            &parent_info.epoch_start_hash,
                            &prev_hash,
                            &current_hash,
                        )?;
                    }
                    current_hash
                } else {
                    // Otherwise, return parent's info.
                    parent_info.epoch_start_hash
                }
            };

            let mut slashed = if epoch_start_hash == current_hash {
                let validators = self.get_validators(parent_info.epoch_start_hash)?;
                parent_info
                    .slashed
                    .into_iter()
                    .filter(|x| validators.validator_to_index.contains_key(x))
                    .collect()
            } else {
                parent_info.slashed
            };
            for validator in slashed_validators {
                slashed.insert(validator);
            }

            let info = ValidatorIndexInfo {
                index,
                epoch_start_hash,
                prev_hash,
                proposals,
                validator_mask,
                slashed,
            };
            store_update.set_ser(COL_PROPOSALS, current_hash.as_ref(), &info)?;
            self.validator_info.insert(current_hash, info);
        }
        Ok(store_update)
    }

    pub fn get_block_proposer_info(
        &mut self,
        epoch_hash: CryptoHash,
        height: BlockIndex,
    ) -> Result<ValidatorStake, Box<dyn std::error::Error>> {
        let validator_assignment = self.get_validators(epoch_hash)?;
        if height < validator_assignment.expected_epoch_start {
            return Err(Box::new(ValidatorError::EpochOutOfBounds));
        }
        let total_seats = validator_assignment.block_producers.len() as u64;
        let block_producer_idx = height % total_seats;
        let validator_idx = validator_assignment.block_producers[block_producer_idx as usize];
        Ok(validator_assignment.validators[validator_idx].clone())
    }
}

#[cfg(test)]
mod test {
    use near_primitives::hash::hash;
    use near_primitives::test_utils::get_key_pair_from_seed;
    use near_store::test_utils::create_test_store;

    use crate::test_utils::*;

    use super::*;

    fn stake(account_id: &str, amount: Balance) -> ValidatorStake {
        let (public_key, _) = get_key_pair_from_seed(account_id);
        ValidatorStake::new(account_id.to_string(), public_key, amount)
    }

    fn config(
        epoch_length: BlockIndex,
        num_shards: ShardId,
        num_block_producers: usize,
        num_fisherman: usize,
        validator_kickout_threshold: f64,
    ) -> ValidatorEpochConfig {
        ValidatorEpochConfig {
            epoch_length,
            rng_seed: [0; 32],
            num_shards,
            num_block_producers,
            block_producers_per_shard: (0..num_shards).map(|_| num_block_producers).collect(),
            avg_fisherman_per_shard: (0..num_shards).map(|_| num_fisherman).collect(),
            validator_kickout_threshold,
        }
    }

    #[test]
    fn test_find_threshold() {
        assert_eq!(find_threshold(&[1_000_000, 1_000_000, 10], 10).unwrap(), 200_000);
        assert_eq!(find_threshold(&[1_000_000_000, 10], 10).unwrap(), 100_000_000);
        assert_eq!(find_threshold(&[1_000_000_000], 1_000_000_000).unwrap(), 1);
        assert_eq!(find_threshold(&[1_000, 1, 1, 1, 1, 1, 1, 1, 1, 1], 1).unwrap(), 1_000);
        assert!(find_threshold(&[1, 1, 2], 100).is_err());
    }

    #[test]
    fn test_proposals_to_assignments() {
        assert_eq!(
            proposals_to_assignments(
                config(2, 2, 1, 1, 0.9),
                &ValidatorAssignment::default(),
                vec![stake("test1", 1_000_000)],
                HashMap::new(),
            )
            .unwrap(),
            assignment(
                vec![("test1", 1_000_000)],
                vec![0],
                vec![vec![0], vec![0]],
                vec![],
                0,
                change_stake(vec![("test1", 1_000_000)])
            )
        );
        assert_eq!(
            proposals_to_assignments(
                ValidatorEpochConfig {
                    epoch_length: 2,
                    rng_seed: [0; 32],
                    num_shards: 5,
                    num_block_producers: 6,
                    block_producers_per_shard: vec![6, 2, 2, 2, 2],
                    avg_fisherman_per_shard: vec![6, 2, 2, 2, 2],
                    validator_kickout_threshold: 0.9,
                },
                &ValidatorAssignment::default(),
                vec![
                    stake("test1", 1_000_000),
                    stake("test2", 1_000_000),
                    stake("test3", 1_000_000)
                ],
                HashMap::new(),
            )
            .unwrap(),
            assignment(
                vec![("test1", 1_000_000), ("test2", 1_000_000), ("test3", 1_000_000)],
                vec![0, 1, 0, 0, 1, 2],
                vec![
                    // Shard 0 is block produced / validated by all block producers & fisherman.
                    vec![0, 1, 0, 0, 1, 2],
                    vec![0, 1],
                    vec![0, 0],
                    vec![1, 2],
                    vec![0, 1]
                ],
                vec![],
                0,
                change_stake(vec![
                    ("test1", 1_000_000),
                    ("test2", 1_000_000),
                    ("test3", 1_000_000)
                ])
            )
        );
    }

    #[test]
    fn test_stake_validator() {
        let store = create_test_store();
        let config = config(1, 1, 2, 2, 0.9);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked)];
        let mut vm =
            ValidatorManager::new(config.clone(), validators.clone(), store.clone()).unwrap();

        let (h0, h1, h2, h3) = (hash(&vec![0]), hash(&vec![1]), hash(&vec![2]), hash(&vec![3]));
        vm.add_proposals(CryptoHash::default(), h0, 0, vec![], vec![], vec![])
            .unwrap()
            .commit()
            .unwrap();

        let expected0 = assignment(
            vec![("test1", amount_staked)],
            vec![0, 0],
            vec![vec![0, 0]],
            vec![],
            1,
            change_stake(vec![("test1", amount_staked)]),
        );
        let mut expected1 = expected0.clone();
        expected1.expected_epoch_start = 2;
        assert_eq!(vm.get_validators(h0).unwrap(), &expected0);
        assert_eq!(vm.get_validators(h1), Err(ValidatorError::EpochOutOfBounds));
        vm.finalize_epoch(&h0, &h0, &h1).unwrap();
        vm.add_proposals(h0, h1, 1, vec![stake("test2", amount_staked)], vec![], vec![])
            .unwrap()
            .commit()
            .unwrap();
        assert_eq!(vm.get_validators(h1).unwrap(), &expected1);
        assert_eq!(vm.get_epoch_offset(h2, 3), Err(ValidatorError::EpochOutOfBounds));
        vm.finalize_epoch(&h1, &h1, &h2).unwrap();
        vm.add_proposals(h1, h2, 2, vec![], vec![], vec![]).unwrap().commit().unwrap();
        let expected2 = assignment(
            vec![("test1", amount_staked), ("test2", amount_staked)],
            vec![0, 1],
            vec![vec![0, 1]],
            vec![],
            3,
            change_stake(vec![("test1", amount_staked), ("test2", amount_staked)]),
        );
        // test2 staked in epoch 1 and therefore should be included in epoch 3.
        assert_eq!(vm.get_validators(h2).unwrap(), &expected2);
        vm.finalize_epoch(&h2, &h2, &h3).unwrap();
        vm.add_proposals(h2, h3, 3, vec![], vec![], vec![]).unwrap().commit().unwrap();
        let mut expected3 = expected2.clone();
        expected3.expected_epoch_start = 4;
        // no validator change in the last epoch
        assert_eq!(vm.get_validators(h3).unwrap(), &expected3);

        // Start another validator manager from the same store to check that it saved the state.
        let mut vm2 = ValidatorManager::new(config, validators, store).unwrap();
        assert_eq!(vm2.get_validators(h3).unwrap(), &expected3);
    }

    /// Test handling forks across the epoch finalization.
    /// Fork with one BP in one chain and 2 BPs in another chain.
    ///     |  /- 1 ----|----4-----|----7---
    ///   x-|-0
    ///     |  \-----2--|-3-----5--|-6-----8
    /// In upper fork, only test1 left + new validator test4.
    /// In lower fork, test1 and test3 are left.
    #[test]
    fn test_fork_finalization() {
        let store = create_test_store();
        let config = config(3, 1, 3, 0, 0.9);
        let amount_staked = 1_000_000;
        let validators = vec![
            stake("test1", amount_staked),
            stake("test2", amount_staked),
            stake("test3", amount_staked),
        ];
        let mut vm =
            ValidatorManager::new(config.clone(), validators.clone(), store.clone()).unwrap();
        let (h0, h1, h2, h3, h4, h5, h6, h7, h8) = (
            hash(&vec![0]),
            hash(&vec![1]),
            hash(&vec![2]),
            hash(&vec![3]),
            hash(&vec![4]),
            hash(&vec![5]),
            hash(&vec![6]),
            hash(&vec![7]),
            hash(&vec![8]),
        );

        vm.add_proposals(CryptoHash::default(), h0, 0, vec![], vec![], vec![])
            .unwrap()
            .commit()
            .unwrap();
        // First epoch_length blocks are all epoch 0x0000.
        assert_eq!(vm.get_epoch_offset(h0, 1).unwrap().0, CryptoHash::default());
        assert_eq!(vm.get_epoch_offset(h0, 2).unwrap().0, CryptoHash::default());

        vm.add_proposals(h0, h1, 1, vec![stake("test4", amount_staked)], vec![], vec![])
            .unwrap()
            .commit()
            .unwrap();
        vm.add_proposals(h0, h2, 2, vec![], vec![], vec![]).unwrap().commit().unwrap();

        // Second epoch_length blocks are all epoch <genesis>.
        assert_eq!(vm.get_epoch_offset(h2, 3).unwrap().0, h0);
        assert_eq!(vm.get_epoch_offset(h1, 4).unwrap().0, h0);
        assert_eq!(vm.get_epoch_offset(h2, 5).unwrap().0, h0);

        vm.finalize_epoch(&h0, &h2, &h3).unwrap();
        vm.add_proposals(h2, h3, 3, vec![], vec![], vec![]).unwrap().commit().unwrap();

        // Block #5 with the real parent #3.
        assert_eq!(vm.get_epoch_offset(h3, 5).unwrap().0, h0);
        vm.finalize_epoch(&h0, &h1, &h4).unwrap();
        vm.add_proposals(h1, h4, 4, vec![], vec![], vec![]).unwrap().commit().unwrap();
        vm.add_proposals(h3, h5, 5, vec![], vec![], vec![]).unwrap().commit().unwrap();
        vm.finalize_epoch(&h3, &h5, &h6).unwrap();
        vm.add_proposals(h5, h6, 6, vec![], vec![], vec![]).unwrap().commit().unwrap();

        // Block #3 has been processed, so ready for next epoch defined by #3.
        assert_eq!(vm.get_epoch_offset(h5, 6).unwrap().0, h3);
        // For block #7, epoch is defined by block #4.
        assert_eq!(vm.get_epoch_offset(h4, 7).unwrap().0, h4);
        // For block 8, epoch is defined by block 2.
        assert_eq!(vm.get_epoch_offset(h6, 8).unwrap(), (h3, 2));

        // genesis validators
        assert_eq!(
            vm.get_validators(h0).unwrap(),
            &assignment(
                vec![("test1", amount_staked), ("test2", amount_staked), ("test3", amount_staked)],
                vec![2, 1, 0],
                vec![vec![2, 1, 0]],
                vec![],
                3,
                change_stake(vec![
                    ("test1", amount_staked),
                    ("test2", amount_staked),
                    ("test3", amount_staked)
                ])
            )
        );
        // Validators for the third epoch in the first fork. Does not have `test1` because it didn't produce
        // any blocks in the first epoch.
        assert_eq!(
            vm.get_validators(h4).unwrap(),
            &assignment(
                vec![("test4", amount_staked), ("test3", amount_staked), ("test2", amount_staked)],
                vec![2, 1, 0],
                vec![vec![2, 1, 0]],
                vec![],
                6,
                change_stake(vec![
                    ("test1", 0),
                    ("test2", amount_staked),
                    ("test3", amount_staked),
                    ("test4", amount_staked)
                ])
            )
        );
        // Validators for the fourth epoch in the second fork. Does not have `test2` because it didn't produce
        // any blocks in the first two epochs in the fork is thus kicked out.
        assert_eq!(
            vm.get_validators(h6).unwrap(),
            &assignment(
                vec![("test1", amount_staked), ("test3", amount_staked)],
                vec![0, 1, 0],
                vec![vec![0, 1, 0]],
                vec![],
                9,
                change_stake(vec![
                    ("test1", amount_staked),
                    ("test2", 0),
                    ("test3", amount_staked)
                ])
            )
        );

        // Finalize another epoch. `test1`, who produced block 0, is kicked out because it didn't produce
        // any more blocks in the next two epochs.
        vm.finalize_epoch(&h4, &h4, &h7).unwrap();
        vm.add_proposals(h4, h7, 7, vec![], vec![], vec![]).unwrap().commit().unwrap();
        assert_eq!(
            vm.get_validators(h7).unwrap(),
            &assignment(
                vec![("test4", amount_staked), ("test2", amount_staked)],
                vec![0, 1, 0],
                vec![vec![0, 1, 0]],
                vec![],
                9,
                change_stake(vec![
                    ("test1", 0),
                    ("test2", amount_staked),
                    ("test3", 0),
                    ("test4", amount_staked)
                ])
            )
        );

        vm.add_proposals(h6, h8, 8, vec![], vec![], vec![]).unwrap().commit().unwrap();

        assert_eq!(vm.get_epoch_offset(h7, 10).unwrap().0, h7);
        assert_eq!(vm.get_epoch_offset(h8, 11).unwrap().0, h6);

        // Add the same slot second time already after epoch is finalized should do nothing.
        vm.add_proposals(h0, h2, 2, vec![], vec![], vec![]).unwrap().commit().unwrap();
    }

    /// In the case where there is only one validator and the
    /// number of blocks produced by the validator is under the
    /// threshold for some given epoch, the validator should not
    /// be kicked out
    #[test]
    fn test_one_validator_kickout() {
        let store = create_test_store();
        let config = config(2, 1, 1, 0, 0.9);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked)];
        let mut vm =
            ValidatorManager::new(config.clone(), validators.clone(), store.clone()).unwrap();
        let (h0, h2, h4) = (hash(&vec![0]), hash(&vec![2]), hash(&vec![4]));
        // this validator only produces one block every epoch whereas they should have produced 2. However, since
        // this is the only validator left, we still keep them as validator.
        vm.add_proposals(CryptoHash::default(), h0, 0, vec![], vec![], vec![])
            .unwrap()
            .commit()
            .unwrap();
        vm.finalize_epoch(&h0, &h0, &h2).unwrap();
        vm.add_proposals(h0, h2, 2, vec![], vec![], vec![]).unwrap().commit().unwrap();
        vm.finalize_epoch(&h2, &h2, &h4).unwrap();
        vm.add_proposals(h2, h4, 4, vec![], vec![], vec![]).unwrap().commit().unwrap();
        assert_eq!(
            vm.get_validators(h2).unwrap(),
            &assignment(
                vec![("test1", amount_staked)],
                vec![0],
                vec![vec![0]],
                vec![],
                4,
                change_stake(vec![("test1", amount_staked)]),
            )
        );
    }

    #[test]
    fn test_fork_at_genesis() {
        let store = create_test_store();
        let config = config(2, 1, 2, 0, 0.9);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut vm =
            ValidatorManager::new(config.clone(), validators.clone(), store.clone()).unwrap();
        let (h0, h1, h2, h3) = (hash(&vec![0]), hash(&vec![1]), hash(&vec![2]), hash(&vec![3]));
        vm.add_proposals(CryptoHash::default(), h0, 0, vec![], vec![], vec![])
            .unwrap()
            .commit()
            .unwrap();
        vm.add_proposals(CryptoHash::default(), h1, 1, vec![], vec![], vec![])
            .unwrap()
            .commit()
            .unwrap();
        vm.finalize_epoch(&h0, &h0, &h2).unwrap();
        vm.add_proposals(h0, h2, 2, vec![], vec![], vec![]).unwrap().commit().unwrap();
        vm.finalize_epoch(&h1, &h1, &h3).unwrap();
        vm.add_proposals(h1, h3, 3, vec![], vec![], vec![]).unwrap().commit().unwrap();
        assert_eq!(
            vm.get_validators(h2).unwrap(),
            &assignment(
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                4,
                change_stake(vec![("test1", 0), ("test2", amount_staked)])
            )
        );
        assert_eq!(
            vm.get_validators(h3).unwrap(),
            &assignment(
                vec![("test1", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                4,
                change_stake(vec![("test1", amount_staked), ("test2", 0)])
            )
        );
    }

    #[test]
    fn test_validator_unstake() {
        let store = create_test_store();
        let config = config(2, 1, 2, 0, 0.9);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut vm =
            ValidatorManager::new(config.clone(), validators.clone(), store.clone()).unwrap();
        let (h0, h1, h2, h3, h4) = (hash(&[0]), hash(&[1]), hash(&[2]), hash(&[3]), hash(&[4]));
        vm.add_proposals(CryptoHash::default(), h0, 0, vec![], vec![], vec![])
            .unwrap()
            .commit()
            .unwrap();
        // test1 unstakes in epoch 1, and should be kicked out in epoch 3 (validators stored at h2).
        vm.add_proposals(h0, h1, 1, vec![stake("test1", 0)], vec![], vec![])
            .unwrap()
            .commit()
            .unwrap();
        vm.finalize_epoch(&h0, &h1, &h2).unwrap();
        vm.add_proposals(h1, h2, 2, vec![], vec![], vec![]).unwrap().commit().unwrap();
        assert_eq!(
            vm.get_validators(h2).unwrap(),
            &assignment(
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                4,
                change_stake(vec![("test1", 0), ("test2", amount_staked)])
            )
        );
        vm.add_proposals(h2, h3, 3, vec![], vec![], vec![]).unwrap().commit().unwrap();
        vm.finalize_epoch(&h2, &h3, &h4).unwrap();
        vm.add_proposals(h3, h4, 4, vec![], vec![], vec![]).unwrap().commit().unwrap();
        assert_eq!(
            vm.get_validators(h4).unwrap(),
            &assignment(
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                6,
                change_stake(vec![("test1", 0), ("test2", amount_staked)])
            )
        );
    }

    #[test]
    fn test_validator_change_of_stake() {
        let store = create_test_store();
        let config = config(2, 1, 2, 0, 0.9);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut vm =
            ValidatorManager::new(config.clone(), validators.clone(), store.clone()).unwrap();
        let (h0, h1, h2) = (hash(&vec![0]), hash(&vec![1]), hash(&vec![2]));
        vm.add_proposals(CryptoHash::default(), h0, 0, vec![], vec![], vec![])
            .unwrap()
            .commit()
            .unwrap();
        // test1 changes their stake to 10, thereby dropping below the threshold and will be kicked out in epoch 3.
        vm.add_proposals(h0, h1, 1, vec![stake("test1", 10)], vec![], vec![])
            .unwrap()
            .commit()
            .unwrap();
        vm.finalize_epoch(&h0, &h1, &h2).unwrap();
        vm.add_proposals(h1, h2, 2, vec![], vec![], vec![]).unwrap().commit().unwrap();
        assert_eq!(
            vm.get_validators(h2).unwrap(),
            &assignment(
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                4,
                change_stake(vec![("test1", 0), ("test2", amount_staked)])
            )
        )
    }

    #[test]
    fn test_get_block_proposer_info() {
        let store = create_test_store();
        let config = config(2, 1, 2, 0, 0.9);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut vm =
            ValidatorManager::new(config.clone(), validators.clone(), store.clone()).unwrap();
        let (h0, h1, h3, h4) = (hash(&vec![0]), hash(&vec![1]), hash(&vec![3]), hash(&vec![4]));
        vm.add_proposals(CryptoHash::default(), h0, 0, vec![], vec![], vec![])
            .unwrap()
            .commit()
            .unwrap();
        vm.add_proposals(h0, h1, 1, vec![], vec![], vec![]).unwrap().commit().unwrap();
        vm.finalize_epoch(&h0, &h1, &h3).unwrap();
        vm.add_proposals(h1, h3, 3, vec![], vec![], vec![]).unwrap().commit().unwrap();
        vm.finalize_epoch(&h3, &h3, &h4).unwrap();
        vm.add_proposals(h3, h4, 4, vec![], vec![], vec![]).unwrap().commit().unwrap();
        let validator_assignment = vm.get_validators(h0).unwrap().clone();
        let block_proposer_info = vm.get_block_proposer_info(h0, 3).unwrap();
        assert_eq!(
            block_proposer_info,
            stake(
                &validator_assignment.validators[validator_assignment.block_producers[1]]
                    .account_id,
                amount_staked
            )
        );
        let block_proposer_info = vm.get_block_proposer_info(h3, 4).unwrap();
        assert_eq!(
            block_proposer_info,
            stake(
                &validator_assignment.validators[validator_assignment.block_producers[0]]
                    .account_id,
                amount_staked
            )
        );
    }

    #[test]
    fn test_slashing() {
        let store = create_test_store();
        let config = config(2, 1, 2, 0, 0.9);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut vm =
            ValidatorManager::new(config.clone(), validators.clone(), store.clone()).unwrap();
        let (h0, h1, h2, h3, h4) = (hash(&[0]), hash(&[1]), hash(&[2]), hash(&[3]), hash(&[4]));
        vm.add_proposals(CryptoHash::default(), h0, 0, vec![], vec![], vec![])
            .unwrap()
            .commit()
            .unwrap();
        vm.add_proposals(h0, h1, 1, vec![], vec!["test1".to_string()], vec![])
            .unwrap()
            .commit()
            .unwrap();
        vm.finalize_epoch(&h0, &h1, &h2).unwrap();
        assert_eq!(
            vm.get_validators(h2).unwrap(),
            &assignment(
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                4,
                change_stake(vec![("test1", 0), ("test2", amount_staked)])
            )
        );
        vm.add_proposals(h1, h2, 2, vec![], vec![], vec![]).unwrap().commit().unwrap();
        let slashed1: Vec<_> =
            vm.get_slashed_validators(&h1).unwrap().clone().into_iter().collect();
        assert_eq!(slashed1, vec!["test1".to_string()]);
        let slashed2: Vec<_> =
            vm.get_slashed_validators(&h2).unwrap().clone().into_iter().collect();
        assert_eq!(slashed2, vec!["test1".to_string()]);
        vm.add_proposals(h2, h3, 3, vec![], vec![], vec![]).unwrap().commit().unwrap();
        vm.finalize_epoch(&h2, &h3, &h4).unwrap();
        assert_eq!(
            vm.get_validators(h4).unwrap(),
            &assignment(
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                6,
                change_stake(vec![("test1", 0), ("test2", amount_staked)])
            )
        );
    }
}
