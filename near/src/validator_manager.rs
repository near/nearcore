use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::iter;
use std::sync::Arc;

use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use serde_derive::{Deserialize, Serialize};

use near_primitives::hash::CryptoHash;
use near_primitives::types::{Balance, BlockIndex, ShardId, ValidatorId, ValidatorStake};
use near_store::{Store, StoreUpdate, COL_PROPOSALS, COL_VALIDATORS};

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

/// Calculates new seat assignments based on current seat assignemnts and proposals.
fn proposals_to_assignments(
    epoch_config: ValidatorEpochConfig,
    current_assignments: &ValidatorAssignment,
    proposals: Vec<ValidatorStake>,
) -> Result<ValidatorAssignment, ValidatorError> {
    // TODO: kick out previous validators that didn't produce blocks.

    // Combine proposals with rollovers.
    let mut ordered_proposals = proposals;
    let mut indices = HashMap::new();
    for (i, p) in ordered_proposals.iter().enumerate() {
        indices.insert(p.account_id.clone(), i);
    }
    for r in current_assignments.validators.iter() {
        match indices.entry(r.account_id.clone()) {
            Entry::Occupied(e) => {
                let i = *e.get();
                ordered_proposals[i].amount += r.amount;
            }
            Entry::Vacant(e) => {
                e.insert(ordered_proposals.len());
                ordered_proposals.push(r.clone());
            }
        }
    }

    // Get the threshold given current number of seats and stakes.
    let num_fisherman_seats: usize = epoch_config.avg_fisherman_per_shard.iter().sum();
    let num_seats = epoch_config.num_block_producers + num_fisherman_seats;
    let stakes = ordered_proposals.iter().map(|p| p.amount).collect::<Vec<_>>();
    let threshold = find_threshold(&stakes, num_seats as u64)?;

    // Duplicate each proposal for number of seats it has.
    let mut dup_proposals = ordered_proposals
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

    // Block producers are aggregated group of first `num_block_producers` proposals.
    let mut block_producers = Vec::new();
    block_producers.resize(ordered_proposals.len(), 0);
    for i in 0..epoch_config.num_block_producers {
        block_producers[dup_proposals[i]] += 1;
    }

    // Collect proposals into block producer assignments.
    let mut chunk_producers: Vec<Vec<(ValidatorId, u64)>> = vec![];
    let mut last_index: usize = 0;
    for num_seats in epoch_config.block_producers_per_shard.iter() {
        let mut cp_to_index: HashMap<ValidatorId, usize> = HashMap::default();
        let mut cp: Vec<(ValidatorId, u64)> = vec![];
        for i in 0..*num_seats {
            let proposal_index = dup_proposals[(i + last_index) % epoch_config.num_block_producers];
            if let Some(j) = cp_to_index.get(&proposal_index) {
                cp[*j as usize].1 += 1;
            } else {
                cp_to_index.insert(proposal_index, cp.len());
                cp.push((proposal_index, 1));
            }
        }
        chunk_producers.push(cp);
        last_index = (last_index + num_seats) % epoch_config.num_block_producers;
    }

    // TODO: implement fishermen allocation.

    Ok(ValidatorAssignment {
        validators: ordered_proposals,
        block_producers,
        chunk_producers,
        fishermen: vec![],
    })
}

/// Epoch config, determines validator assignment for given epoch.
/// Can change from epoch to epoch depending on the sharding and other parameters, etc.
#[derive(Clone)]
pub struct ValidatorEpochConfig {
    /// Epoch length in blocks.
    pub epoch_length: BlockIndex,
    /// Source of randomnes.
    pub rng_seed: [u8; 32],
    /// Number of shards currently.
    pub num_shards: ShardId,
    /// Number of block producers.
    pub num_block_producers: ValidatorId,
    /// Number of block producers per each shard.
    pub block_producers_per_shard: Vec<ValidatorId>,
    /// Expected number of fisherman per each shard.
    pub avg_fisherman_per_shard: Vec<ValidatorId>,
}

/// Information about validator seat assignments.
#[derive(Default, Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct ValidatorAssignment {
    /// List of current validators.
    pub validators: Vec<ValidatorStake>,
    /// Weights for each of the validators responsible for block production.
    pub block_producers: Vec<u64>,
    /// Per each shard, ids and seats of validators that are responsible.
    pub chunk_producers: Vec<Vec<(ValidatorId, u64)>>,
    /// Weight of given validator used to determine how many shards they will validate.
    pub fishermen: Vec<(ValidatorId, u64)>,
}

/// Information per each index about validators.
#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct ValidatorIndexInfo {
    pub index: BlockIndex,
    pub prev_hash: CryptoHash,
    pub epoch_start_hash: CryptoHash,
    pub proposals: Vec<ValidatorStake>,
}

/// Manages current validators and validator proposals in the current epoch across different forks.
pub struct ValidatorManager {
    store: Arc<Store>,
    /// Current epoch config.
    /// TODO: must be dynamically changing over time, so there should be a way to change it.
    config: ValidatorEpochConfig,

    last_epoch: CryptoHash,
    epoch_validators: HashMap<CryptoHash, ValidatorAssignment>,
}

impl ValidatorManager {
    pub fn new(
        initial_epoch_config: ValidatorEpochConfig,
        initial_validators: Vec<ValidatorStake>,
        store: Arc<Store>,
    ) -> Result<Self, ValidatorError> {
        let mut epoch_validators = HashMap::default();
        let last_epoch = match store.get_ser(COL_PROPOSALS, LAST_EPOCH_KEY) {
            // TODO: check consistency of the db by querying it here?
            Ok(Some(value)) => value,
            Ok(None) => {
                let genesis_hash = CryptoHash::default();
                let initial_assigment = proposals_to_assignments(
                    initial_epoch_config.clone(),
                    &ValidatorAssignment::default(),
                    initial_validators,
                )?;

                let mut store_update = store.store_update();
                store_update.set_ser(
                    COL_PROPOSALS,
                    genesis_hash.as_ref(),
                    &ValidatorIndexInfo {
                        index: 0,
                        prev_hash: genesis_hash,
                        epoch_start_hash: genesis_hash,
                        proposals: vec![],
                    },
                )?;
                store_update.commit()?;

                epoch_validators.insert(genesis_hash, initial_assigment);
                genesis_hash
            }
            Err(err) => return Err(ValidatorError::Other(err.to_string())),
        };
        Ok(ValidatorManager { store, config: initial_epoch_config, last_epoch, epoch_validators })
    }

    fn get_index_info(&self, hash: CryptoHash) -> Result<ValidatorIndexInfo, ValidatorError> {
        self.store.get_ser(COL_PROPOSALS, hash.as_ref())?.ok_or(ValidatorError::MissingBlock(hash))
    }

    pub fn get_epoch_offset(
        &self,
        parent_hash: CryptoHash,
        index: BlockIndex,
    ) -> Result<(CryptoHash, BlockIndex), ValidatorError> {
        // TODO: handle that config epoch length can change over time from runtime.
        // First two epochs are special - they are referring to genesis block.
        if index < self.config.epoch_length * 2 + 1 {
            return Ok((CryptoHash::default(), index % self.config.epoch_length));
        }
        let parent_info =
            self.get_index_info(parent_hash).map_err(|_| ValidatorError::EpochOutOfBounds)?;
        let (epoch_start_index, epoch_start_parent_hash) = if parent_hash == parent_info.epoch_start_hash {
            (parent_info.index, parent_info.prev_hash)
        } else {
            let epoch_start_info = self.get_index_info(parent_info.epoch_start_hash)?;
            (epoch_start_info.index, epoch_start_info.prev_hash)
        };

        if epoch_start_index + self.config.epoch_length <= index {
            // If this is next epoch index, return parent's epoch hash and 0 as offset.
            Ok((parent_info.epoch_start_hash, 0))
        } else {
            // If index is within the same epoch as it's parent, return it's epoch parent and current offset from this epoch start.
            let prev_epoch_info = self.get_index_info(epoch_start_parent_hash)?;
            Ok((prev_epoch_info.epoch_start_hash, index - epoch_start_index))
        }
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
        match self.epoch_validators.get(&epoch_hash) {
            Some(validators) => Ok(validators),
            None => Err(ValidatorError::Other("Should not happened".to_string())),
        }
    }

    fn finalize_epoch(
        &mut self,
        epoch_hash: CryptoHash,
        last_hash: CryptoHash,
        new_hash: CryptoHash,
    ) -> Result<(), ValidatorError> {
        let mut proposals = vec![];
        let mut hash = last_hash;
        loop {
            let info = self.get_index_info(hash)?;
            if info.epoch_start_hash != epoch_hash || info.prev_hash == hash {
                break;
            }
            proposals.extend(info.proposals);
            hash = info.prev_hash;
        }
        let mut store_update = self.store.store_update();

        let assignment = proposals_to_assignments(
            self.config.clone(),
            self.get_validators(epoch_hash)?,
            proposals,
        )?;

        self.last_epoch = new_hash;
        store_update.set_ser(COL_VALIDATORS, new_hash.as_ref(), &assignment)?;
        store_update.set_ser(COL_PROPOSALS, LAST_EPOCH_KEY, &epoch_hash)?;
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
    ) -> Result<StoreUpdate, ValidatorError> {
        let mut store_update = self.store.store_update();
        if self.store.get(COL_PROPOSALS, current_hash.as_ref())?.is_none() {
            // TODO: keep track of size here to make sure we can't be spammed storing non interesting forks.
            let parent_info = self.get_index_info(prev_hash)?;
            let epoch_start_hash = if prev_hash == CryptoHash::default() {
                // If this is first block after genesis, we also save genesis validators for these epoch.
                let mut store_update = self.store.store_update();
                let genesis_validators = self.get_validators(CryptoHash::default())?;
                store_update.set_ser(COL_VALIDATORS, current_hash.as_ref(), genesis_validators)?;
                store_update.commit().map_err(|err| ValidatorError::Other(err.to_string()))?;

                current_hash
            } else {
                let epoch_start_info = self.get_index_info(parent_info.epoch_start_hash)?;
                if epoch_start_info.index + self.config.epoch_length <= index {
                    // This is first block of the next epoch, finalize it and return current hash and index as epoch hash/start.
                    self.finalize_epoch(parent_info.epoch_start_hash, prev_hash, current_hash)?;
                    current_hash
                } else {
                    // Otherwise, return parent's info.
                    parent_info.epoch_start_hash
                }
            };
            let info = ValidatorIndexInfo { index, epoch_start_hash, prev_hash, proposals };
            store_update.set_ser(COL_PROPOSALS, current_hash.as_ref(), &info)?;
        }
        Ok(store_update)
    }
}

#[cfg(test)]
mod test {
    use near_primitives::hash::hash;
    use near_primitives::test_utils::get_key_pair_from_seed;
    use near_store::test_utils::create_test_store;

    use super::*;

    fn stake(account_id: &str, amount: Balance) -> ValidatorStake {
        let (public_key, _) = get_key_pair_from_seed(account_id);
        ValidatorStake::new(account_id.to_string(), public_key, amount)
    }

    fn assignment(
        mut accounts: Vec<(&str, Balance)>,
        block_producers: Vec<u64>,
        chunk_producers: Vec<Vec<(usize, u64)>>,
        fishermen: Vec<(usize, u64)>,
    ) -> ValidatorAssignment {
        ValidatorAssignment {
            validators: accounts
                .drain(..)
                .map(|(account_id, amount)| ValidatorStake {
                    account_id: account_id.to_string(),
                    public_key: get_key_pair_from_seed(account_id).0,
                    amount,
                })
                .collect(),
            block_producers,
            chunk_producers,
            fishermen,
        }
    }

    fn config(
        epoch_length: BlockIndex,
        num_shards: ShardId,
        num_block_producers: usize,
        num_fisherman: usize,
    ) -> ValidatorEpochConfig {
        ValidatorEpochConfig {
            epoch_length,
            rng_seed: [0; 32],
            num_shards,
            num_block_producers,
            block_producers_per_shard: (0..num_shards).map(|_| num_block_producers).collect(),
            avg_fisherman_per_shard: (0..num_shards).map(|_| num_fisherman).collect(),
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
                config(2, 2, 1, 1),
                &ValidatorAssignment::default(),
                vec![stake("test1", 1_000_000)]
            )
            .unwrap(),
            assignment(
                vec![("test1", 1_000_000)],
                vec![1],
                vec![vec![(0, 1)], vec![(0, 1)]],
                vec![]
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
                    avg_fisherman_per_shard: vec![6, 2, 2, 2, 2]
                },
                &ValidatorAssignment::default(),
                vec![
                    stake("test1", 1_000_000),
                    stake("test2", 1_000_000),
                    stake("test3", 1_000_000)
                ]
            )
            .unwrap(),
            assignment(
                vec![("test1", 1_000_000), ("test2", 1_000_000), ("test3", 1_000_000)],
                vec![3, 2, 1],
                vec![
                    // Shard 0 is block produced / validated by all block producers & fisherman.
                    vec![(0, 3), (1, 2), (2, 1)],
                    vec![(0, 1), (1, 1)],
                    vec![(0, 2)],
                    vec![(1, 1), (2, 1)],
                    vec![(0, 1), (1, 1)]
                ],
                vec![]
            )
        );
    }

    #[test]
    fn test_stake_validator() {
        let store = create_test_store();
        let config = config(1, 1, 2, 2);
        let validators = vec![stake("test1", 1_000_000)];
        let mut vm =
            ValidatorManager::new(config.clone(), validators.clone(), store.clone()).unwrap();
        let expected0 = assignment(vec![("test1", 1_000_000)], vec![2], vec![vec![(0, 2)]], vec![]);
        let (h0, h1, h2, h3) =
            (CryptoHash::default(), hash(&vec![1]), hash(&vec![2]), hash(&vec![3]));
        assert_eq!(vm.get_validators(vm.get_epoch_offset(h0, 1).unwrap().0).unwrap(), &expected0);
        assert_eq!(vm.get_validators(vm.get_epoch_offset(h1, 2).unwrap().0).unwrap(), &expected0);
        assert_eq!(vm.get_epoch_offset(h2, 3), Err(ValidatorError::EpochOutOfBounds));
        assert_eq!(vm.get_validators(h1), Err(ValidatorError::EpochOutOfBounds));
        vm.add_proposals(h0, h1, 1, vec![stake("test2", 1_000_000)]).unwrap().commit().unwrap();
        vm.add_proposals(h1, h2, 2, vec![]).unwrap().commit().unwrap();
        let expected = assignment(
            vec![("test2", 1_000_000), ("test1", 1_000_000)],
            vec![1, 1],
            vec![vec![(0, 1), (1, 1)]],
            vec![],
        );
        assert_eq!(vm.get_validators(vm.get_epoch_offset(h2, 3).unwrap().0).unwrap(), &expected);
        vm.add_proposals(h2, h3, 3, vec![]).unwrap().commit().unwrap();
        assert_eq!(vm.get_validators(vm.get_epoch_offset(h3, 4).unwrap().0).unwrap(), &expected);

        // Start another validator manager from the same store to check that it saved the state.
        let mut vm2 = ValidatorManager::new(config, validators, store).unwrap();
        assert_eq!(vm2.get_validators(vm.get_epoch_offset(h3, 4).unwrap().0).unwrap(), &expected);
    }

    /// Test handling forks across the epoch finalization.
    /// Fork with one BP in one chain and 2 BPs in another chain.
    ///     /- 1 --------|-4---------|-7---
    ///   0
    ///     \-----2---3--|----5---6--|----8
    /// In upper fork, only test1 left + new validator test4.
    /// In lower fork, test2 and test3 are left.
    #[test]
    fn test_fork_finalization() {
        let store = create_test_store();
        let config = config(3, 1, 3, 0);
        let validators =
            vec![stake("test1", 1_000_000), stake("test2", 1_000_000), stake("test3", 1_000_000)];
        let mut vm =
            ValidatorManager::new(config.clone(), validators.clone(), store.clone()).unwrap();
        let (h0, h1, h2, h3, h4, h5, h6, h7, h8) = (
            CryptoHash::default(),
            hash(&vec![1]),
            hash(&vec![2]),
            hash(&vec![3]),
            hash(&vec![4]),
            hash(&vec![5]),
            hash(&vec![6]),
            hash(&vec![7]),
            hash(&vec![8]),
        );

        // First 2 * epoch_length blocks are all epoch "0".
        for i in 0..6 {
            assert_eq!(vm.get_epoch_offset(h0, i).unwrap().0, h0);
        }

        vm.add_proposals(h0, h1, 1, vec![stake("test4", 1_000_000)]).unwrap().commit().unwrap();
        vm.add_proposals(h0, h2, 2, vec![]).unwrap().commit().unwrap();
        vm.add_proposals(h2, h3, 3, vec![]).unwrap().commit().unwrap();
        vm.add_proposals(h1, h4, 4, vec![]).unwrap().commit().unwrap();
        vm.add_proposals(h3, h5, 5, vec![]).unwrap().commit().unwrap();
        vm.add_proposals(h5, h6, 6, vec![]).unwrap().commit().unwrap();

        // For block 7, epoch is defined by block 1.
        assert_eq!(vm.get_epoch_offset(h4, 7).unwrap(), (h4, 0));
        // For block 8, epoch is defined by block 2.
        assert_eq!(vm.get_epoch_offset(h6, 8).unwrap(), (h5, 0));

        assert_eq!(
            vm.get_validators(h0).unwrap(),
            &assignment(
                vec![("test1", 1_000_000), ("test2", 1_000_000), ("test3", 1_000_000)],
                vec![1, 1, 1],
                vec![vec![(2, 1), (1, 1), (0, 1)]],
                vec![]
            )
        );
        assert_eq!(
            vm.get_validators(h4).unwrap(),
            // TODO: kick out 2, 3 from here.
            &assignment(
                vec![
                    ("test4", 1_000_000),
                    ("test1", 1_000_000),
                    ("test2", 1_000_000),
                    ("test3", 1_000_000)
                ],
                vec![1, 1, 0, 1],
                vec![vec![(0, 1), (3, 1), (1, 1)]],
                vec![]
            )
        );
        assert_eq!(
            vm.get_validators(h5).unwrap(),
            &assignment(
                vec![("test1", 1_000_000), ("test2", 1_000_000), ("test3", 1_000_000)],
                vec![1, 1, 1],
                vec![vec![(2, 1), (1, 1), (0, 1)]],
                vec![]
            )
        );

        // Finalize another epoch.
        vm.add_proposals(h4, h7, 7, vec![]).unwrap().commit().unwrap();
        vm.add_proposals(h6, h8, 8, vec![]).unwrap().commit().unwrap();

        assert_eq!(vm.get_epoch_offset(h7, 10).unwrap().0, h7);
        assert_eq!(vm.get_epoch_offset(h8, 11).unwrap().0, h8);

        // Add the same slot second time already after epoch is finalized should do nothing.
        vm.add_proposals(h0, h2, 2, vec![]).unwrap().commit().unwrap();
    }
}
