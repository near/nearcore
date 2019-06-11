use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::iter;
use std::sync::Arc;

use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use serde_derive::{Deserialize, Serialize};

use near_primitives::hash::CryptoHash;
use near_primitives::types::{Balance, Epoch, MerkleHash, ShardId, ValidatorId, ValidatorStake};
use near_primitives::utils::index_to_bytes;
use near_store::{Store, COL_VALIDATORS};

#[derive(Eq, PartialEq)]
pub enum ValidatorError {
    /// Error calculating threshold from given stakes for given number of seats.
    /// Only should happened if calling code doesn't check for integer value of stake > number of seats.
    ThresholdError(Balance, u64),
    /// Requesting validators for an epoch that wasn't computed yet.
    EpochOutOfBounds,
    /// Number of selected seats doesn't match requested.
    SelectedSeatsMismatch(u64, ValidatorId),
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
            ValidatorError::Other(err) => write!(f, "Other: {}", err),
        }
    }
}

impl fmt::Display for ValidatorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
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

const LAST_EPOCH_KEY: &[u8] = b"LAST_EPOCH";

/// Epoch config, determines validator assignment for given epoch.
/// Can change from epoch to epoch depending on the sharding and other parameters, etc.
#[derive(Clone)]
pub struct ValidatorEpochConfig {
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

/// Manages current validators and validator proposals in the current epoch across different forks.
pub struct ValidatorManager {
    store: Arc<Store>,
    last_epoch: Epoch,
    proposals: HashMap<CryptoHash, Vec<ValidatorStake>>,
    epoch_validators: HashMap<Epoch, ValidatorAssignment>,
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

impl ValidatorManager {
    pub fn new(
        initial_epoch_config: ValidatorEpochConfig,
        initial_validators: Vec<ValidatorStake>,
        store: Arc<Store>,
    ) -> Result<Self, ValidatorError> {
        let proposals = HashMap::default();
        let mut epoch_validators = HashMap::default();
        let last_epoch = match store.get_ser(COL_VALIDATORS, LAST_EPOCH_KEY) {
            // TODO: check consistency of the db by querying it here?
            Ok(Some(value)) => value,
            Ok(None) => {
                let initial_assigment = proposals_to_assignments(
                    initial_epoch_config,
                    &ValidatorAssignment::default(),
                    initial_validators,
                )?;
                epoch_validators.insert(0, initial_assigment.clone());
                epoch_validators.insert(1, initial_assigment);
                0
            }
            Err(err) => return Err(ValidatorError::Other(err.to_string())),
        };
        Ok(ValidatorManager { store, last_epoch, proposals, epoch_validators })
    }

    #[inline]
    pub fn last_epoch(&self) -> Epoch {
        self.last_epoch
    }

    pub fn get_validators(&mut self, epoch: Epoch) -> Result<&ValidatorAssignment, ValidatorError> {
        if !self.epoch_validators.contains_key(&epoch) {
            match self
                .store
                .get_ser(COL_VALIDATORS, &index_to_bytes(epoch))
                .map_err(|err| ValidatorError::Other(err.to_string()))?
            {
                Some(validators) => self.epoch_validators.insert(epoch, validators),
                None => return Err(ValidatorError::EpochOutOfBounds),
            };
        }
        match self.epoch_validators.get(&epoch) {
            Some(validators) => Ok(validators),
            None => Err(ValidatorError::Other("Should not happened".to_string())),
        }
    }

    /// Add proposals from given header into validators.
    pub fn add_proposals(
        &mut self,
        prev_state_root: MerkleHash,
        new_state_root: MerkleHash,
        mut proposals: Vec<ValidatorStake>,
    ) {
        // TODO: keep track of size here to make sure we can't be spammed storing all forks.
        let mut current_proposals = self.proposals.remove(&prev_state_root).unwrap_or(vec![]);
        current_proposals.append(&mut proposals);
        self.proposals.insert(new_state_root, current_proposals);
    }

    /// Call when `epoch` is finished to compute `epoch` + 2 validators.
    pub fn finalize_epoch(
        &mut self,
        epoch: Epoch,
        next_epoch_config: ValidatorEpochConfig,
        state_root: MerkleHash,
    ) -> Result<(), ValidatorError> {
        // If there are any proposals in given branch.
        if let Some(proposals) = self.proposals.remove(&state_root) {
            let mut store_update = self.store.store_update();
            let assignment = proposals_to_assignments(
                next_epoch_config,
                self.get_validators(epoch)?,
                proposals,
            )?;
            self.last_epoch = epoch + 1;
            store_update.set_ser(COL_VALIDATORS, &index_to_bytes(epoch + 2), &assignment)?;
            store_update.set_ser(COL_VALIDATORS, LAST_EPOCH_KEY, &self.last_epoch)?;
            store_update.commit().map_err(|err| ValidatorError::Other(err.to_string()))?;
        }
        self.proposals.clear();
        Ok(())
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
        num_shards: ShardId,
        num_block_producers: usize,
        num_fisherman: usize,
    ) -> ValidatorEpochConfig {
        ValidatorEpochConfig {
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
                config(2, 1, 1),
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
        let config = config(1, 2, 2);
        let validators = vec![stake("test1", 1_000_000)];
        let mut am =
            ValidatorManager::new(config.clone(), validators.clone(), store.clone()).unwrap();
        let (sr1, sr2) = (hash(&vec![1]), hash(&vec![2]));
        am.add_proposals(sr1, sr2, vec![stake("test2", 1_000_000)]);
        let expected0 = assignment(vec![("test1", 1_000_000)], vec![2], vec![vec![(0, 2)]], vec![]);
        assert_eq!(am.get_validators(0).unwrap(), &expected0);
        assert_eq!(am.get_validators(1).unwrap(), &expected0);
        assert_eq!(am.get_validators(2), Err(ValidatorError::EpochOutOfBounds));
        am.finalize_epoch(0, config.clone(), sr2).unwrap();
        assert_eq!(am.last_epoch(), 1);
        assert_eq!(
            am.get_validators(2).unwrap(),
            &assignment(
                vec![("test2", 1_000_000), ("test1", 1_000_000)],
                vec![1, 1],
                vec![vec![(0, 1), (1, 1)]],
                vec![]
            )
        );

        // Start another validator manager from the same store to check that it saved the state.
        let mut am2 = ValidatorManager::new(config, validators, store).unwrap();
        assert_eq!(am2.last_epoch(), 1);
        assert_eq!(
            am2.get_validators(2).unwrap(),
            &assignment(
                vec![("test2", 1_000_000), ("test1", 1_000_000)],
                vec![1, 1],
                vec![vec![(0, 1), (1, 1)]],
                vec![]
            )
        );
    }

    /// Test handling forks across the epoch finalization.
    #[test]
    #[ignore]
    fn test_fork_finalization() {
        // TODO: figure out which fork to finalize.
    }
}
