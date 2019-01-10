use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;

use rand::{Rng, SeedableRng, StdRng};
use std::iter;
use std::mem;

use chain::{BlockChain, SignedBlock};
use primitives::hash::CryptoHash;
use primitives::signature::PublicKey;
use primitives::types::{AccountId, AuthorityMask, BlockId};
use types::{SignedBeaconBlock, SignedBeaconBlockHeader};

/// Stores authority and its stake.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuthorityStake {
    /// Account that stakes money.
    pub account_id: AccountId,
    /// Public key of the proposed authority.
    pub public_key: PublicKey,
    /// Stake / weight of the authority.
    pub amount: u64,
}

impl PartialEq for AuthorityStake {
    fn eq(&self, other: &Self) -> bool {
        self.account_id == other.account_id
        && self.public_key == other.public_key
    }
}

impl Eq for AuthorityStake {}

/// Configure the authority rotation.
pub struct AuthorityConfig {
    /// List of initial proposals at genesis block.
    pub initial_proposals: Vec<AuthorityStake>,
    /// Authority epoch length.
    pub epoch_length: u64,
    /// Number of seats per slot.
    pub num_seats_per_slot: u64,
}

type Epoch = u64;
type Slot = u64;

fn find_threshold(stakes: &[u64], num_seats: u64) -> Result<u64, String> {
    let stakes_sum: u64 = stakes.iter().sum();
    if stakes_sum < num_seats {
        return Err(format!(
            "Total stake {} must be higher than the number of seats {}",
            stakes_sum, num_seats
        ));
    }
    let (mut left, mut right) = (1u64, stakes_sum + 1);
    'outer: loop {
        if left == right - 1 {
            break Ok(left);
        }
        let mid = (left + right) / 2;
        let mut current_sum = 0u64;
        for item in stakes.iter() {
            current_sum += item / mid;
            if current_sum >= num_seats {
                left = mid;
                continue 'outer;
            }
        }
        right = mid;
    }
}

/// Keeps track and selects authorities for given blockchain.
/// To participate in epoch E an authority must submit a proposal in epoch E-2.
/// Those authorities that submitted proposals in epoch E-2 and those that participated in epoch E-2
/// are used in authority selection for epoch E. For each authority the stake used in selection is
/// computed as: <amount staked in E-2> - <amount not used in E-2> + <proposed amount in E-2>.
pub struct Authority {
    /// Authority configuration.
    authority_config: AuthorityConfig,
    /// Proposals per slot in which they occur.
    proposals: HashMap<Slot, Vec<AuthorityStake>>,
    /// Participation of authorities per slot in which they have happened.
    participation: HashMap<Slot, AuthorityMask>,
    /// Records the blocks that it processed for the given blocks.
    processed_blocks: HashMap<Epoch, HashSet<Slot>>,

    // The following is a derived information which we do not want to recompute.
    /// Computed thresholds for each epoch.
    thresholds: HashMap<Epoch, u64>,
    /// Authorities that were accepted for the given slots.
    accepted_authorities: HashMap<Slot, Vec<AuthorityStake>>,
}

impl Authority {
    #[inline]
    fn slot_to_epoch(&self, slot: Slot) -> Epoch {
        // The genesis block has slot 0 and is not a part of any epoch. So slots are shifted by 1
        // with respect to epochs.
        (slot - 1) / self.authority_config.epoch_length
    }

    #[inline]
    fn epoch_to_slots(&self, epoch: Epoch) -> impl Iterator<Item = Slot> {
        // Because of the genesis block that has slot 0 and is not in any epoch,
        // slots are shifted by 1.
        epoch * self.authority_config.epoch_length + 1
            ..=(epoch + 1) * self.authority_config.epoch_length  // Without ..= it needs + 1.
    }

    /// Initializes authorities from the config and the past blocks in the beaconchain.
    pub fn new(
        authority_config: AuthorityConfig,
        blockchain: &BlockChain<SignedBeaconBlock>,
    ) -> Self {
        // TODO: cache authorities in the Storage, to not need to process the whole chain.
        let mut result = Self {
            authority_config,
            proposals: HashMap::new(),
            participation: HashMap::new(),
            processed_blocks: HashMap::new(),
            thresholds: HashMap::new(),
            accepted_authorities: HashMap::new(),
        };
        // Initial authorities operate for the first two epochs.
        let (accepted_authorities, threshold) = result.compute_threshold_accepted(
            &CryptoHash::default(),
            result.authority_config.initial_proposals.to_vec(),
            vec![],
        );
        let mut slot = 0;
        for epoch in 0..=1 {
            result.thresholds.insert(epoch, threshold);
            for slot_auth in &accepted_authorities {
                slot += 1;
                result.accepted_authorities.insert(slot, slot_auth.to_vec());
            }
        }
        // Catch up with the blockchain. Note, the last block is allowed to progress while we
        // are iterating.
        // TODO: Take care of the fork being changed while we are iterating.
        let mut index = 1;
        while index <= blockchain.best_block().header().body.index {
            let header = blockchain
                .get_header(&BlockId::Number(index))
                .expect("Blockchain missing past block");
            result.process_block_header(&header);
            index += 1;
        }
        result
    }

    /// Computes accepted authorities and the threshold from the given proposals.
    /// Args:
    ///     seed: for shuffling;
    ///     proposals: new proposals that were made in that epoch;
    ///     rollovers: adjusted proposals that should be rolled over from that epoch.
    fn compute_threshold_accepted(
        &self,
        seed: &CryptoHash,
        proposals: Vec<AuthorityStake>,
        mut rollovers: Vec<AuthorityStake>,
    ) -> (Vec<Vec<AuthorityStake>>, u64) {
        // Combine proposals with rollovers.
        let mut ordered_proposals = proposals;
        let mut indices = HashMap::new();
        for (i, p) in ordered_proposals.iter().enumerate() {
            indices.insert(p.account_id.clone(), i);
        }
        for r in rollovers.drain(..) {
            match indices.entry(r.account_id.clone()) {
                Entry::Occupied(mut e) => {
                    let i = *e.get();
                    ordered_proposals[i].amount += r.amount;
                }
                Entry::Vacant(mut e) => {
                    e.insert(ordered_proposals.len());
                    ordered_proposals.push(r);
                }
            }
        }

        // Get the threshold.
        let num_seats =
            self.authority_config.num_seats_per_slot * self.authority_config.epoch_length;
        let stakes: Vec<_> = ordered_proposals.iter().map(|p| p.amount).collect();
        let threshold = find_threshold(&stakes, num_seats)
            .expect("Threshold is not found for given proposals");
        // Duplicate proposals per each seat that they get.
        let mut dup_proposals: Vec<_> = ordered_proposals.iter()
            .flat_map(|p| {
                iter::repeat(p).cloned().take((p.amount / threshold) as usize)
            })
            .collect();
        assert!(
            dup_proposals.len() >= num_seats as usize,
            "Number of selected seats {} < total number of seats {}",
            dup_proposals.len(),
            num_seats
        );
        // Shuffle duplicate proposals.
        let seed: Vec<usize> = seed.as_ref().iter().map(|i| *i as usize).collect();
        let mut rng: StdRng = SeedableRng::from_seed(seed.as_ref());
        rng.shuffle(&mut dup_proposals);

        // Distribute proposals into slots.
        let mut result = vec![];
        let mut curr = vec![];
        for proposal in dup_proposals.drain(..).take(num_seats as usize) {
            curr.push(AuthorityStake {
                account_id: proposal.account_id,
                public_key: proposal.public_key,
                amount: threshold,
            });
            if curr.len() == self.authority_config.num_seats_per_slot as usize {
                result.push(mem::replace(&mut curr, vec![]));
            }
        }
        (result, threshold)
    }

    /// Computes accepted authorities for the given epoch.
    fn compute_accepted_authorities(&mut self, epoch: Epoch) {
        // Get threshold used for epoch-2. There might be no threshold if we have some missing
        // blocks in epoch-4.
        if let Some(threshold) = { self.thresholds.get(&(epoch - 2)).cloned() } {
            // First, compute the rollovers. Using Vec for rollovers to enforce determinism.
            let mut ordered_rollovers: Vec<AuthorityStake> = vec![];
            let mut indices = HashMap::new();
            let mut penalties = HashMap::new();
            for s in self.epoch_to_slots(epoch - 2) {
                let accepted = self.accepted_authorities[&s].iter();
                let participation = self.participation[&s].iter();
                for (acc, participated) in accepted.zip(participation) {
                    if *participated {
                        match indices.entry(acc.account_id.clone()) {
                            Entry::Occupied(mut e) => {
                                let el: &mut AuthorityStake = &mut ordered_rollovers[*e.get()];
                                el.amount += threshold;
                            }
                            Entry::Vacant(mut e) => {
                                e.insert(ordered_rollovers.len());
                                ordered_rollovers.push(acc.clone());
                            }
                        }
                    } else {
                        match penalties.entry(acc.account_id.clone()) {
                            Entry::Occupied(mut e) => {
                                *e.get_mut() += threshold;
                            }
                            Entry::Vacant(mut e) => {
                                e.insert(threshold);
                            }
                        }
                    }
                }
            }
            // Apply penalties.
            let rollovers: Vec<_> = ordered_rollovers.drain(..).filter(|r| {
                if let Some(p) = penalties.get(&r.account_id) {
                    if *p > r.amount {
                        return false;
                    }
                }
                true
            }).collect();

            // Second, use the proposals and the rollovers.
            // TODO(#308): Use proper seed.
            let (mut accepted_authorities, new_threshold) = {
                let proposals = self
                    .epoch_to_slots(epoch - 2)
                    .flat_map(|s| self.proposals[&s].iter().cloned());
                self.compute_threshold_accepted(&CryptoHash::default(), proposals.collect(), rollovers)
            };
            self.thresholds.insert(epoch, new_threshold);
            let slots: Vec<_> = self.epoch_to_slots(epoch).collect();
            self.accepted_authorities
                .extend(slots.iter().cloned().zip(accepted_authorities.drain(..)));
        }
    }

    /// Record proposals and participation from the given block.
    pub fn process_block_header(&mut self, header: &SignedBeaconBlockHeader) {
        // Skip genesis block or if this block was already recorded.
        let slot = header.body.index;
        if slot > 0 && !self.proposals.contains_key(&slot) {
            self.proposals.insert(slot, header.body.authority_proposal.to_vec());
            self.participation.insert(slot, header.authority_mask.to_vec());

            // Update the tracker of processed slots.
            let epoch = self.slot_to_epoch(slot);
            let all_slots_processed = {
                let mut processed_slots =
                    self.processed_blocks.entry(epoch).or_insert_with(HashSet::new);
                processed_slots.insert(slot);
                processed_slots.len() == self.authority_config.epoch_length as usize
            };
            // Check if we have processed all slots from the given epoch.
            if all_slots_processed {
                // Compute accepted authorities for epoch+2.
                self.compute_accepted_authorities(epoch + 2);
            }
        }
    }

    /// Returns authorities for given block number.
    pub fn get_authorities(&self, slot: Slot) -> Result<Vec<AuthorityStake>, String> {
        if slot == 0 {
            // Genesis block has no authorities.
            Ok(vec![])
        } else if let Some(result) = self.accepted_authorities.get(&slot) {
            Ok(result.to_vec())
        } else {
            let epoch = self.slot_to_epoch(slot);
            Err(format!(
                "Authorities for slot {} (epoch {}) are not available, because for epoch {} only {} out of {} blocks are known",
                slot,
                epoch,
                epoch as i64 -2,
                self.processed_blocks.get(&slot).map(|m| m.len()).unwrap_or(0),
                self.authority_config.epoch_length,
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use chain::{SignedBlock, SignedHeader};
    use primitives::hash::CryptoHash;
    use primitives::signature::get_keypair;
    use storage::test_utils::MemoryStorage;

    use super::*;

    fn get_test_config(
        num_authorities: u32,
        epoch_length: u64,
        num_seats_per_slot: u64,
    ) -> AuthorityConfig {
        let mut initial_authorities = vec![];
        for i in 0..num_authorities {
            let (public_key, _) = get_keypair();
            initial_authorities.push(AuthorityStake { account_id: i.to_string(), public_key, amount: 100 });
        }
        AuthorityConfig { initial_proposals: initial_authorities, epoch_length, num_seats_per_slot }
    }

    fn test_blockchain(num_blocks: u64) -> BlockChain<SignedBeaconBlock> {
        let storage = Arc::new(MemoryStorage::default());
        let mut last_block =
            SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
        let bc = BlockChain::new(last_block.clone(), storage);
        for i in 1..num_blocks {
            let block =
                SignedBeaconBlock::new(i, last_block.block_hash(), vec![], CryptoHash::default());
            bc.insert_block(block.clone());
            last_block = block;
        }
        bc
    }

    #[test]
    fn test_single_authority() {
        let authority_config = get_test_config(1, 10, 5);
        let initial_authorities = authority_config.initial_proposals.to_vec();
        let bc = test_blockchain(0);
        let mut authority = Authority::new(authority_config, &bc);
        let mut prev_hash = bc.genesis_hash;
        let num_seats = authority.get_authorities(1).unwrap().iter().map(|x| x.account_id == initial_authorities[0].account_id).count();
        for i in 1..11 {
            let block = SignedBeaconBlock::new(i, prev_hash, vec![], CryptoHash::default());
            let mut header = block.header();
            header.authority_mask = (0..num_seats).map(|_| true).collect();
            authority.process_block_header(&header);
            prev_hash = header.block_hash();
        }
    }

    #[test]
    fn test_authority_genesis() {
        let authority_config = get_test_config(4, 2, 2);
        let initial_authorities = authority_config.initial_proposals.to_vec();
        let bc = test_blockchain(0);
        let mut authority = Authority::new(authority_config, &bc);
        assert_eq!(authority.get_authorities(0).unwrap(), vec![]);
        assert_eq!(
            authority.get_authorities(1).unwrap(),
            vec![initial_authorities[0].clone(), initial_authorities[3].clone()]
        );
        assert_eq!(
            authority.get_authorities(2).unwrap(),
            vec![initial_authorities[2].clone(), initial_authorities[1].clone()]
        );
        assert_eq!(
            authority.get_authorities(3).unwrap(),
            vec![initial_authorities[0].clone(), initial_authorities[3].clone()]
        );
        assert_eq!(
            authority.get_authorities(4).unwrap(),
            vec![initial_authorities[2].clone(), initial_authorities[1].clone()]
        );
        assert!(authority.get_authorities(5).is_err());
        let block1 = SignedBeaconBlock::new(1, bc.genesis_hash, vec![], CryptoHash::default());
        let mut header1 = block1.header();
        // Authority #1 didn't show up.
        header1.authority_mask = vec![true, false];
        let block2 = SignedBeaconBlock::new(2, header1.block_hash(), vec![], CryptoHash::default());
        let mut header2 = block2.header();
        header2.authority_mask = vec![true, true];
        authority.process_block_header(&header1);
        authority.process_block_header(&header2);
        assert_eq!(
            authority.get_authorities(5).unwrap(),
            vec![initial_authorities[2].clone(), initial_authorities[0].clone()]
        );
        assert_eq!(
            authority.get_authorities(6).unwrap(),
            vec![initial_authorities[0].clone(), initial_authorities[1].clone()]
        );
    }

    #[test]
    fn test_find_threshold() {
        assert_eq!(find_threshold(&[1000000, 1000000, 10], 10).unwrap(), 200000);
        assert_eq!(find_threshold(&[1000000000, 10], 10).unwrap(), 100000000);
        assert_eq!(find_threshold(&[1000000000], 1000000000).unwrap(), 1);
        assert_eq!(find_threshold(&[1000, 1, 1, 1, 1, 1, 1, 1, 1, 1], 1).unwrap(), 1000);
        assert!(find_threshold(&[1, 1, 2], 100).is_err());
    }
}
