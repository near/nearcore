use std::collections::HashMap;

use rand::{Rng, SeedableRng, StdRng};
use std::mem;

use chain::{BlockChain, SignedBlock};
use primitives::hash::CryptoHash;
use primitives::signature::PublicKey;
use primitives::types::{AccountId, BlockId, AuthorityMask};
use types::{SignedBeaconBlock, SignedBeaconBlockHeader};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct AuthorityProposal {
    /// Account that stakes money.
    pub account_id: AccountId,
    /// Public key of the proposed authority.
    pub public_key: PublicKey,
    /// Stake / weight of the authority.
    pub amount: u64,
}

/// Configure the authority rotation.
pub struct AuthorityConfig {
    /// List of initial proposals at genesis block.
    pub initial_proposals: Vec<AuthorityProposal>,
    /// Authority epoch length.
    pub epoch_length: u64,
    /// Number of seats per slot.
    pub num_seats_per_slot: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectedAuthority {
    pub account_id: AccountId,
    pub public_key: PublicKey,
}

#[derive(Clone)]
struct RecordedProposal {
    pub public_key: PublicKey,
    /// Stake is either positive for proposal or negative for kicked out accounts.
    pub stake: i64,
}

type Epoch = u64;
type Slot = u64;

pub struct Authority {
    /// Authority configuration.
    authority_config: AuthorityConfig,
    /// Proposals per slot in which they occur.
    proposals: HashMap<Slot, Vec<AuthorityProposal>>,
    /// Participation of authorities per slot in which they have happened.
    participation: HashMap<Slot, AuthorityMask>,
    /// Computed thresholds for each epoch.
    thresholds: HashMap<Epoch, u64>,
    /// Authorities that were accepted for the given slots.
    accepted_authorities: HashMap<Slot, Vec<AuthorityProposal>>,
}

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
impl Authority {
    #[inline]
    fn slot_to_epoch(&self, slot: Slot) -> Epoch {
        // The genesis block has slot 0 and is not a part of any epoch. So slots are shifted by 1
        // with respect to epochs.
        (slot - 1) % self.authority_config.epoch_length
    }

    // TODO: figure out a way to generalize Authority selection process, by providing AuthoritySelector.

    /// Builds authority for given valid blockchain.
    /// Starting from best block, figure out current authorities.
    pub fn new(
        authority_config: AuthorityConfig,
        blockchain: &BlockChain<SignedBeaconBlock>,
    ) -> Self {
        let mut authority = Authority {
            authority_config,
            current: HashMap::default(),
            thresholds: HashMap::default(),
            proposals: HashMap::default(),
            current_epoch: 0,
            accepted_proposals: HashMap::default(),
        };

        // TODO: cache authorities in the Storage, to not need to process the whole chain.
        let (initial_authorities, threshold) = authority.get_epoch_authorities(
            &CryptoHash::default(),
            &authority.authority_config.initial_proposals,
        );
        // Initial authorities operate for first two epochs.
        for (index, value) in initial_authority.iter() {
            authority.current.insert(*index, value.clone());
            authority
                .current
                .insert(*index + authority.authority_config.epoch_length, value.clone());
        }
        authority.thresholds.insert(0, threshold);
        authority.thresholds.insert(1, threshold);
        authority
            .accepted_proposals
            .insert(0, authority.authority_config.initial_proposals.clone());
        authority
            .accepted_proposals
            .insert(1, authority.authority_config.initial_proposals.clone());

        let last_index = blockchain.best_block().header().body.index;
        for index in 1..last_index {
            // TODO: handle if block is not found.
            if let Some(header) = blockchain.get_header(&BlockId::Number(index)) {
                authority.process_block_header(&header);
            }
        }

        authority
    }

    pub fn process_block_header(&mut self, header: &SignedBeaconBlockHeader) {
        // Always skip genesis block.
        if header.body.index == 0 {
            return;
        }
        for authority_proposal in header.body.authority_proposal.iter() {
            self.proposals.insert(
                authority_proposal.account_id,
                RecordedProposal {
                    public_key: authority_proposal.public_key,
                    stake: authority_proposal.amount as i64,
                },
            );
        }
        let header_authorities =
            self.get_authorities(header.body.index).expect("Processing block has unexpected index");
        for (i, participated) in header.authority_mask.iter().enumerate() {
            if !participated {
                let threshold = *self
                    .thresholds
                    .get(&self.current_epoch)
                    .expect("Missing threshold for current epoch")
                    as i64;
                let recorded_proposal =
                    self.proposals.entry(header_authorities[i].account_id).or_insert(
                        RecordedProposal { public_key: header_authorities[i].public_key, stake: 0 },
                    );
                recorded_proposal.stake -= threshold;
            }
        }
        // What is the epoch of the next block.
        let next_epoch = ((header.body.index + 1) - 1) / self.authority_config.epoch_length;
        // Check if epoch of the next block is not current epoch, recalculate authorities for
        // current_epoch + 2.
        if next_epoch != self.current_epoch {
            let mut new_proposals: Vec<AuthorityProposal> = self
                .proposals
                .iter()
                .filter_map(|(account_id, recorded_proposal)| {
                    if recorded_proposal.stake > 0 {
                        Some(AuthorityProposal {
                            account_id: *account_id,
                            public_key: recorded_proposal.public_key,
                            amount: recorded_proposal.stake as u64,
                        })
                    } else {
                        None
                    }
                })
                .collect();
            for proposal in self
                .accepted_proposals
                .get(&self.current_epoch)
                .expect("Missing proposals for current epoch")
                .iter()
            {
                let amount = self
                    .proposals
                    .get(&proposal.account_id)
                    .unwrap_or(&RecordedProposal { public_key: proposal.public_key, stake: 0 })
                    .stake;
                if (amount < 0 && proposal.amount > (-amount) as u64) || amount == 0 {
                    new_proposals.push(proposal.clone());
                }
            }
            let (authorities, threshold) =
                self.proposals_to_authority(&CryptoHash::default(), &new_proposals, 2);
            self.current.extend(authorities);
            self.thresholds.insert(next_epoch, threshold);
            self.current_epoch = next_epoch;
            self.proposals = HashMap::default();
            self.accepted_proposals.insert(next_epoch, new_proposals);
            // TODO: clean up current for old epochs.
        }
    }

    /// Computes vector of authorities for each slot in an epoch.
    fn get_epoch_authorities(
        &self,
        seed: &CryptoHash,
        proposals: &[AuthorityProposal],
    ) -> (Vec<Vec<SelectedAuthority>>, u64) {
        let num_seats =
            self.authority_config.num_seats_per_slot * self.authority_config.epoch_length;
        let proposal_amounts: Vec<_> = proposals.iter().map(|p| p.amount).collect();
        let threshold = find_threshold(proposal_amounts.as_slice(), num_seats)
            .expect("Threshold is not found for given proposals.");

        let mut dup_proposals = vec![];
        for item in proposals {
            if item.amount >= threshold {
                for _ in 0..item.amount / threshold {
                    dup_proposals.push(SelectedAuthority {
                        account_id: item.account_id,
                        public_key: item.public_key,
                    });
                }
            }
        }
        assert!(
            dup_proposals.len() >= num_seats as usize,
            "Number of selected seats {} < total number of seats {}",
            dup_proposals.len(),
            num_seats
        );

        // Shuffle proposals.
        let seed: Vec<usize> = seed.as_ref().iter().map(|i| *i as usize).collect();
        let mut rng: StdRng = SeedableRng::from_seed(seed.as_ref());
        rng.shuffle(&mut dup_proposals);

        // Truncate excessive seats.
        dup_proposals.truncate(num_seats as usize);
        let mut result = vec![];
        let mut curr = vec![];
        for proposal in dup_proposals {
            curr.push(proposal);
            if curr.len() == self.authority_config.num_seats_per_slot {
                result.push(mem::replace(&mut curr, vec![]));
            }
        }
        (result, threshold)
    }

    /// Returns authorities for given block number.
    pub fn get_authorities(&self, index: u64) -> Result<Vec<SelectedAuthority>, String> {
        if index == 0 {
            // Genesis block has no authorities.
            Ok(vec![])
        } else if self.current.contains_key(&index) {
            Ok(self.current[&index].clone())
        } else {
            Err(format!(
                "Authority for index {} is not found, current epoch {} has indices [{}, {}]",
                index,
                self.current_epoch,
                self.current_epoch * self.authority_config.epoch_length,
                (self.current_epoch + 1) * self.authority_config.epoch_length
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use chain::{SignedBlock, SignedHeader};
    use primitives::hash::{hash_struct, CryptoHash};
    use primitives::signature::get_keypair;
    use storage::test_utils::MemoryStorage;

    use super::*;

    fn get_test_config(
        num_authorities: u32,
        epoch_length: u64,
        num_seats_per_slot: u64,
    ) -> AuthorityConfig {
        let mut initial_authorities = vec![];
        for _ in 0..num_authorities {
            let (public_key, _) = get_keypair();
            let account_id = hash_struct(&public_key);
            initial_authorities.push(AuthorityProposal { account_id, public_key, amount: 100 });
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
    fn test_authority_genesis() {
        let authority_config = get_test_config(4, 2, 2);
        let initial_authorities: Vec<SelectedAuthority> = authority_config
            .initial_proposals
            .iter()
            .map(|a| SelectedAuthority { account_id: a.account_id, public_key: a.public_key })
            .collect();
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
            vec![initial_authorities[1].clone(), initial_authorities[0].clone()]
        );
        assert_eq!(
            authority.get_authorities(6).unwrap(),
            vec![initial_authorities[0].clone(), initial_authorities[2].clone()]
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
