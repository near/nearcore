use chain::BlockChain;
use primitives::hash::CryptoHash;
use primitives::signature::PublicKey;
use primitives::types::BlockId;
use rand::{Rng, SeedableRng, StdRng};
use std::collections::HashMap;
use types::{AuthorityProposal, BeaconBlock, BeaconBlockHeader};

/// Configure the authority rotation.
pub struct AuthorityConfig {
    /// List of initial authorities at genesis block.
    pub initial_authorities: Vec<AuthorityProposal>,
    /// Authority epoch length.
    pub epoch_length: u64,
    /// Number of seats per slot.
    pub num_seats_per_slot: u64,
}

pub struct Authority {
    /// Authority configuation.
    authority_config: AuthorityConfig,
    /// Current epoch that is cached.
    current_epoch: u64,
    /// Cache of current authorities for given index.
    current: HashMap<u64, Vec<PublicKey>>,
    /// Cache of current threshold.
    current_threshold: HashMap<u64, u64>,
    /// Proposals in the given epoch.
    proposals: HashMap<String, i64>,
    /// Proposals per epoch.
    accepted_proposals: HashMap<u64, Vec<AuthorityProposal>>,
}

/// Finds threshold for given proposals and number of seats.
fn find_threshold(proposed: &[u64], num_seats: u64) -> Result<u64, String> {
    let sum = proposed.iter().sum();
    for item in proposed.iter() {
        if *item < num_seats {
            return Err(format!(
                "Proposed {} must be higher then number of seats {}",
                item, num_seats
            ));
        }
    }
    let (mut left, mut right, mut result) = (2, sum, 1);
    while left <= right {
        let mid = (left + right) / 2;
        let (mut current_sum, mut ok) = (0, false);
        for item in proposed.iter() {
            current_sum += item / mid;
            if current_sum >= num_seats {
                ok = true;
                break;
            }
        }
        if !ok {
            right = mid - 1;
        } else {
            result = mid;
            left = mid + 1;
        }
    }
    Ok(result)
}

/// Keeps track and selects authorities for given blockchain.
impl Authority {
    // TODO: figure out a way to generalize Authority selection process, by providing AuthoritySelector.

    /// Builds authority for given valid blockchain.
    /// Starting from best block, figure out current authorities.
    pub fn new(authority_config: AuthorityConfig, blockchain: &BlockChain<BeaconBlock>) -> Self {
        let mut authority = Authority {
            authority_config,
            current: HashMap::default(),
            current_threshold: HashMap::default(),
            proposals: HashMap::default(),
            current_epoch: 0,
            accepted_proposals: HashMap::default(),
        };

        // TODO: cache authorities in the Storage, to not need to process the whole chain.
        let (initial_authority, threshold) = authority.proposals_to_authority(
            &CryptoHash::default(),
            &authority.authority_config.initial_authorities,
            0,
        );
        // Initial authorities operate for first two epochs.
        for (index, value) in initial_authority.iter() {
            authority.current.insert(*index, value.clone());
            authority
                .current
                .insert(*index + authority.authority_config.epoch_length, value.clone());
        }
        authority.current_threshold.insert(0, threshold);
        authority.current_threshold.insert(1, threshold);
        authority
            .accepted_proposals
            .insert(0, authority.authority_config.initial_authorities.clone());
        authority
            .accepted_proposals
            .insert(1, authority.authority_config.initial_authorities.clone());

        let last_index = blockchain.best_block().header.body.index;
        for index in 1..last_index {
            // TODO: handle if block is not found.
            if let Some(header) = blockchain.get_header(&BlockId::Number(index)) {
                authority.process_block_header(&header);
            }
        }

        authority
    }

    pub fn process_block_header(&mut self, header: &BeaconBlockHeader) {
        // Always skip genesis block.
        if header.body.index == 0 {
            return;
        }
        for authority_proposal in header.body.authority_proposal.iter() {
            self.proposals.insert(
                authority_proposal.public_key.to_string(),
                authority_proposal.amount as i64,
            );
        }
        let header_authorities =
            self.get_authorities(header.body.index).expect("Processing block has unexpected index");
        for (i, participated) in header.authority_mask.iter().enumerate() {
            if !participated {
                let threshold = *self
                    .current_threshold
                    .get(&self.current_epoch)
                    .expect("Missing threshold for current epoch") as i64;
                *self.proposals.entry(header_authorities[i].to_string()).or_insert(0) -= threshold;
            }
        }
        let next_epoch = header.body.index / self.authority_config.epoch_length;
        if next_epoch != self.current_epoch {
            let mut new_proposals: Vec<AuthorityProposal> = self
                .proposals
                .iter()
                .filter_map(|(public_key, amount)| {
                    if *amount > 0 {
                        Some(AuthorityProposal {
                            public_key: public_key.into(),
                            amount: *amount as u64,
                        })
                    } else {
                        None
                    }
                }).collect();
            for proposal in self
                .accepted_proposals
                .get(&self.current_epoch)
                .expect("Missing proposals for current epoch")
                .iter()
            {
                let amount = *self.proposals.get(&proposal.public_key.to_string()).unwrap_or(&0);
                if (amount < 0 && proposal.amount > (-amount) as u64) || amount == 0 {
                    new_proposals.push(proposal.clone());
                }
            }
            let (authorities, threshold) =
                self.proposals_to_authority(&CryptoHash::default(), &new_proposals, 2);
            self.current.extend(authorities);
            self.current_threshold.insert(next_epoch, threshold);
            self.current_epoch = next_epoch;
            self.proposals = HashMap::default();
            self.accepted_proposals.insert(next_epoch, new_proposals);
            // TODO: clean up current for old epochs.
        }
    }

    fn proposals_to_authority(
        &self,
        seed: &CryptoHash,
        proposals: &[AuthorityProposal],
        epoch_offset: u64,
    ) -> (HashMap<u64, Vec<PublicKey>>, u64) {
        let num_seats =
            self.authority_config.num_seats_per_slot * self.authority_config.epoch_length;
        let mut result = HashMap::default();
        let proposal_amounts: Vec<u64> = proposals.iter().map(|p| p.amount).collect();
        let threshold = find_threshold(proposal_amounts.as_slice(), num_seats)
            .expect("Threshold is not found for given proposals.");

        let mut dup_proposals = vec![];
        for item in proposals {
            if item.amount >= threshold {
                for _ in 0..item.amount / threshold {
                    dup_proposals.push(item.public_key);
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

        // Distribute proposals into slots.
        for i in 0..self.authority_config.epoch_length {
            let start = (i * self.authority_config.num_seats_per_slot) as usize;
            let end = ((i + 1) * self.authority_config.num_seats_per_slot) as usize;
            result.insert(
                (self.current_epoch + epoch_offset) * self.authority_config.epoch_length + i + 1,
                dup_proposals[start..end].to_vec(),
            );
        }
        (result, threshold)
    }

    /// Returns authorities for given block number.
    pub fn get_authorities(&self, index: u64) -> Result<Vec<PublicKey>, String> {
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
    use super::*;

    use primitives::hash::CryptoHash;
    use primitives::signature::get_keypair;
    use primitives::traits::{Block, Header};
    use primitives::types::MerkleHash;
    use std::sync::Arc;
    use storage::test_utils::MemoryStorage;

    fn get_test_config(
        num_authorities: u32,
        epoch_length: u64,
        num_seats_per_slot: u64,
    ) -> AuthorityConfig {
        let mut initial_authorities = vec![];
        for _ in 0..num_authorities {
            let (public_key, _) = get_keypair();
            initial_authorities.push(AuthorityProposal { public_key, amount: 100 });
        }
        AuthorityConfig { initial_authorities, epoch_length, num_seats_per_slot }
    }

    fn test_blockchain(num_blocks: u64) -> BlockChain<BeaconBlock> {
        let storage = Arc::new(MemoryStorage::default());
        let mut last_block =
            BeaconBlock::new(0, CryptoHash::default(), MerkleHash::default(), vec![]);
        let bc = BlockChain::new(last_block.clone(), storage);
        for i in 1..num_blocks {
            let block = BeaconBlock::new(i, last_block.hash(), MerkleHash::default(), vec![]);
            bc.insert_block(block.clone());
            last_block = block;
        }
        bc
    }

    #[test]
    fn test_authority_genesis() {
        let authority_config = get_test_config(4, 2, 2);
        let initial_authorities: Vec<PublicKey> =
            authority_config.initial_authorities.iter().map(|a| a.public_key).collect();
        let bc = test_blockchain(0);
        let mut authority = Authority::new(authority_config, &bc);
        assert_eq!(authority.get_authorities(0).unwrap(), vec![]);
        assert_eq!(
            authority.get_authorities(1).unwrap(),
            vec![initial_authorities[0], initial_authorities[3]]
        );
        assert_eq!(
            authority.get_authorities(2).unwrap(),
            vec![initial_authorities[2], initial_authorities[1]]
        );
        assert_eq!(
            authority.get_authorities(3).unwrap(),
            vec![initial_authorities[0], initial_authorities[3]]
        );
        assert_eq!(
            authority.get_authorities(4).unwrap(),
            vec![initial_authorities[2], initial_authorities[1]]
        );
        assert!(authority.get_authorities(5).is_err());
        let mut header1 = BeaconBlockHeader::empty(1, bc.genesis_hash, MerkleHash::default());
        // Authority #1 didn't show up.
        header1.authority_mask = vec![true, false];
        let mut header2 = BeaconBlockHeader::empty(2, header1.hash(), MerkleHash::default());
        header2.authority_mask = vec![true, true];
        authority.process_block_header(&header1);
        authority.process_block_header(&header2);
        assert_eq!(
            authority.get_authorities(5).unwrap(),
            vec![initial_authorities[1], initial_authorities[0]]
        );
        assert_eq!(
            authority.get_authorities(6).unwrap(),
            vec![initial_authorities[0], initial_authorities[2]]
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
