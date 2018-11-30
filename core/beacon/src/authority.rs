use chain::BlockChain;
use primitives::signature::PublicKey;
use primitives::types::BlockId;
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
    /// Proposals in the given epoch.
    proposals: Vec<AuthorityProposal>,
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
            proposals: vec![],
            current_epoch: 0,
        };

        // TODO: cache authorities in the Storage, to not need to process the whole chain.
        let initial_authority =
            authority.proposals_to_authority(0, &authority.authority_config.initial_authorities);
        // Initial authorities operate for first two epochs.
        for (index, value) in initial_authority.iter() {
            authority.current.insert(*index, value.clone());
            authority
                .current
                .insert(*index + authority.authority_config.epoch_length, value.clone());
        }

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
        for authority_proposal in header.body.authority_proposal.iter() {
            self.proposals.push(authority_proposal.clone());
        }
        let epoch = header.body.index / self.authority_config.epoch_length;
        if epoch != self.current_epoch {
            let authorities = self.proposals_to_authority(0, &self.proposals);
            self.current.extend(authorities);
            // TODO: clean up current for old epochs.
        }
    }

    fn proposals_to_authority(
        &self,
        _seed: u64,
        proposals: &[AuthorityProposal],
    ) -> HashMap<u64, Vec<PublicKey>> {
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
        // assert_eq!(dup_proposals.len(), num_seats);
        for i in 0..self.authority_config.epoch_length {
            //            let start: usize = (i * self.authority_config.num_seats_per_slot).into();
            //            let authorities = dup_proposals[usize::from(i * self.authority_config.num_seats_per_slot)..usize::from((i + 1) * self.authority_config.num_seats_per_slot)];
            let authorities = dup_proposals.clone();
            result.insert(
                self.current_epoch * self.authority_config.epoch_length + i + 1,
                authorities,
            );
        }
        result
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
    use primitives::traits::Block;
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
        let authority_config = get_test_config(5, 2, 2);
        let initial_authorities: Vec<PublicKey> =
            authority_config.initial_authorities.iter().map(|a| a.public_key).collect();
        let bc = test_blockchain(0);
        let mut authority = Authority::new(authority_config, &bc);
        assert_eq!(authority.get_authorities(0).unwrap(), vec![]);
        assert_eq!(authority.get_authorities(1).unwrap(), initial_authorities);
        assert_eq!(authority.get_authorities(2).unwrap(), initial_authorities);
        assert_eq!(authority.get_authorities(3).unwrap(), initial_authorities);
        assert_eq!(authority.get_authorities(4).unwrap(), initial_authorities);
        assert!(authority.get_authorities(5).is_err());
        let header = BeaconBlockHeader::new(
            1,
            bc.genesis_hash,
            MerkleHash::default(),
            MerkleHash::default(),
            primitives::signature::DEFAULT_SIGNATURE,
            vec![true, false, true, true, true],
            vec![],
        );
        authority.process_block_header(&header);
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
