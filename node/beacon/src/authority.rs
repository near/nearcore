use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::iter;
use std::mem;
use std::sync::{Arc, RwLock};

use log::Level::Debug;
use rand::{rngs::StdRng, SeedableRng, seq::SliceRandom};

use configs::AuthorityConfig;
use primitives::beacon::SignedBeaconBlockHeader;
use primitives::hash::CryptoHash;
use primitives::types::{AuthorityStake, BlockId, Epoch, Slot};
use storage::BeaconChainStorage;

use crate::beacon_chain::BeaconBlockChain;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

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
    /// beacon chain storage
    storage: Arc<RwLock<BeaconChainStorage>>,
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
            ..=(epoch + 1) * self.authority_config.epoch_length // Without ..= it needs + 1.
    }

    /// Initializes authorities from the config and the past blocks in the beaconchain.
    pub fn new(
        authority_config: AuthorityConfig,
        blockchain: &BeaconBlockChain,
        storage: Arc<RwLock<BeaconChainStorage>>
    ) -> Self {
        // TODO: cache authorities in the Storage, to not need to process the whole chain.
        let mut result = Self {
            authority_config,
            storage,
        };
        if result.storage.write().expect(POISONED_LOCK_ERR).is_authority_empty() {
            // Initial authorities operate for the first two epochs.
            let (accepted_authorities, threshold) = result.compute_threshold_accepted(
                &CryptoHash::default(),
                result.authority_config.initial_proposals.to_vec(),
                vec![],
            );
            let mut slot = 0;
            {
                let mut storage = result.storage.write().expect(POISONED_LOCK_ERR);
                for epoch in 0..=1 {
                    storage.set_threshold(epoch, threshold);
                    for slot_auth in &accepted_authorities {
                        slot += 1;
                        storage.set_accepted_authorities(slot, slot_auth.to_vec());
                    }
                }
            }
            
            // Catch up with the blockchain. Note, the last block is allowed to progress while we
            // are iterating.
            // TODO: Take care of the fork being changed while we are iterating.
            let mut index = 1;
            let mut last_progress = 101;
            while index <= blockchain.best_header().body.index {
                if log_enabled!(target: "client", Debug) {
                    let best_block_index = blockchain.best_header().body.index;
                    let progress = index * 100 / best_block_index;
                    if progress != last_progress {
                        debug!(target: "client", "Processing blocks {} out of {}", index, best_block_index);
                        last_progress = progress;
                    }
                }
                let header = blockchain
                    .get_header(&BlockId::Number(index))
                    .expect("Blockchain missing past block");
                result.process_block_header(&header);
                index += 1;
            }
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
                Entry::Occupied(e) => {
                    let i = *e.get();
                    ordered_proposals[i].amount += r.amount;
                }
                Entry::Vacant(e) => {
                    e.insert(ordered_proposals.len());
                    ordered_proposals.push(r);
                }
            }
        }

        // Get the threshold.
        let num_seats =
            self.authority_config.num_seats_per_slot * self.authority_config.epoch_length;
        let stakes: Vec<_> = ordered_proposals.iter().map(|p| p.amount).collect();
        let threshold =
            find_threshold(&stakes, num_seats).expect("Threshold is not found for given proposals");
        // Duplicate proposals per each seat that they get.
        let mut dup_proposals: Vec<_> = ordered_proposals
            .iter()
            .flat_map(|p| iter::repeat(p).cloned().take((p.amount / threshold) as usize))
            .collect();
        assert!(
            dup_proposals.len() >= num_seats as usize,
            "Number of selected seats {} < total number of seats {}",
            dup_proposals.len(),
            num_seats
        );
        // Shuffle duplicate proposals.
        let mut rng_seed = [0; 32];
        rng_seed.copy_from_slice(seed.as_ref());
        let mut rng: StdRng = SeedableRng::from_seed(rng_seed);
        dup_proposals.shuffle(&mut rng);

        // Distribute proposals into slots.
        let mut result = vec![];
        let mut curr = vec![];
        for proposal in dup_proposals.drain(..).take(num_seats as usize) {
            curr.push(AuthorityStake {
                account_id: proposal.account_id,
                public_key: proposal.public_key,
                bls_public_key: proposal.bls_public_key,
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
        let mut storage = self.storage.write().expect(POISONED_LOCK_ERR);
        if let Some(threshold) = storage.get_threshold(epoch - 2).cloned() {
            // First, compute the rollovers. Using Vec for rollovers to enforce determinism.
            let mut ordered_rollovers: Vec<AuthorityStake> = vec![];
            let mut indices = HashMap::new();
            let mut penalties = HashMap::new();
            for s in self.epoch_to_slots(epoch - 2) {
                let accepted = storage
                    .get_accepted_authorities(s)
                    .cloned()
                    .unwrap_or_else(Vec::new)
                    .into_iter();
                let participation = storage
                    .get_participation(s)
                    .map(|x| x.iter())
                    .unwrap_or_else(|| [].iter());
                    
                for (acc, participated) in accepted.zip(participation) {
                    if *participated {
                        match indices.entry(acc.account_id.clone()) {
                            Entry::Occupied(e) => {
                                let el: &mut AuthorityStake = &mut ordered_rollovers[*e.get()];
                                el.amount += threshold;
                            }
                            Entry::Vacant(e) => {
                                e.insert(ordered_rollovers.len());
                                ordered_rollovers.push(acc.clone());
                            }
                        }
                    } else {
                        match penalties.entry(acc.account_id.clone()) {
                            Entry::Occupied(mut e) => {
                                *e.get_mut() += threshold;
                            }
                            Entry::Vacant(e) => {
                                e.insert(threshold);
                            }
                        }
                    }
                }
            }
            // Apply penalties.
            let rollovers: Vec<_> = ordered_rollovers
                .drain(..)
                .filter(|r| {
                    if let Some(p) = penalties.get(&r.account_id) {
                        if *p > r.amount {
                            return false;
                        }
                    }
                    true
                })
                .collect();

            // Second, use the proposals and the rollovers.
            // TODO(#308): Use proper seed.
            let (mut accepted_authorities, new_threshold) = {
                let mut proposals = vec![];
                for s in self.epoch_to_slots(epoch - 2) {
                    let new_proposals = storage.get_proposal(s).cloned().unwrap_or_else(|| vec![]);
                    proposals.extend(new_proposals);
                }
                self.compute_threshold_accepted(
                    &CryptoHash::default(),
                    proposals,
                    rollovers,
                )
            };
            storage.set_threshold(epoch, new_threshold);
            let slots: Vec<_> = self.epoch_to_slots(epoch).collect();
            storage.extend_accepted_authorities(
                slots.iter().cloned().zip(accepted_authorities.drain(..)).collect()
            );
        }
    }

    /// Record proposals and participation from the given block.
    pub fn process_block_header(&mut self, header: &SignedBeaconBlockHeader) {
        // Skip genesis block or if this block was already recorded.
        let slot = header.body.index;
        if slot > 0 && self.storage.write().expect(POISONED_LOCK_ERR).get_proposal(slot).is_none() {
            let (all_slots_processed, epoch) = {
                let mut storage = self.storage.write().expect(POISONED_LOCK_ERR);
                storage.set_proposal(slot, header.body.authority_proposal.to_vec());
                storage.set_participation(slot, header.signature.authority_mask.to_vec());

                // Update the tracker of processed slots.
                let epoch = self.slot_to_epoch(slot);
                let mut processed_slots = 
                    if let Some(slots) = storage.get_processed_blocks(epoch) {
                        slots.clone()
                    } else {
                        HashSet::new()
                    };
                processed_slots.insert(slot);
                let len = processed_slots.len();
                storage.set_processed_blocks(epoch, processed_slots);
                (len == self.authority_config.epoch_length as usize, epoch)
            };
            
            // Check if we have processed all slots from the given epoch.
            if all_slots_processed {
                // Compute accepted authorities for epoch+2.
                self.compute_accepted_authorities(epoch + 2);
                // TODO: figure out the best way to do pruning
                //self.storage.write().expect(POISONED_LOCK_ERR).prune_authority_storage(
                //    &|k| self.slot_to_epoch(k) >= epoch,
                //    &|k| k >= epoch,
                //);
            }
        }
    }

    /// Returns authorities for given block number.
    pub fn get_authorities(&self, slot: Slot) -> Result<Vec<AuthorityStake>, String> {
        let mut storage = self.storage.write().expect(POISONED_LOCK_ERR);
        if slot == 0 {
            // Genesis block has no authorities.
            Ok(vec![])
        } else if let Some(result) = storage.get_accepted_authorities(slot) {
            Ok(result.to_vec())
        } else {
            let epoch = self.slot_to_epoch(slot);
            Err(format!(
                "Authorities for slot {} (epoch {}) are not available, because for epoch {} only {} out of {} blocks are known",
                slot,
                epoch,
                epoch as i64 -2,
                storage.get_processed_blocks(slot).map(HashSet::len).unwrap_or(0),
                self.authority_config.epoch_length,
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use configs::ChainSpec;
    use primitives::aggregate_signature::BlsSecretKey;
    use primitives::beacon::SignedBeaconBlock;
    use primitives::block_traits::{SignedBlock, SignedHeader};
    use primitives::hash::CryptoHash;
    use primitives::signature::get_key_pair;
    use storage::test_utils::create_beacon_shard_storages;
    use chain::test_utils::get_blockchain_storage;

    use crate::beacon_chain::BeaconClient;

    use super::*;

    fn get_test_chainspec(
        num_authorities: u32,
        beacon_chain_epoch_length: u64,
        beacon_chain_num_seats_per_slot: u64,
    ) -> ChainSpec {
        let mut initial_authorities = vec![];
        for i in 0..num_authorities {
            let (public_key, _) = get_key_pair();
            let bls_public_key = BlsSecretKey::generate().get_public_key();
            initial_authorities.push((
                i.to_string(),
                public_key.to_readable(),
                bls_public_key.to_readable(),
                100,
            ));
        }
        ChainSpec {
            accounts: Default::default(),
            genesis_wasm: Default::default(),
            initial_authorities,
            beacon_chain_epoch_length,
            beacon_chain_num_seats_per_slot,
            boot_nodes: Default::default(),
        }
    }

    fn test_blockchain(num_blocks: u64, chain_spec: &ChainSpec) -> BeaconClient {
        let storage = create_beacon_shard_storages().0;
        let mut last_block =
            SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
        let bc = BeaconClient::new(last_block.clone(), chain_spec, storage);
        for i in 1..num_blocks {
            let block =
                SignedBeaconBlock::new(i, last_block.block_hash(), vec![], CryptoHash::default());
            bc.chain.insert_block(block.clone());
            last_block = block;
        }
        bc
    }

    #[test]
    fn test_single_authority() {
        let chain_spec = get_test_chainspec(1, 10, 5);
        let bc = test_blockchain(0, &chain_spec);
        let config = bc.authority.read().unwrap().authority_config.clone();
        let initial_authorities = config.initial_proposals.to_vec();
        let mut authority = bc.authority.write().unwrap();
        let mut prev_hash = bc.chain.best_hash();
        let num_seats = authority
            .get_authorities(1)
            .unwrap()
            .iter()
            .map(|x| x.account_id == initial_authorities[0].account_id)
            .count();
        for i in 1..11 {
            let block = SignedBeaconBlock::new(i, prev_hash, vec![], CryptoHash::default());
            let mut header = block.header();
            header.signature.authority_mask = (0..num_seats).map(|_| true).collect();
            authority.process_block_header(&header);
            prev_hash = header.block_hash();
        }
    }

    #[test]
    fn test_authority_genesis() {
        let chain_spec = get_test_chainspec(4, 2, 2);
        let bc = test_blockchain(0, &chain_spec);
        let config = bc.authority.read().unwrap().authority_config.clone();
        let initial_authorities = config.initial_proposals.to_vec();
        let mut authority = bc.authority.write().unwrap();
        assert_eq!(authority.get_authorities(0).unwrap(), vec![]);
        assert_eq!(
            authority.get_authorities(1).unwrap(),
            vec![initial_authorities[0].clone(), initial_authorities[3].clone()]
        );
        assert_eq!(
            authority.get_authorities(2).unwrap(),
            vec![initial_authorities[1].clone(), initial_authorities[2].clone()]
        );
        assert_eq!(
            authority.get_authorities(3).unwrap(),
            vec![initial_authorities[0].clone(), initial_authorities[3].clone()]
        );
        assert_eq!(
            authority.get_authorities(4).unwrap(),
            vec![initial_authorities[1].clone(), initial_authorities[2].clone()]
        );
        assert!(authority.get_authorities(5).is_err());
        let block1 = SignedBeaconBlock::new(1, bc.chain.genesis_hash(), vec![], CryptoHash::default());
        let mut header1 = block1.header();
        // Authority #1 didn't show up.
        header1.signature.authority_mask = vec![true, false];
        let block2 = SignedBeaconBlock::new(2, header1.block_hash(), vec![], CryptoHash::default());
        let mut header2 = block2.header();
        header2.signature.authority_mask = vec![true, true];
        authority.process_block_header(&header1);
        authority.process_block_header(&header2);
        assert_eq!(
            authority.get_authorities(5).unwrap(),
            vec![initial_authorities[0].clone(), initial_authorities[2].clone()]
        );
        assert_eq!(
            authority.get_authorities(6).unwrap(),
            vec![initial_authorities[2].clone(), initial_authorities[1].clone()]
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

    #[test]
    fn test_write_to_storage() {
        let chain_spec = get_test_chainspec(4, 2, 2);
        let bc = test_blockchain(0, &chain_spec);
        let mut authority = bc.authority.write().unwrap();
        let block1 = SignedBeaconBlock::new(1, bc.chain.genesis_hash(), vec![], CryptoHash::default());
        let mut header1 = block1.header();
        header1.signature.authority_mask = vec![true, true];
        let block2 = SignedBeaconBlock::new(2, header1.block_hash(), vec![], CryptoHash::default());
        let mut header2 = block2.header();
        header2.signature.authority_mask = vec![true, true];
        authority.process_block_header(&header1);
        authority.process_block_header(&header2);
        let next_authorities = authority.get_authorities(3);
        assert!(next_authorities.is_ok());

        let genesis_block = SignedBeaconBlock::new(
            0, CryptoHash::default(), vec![], CryptoHash::default()
        );

        let bc1 = BeaconClient::new(
            genesis_block,
            &chain_spec,
            get_blockchain_storage(bc.chain)
        );
        let authority = bc1.authority.write().unwrap();
        assert_eq!(authority.get_authorities(3), next_authorities);
    }
}
