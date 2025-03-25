use borsh::BorshDeserialize;
use near_chain::{BlockHeader, Doomslug, DoomslugThresholdMode};
use near_chain_configs::GenesisConfig;
use near_crypto::Signature;
use near_epoch_manager::proposals_to_epoch_info;
use near_primitives::block::{Approval, ApprovalInner};
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::{AllEpochConfig, EpochConfig, AGGREGATOR_KEY};
use near_primitives::hash::CryptoHash;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, ApprovalStake, BlockHeight, EpochId, ProtocolVersion};
use near_primitives::version::ProtocolFeature;
use near_store::{DBCol, Store};
use nearcore::NearConfig;
use std::collections::{HashMap, HashSet};

#[derive(clap::Args)]
pub struct EpochSyncTestCommand {}

impl EpochSyncTestCommand {
    pub fn run(&self, config: NearConfig, store: Store) -> anyhow::Result<()> {
        // Start from the genesis.
        let initial_epoch_config = EpochConfig::from(&config.genesis.config);
        let validator_reward =
            HashMap::from([(config.genesis.config.protocol_treasury_account.clone(), 0u128)]);
        let initial_epoch_info = proposals_to_epoch_info(
            &initial_epoch_config,
            [0; 32],
            &EpochInfo::default(),
            config.genesis.config.validators(),
            HashMap::default(),
            validator_reward,
            0,
            config.genesis.config.protocol_version,
            config.genesis.config.protocol_version,
            false,
        )?;

        // Load all epochs so we can verify them one by one.
        let epochs = store
            .iter(DBCol::EpochInfo)
            .map(Result::unwrap)
            .filter_map(|(key, value)| {
                if key.as_ref() == AGGREGATOR_KEY {
                    None
                } else {
                    Some((
                        EpochId::try_from_slice(key.as_ref()).unwrap(),
                        EpochInfo::try_from_slice(value.as_ref()).unwrap(),
                    ))
                }
            })
            .collect::<HashMap<_, _>>();
        let epoch_id_by_height = epochs
            .iter()
            .map(|(epoch_id, epoch_info)| (epoch_info.epoch_height(), *epoch_id))
            .collect::<HashMap<_, _>>();

        let max_epoch_height = *epoch_id_by_height.keys().max().unwrap();
        println!("Max epoch height: {}", max_epoch_height);

        // Start from the genesis producers - that's the only thing we trust.
        let mut current_block_producers = Self::get_block_producers(&initial_epoch_info);
        for epoch_height in 1..max_epoch_height - 2 {
            println!(
                "Checking epoch height: {} with {} block producers",
                epoch_height,
                current_block_producers.len()
            );
            // Use the T + 2 epoch ID to figure out the last block hash of the current epoch.
            // Then look at the last final block (which must be last block's grandparent). We use
            // that block's next_bp_hash to verify the next epoch's block producers, and we use
            // that block's next block's approvals to verify the validity of the block itself
            // against the current block producers that we trust are correct.
            let epoch_id = epoch_id_by_height[&epoch_height];
            let epoch_info = epochs[&epoch_id].clone();
            let next_epoch_id = epoch_id_by_height[&(epoch_height + 1)];
            let next_epoch_info = epochs[&next_epoch_id].clone();
            let next_next_epoch_id = epoch_id_by_height[&(epoch_height + 2)];
            let last_block_hash = next_next_epoch_id.0;
            let block_header = store
                .get_ser::<BlockHeader>(DBCol::BlockHeader, last_block_hash.as_bytes())?
                .unwrap();
            let last_final_block_hash = *block_header.last_final_block();
            let last_final_block_header = store
                .get_ser::<BlockHeader>(DBCol::BlockHeader, last_final_block_hash.as_bytes())?
                .unwrap();
            let next_block_after_last_final = *block_header.last_ds_final_block();
            let next_block_after_last_final_header = store
                .get_ser::<BlockHeader>(DBCol::BlockHeader, next_block_after_last_final.as_bytes())?
                .unwrap();
            assert_eq!(*block_header.prev_hash(), next_block_after_last_final);
            assert_eq!(*next_block_after_last_final_header.prev_hash(), last_final_block_hash);

            let next_bp_hash = *last_final_block_header.next_bp_hash();
            let next_block_producers = Self::get_block_producers(&next_epoch_info);
            // TODO: This is not completely secure, as the protocol version is untrusted. However, we might be able to
            // take advantage of BlockHeaderV3's epoch_sync_data_hash field to verify that the EpochInfo is indeed
            // trustworthy. Gotta be careful to make sure the reasoning is airtight though.
            let bp_hash_computed =
                Self::compute_bp_hash(epoch_info.protocol_version(), next_block_producers.clone());
            assert_eq!(next_bp_hash, bp_hash_computed);

            let approvers = self
                .get_all_block_approvers_ordered(&current_block_producers, &next_block_producers);
            if epoch_height > 1 {
                Self::verify_approval(
                    last_final_block_hash,
                    next_block_after_last_final_header.height(),
                    &approvers,
                    &next_block_after_last_final_header.approvals(),
                )?;
            }

            // Once we've verified the handoff to the next block producers, we can trust them for the
            // verification of the next epoch.
            current_block_producers = next_block_producers;
        }
        Ok(())
    }

    fn compute_bp_hash(protocol_version: u32, validator_stakes: Vec<ValidatorStake>) -> CryptoHash {
        if ProtocolFeature::BlockHeaderV3.enabled(protocol_version) {
            CryptoHash::hash_borsh_iter(validator_stakes)
        } else {
            let validator_stakes = validator_stakes.into_iter().map(|bp| bp.into_v1());
            CryptoHash::hash_borsh_iter(validator_stakes)
        }
    }

    fn get_block_producers(epoch_info: &EpochInfo) -> Vec<ValidatorStake> {
        let mut block_producers = Vec::new();
        let mut seen_validators = HashSet::new();
        for bp_index in epoch_info.block_producers_settlement() {
            if seen_validators.insert(*bp_index) {
                block_producers.push(epoch_info.get_validator(*bp_index));
            }
        }
        block_producers
    }

    pub fn get_all_block_approvers_ordered(
        &self,
        current_block_producers: &Vec<ValidatorStake>,
        next_block_producers: &Vec<ValidatorStake>,
    ) -> Vec<ApprovalStake> {
        let mut settlement = current_block_producers.clone();
        let settlement_epoch_boundary = settlement.len();

        settlement.extend(next_block_producers.clone());

        let mut result = vec![];
        let mut validators: HashMap<AccountId, usize> = HashMap::default();
        for (ord, validator_stake) in settlement.into_iter().enumerate() {
            let account_id = validator_stake.account_id();
            match validators.get(account_id) {
                None => {
                    validators.insert(account_id.clone(), result.len());
                    result
                        .push(validator_stake.get_approval_stake(ord >= settlement_epoch_boundary));
                }
                Some(old_ord) => {
                    if ord >= settlement_epoch_boundary {
                        result[*old_ord].stake_next_epoch = validator_stake.stake();
                    };
                }
            };
        }
        result
    }

    fn verify_approval(
        prev_block_hash: CryptoHash,
        block_height: BlockHeight,
        approvers: &[ApprovalStake],
        approvals: &[Option<Box<Signature>>],
    ) -> anyhow::Result<()> {
        if approvals.len() > approvers.len() {
            anyhow::bail!("Too many approvals");
        }

        let message_to_sign =
            Approval::get_data_for_sig(&ApprovalInner::Endorsement(prev_block_hash), block_height);

        for (index, (validator, may_be_signature)) in
            approvers.iter().zip(approvals.iter()).enumerate()
        {
            if let Some(signature) = may_be_signature {
                if !signature.verify(message_to_sign.as_ref(), &validator.public_key) {
                    anyhow::bail!("Invalid signature for {:?} (#{})", validator.account_id, index);
                }
            }
        }

        let stakes = approvers
            .iter()
            .map(|x| (x.stake_this_epoch, x.stake_next_epoch, false))
            .collect::<Vec<_>>();
        if !Doomslug::can_approved_block_be_produced(
            DoomslugThresholdMode::TwoThirds,
            approvals,
            &stakes,
        ) {
            anyhow::bail!("Not enough approvals");
        }

        Ok(())
    }
}
