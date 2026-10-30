use super::sender::AuthenticatedSender;
use super::{DataId, PartsOutcome, SenderFault, SpiceData, SpiceDataManager, producers_in_epoch};
use itertools::Itertools as _;
use near_chain::Error;
use near_crypto::PublicKey;
use near_primitives::block_header::BlockHeader;
use near_primitives::errors::EpochError;
use near_primitives::spice::partial_data::{
    SpiceDataCommitment, SpiceDataPart, SpicePartialData, SpiceVerifiedPartialData,
};
use near_primitives::types::{AccountId, EpochId};

/// Parts received for an item whose block was not processed yet.
pub(super) struct ParkedParts {
    sender: AuthenticatedSender,
    id: DataId,
    commitment: SpiceDataCommitment,
    parts: Vec<SpiceDataPart>,
}

impl SpiceDataManager {
    /// Records the block as processed and feeds the parts parked for it to its items. Returns the
    /// data decoded.
    pub(super) fn release_parked(&mut self, block: &BlockHeader) -> Vec<(DataId, SpiceData)> {
        self.processed_blocks.insert(*block.hash(), block.height());
        let mut decoded = Vec::new();
        for ParkedParts { sender, id, commitment, parts } in
            self.orphans.pop(block.hash()).unwrap_or_default()
        {
            let Some(item) = self.items.get(&id) else {
                continue;
            };
            let producer_index = self.producer_signed_by(item, &sender);
            let result =
                producer_index.and_then(|index| self.add_parts(&id, index, &commitment, parts));
            match result {
                Ok(PartsOutcome::Decoded(data)) => decoded.push((id, data)),
                Ok(
                    PartsOutcome::Collecting
                    | PartsOutcome::AlreadySettled
                    | PartsOutcome::NotWanted,
                ) => {}
                Err(fault) => {
                    tracing::debug!(target: "spice_data_distribution", ?fault, ?id, ?sender, "rejected parked parts");
                }
            }
        }
        decoded
    }

    /// Parks parts for an untracked item until its block's trigger.
    pub(super) fn handle_untracked_item(
        &mut self,
        id: DataId,
        data: SpicePartialData,
    ) -> Result<PartsOutcome, SenderFault> {
        if self.processed_blocks.contains_key(id.block_hash()) {
            return Ok(PartsOutcome::NotWanted);
        }
        let header = self.chain_store.get_block_header_if_exists(id.block_hash());
        if header
            .as_ref()
            .is_some_and(|header| self.heights.at_or_below_final_execution(header.height()))
        {
            return Ok(PartsOutcome::NotWanted);
        }
        let block_epochs = match &header {
            Some(header) => vec![(*header.epoch_id(), *header.next_epoch_id())],
            None => {
                let header_head =
                    self.chain_store.header_head().expect("header head should be stored");
                // The epoch after the next is not known yet, so a block in the next epoch takes
                // the next epoch's keys only.
                vec![
                    (header_head.epoch_id, header_head.next_epoch_id),
                    (header_head.next_epoch_id, header_head.next_epoch_id),
                ]
            }
        };
        let keys = match self.sender_keys(&id, data.sender(), &block_epochs) {
            Ok(keys) => keys,
            // The block's epoch data may not be stored yet, as during epoch sync, or at all.
            Err(err) => {
                tracing::debug!(target: "spice_data_distribution", ?err, block_hash = %id.block_hash(), "epoch data of a block is unreadable; dropping its parts");
                return Ok(PartsOutcome::NotWanted);
            }
        };
        if keys.is_empty() {
            // Without the header the block's epoch is a guess, so a miss is no proof of a fault.
            return match header {
                Some(_) => Err(SenderFault::NotAProducer),
                None => Ok(PartsOutcome::NotWanted),
            };
        }
        let (sender, SpiceVerifiedPartialData { commitment, parts, .. }) =
            AuthenticatedSender::verify(data, &keys).ok_or(SenderFault::InvalidSignature)?;
        // A block off the canonical chain at or below the final head is never processed.
        if let Some(header) = &header {
            if self.heights.at_or_below_final_head(header.height())
                && !matches!(self.canonical_block_hash(header.height()), Ok(Some(hash)) if hash == *header.hash())
            {
                return Ok(PartsOutcome::NotWanted);
            }
        }
        let block_hash = *id.block_hash();
        self.orphans.get_or_insert_mut(block_hash, Vec::new).push(ParkedParts {
            sender,
            id,
            commitment,
            parts,
        });
        Ok(PartsOutcome::Collecting)
    }

    /// The keys `sender` may sign `id`'s parts with if it produces `id` for a block in one of
    /// `block_epochs`, each a block's epoch and the next. An epoch whose layout lacks the item's
    /// shard adds none.
    fn sender_keys(
        &self,
        id: &DataId,
        sender: &AccountId,
        block_epochs: &[(EpochId, EpochId)],
    ) -> Result<Vec<PublicKey>, Error> {
        let [source_shard, _] = id.shard_endpoints();
        let mut keys = Vec::new();
        for (epoch_id, next_epoch_id) in block_epochs {
            // The validator lookup comes first, so a non-validator costs no producer list.
            let sender_keys = match self.epoch_manager.get_validator_signing_keys(
                epoch_id,
                next_epoch_id,
                sender,
            ) {
                Ok(sender_keys) => sender_keys,
                Err(EpochError::NotAValidator(..)) => continue,
                Err(err) => return Err(err.into()),
            };
            let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
            if !shard_layout.shard_ids().contains(&source_shard) {
                continue;
            }
            let epoch_info = self.epoch_manager.get_epoch_info(epoch_id)?;
            if !producers_in_epoch(&epoch_info, &shard_layout, id)?.contains(sender) {
                continue;
            }
            for key in sender_keys {
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
        }
        Ok(keys)
    }
}
