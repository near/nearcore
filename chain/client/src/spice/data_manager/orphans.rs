use super::sender::AuthenticatedSender;
use super::{DataId, Decoded, PartsOutcome, SenderFault, SpiceDataManager, producers_in_epoch};
use bytesize::ByteSize;
use itertools::Itertools as _;
use near_chain::Error;
use near_crypto::PublicKey;
use near_primitives::block_header::BlockHeader;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::MerklePathItem;
use near_primitives::spice::partial_data::{
    SpiceDataCommitment, SpiceDataPart, SpicePartialData, SpiceVerifiedPartialData,
};
use near_primitives::types::{AccountId, BlockHeight, EpochId};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem;

/// Bytes of parked messages one sender may be charged for at a time.
pub(super) const MAX_PARKED_BYTES_PER_SENDER: u64 = ByteSize::mib(16).0;

/// Charged for each parked message on top of the bytes its parts and their proofs hold.
pub(super) const PARKED_MESSAGE_CHARGE: u64 = ByteSize::kib(32).0;

/// Final-head heights a parked block is kept for, counted from where the final head stood when
/// its first message was parked: as many blocks as a sender's cap holds at two messages each.
pub(super) const PARKED_BLOCK_MAX_AGE: BlockHeight =
    MAX_PARKED_BYTES_PER_SENDER / PARKED_MESSAGE_CHARGE / 2;

/// Parts received for an item whose block was not processed yet.
#[derive(Debug)]
pub(super) struct ParkedParts {
    sender: AuthenticatedSender,
    id: DataId,
    commitment: SpiceDataCommitment,
    parts: Vec<SpiceDataPart>,
}

impl ParkedParts {
    #[cfg(test)]
    pub(super) fn new(
        sender: AuthenticatedSender,
        id: DataId,
        commitment: SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
    ) -> Self {
        Self { sender, id, commitment, parts }
    }

    /// What parking the message costs its sender.
    fn charge(&self) -> u64 {
        self.parts.iter().fold(PARKED_MESSAGE_CHARGE, |charge, part| {
            let proof_bytes = part.merkle_proof.capacity() * size_of::<MerklePathItem>();
            charge.saturating_add(part.part.len() as u64).saturating_add(proof_bytes as u64)
        })
    }
}

/// Outcome of parking a message.
#[must_use]
#[derive(Debug, PartialEq, Eq)]
pub(super) enum ParkOutcome {
    Parked,
    /// Parking it would take the sender over its cap; nothing changed.
    SenderOverCap,
}

/// Messages parked until their block is processed, with what each sender is charged for them.
/// A sender's charge is the sum of its parked messages' charges, and stays within
/// `MAX_PARKED_BYTES_PER_SENDER`.
#[derive(Default)]
pub(super) struct OrphanPool {
    blocks: HashMap<CryptoHash, ParkedBlock>,
    /// The parked blocks by the final head height at which their first message was parked.
    by_parked_at: BTreeMap<BlockHeight, HashSet<CryptoHash>>,
    charged: HashMap<AccountId, u64>,
}

struct ParkedBlock {
    parked_at: BlockHeight,
    messages: Vec<ParkedParts>,
}

impl OrphanPool {
    /// Parks `parked` for `block_hash`, with the final head at `final_height`, unless its sender
    /// would go over its cap.
    pub(super) fn park(
        &mut self,
        block_hash: CryptoHash,
        parked: ParkedParts,
        final_height: BlockHeight,
    ) -> ParkOutcome {
        let charged = self.charged.get(parked.sender.account_id()).copied().unwrap_or(0);
        let Some(new_charge) = charged
            .checked_add(parked.charge())
            .filter(|charge| *charge <= MAX_PARKED_BYTES_PER_SENDER)
        else {
            return ParkOutcome::SenderOverCap;
        };
        self.charged.insert(parked.sender.account_id().clone(), new_charge);
        let block = self.blocks.entry(block_hash).or_insert_with(|| {
            self.by_parked_at.entry(final_height).or_default().insert(block_hash);
            ParkedBlock { parked_at: final_height, messages: Vec::new() }
        });
        block.messages.push(parked);
        ParkOutcome::Parked
    }

    /// Removes the messages parked for `block_hash` and refunds their senders.
    pub(super) fn take(&mut self, block_hash: &CryptoHash) -> Vec<ParkedParts> {
        let Some(ParkedBlock { parked_at, messages }) = self.blocks.remove(block_hash) else {
            return Vec::new();
        };
        if let Some(hashes) = self.by_parked_at.get_mut(&parked_at) {
            hashes.remove(block_hash);
            if hashes.is_empty() {
                self.by_parked_at.remove(&parked_at);
            }
        }
        for parked in &messages {
            self.refund(parked);
        }
        messages
    }

    /// Drops, and refunds, the blocks parked `PARKED_BLOCK_MAX_AGE` or more heights below
    /// `final_height`.
    pub(super) fn drop_aged(&mut self, final_height: BlockHeight) {
        let Some(oldest_kept) =
            final_height.checked_sub(PARKED_BLOCK_MAX_AGE).and_then(|aged| aged.checked_add(1))
        else {
            return;
        };
        let kept = self.by_parked_at.split_off(&oldest_kept);
        for block_hash in mem::replace(&mut self.by_parked_at, kept).into_values().flatten() {
            let block = self.blocks.remove(&block_hash).expect("indexed blocks should be parked");
            for parked in &block.messages {
                self.refund(parked);
            }
        }
    }

    fn refund(&mut self, parked: &ParkedParts) {
        let account_id = parked.sender.account_id();
        let charged = self.charged.get_mut(account_id).expect("a parked sender should be charged");
        *charged -= parked.charge();
        if *charged == 0 {
            self.charged.remove(account_id);
        }
    }
}

impl SpiceDataManager {
    /// Records the block as processed and feeds the parts parked for it to its items. Returns the
    /// data decoded.
    pub(super) fn release_parked(&mut self, block: &BlockHeader) -> Vec<Decoded> {
        self.processed_blocks.insert(*block.hash(), block.height());
        let mut decoded = Vec::new();
        for ParkedParts { sender, id, commitment, parts } in self.orphans.take(block.hash()) {
            let Some(item) = self.items.get(&id) else {
                continue;
            };
            let producer_index = self.producer_signed_by(item, &sender);
            let result =
                producer_index.and_then(|index| self.add_parts(&id, index, &commitment, parts));
            match result {
                Ok(PartsOutcome::Decoded(data)) => decoded.push(data),
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
        if header.as_ref().is_some_and(|header| self.heights.expired(&id, header.height())) {
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
        if header
            .as_ref()
            .is_some_and(|header| self.is_fork_at_or_below_final_head(header).unwrap_or(true))
        {
            return Ok(PartsOutcome::NotWanted);
        }
        let final_height = match self.chain_store.final_head() {
            Ok(final_head) => final_head.height,
            Err(err) => {
                tracing::error!(target: "spice_data_distribution", ?err, "failed to read the final head");
                return Ok(PartsOutcome::NotWanted);
            }
        };
        let block_hash = *id.block_hash();
        let parked = ParkedParts { sender, id, commitment, parts };
        match self.orphans.park(block_hash, parked, final_height) {
            ParkOutcome::Parked => Ok(PartsOutcome::Collecting),
            ParkOutcome::SenderOverCap => Ok(PartsOutcome::NotWanted),
        }
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
        let source_shard = id.producing_chunk().shard_id;
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
