use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use lru::LruCache;
use near_async::MultiSend;
use near_async::MultiSenderFrom;
use near_async::messaging::CanSend;
use near_async::messaging::Handler;
use near_async::messaging::Sender;
use near_chain::Block;
use near_chain::spice_core::CoreStatementsProcessor;
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_network::spice_data_distribution::SpiceDataCommitment;
use near_network::spice_data_distribution::SpiceDataIdentifier;
use near_network::spice_data_distribution::SpiceDataPart;
use near_network::spice_data_distribution::SpiceIncomingPartialData;
use near_network::spice_data_distribution::SpicePartialData;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt as _;
use near_primitives::errors::EpochError;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::merklize;
use near_primitives::merkle::verify_path_with_index;
use near_primitives::reed_solomon;
use near_primitives::reed_solomon::ReedSolomonEncoderDeserialize;
use near_primitives::reed_solomon::ReedSolomonPartsTracker;
use near_primitives::reed_solomon::{ReedSolomonEncoderCache, ReedSolomonEncoderSerialize};
use near_primitives::sharding::ReceiptProof;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::spice_state_witness::SpiceChunkStateWitness;
use near_primitives::types::AccountId;
use near_primitives::types::EpochId;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;

use crate::chunk_executor_actor::ExecutorIncomingUnverifiedReceipts;
use crate::chunk_executor_actor::ProcessedBlock;
use crate::chunk_executor_actor::receipt_proof_exists;
use crate::spice_chunk_validator_actor::SpiceChunkStateWitnessMessage;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("Near chain error: {0}")]
    NearChainError(#[from] near_chain::Error),
    #[error("sender is not in the set of producers")]
    SenderIsNotProducer,
    #[error("node is not in the set of recipients")]
    NodeIsNotRecipient,
    #[error("data is already decoded")]
    DataIsAlreadyDecoded,
    #[error("receipts are already known")]
    ReceiptsAreKnown,
    #[error("witness id shard_id in invalid")]
    InvalidWitnessShardId,
    #[error("decoded witness shard_id in invalid")]
    InvalidDecodedWitnessShardId,
    #[error("decoded witness block hash in invalid")]
    InvalidDecodedWitnessBlockHash,
    #[error("witness is already validated")]
    WitnessAlreadyValidated,
    #[error("part doesn't match commitment root")]
    InvalidCommitmentRoot,
    #[error("decoded data doesn't match commitment hash")]
    InvalidCommitmentHash,
    #[error("receipt proof id to_shard_id is invalid")]
    InvalidReceiptToShardId,
    #[error("decoded receipt proof to_shard_id is invalid")]
    InvalidDecodedReceiptToShardId,
    #[error("receipt proof id from_shard_id is invalid")]
    InvalidReceiptFromShardId,
    #[error("decoded receipt proof from_shard_id is invalid")]
    InvalidDecodedReceiptFromShardId,
    #[error("parts is empty")]
    PartsIsEmpty,
    #[error("decoded data doesn't match id")]
    IdAndDataMismatch,
    #[error("error decoding the data: {0}")]
    DecodeError(std::io::Error),
    #[error("other error: {0}")]
    Other(&'static str),
}

impl From<EpochError> for Error {
    fn from(value: EpochError) -> Self {
        Error::NearChainError(near_chain::Error::from(value))
    }
}

// TODO(spice): Separate actor into separate sender and receiver actors.
pub struct SpiceDataDistributorActor {
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub(crate) core_processor: CoreStatementsProcessor,
    rs_encoders: ReedSolomonEncoderCache,
    validator_signer: MutableValidatorSigner,

    network_adapter: PeerManagerAdapter,
    executor_sender: Sender<ExecutorIncomingUnverifiedReceipts>,
    witness_validator_sender: Sender<SpanWrapped<SpiceChunkStateWitnessMessage>>,

    // TODO(spice): handle the possibility of receiving parts for dubious data.
    data_parts: HashMap<(SpiceDataIdentifier, SpiceDataCommitment), DataPartsEntry>,

    /// SpicePartialData which we cannot decode or validate yet because of missing corresponding block.
    /// Key is block hash, value is data with sender
    pending_partial_data: LruCache<CryptoHash, Vec<SpicePartialData>>,
}

impl near_async::messaging::Actor for SpiceDataDistributorActor {}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct SpiceDataDistributorAdapter {
    pub receipts: Sender<SpiceDistributorOutgoingReceipts>,
    pub witness: Sender<SpiceDistributorStateWitness>,
}

struct DataPartsEntry {
    decoded: bool,
    tracker: ReedSolomonPartsTracker<SpiceData>,
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
enum SpiceData {
    ReceiptProof(ReceiptProof),
    StateWitness(Box<SpiceChunkStateWitness>),
}

impl ReedSolomonEncoderSerialize for SpiceData {}

impl ReedSolomonEncoderDeserialize for SpiceData {}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct SpiceDistributorOutgoingReceipts {
    pub block_hash: CryptoHash,
    pub receipt_proofs: Vec<ReceiptProof>,
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct SpiceDistributorStateWitness {
    pub state_witness: SpiceChunkStateWitness,
}

impl Handler<SpiceDistributorOutgoingReceipts> for SpiceDataDistributorActor {
    fn handle(
        &mut self,
        SpiceDistributorOutgoingReceipts {
            block_hash,
            receipt_proofs,
        }: SpiceDistributorOutgoingReceipts,
    ) {
        for proof in receipt_proofs {
            let data_id = SpiceDataIdentifier::ReceiptProof {
                block_hash,
                from_shard_id: proof.1.from_shard_id,
                to_shard_id: proof.1.to_shard_id,
            };
            if let Err(err) = self.distribute_data(data_id.clone(), &SpiceData::ReceiptProof(proof))
            {
                tracing::error!(target: "spice_data_distribution", ?err, ?data_id, "failed to distribute receipt proof");
            }
        }
    }
}

impl Handler<SpiceDistributorStateWitness> for SpiceDataDistributorActor {
    fn handle(
        &mut self,
        SpiceDistributorStateWitness { state_witness }: SpiceDistributorStateWitness,
    ) {
        let chunk_id = state_witness.chunk_id();
        let data_id = SpiceDataIdentifier::Witness {
            block_hash: chunk_id.block_hash,
            shard_id: chunk_id.shard_id,
        };
        // TODO(spice): compress witness before distributing.
        if let Err(err) =
            self.distribute_data(data_id.clone(), &SpiceData::StateWitness(Box::new(state_witness)))
        {
            tracing::error!(target: "spice_data_distribution", ?err, ?data_id, "failed to distribute state witness");
        }
    }
}

impl Handler<SpiceIncomingPartialData> for SpiceDataDistributorActor {
    fn handle(&mut self, SpiceIncomingPartialData { data }: SpiceIncomingPartialData) {
        let data_id = data.id.clone();
        let commitment = data.commitment.clone();
        if let Err(err) = self.receive_data(data) {
            // TODO(spice): Implement banning or de-prioritization of nodes from which we receive
            // invalid data.
            tracing::error!(target: "spice_data_distribution", ?err, ?data_id, ?commitment, "failed to handle receiving partial data");
        }
    }
}

impl Handler<ProcessedBlock> for SpiceDataDistributorActor {
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        if let Err(err) = self.process_pending_partial_data(block_hash) {
            tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, "failure when processing pending partial data");
        }
    }
}

impl SpiceDataDistributorActor {
    pub fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        chain_store: ChainStoreAdapter,
        core_processor: CoreStatementsProcessor,
        validator_signer: MutableValidatorSigner,
        network_adapter: PeerManagerAdapter,
        executor_sender: Sender<ExecutorIncomingUnverifiedReceipts>,
        witness_validator_sender: Sender<SpanWrapped<SpiceChunkStateWitnessMessage>>,
    ) -> Self {
        const DATA_PARTS_RATIO: f64 = 0.6;
        const PENDING_PARTIAL_DATA_CAP: NonZeroUsize = NonZeroUsize::new(10).unwrap();
        Self {
            // TODO(spice): Evaluate whether the same data parts ratio makes sense for all data
            // distributed.
            rs_encoders: ReedSolomonEncoderCache::new(DATA_PARTS_RATIO),
            data_parts: HashMap::new(),
            epoch_manager,
            chain_store,
            core_processor,
            validator_signer,
            network_adapter,
            executor_sender,
            witness_validator_sender,
            pending_partial_data: LruCache::new(PENDING_PARTIAL_DATA_CAP),
        }
    }

    // TODO(spice): before distributing persist data keyed by id to allow it being re-requested.
    fn distribute_data(
        &mut self,
        data_id: SpiceDataIdentifier,
        data: &SpiceData,
    ) -> Result<(), Error> {
        let block = self.chain_store.get_block(data_id.block_hash())?;
        let Some(signer) = self.validator_signer.get() else {
            debug_assert!(false);
            return Err(Error::Other("trying to distribute data without validator_signer"));
        };
        let me = signer.validator_id();
        let (recipients, producers) = self.recipients_and_producers(&data_id, &block)?;
        if !producers.contains(me) {
            // TODO(spice): In chunk executor make sure we don't try to send out receipts and witnesses
            // if we aren't a respective producer (though still may be tracking shards) and make
            // this if check into debug_assert that producers never contain me.
            return Ok(());
        }
        debug_assert!(!recipients.contains(me));
        let me_ord = producers.iter().position(|p| p == me).unwrap();

        let encoder = self.rs_encoders.entry(producers.len());
        let (mut boxed_parts, encoded_length) = encoder.encode(data);
        debug_assert_eq!(boxed_parts.len(), producers.len());

        let parts: Vec<&[u8]> =
            boxed_parts.iter().map(|x| x.as_deref().unwrap()).collect::<Vec<_>>();
        let (merkle_root, mut merkle_proofs) = merklize(&parts);
        // TODO(spice): As an optimization we should be able to avoid serializing data both in
        // encode and to compute hash.
        let data_hash = hash(&borsh::to_vec(&data).unwrap());
        let commitment = SpiceDataCommitment {
            hash: data_hash,
            root: merkle_root,
            encoded_length: encoded_length as u64,
        };

        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::SpicePartialData {
                partial_data: SpicePartialData {
                    id: data_id,
                    commitment,
                    parts: vec![SpiceDataPart {
                        part_ord: me_ord as u64,
                        part: boxed_parts[me_ord].take().unwrap(),
                        merkle_proof: merkle_proofs.swap_remove(me_ord),
                    }],
                    sender: me.clone(),
                },
                recipients,
            },
        ));
        Ok(())
    }

    // TODO(spice): Implement dynamically changing the recipients for witness if relevant chunk
    // isn't endorsed for too long.
    // TODO(spice): Cache the results since likely they would be used often.
    fn recipients_and_producers(
        &self,
        data_id: &SpiceDataIdentifier,
        block: &Block,
    ) -> Result<(HashSet<AccountId>, Vec<AccountId>), Error> {
        let (recipients, producers) = match data_id {
            SpiceDataIdentifier::ReceiptProof { from_shard_id, to_shard_id, block_hash } => {
                debug_assert_eq!(block.hash(), block_hash);
                let epoch_id = block.header().epoch_id();
                let next_block_epoch_id =
                    self.epoch_manager.get_epoch_id_from_prev_block(block_hash)?;
                // TODO(spice-resharding): validate whether from_shard_id and to_shard_id would be
                // correct when resharding.
                let producers = self
                    .epoch_manager
                    .get_epoch_chunk_producers_for_shard(&epoch_id, *from_shard_id)?;
                let recipients = self
                    .epoch_manager
                    .get_epoch_chunk_producers_for_shard(&next_block_epoch_id, *to_shard_id)?;
                (recipients, producers)
            }
            SpiceDataIdentifier::Witness { block_hash, shard_id } => {
                debug_assert_eq!(block.hash(), block_hash);
                let epoch_id = block.header().epoch_id();
                let producers =
                    self.epoch_manager.get_epoch_chunk_producers_for_shard(epoch_id, *shard_id)?;
                let height_created = block
                    .chunks()
                    .iter_raw()
                    .find(|chunk| &chunk.shard_id() == shard_id)
                    .map(ShardChunkHeader::height_created)
                    .ok_or(Error::InvalidWitnessShardId)?;
                let validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
                    epoch_id,
                    *shard_id,
                    height_created,
                )?;
                let recipients = validator_assignments.ordered_chunk_validators();
                (recipients, producers)
            }
        };
        // Since producers would produce the data anyway they shouldn't be in the recipients set.
        let mut recipients_set: HashSet<_> = HashSet::from_iter(recipients.into_iter());
        for account in &producers {
            recipients_set.remove(account);
        }
        Ok((recipients_set, producers))
    }

    pub(crate) fn receive_data(&mut self, data: SpicePartialData) -> Result<(), Error> {
        let block_hash = data.id.block_hash();
        let block = match self.chain_store.get_block(block_hash) {
            Ok(block) => block,
            Err(near_chain::Error::DBNotFoundErr(_)) => {
                return self.add_pending_partial_data(data);
            }
            Err(err) => return Err(err.into()),
        };
        self.receive_data_with_block(data, &block)
    }

    fn add_pending_partial_data(&mut self, data: SpicePartialData) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            return Err(Error::Other("cannot receive data without validator_signer"));
        };
        let me = signer.validator_id();

        let id = &data.id;
        let possible_epoch_ids = self.possible_epoch_ids(id)?;
        let sender = &data.sender;
        if !self.possible_producers(id, &possible_epoch_ids)?.contains(sender) {
            return Err(Error::SenderIsNotProducer);
        }
        if !self.possible_recipients(id, &possible_epoch_ids)?.contains(me) {
            return Err(Error::NodeIsNotRecipient);
        }
        if data.parts.is_empty() {
            return Err(Error::PartsIsEmpty);
        }
        // TODO(spice): Verify that size of partial data isn't too large.
        self.pending_partial_data.get_or_insert_mut(*id.block_hash(), Vec::new).push(data);
        Ok(())
    }

    fn receive_data_with_block(
        &mut self,
        SpicePartialData { id, commitment, parts, sender }: SpicePartialData,
        block: &Block,
    ) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            return Err(Error::Other("cannot receive data without validator_signer"));
        };
        let me = signer.validator_id();

        self.verify_data_id(&id, block)?;
        let (recipients, producers) = self.recipients_and_producers(&id, block)?;
        if !producers.contains(&sender) {
            return Err(Error::SenderIsNotProducer);
        }
        if !recipients.contains(me) {
            return Err(Error::NodeIsNotRecipient);
        }
        let data_parts_key = (id.clone(), commitment.clone());
        self.verify_data_is_relevant(me, &data_parts_key, block)?;
        // TODO(spice): Check that encoded_length isn't too large.
        let encoded_length = commitment.encoded_length;
        let total_parts = producers.len();
        let entry = self.data_parts.entry(data_parts_key).or_insert_with(|| {
            let encoder = self.rs_encoders.entry(total_parts);
            DataPartsEntry {
                decoded: false,
                tracker: ReedSolomonPartsTracker::new(encoder, encoded_length as usize),
            }
        });
        for SpiceDataPart { part_ord, part, merkle_proof } in parts {
            if !verify_path_with_index(
                commitment.root,
                &merkle_proof,
                &part,
                part_ord,
                total_parts as u64,
            ) {
                return Err(Error::InvalidCommitmentRoot);
            }
            // TODO(spice): Verify that size of partial data isn't too large.
            let create_decode_span = None;
            match entry.tracker.insert_part(part_ord as usize, part, create_decode_span) {
                reed_solomon::InsertPartResult::Accepted => {}
                reed_solomon::InsertPartResult::PartAlreadyAvailable => {}
                reed_solomon::InsertPartResult::InvalidPartOrd => {
                    debug_assert!(
                        false,
                        "verification with merkle_proof should make sure part_ord is correct"
                    );
                    return Err(Error::Other(
                        "verification with merkle_proof passed, but part_ord is still invalid",
                    ));
                }
                reed_solomon::InsertPartResult::Decoded(Ok(data)) => {
                    entry.decoded = true;
                    let data_hash = hash(&borsh::to_vec(&data).unwrap());
                    if data_hash != commitment.hash {
                        return Err(Error::InvalidCommitmentHash);
                    }
                    match data {
                        SpiceData::ReceiptProof(receipt_proof) => {
                            let SpiceDataIdentifier::ReceiptProof {
                                block_hash,
                                from_shard_id,
                                to_shard_id,
                            } = id
                            else {
                                return Err(Error::IdAndDataMismatch);
                            };
                            if to_shard_id != receipt_proof.1.to_shard_id {
                                return Err(Error::InvalidDecodedReceiptToShardId);
                            }
                            if from_shard_id != receipt_proof.1.from_shard_id {
                                return Err(Error::InvalidDecodedReceiptFromShardId);
                            }
                            self.executor_sender.send(ExecutorIncomingUnverifiedReceipts {
                                receipt_proof,
                                block_hash,
                            });
                        }
                        SpiceData::StateWitness(witness) => {
                            let SpiceDataIdentifier::Witness { block_hash, shard_id } = &id else {
                                return Err(Error::IdAndDataMismatch);
                            };
                            let chunk_id = witness.chunk_id();
                            if &chunk_id.shard_id != shard_id {
                                return Err(Error::InvalidDecodedWitnessShardId);
                            }
                            if &chunk_id.block_hash != block_hash {
                                return Err(Error::InvalidDecodedWitnessBlockHash);
                            }

                            self.witness_validator_sender.send(
                                SpiceChunkStateWitnessMessage {
                                    witness: *witness,
                                    raw_witness_size: encoded_length as usize,
                                }
                                .span_wrap(),
                            );
                        }
                    }
                }
                reed_solomon::InsertPartResult::Decoded(Err(err)) => {
                    return Err(Error::DecodeError(err));
                }
            }
        }
        Ok(())
    }

    fn verify_data_is_relevant(
        &self,
        me: &AccountId,
        data_parts_key: &(SpiceDataIdentifier, SpiceDataCommitment),
        block: &Block,
    ) -> Result<(), Error> {
        if let Some(entry) = self.data_parts.get(&data_parts_key) {
            if entry.decoded {
                return Err(Error::DataIsAlreadyDecoded);
            }
        }
        let id = &data_parts_key.0;
        match id {
            SpiceDataIdentifier::ReceiptProof { block_hash, from_shard_id, to_shard_id } => {
                debug_assert_eq!(block_hash, block.hash());
                if receipt_proof_exists(
                    &self.chain_store.store(),
                    block_hash,
                    *to_shard_id,
                    *from_shard_id,
                )
                .map_err(near_chain::Error::from)?
                {
                    return Err(Error::ReceiptsAreKnown);
                }
            }
            SpiceDataIdentifier::Witness { block_hash, shard_id } => {
                debug_assert_eq!(block_hash, block.hash());
                // TODO(spice): Check for unsuccessful validations as well.
                if self
                    .core_processor
                    .endorsement_exists(block_hash, *shard_id, me)
                    .map_err(near_chain::Error::from)?
                {
                    return Err(Error::WitnessAlreadyValidated);
                }
            }
        }
        Ok(())
    }

    fn verify_data_id(&self, id: &SpiceDataIdentifier, block: &Block) -> Result<(), Error> {
        match id {
            SpiceDataIdentifier::ReceiptProof { block_hash, from_shard_id, to_shard_id } => {
                debug_assert_eq!(block_hash, block.hash());
                let shard_layout =
                    self.epoch_manager.get_shard_layout(block.header().epoch_id())?;
                let shard_ids: HashSet<_> = shard_layout.shard_ids().collect();
                if !shard_ids.contains(from_shard_id) {
                    return Err(Error::InvalidReceiptFromShardId);
                }
                // TODO(spice-resharding): If to_shard_id may be from the next_epoch this check
                // needs to be adjusted.
                if !shard_ids.contains(to_shard_id) {
                    return Err(Error::InvalidReceiptToShardId);
                }
            }
            SpiceDataIdentifier::Witness { block_hash, shard_id } => {
                debug_assert_eq!(block_hash, block.hash());
                let shard_layout =
                    self.epoch_manager.get_shard_layout(block.header().epoch_id())?;
                let shard_ids: HashSet<_> = shard_layout.shard_ids().collect();
                if !shard_ids.contains(shard_id) {
                    return Err(Error::InvalidWitnessShardId);
                }
            }
        }
        Ok(())
    }

    fn possible_epoch_ids(&self, id: &SpiceDataIdentifier) -> Result<Vec<EpochId>, Error> {
        let block_hash = id.block_hash();
        let possible_epoch_ids = if self.chain_store.block_exists(block_hash)? {
            let epoch_id = self.epoch_manager.get_epoch_id(block_hash)?;
            vec![epoch_id]
        } else {
            let final_head = self.chain_store.final_head()?;
            // Since block doesn't exist it has to be after the final head.
            // Here we assume we aren't catching up.
            // TODO(spice): consider if this needs to be adjusted when implementing various syncs.
            vec![final_head.epoch_id, final_head.next_epoch_id]
        };
        Ok(possible_epoch_ids)
    }

    fn possible_producers(
        &self,
        id: &SpiceDataIdentifier,
        possible_epoch_ids: &[EpochId],
    ) -> Result<HashSet<AccountId>, Error> {
        let mut possible_producers = HashSet::new();
        for epoch_id in possible_epoch_ids {
            match id {
                SpiceDataIdentifier::Witness { shard_id, .. } => {
                    possible_producers.extend(
                        self.epoch_manager
                            .get_epoch_chunk_producers_for_shard(&epoch_id, *shard_id)?
                            .into_iter(),
                    );
                }
                SpiceDataIdentifier::ReceiptProof { from_shard_id, .. } => {
                    possible_producers.extend(
                        self.epoch_manager
                            .get_epoch_chunk_producers_for_shard(&epoch_id, *from_shard_id)?
                            .into_iter(),
                    );
                }
            }
        }
        Ok(possible_producers)
    }

    fn possible_recipients(
        &self,
        id: &SpiceDataIdentifier,
        possible_epoch_ids: &[EpochId],
    ) -> Result<HashSet<AccountId>, Error> {
        let mut possible_recipients = HashSet::new();
        for epoch_id in possible_epoch_ids {
            match id {
                SpiceDataIdentifier::Witness { .. } => {
                    let epoch_info = self.epoch_manager.get_epoch_info(epoch_id)?;
                    possible_recipients
                        .extend(epoch_info.validators_iter().map(|stake| stake.take_account_id()));
                }
                SpiceDataIdentifier::ReceiptProof { to_shard_id, .. } => {
                    possible_recipients.extend(
                        self.epoch_manager
                            .get_epoch_chunk_producers_for_shard(&epoch_id, *to_shard_id)?
                            .into_iter(),
                    );
                }
            }
        }
        Ok(possible_recipients)
    }

    fn process_pending_partial_data(&mut self, block_hash: CryptoHash) -> Result<(), Error> {
        let ready_data = self.pending_partial_data.pop(&block_hash).unwrap_or_default();
        if ready_data.is_empty() {
            return Ok(());
        }
        let block = self.chain_store.get_block(&block_hash)?;
        for data in ready_data {
            let data_id = data.id.clone();
            let commitment = data.commitment.clone();
            if let Err(err) = self.receive_data_with_block(data, &block) {
                tracing::error!(target: "spice_data_distribution", ?err, ?data_id, ?commitment, "failed to process partial data");
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn pending_partial_data_size(&self) -> usize {
        self.pending_partial_data.len()
    }
}
