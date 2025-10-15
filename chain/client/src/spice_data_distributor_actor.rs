use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use itertools::Itertools as _;
use lru::LruCache;
use near_async::MultiSend;
use near_async::MultiSenderFrom;
use near_async::futures::DelayedActionRunner;
use near_async::futures::DelayedActionRunnerExt as _;
use near_async::messaging::CanSend;
use near_async::messaging::Handler;
use near_async::messaging::Sender;
use near_async::time::Duration;
use near_chain::Block;
use near_chain::spice_core::CoreStatementsProcessor;
use near_chain_configs::MutableValidatorSigner;
use near_chain_primitives::ApplyChunksMode;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::spice_data_distribution::SpiceIncomingPartialData;
use near_network::spice_data_distribution::SpicePartialDataRequest;
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
use near_primitives::spice_partial_data::SpiceDataCommitment;
use near_primitives::spice_partial_data::SpiceDataIdentifier;
use near_primitives::spice_partial_data::SpiceDataPart;
use near_primitives::spice_partial_data::SpicePartialData;
use near_primitives::spice_partial_data::SpiceVerifiedPartialData;
use near_primitives::stateless_validation::spice_state_witness::SpiceChunkStateWitness;
use near_primitives::types::AccountId;
use near_primitives::types::EpochId;
use near_primitives::types::ShardId;
use near_primitives::types::validator_stake::ValidatorStake;
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
    #[error("witness id shard_id in invalid")]
    InvalidWitnessShardId,
    #[error("decoded witness shard_id in invalid")]
    InvalidDecodedWitnessShardId,
    #[error("decoded witness block hash in invalid")]
    InvalidDecodedWitnessBlockHash,
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
    #[error("data sender is not a validator")]
    SenderIsNotValidator,
    #[error("partial data signature is invalid")]
    InvalidPartialDataSignature,
    #[error(transparent)]
    DataIsKnown(#[from] DataIsKnownError),
    #[error("error decoding the data: {0}")]
    DecodeError(std::io::Error),
    #[error("other error: {0}")]
    Other(&'static str),
}

impl From<EpochError> for Error {
    fn from(value: EpochError) -> Self {
        match value {
            EpochError::NotAValidator(..) => Error::SenderIsNotValidator,
            _ => Error::NearChainError(near_chain::Error::from(value)),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DataIsKnownError {
    #[error("witness is already validated")]
    WitnessValidated(ShardId),
    #[error("receipts are already known")]
    ReceiptsKnown { from_shard_id: ShardId, to_shard_id: ShardId },
    #[error("witness is already decoded")]
    WitnessDecoded(ShardId),
    #[error("receipts are already decoded")]
    ReceiptsDecoded { from_shard_id: ShardId, to_shard_id: ShardId },
}

impl DataIsKnownError {
    fn to_data_id(&self, block_hash: CryptoHash) -> SpiceDataIdentifier {
        match self {
            DataIsKnownError::WitnessValidated(shard_id)
            | DataIsKnownError::WitnessDecoded(shard_id) => {
                SpiceDataIdentifier::Witness { block_hash, shard_id: *shard_id }
            }
            DataIsKnownError::ReceiptsKnown { from_shard_id, to_shard_id }
            | DataIsKnownError::ReceiptsDecoded { from_shard_id, to_shard_id } => {
                SpiceDataIdentifier::ReceiptProof {
                    block_hash,
                    from_shard_id: *from_shard_id,
                    to_shard_id: *to_shard_id,
                }
            }
        }
    }

    fn decoded(data_id: &SpiceDataIdentifier) -> Self {
        match data_id {
            SpiceDataIdentifier::ReceiptProof { from_shard_id, to_shard_id, block_hash: _ } => {
                Self::ReceiptsDecoded { from_shard_id: *from_shard_id, to_shard_id: *to_shard_id }
            }
            SpiceDataIdentifier::Witness { shard_id, block_hash: _ } => {
                Self::WitnessDecoded(*shard_id)
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiveDataError {
    #[error("failed receiving data with relevant block available")]
    ReceivingDataWithBlock(Error),
    #[error("failed receiving data with no block available")]
    ReceivingDataWithoutBlock(Error),
    #[error("Near chain error: {0}")]
    NearChainError(#[from] near_chain::Error),
}

impl ReceiveDataError {
    fn data_is_known_error(&self) -> Option<&DataIsKnownError> {
        let inner = match self {
            ReceiveDataError::ReceivingDataWithBlock(error)
            | ReceiveDataError::ReceivingDataWithoutBlock(error) => error,
            ReceiveDataError::NearChainError(_) => return None,
        };
        let Error::DataIsKnown(err) = &inner else {
            return None;
        };
        Some(err)
    }
}

// TODO(spice): Separate actor into separate sender and receiver actors.
pub struct SpiceDataDistributorActor {
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub(crate) core_processor: CoreStatementsProcessor,
    rs_encoders: ReedSolomonEncoderCache,
    validator_signer: MutableValidatorSigner,
    shard_tracker: ShardTracker,

    network_adapter: PeerManagerAdapter,
    executor_sender: Sender<ExecutorIncomingUnverifiedReceipts>,
    witness_validator_sender: Sender<SpanWrapped<SpiceChunkStateWitnessMessage>>,

    // TODO(spice): handle the possibility of receiving parts for dubious data.
    data_parts: HashMap<(SpiceDataIdentifier, SpiceDataCommitment), DataPartsEntry>,

    /// Spice Partial Data which we cannot decode or validate yet because of missing corresponding block.
    /// Key is block hash, value is data with sender
    pending_partial_data: LruCache<CryptoHash, Vec<SpiceVerifiedPartialData>>,

    // TODO(spice): Persist data we are waiting on.
    waiting_on_data: HashSet<SpiceDataIdentifier>,
    // TODO(spice): Persist data we are distributing. Likely, witnesses should be available
    // from certification head onward while receipts should be available for regular gc window.
    recent_distribution_data: LruCache<SpiceDataIdentifier, RecentDistributionData>,
}

struct RecentDistributionData {
    parts: Vec<SpiceDataPart>,
    commitment: SpiceDataCommitment,
}

impl near_async::messaging::Actor for SpiceDataDistributorActor {
    fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.schedule_data_fetching(ctx);
    }
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct SpiceDataDistributorAdapter {
    pub receipts: Sender<SpiceDistributorOutgoingReceipts>,
    pub witness: Sender<SpiceDistributorStateWitness>,
}

struct DataPartsEntry {
    decoded: bool,
    tracker: ReedSolomonPartsTracker<SpiceData>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
enum SpiceData {
    ReceiptProof(ReceiptProof),
    StateWitness(Box<SpiceChunkStateWitness>),
}

impl ReedSolomonEncoderSerialize for SpiceData {}

impl ReedSolomonEncoderDeserialize for SpiceData {}

#[derive(Debug)]
pub struct SpiceDistributorOutgoingReceipts {
    pub block_hash: CryptoHash,
    pub receipt_proofs: Vec<ReceiptProof>,
}

#[derive(Debug)]
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
        let block_hash = *data.block_hash();
        let sender = data.sender().clone();
        if let Err(err) = self.receive_data(data) {
            if let Some(err) = err.data_is_known_error() {
                let data_id = err.to_data_id(block_hash);
                self.waiting_on_data.remove(&data_id);
                tracing::debug!(target: "spice_data_distribution", ?err, ?data_id, ?sender, "received data we already have");
                return;
            }
            // TODO(spice): Implement banning or de-prioritization of nodes from which we receive
            // invalid data.
            tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, ?sender, "failed to handle receiving partial data");
            return;
        };
    }
}

impl Handler<SpicePartialDataRequest> for SpiceDataDistributorActor {
    fn handle(&mut self, msg: SpicePartialDataRequest) -> () {
        if let Err(err) = self.handle_partial_data_request(msg) {
            tracing::error!(target: "spice_data_distribution", ?err, "failure when handling partial data request");
        }
    }
}

impl Handler<ProcessedBlock> for SpiceDataDistributorActor {
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        if let Err(err) = self.start_waiting_on_data(&block_hash) {
            tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, "failure when starting waiting on data");
        }
        if let Err(err) = self.process_pending_partial_data(&block_hash) {
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
        shard_tracker: ShardTracker,
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
            shard_tracker,
            network_adapter,
            executor_sender,
            witness_validator_sender,
            pending_partial_data: LruCache::new(PENDING_PARTIAL_DATA_CAP),
            waiting_on_data: HashSet::new(),
            // This data will eventually be persisted so the size of this cache isn't that
            // critical to get right at the moment.
            recent_distribution_data: LruCache::new(NonZeroUsize::new(2000).unwrap()),
        }
    }

    // TODO(spice): before distributing persist data keyed by id to allow it being re-requested.
    fn distribute_data(
        &mut self,
        data_id: SpiceDataIdentifier,
        data: &SpiceData,
    ) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            debug_assert!(false);
            return Err(Error::Other("trying to distribute data without validator_signer"));
        };
        let me = signer.validator_id();
        let block = self.chain_store.get_block(data_id.block_hash())?;
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
        let (boxed_parts, encoded_length) = encoder.encode(data);
        debug_assert_eq!(boxed_parts.len(), producers.len());

        let parts: Vec<&[u8]> =
            boxed_parts.iter().map(|x| x.as_deref().unwrap()).collect::<Vec<_>>();
        let (merkle_root, merkle_proofs) = merklize(&parts);
        // TODO(spice): As an optimization we should be able to avoid serializing data both in
        // encode and to compute hash.
        let data_hash = hash(&borsh::to_vec(&data).unwrap());
        let commitment = SpiceDataCommitment {
            hash: data_hash,
            root: merkle_root,
            encoded_length: encoded_length as u64,
        };

        debug_assert_eq!(boxed_parts.len(), merkle_proofs.len());
        let spice_parts = boxed_parts
            .into_iter()
            .zip(merkle_proofs)
            .enumerate()
            .map(|(part_ord, (boxed_part, merkle_proof))| SpiceDataPart {
                part_ord: part_ord as u64,
                part: boxed_part.unwrap(),
                merkle_proof,
            })
            .collect_vec();

        let my_part = spice_parts[me_ord].clone();

        self.recent_distribution_data.push(
            data_id.clone(),
            RecentDistributionData { parts: spice_parts, commitment: commitment.clone() },
        );

        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::SpicePartialData {
                partial_data: SpicePartialData::new(data_id, commitment, vec![my_part], &signer),
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
                let validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
                    epoch_id,
                    *shard_id,
                    block.header().height(),
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

    pub(crate) fn receive_data(&mut self, data: SpicePartialData) -> Result<(), ReceiveDataError> {
        let block_hash = data.block_hash();
        let block = match self.chain_store.get_block(block_hash) {
            Ok(block) => block,
            Err(near_chain::Error::DBNotFoundErr(_)) => {
                return self
                    .add_pending_partial_data(data)
                    .map_err(ReceiveDataError::ReceivingDataWithoutBlock);
            }
            Err(err) => return Err(err.into()),
        };
        self.receive_data_with_block(data, &block).map_err(ReceiveDataError::ReceivingDataWithBlock)
    }

    fn add_pending_partial_data(&mut self, data: SpicePartialData) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            return Err(Error::Other("cannot receive data without validator_signer"));
        };
        let me = signer.validator_id();

        let possible_epoch_ids = self.possible_epoch_ids(data.block_hash())?;
        let validator =
            self.get_sender_validator_from_possible_epoch_ids(&possible_epoch_ids, data.sender())?;

        let data =
            data.into_verified(validator.public_key()).ok_or(Error::InvalidPartialDataSignature)?;

        let id = &data.id;
        let sender = &data.sender;
        if !self.possible_producers(id, &possible_epoch_ids)?.contains(sender) {
            return Err(Error::SenderIsNotProducer);
        }
        if !self.is_pending_data_needed(me, id, &possible_epoch_ids)? {
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
        partial_data: SpicePartialData,
        block: &Block,
    ) -> Result<(), Error> {
        let sender_validator = self
            .epoch_manager
            .get_validator_by_account_id(block.header().epoch_id(), partial_data.sender())?;
        let partial_data = partial_data
            .into_verified(sender_validator.public_key())
            .ok_or(Error::InvalidPartialDataSignature)?;

        self.receive_verified_data_with_block(partial_data, block)
    }

    fn receive_verified_data_with_block(
        &mut self,
        SpiceVerifiedPartialData { id, commitment, parts, sender }: SpiceVerifiedPartialData,
        block: &Block,
    ) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            return Err(Error::Other("cannot receive data without validator_signer"));
        };
        let me = signer.validator_id();

        self.verify_data_id(&id, block)?;
        let (_recipients, producers) = self.recipients_and_producers(&id, block)?;
        if !producers.contains(&sender) {
            return Err(Error::SenderIsNotProducer);
        }
        if !self.is_data_needed(&id, &block)? {
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
                            // TODO(spice): Handle the possibility of receiving invalid receipts in
                            // which case we would need to request them again from different
                            // producer(s).
                            self.waiting_on_data.remove(&id);
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

                            // TODO(spice): Handle the possibility of receiving invalid witness in
                            // which case we would need to request them again from different
                            // producer(s).
                            self.waiting_on_data.remove(&id);
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
                return Err(DataIsKnownError::decoded(&data_parts_key.0).into());
            }
        }
        let id = &data_parts_key.0;
        self.verify_data_is_unknown(me, block, id)
    }

    fn verify_data_is_unknown(
        &self,
        me: &AccountId,
        block: &Block,
        id: &SpiceDataIdentifier,
    ) -> Result<(), Error> {
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
                    return Err(DataIsKnownError::ReceiptsKnown {
                        from_shard_id: *from_shard_id,
                        to_shard_id: *to_shard_id,
                    }
                    .into());
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
                    return Err(DataIsKnownError::WitnessValidated(*shard_id).into());
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

    fn get_sender_validator_from_possible_epoch_ids(
        &self,
        possible_epoch_ids: &[EpochId],
        sender: &AccountId,
    ) -> Result<ValidatorStake, Error> {
        for epoch_id in possible_epoch_ids {
            if let Ok(validator) = self.epoch_manager.get_validator_by_account_id(&epoch_id, sender)
            {
                return Ok(validator);
            }
        }
        Err(Error::SenderIsNotValidator)
    }

    fn possible_epoch_ids(&self, block_hash: &CryptoHash) -> Result<Vec<EpochId>, Error> {
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

    fn is_data_needed(&self, id: &SpiceDataIdentifier, block: &Block) -> Result<bool, Error> {
        let signer = self.validator_signer.get();
        let me = signer.as_ref().map(|signer| signer.validator_id());
        match id {
            SpiceDataIdentifier::Witness { block_hash, shard_id } => {
                assert_eq!(block_hash, block.hash());
                let Some(me) = me else {
                    return Ok(false);
                };
                let validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
                    block.header().epoch_id(),
                    *shard_id,
                    block.header().height(),
                )?;
                Ok(validator_assignments.contains(&me))
            }
            SpiceDataIdentifier::ReceiptProof { to_shard_id, .. } => {
                // We need a receipts from a block only if we would want to apply a block after.
                let prev_hash = block.hash();
                Ok(self.shard_tracker.should_apply_chunk(
                    ApplyChunksMode::IsCaughtUp,
                    prev_hash,
                    *to_shard_id,
                ))
            }
        }
    }

    fn is_pending_data_needed(
        &self,
        me: &AccountId,
        id: &SpiceDataIdentifier,
        possible_epoch_ids: &[EpochId],
    ) -> Result<bool, Error> {
        for epoch_id in possible_epoch_ids {
            match id {
                SpiceDataIdentifier::Witness { .. } => {
                    let epoch_info = self.epoch_manager.get_epoch_info(epoch_id)?;
                    if epoch_info
                        .validators_iter()
                        .map(|stake| stake.take_account_id())
                        .contains(me)
                    {
                        return Ok(true);
                    }
                }
                SpiceDataIdentifier::ReceiptProof { to_shard_id, .. } => {
                    // TODO(spice): Use information in shard_tracker and epoch manager to assess if we
                    // need this data.
                    let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
                    if shard_layout.shard_ids().contains(to_shard_id) {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    fn process_pending_partial_data(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let ready_data = self.pending_partial_data.pop(&block_hash).unwrap_or_default();
        if ready_data.is_empty() {
            return Ok(());
        }
        let block = self.chain_store.get_block(&block_hash)?;
        for data in ready_data {
            let data_id = data.id.clone();
            let commitment = data.commitment.clone();
            if let Err(err) = self.receive_verified_data_with_block(data, &block) {
                if let Error::DataIsKnown(err) = err {
                    self.waiting_on_data.remove(&data_id);
                    tracing::debug!(target: "spice_data_distribution", ?err, ?data_id, ?commitment, "processing data we already have");
                } else {
                    tracing::error!(target: "spice_data_distribution", ?err, ?data_id, ?commitment, "failed to process partial data");
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn pending_partial_data_size(&self) -> usize {
        self.pending_partial_data.len()
    }

    // TODO(spice): Do not request data we already decoded.
    // TODO(spice): Implement a state machine to track all the data we produce or may need. This
    // would help make sure that we cannot have and request data at the same time.
    fn start_waiting_on_data(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let signer = self.validator_signer.get();
        let me = signer.as_ref().map(|signer| signer.validator_id());
        // TODO(spice): Allow requesting data without signer using route back.
        let Some(me) = me else {
            return Ok(());
        };

        let block = self.chain_store.get_block(block_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&block.header().epoch_id())?;

        let shards_we_apply: HashSet<ShardId> = shard_layout
            .shard_ids()
            .filter(|shard_id| {
                // We need a receipts from a block only if we would want to apply a block after.
                let prev_hash = block.hash();
                self.shard_tracker.should_apply_chunk(
                    ApplyChunksMode::IsCaughtUp,
                    prev_hash,
                    *shard_id,
                )
            })
            .collect();

        for shard_id in shard_layout.shard_ids() {
            // If we will apply chunk we will also produce endorsement so no need to request
            // witness from elsewhere.
            if shards_we_apply.contains(&shard_id) {
                continue;
            }

            let validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
                block.header().epoch_id(),
                shard_id,
                block.header().height(),
            )?;
            if validator_assignments.contains(me) {
                self.waiting_on_data
                    .insert(SpiceDataIdentifier::Witness { block_hash: *block_hash, shard_id });
            }
        }

        for from_shard_id in shard_layout.shard_ids() {
            if shards_we_apply.contains(&from_shard_id) {
                continue;
            }
            for to_shard_id in shards_we_apply.iter().copied() {
                self.waiting_on_data.insert(SpiceDataIdentifier::ReceiptProof {
                    block_hash: *block_hash,
                    from_shard_id,
                    to_shard_id,
                });
            }
        }
        Ok(())
    }

    fn schedule_data_fetching(&self, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.request_waiting_on_data();

        ctx.run_later(
            "SpiceDataDistributorActor request waiting on data",
            // TODO(spice): Make duration configurable.
            Duration::milliseconds(1000),
            move |act, ctx| {
                act.schedule_data_fetching(ctx);
            },
        );
    }

    fn request_waiting_on_data(&self) {
        // TODO(spice): Allow requesting data without signer using route back.
        let Some(signer) = self.validator_signer.get() else {
            tracing::debug!(target: "spice_data_distribution", "no validator signer to request waiting on data");
            return;
        };
        let me = signer.validator_id();
        for id in &self.waiting_on_data {
            let block = self
                .chain_store
                .get_block(id.block_hash())
                .expect("block for which we wait on data should always be available");
            let (_recipients, mut producers) = self.recipients_and_producers(&id, &block).expect(
                "producers and recipients that we wait on data for should always be available",
            );
            assert!(!producers.contains(me));
            assert!(!producers.is_empty());

            // TODO(spice): Implement requesting only the parts we are still missing from random
            // producers.
            // TODO(spice): Request data only we know may be available. (For example based on
            // execution and certification heads.)
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::SpicePartialDataRequest {
                    request: SpicePartialDataRequest { data_id: id.clone(), requester: me.clone() },
                    producer: producers.swap_remove(0),
                },
            ));
        }
    }

    fn handle_partial_data_request(
        &mut self,
        SpicePartialDataRequest { data_id, requester }: SpicePartialDataRequest,
    ) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            return Err(Error::Other(
                "without validator signer we cannot handle partial data requests",
            ));
        };

        let Some(data) = self.recent_distribution_data.get(&data_id) else {
            // TODO(spice): Make sure we send requests for data only after we know it may be
            // available and make this into error.
            tracing::debug!(target:"spice_data_distribution", ?data_id, ?requester,"received request for unknown data");
            return Ok(());
        };
        // TODO(spice): Check that requester is one of the recipients and implement a
        // lower-priority way for other nodes that aren't validators (e.g. rpc nodes) to get
        // data they require.

        let recipients = HashSet::from([requester]);
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::SpicePartialData {
                partial_data: SpicePartialData::new(
                    data_id,
                    data.commitment.clone(),
                    data.parts.clone(),
                    &signer,
                ),
                recipients,
            },
        ));
        Ok(())
    }
}
