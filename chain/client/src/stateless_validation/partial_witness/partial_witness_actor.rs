use super::encoding::CONTRACT_DEPLOYS_RATIO_DATA_PARTS;
pub use super::encoding::WITNESS_RATIO_DATA_PARTS;
use super::partial_deploys_tracker::PartialEncodedContractDeploysTracker;
use super::partial_witness_tracker::PartialEncodedStateWitnessTracker;
use crate::metrics;
use crate::stateless_validation::chunk_validation_actor::ChunkValidationSenderForPartialWitness;
use crate::stateless_validation::contracts_cache_contains_contract;
use crate::stateless_validation::state_witness_tracker::ChunkStateWitnessTracker;
use crate::stateless_validation::validate::{
    ChunkRelevance, validate_chunk_contract_accesses, validate_contract_code_request,
    validate_partial_encoded_contract_deploys, validate_partial_encoded_state_witness,
};
use itertools::Itertools;
use lru::LruCache;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{Actor, CanSend, Handler, Sender};
use near_async::time::Clock;
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::Error;
use near_chain::types::RuntimeAdapter;
use near_chain_configs::MutableValidatorSigner;
use near_client_primitives::types::BlockNotificationMessage;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_network::state_witness::{
    ChunkContractAccessesMessage, ChunkStateWitnessAckMessage, ContractCodeRequestMessage,
    ContractCodeResponseMessage, PartialEncodedContractDeploysMessage,
    PartialEncodedStateWitnessForwardMessage, PartialEncodedStateWitnessMessage,
};
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::{
    REED_SOLOMON_MAX_PARTS, ReedSolomonEncoder, ReedSolomonEncoderCache,
};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractAccesses, ChunkContractDeploys, CodeBytes, CodeHash, ContractCodeRequest,
    ContractCodeResponse, ContractUpdates, MainTransitionKey, PartialEncodedContractDeploys,
    PartialEncodedContractDeploysPart,
};
use near_primitives::stateless_validation::partial_witness::VersionedPartialEncodedStateWitness;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessAck, EncodedChunkStateWitness,
};
use near_primitives::stateless_validation::stored_chunk_state_transition_data::StoredChunkStateTransitionData;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::utils::compression::CompressedData;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::ProtocolVersion;
use near_store::adapter::trie_store::TrieStoreAdapter;
use near_store::{DBCol, StorageError, TrieDBStorage, TrieStorage};
use near_vm_runner::ContractCode;
use parking_lot::Mutex;
use rand::Rng;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

const PROCESSED_CONTRACT_CODE_REQUESTS_CACHE_SIZE: usize = 30;

/// Capacity (in distinct `prev_block_hash` keys) of the small cache used to
/// defer V2 witnesses whose prev block is not yet in `DBCol::ChunkProducers`.
const PENDING_V2_WITNESS_CACHE_SIZE: usize = 64;

/// Small cache for V2 witnesses deferred when their `prev_block_hash` is not
/// yet in `DBCol::ChunkProducers`. Replayed on `BlockNotificationMessage`.
/// PR 4a replaces this with a proper bounded LRU plus per-chunk bucketing
/// and observability. For now this is a minimal implementation that exists
/// only to keep liveness during EarlyKickout rollout and epoch-sync edge
/// cases. Witnesses for orphaned forks expire naturally via LRU eviction —
/// the producer retransmits on the canonical fork.
struct PendingV2WitnessCache {
    /// Keyed by `prev_block_hash`. Each entry is the list of deferred
    /// witnesses queued for that block (covers multiple parts/validators).
    entries: LruCache<CryptoHash, Vec<VersionedPartialEncodedStateWitness>>,
}

impl PendingV2WitnessCache {
    fn new() -> Self {
        Self { entries: LruCache::new(NonZeroUsize::new(PENDING_V2_WITNESS_CACHE_SIZE).unwrap()) }
    }

    fn insert(
        &mut self,
        prev_block_hash: CryptoHash,
        witness: VersionedPartialEncodedStateWitness,
    ) {
        if let Some(pending) = self.entries.get_mut(&prev_block_hash) {
            pending.push(witness);
            return;
        }
        self.entries.put(prev_block_hash, vec![witness]);
    }

    fn drain(&mut self, prev_block_hash: &CryptoHash) -> Vec<VersionedPartialEncodedStateWitness> {
        self.entries.pop(prev_block_hash).unwrap_or_default()
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

pub struct PartialWitnessActor {
    /// Adapter to send messages to the network.
    network_adapter: PeerManagerAdapter,
    /// Validator signer to sign the state witness. This field is mutable and optional. Use with caution!
    /// Lock the value of mutable validator signer for the duration of a request to ensure consistency.
    /// Please note that the locked value should not be stored anywhere or passed through the thread boundary.
    my_signer: MutableValidatorSigner,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime: Arc<dyn RuntimeAdapter>,
    /// Tracks the parts of the state witness sent from chunk producers to chunk validators.
    partial_witness_tracker: Arc<PartialEncodedStateWitnessTracker>,
    partial_deploys_tracker: PartialEncodedContractDeploysTracker,
    /// Tracks a collection of state witnesses sent from chunk producers to chunk validators.
    state_witness_tracker: Arc<Mutex<ChunkStateWitnessTracker>>,
    /// Reed Solomon encoder for encoding state witness parts.
    /// We keep one wrapper for each length of chunk_validators to avoid re-creating the encoder.
    witness_encoders: ReedSolomonEncoderCache,
    /// Same as above for contract deploys.
    contract_deploys_encoders: ReedSolomonEncoderCache,
    compile_contracts_spawner: Arc<dyn AsyncComputationSpawner>,
    partial_witness_spawner: Arc<dyn AsyncComputationSpawner>,
    /// Spawner for witness creation tasks.
    witness_creation_spawner: Arc<dyn AsyncComputationSpawner>,
    /// AccountId in the key corresponds to the requester (chunk validator).
    processed_contract_code_requests: LruCache<(ChunkProductionKey, AccountId), ()>,
    /// V2 witnesses deferred on `DBCol::ChunkProducers` miss. Drained on
    /// `BlockNotificationMessage`. See `PendingV2WitnessCache`.
    pending_v2_witnesses: Arc<Mutex<PendingV2WitnessCache>>,
}

impl Actor for PartialWitnessActor {}

#[derive(Debug)]
pub struct DistributeStateWitnessRequest {
    pub state_witness: ChunkStateWitness,
    pub contract_updates: ContractUpdates,
    pub main_transition_shard_id: ShardId,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct PartialWitnessSenderForClient {
    pub distribute_chunk_state_witness: Sender<DistributeStateWitnessRequest>,
    pub block_notification: Sender<BlockNotificationMessage>,
}

impl Handler<DistributeStateWitnessRequest> for PartialWitnessActor {
    fn handle(&mut self, msg: DistributeStateWitnessRequest) {
        if let Err(err) = self.handle_distribute_state_witness_request(msg) {
            tracing::error!(target: "client", ?err, "failed to handle distribute chunk state witness request");
        }
    }
}

impl Handler<ChunkStateWitnessAckMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: ChunkStateWitnessAckMessage) {
        self.handle_chunk_state_witness_ack(msg.0);
    }
}

impl Handler<PartialEncodedStateWitnessMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: PartialEncodedStateWitnessMessage) {
        if let Err(err) = self.handle_partial_encoded_state_witness(msg.0) {
            tracing::error!(target: "client", ?err, "failed to handle partial encoded state witness message");
        }
    }
}

impl Handler<PartialEncodedStateWitnessForwardMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: PartialEncodedStateWitnessForwardMessage) {
        if let Err(err) = self.handle_partial_encoded_state_witness_forward(msg.0) {
            tracing::error!(target: "client", ?err, "failed to handle partial encoded state witness forward message");
        }
    }
}

impl Handler<ChunkContractAccessesMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: ChunkContractAccessesMessage) {
        if let Err(err) = self.handle_chunk_contract_accesses(msg.0) {
            tracing::error!(target: "client", ?err, "failed to handle chunk contract accesses message");
        }
    }
}

impl Handler<PartialEncodedContractDeploysMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: PartialEncodedContractDeploysMessage) {
        if let Err(err) = self.handle_partial_encoded_contract_deploys(msg.0) {
            tracing::error!(target: "client", ?err, "failed to handle partial encoded contract deploys message");
        }
    }
}

impl Handler<ContractCodeRequestMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: ContractCodeRequestMessage) {
        if let Err(err) = self.handle_contract_code_request(msg.0) {
            tracing::error!(target: "client", ?err, "failed to handle contract code request message");
        }
    }
}

impl Handler<ContractCodeResponseMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: ContractCodeResponseMessage) {
        if let Err(err) = self.handle_contract_code_response(msg.0) {
            tracing::error!(target: "client", ?err, "failed to handle contract code response message");
        }
    }
}

impl Handler<BlockNotificationMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: BlockNotificationMessage) {
        let BlockNotificationMessage { block } = msg;
        self.handle_block_notification(block.hash());
    }
}

impl PartialWitnessActor {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        chunk_validation_sender: ChunkValidationSenderForPartialWitness,
        my_signer: MutableValidatorSigner,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime: Arc<dyn RuntimeAdapter>,
        compile_contracts_spawner: Arc<dyn AsyncComputationSpawner>,
        partial_witness_spawner: Arc<dyn AsyncComputationSpawner>,
        witness_creation_spawner: Arc<dyn AsyncComputationSpawner>,
    ) -> Self {
        let partial_witness_tracker = Arc::new(PartialEncodedStateWitnessTracker::new(
            chunk_validation_sender,
            epoch_manager.clone(),
        ));
        Self {
            network_adapter,
            my_signer,
            epoch_manager,
            partial_witness_tracker,
            partial_deploys_tracker: PartialEncodedContractDeploysTracker::new(),
            state_witness_tracker: Arc::new(Mutex::new(ChunkStateWitnessTracker::new(clock))),
            runtime,
            witness_encoders: ReedSolomonEncoderCache::new(WITNESS_RATIO_DATA_PARTS),
            contract_deploys_encoders: ReedSolomonEncoderCache::new(
                CONTRACT_DEPLOYS_RATIO_DATA_PARTS,
            ),
            compile_contracts_spawner,
            partial_witness_spawner,
            witness_creation_spawner,
            processed_contract_code_requests: LruCache::new(
                NonZeroUsize::new(PROCESSED_CONTRACT_CODE_REQUESTS_CACHE_SIZE).unwrap(),
            ),
            pending_v2_witnesses: Arc::new(Mutex::new(PendingV2WitnessCache::new())),
        }
    }

    fn handle_distribute_state_witness_request(
        &mut self,
        msg: DistributeStateWitnessRequest,
    ) -> Result<(), Error> {
        let DistributeStateWitnessRequest {
            state_witness,
            contract_updates: ContractUpdates { contract_accesses, contract_deploys },
            main_transition_shard_id,
        } = msg;

        let _span = tracing::debug_span!(
            target: "client",
            "distribute_chunk_state_witness",
            chunk_hash=?state_witness.chunk_header().chunk_hash(),
            height=state_witness.chunk_header().height_created(),
            shard_id=%state_witness.chunk_header().shard_id(),
            tag_block_production=true,
        )
        .entered();

        // We send the state-witness and contract-updates in the following order:
        // 1. We send the hashes of the contract code accessed (if contract code is excluded from witness and any contracts are called)
        //    before the state witness in order to allow validators to check and request missing contract code, while waiting for witness parts.
        // 2. We send the state witness parts to witness-part owners.
        // 3. We send the contract deploys parts to other validators (that do not validate the witness in this turn). This is lower priority
        //    since the newly-deployed contracts will be needed by other validators in later turns.

        let signer = self.my_validator_signer()?;
        let key = state_witness.chunk_production_key();
        let chunk_validators = self
            .epoch_manager
            .get_chunk_validator_assignments(&key.epoch_id, key.shard_id, key.height_created)
            .expect("Chunk validators must be defined")
            .ordered_chunk_validators();

        if !contract_accesses.is_empty() {
            self.send_contract_accesses_to_chunk_validators(
                key.clone(),
                contract_accesses,
                MainTransitionKey {
                    block_hash: state_witness.main_state_transition().block_hash,
                    shard_id: main_transition_shard_id,
                },
                &chunk_validators,
                &signer,
            );
        }

        // Move witness compression, encoding and distribution to the witness_creation_spawner
        // to avoid blocking the actor thread
        let encoder = self.witness_encoders.entry(chunk_validators.len());
        let network_adapter = self.network_adapter.clone();
        let state_witness_tracker = self.state_witness_tracker.clone();
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(&key.epoch_id)?;

        self.witness_creation_spawner.spawn("compress_and_distribute_witness", move || {
            if let Err(err) = Self::compress_and_distribute_witness(
                state_witness,
                chunk_validators,
                (*signer).clone(),
                network_adapter,
                state_witness_tracker,
                encoder,
                protocol_version,
            ) {
                tracing::error!(target: "client", ?err, "failed to compress and distribute chunk state witness");
            }
        });

        if !contract_deploys.is_empty() {
            self.send_chunk_contract_deploys_parts(key, contract_deploys)?;
        }
        Ok(())
    }

    fn compress_and_distribute_witness(
        state_witness: ChunkStateWitness,
        chunk_validators: Vec<AccountId>,
        signer: ValidatorSigner,
        network_adapter: PeerManagerAdapter,
        state_witness_tracker: Arc<Mutex<ChunkStateWitnessTracker>>,
        encoder: Arc<ReedSolomonEncoder>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), Error> {
        let witness_bytes = compress_witness(&state_witness)?;

        Self::send_state_witness_parts(
            encoder,
            *state_witness.epoch_id(),
            state_witness.chunk_header(),
            witness_bytes,
            &chunk_validators,
            &signer,
            &network_adapter,
            &state_witness_tracker,
            protocol_version,
        );

        Ok(())
    }

    fn generate_contract_deploys_parts(
        &mut self,
        key: &ChunkProductionKey,
        deploys: ChunkContractDeploys,
    ) -> Result<Vec<(AccountId, PartialEncodedContractDeploys)>, Error> {
        let part_owners = self.ordered_contract_deploys_part_owners(key)?;
        // Note that target validators do not include the chunk producers, and thus in some case
        // (eg. tests or small networks) there may be no other validators to send the new contracts to.
        if part_owners.is_empty() {
            return Ok(vec![]);
        }

        let encoder = self.contract_deploys_encoder(part_owners.len());
        let (parts, encoded_length) = encoder.encode(&deploys);
        let signer = self.my_validator_signer()?;

        Ok(part_owners
            .into_par_iter()
            .zip_eq(parts)
            .enumerate()
            .map(|(part_ord, (validator, part))| {
                let partial_deploys = PartialEncodedContractDeploys::new(
                    key.clone(),
                    PartialEncodedContractDeploysPart {
                        part_ord,
                        data: part.unwrap(),
                        encoded_length,
                    },
                    &signer,
                );
                (validator, partial_deploys)
            })
            .collect())
    }

    // Break the state witness into parts and send each part to the corresponding chunk validator owner.
    // The chunk validator owner will then forward the part to all other chunk validators.
    // Each chunk validator would collect the parts and reconstruct the state witness.
    fn send_state_witness_parts(
        encoder: Arc<ReedSolomonEncoder>,
        epoch_id: EpochId,
        chunk_header: &ShardChunkHeader,
        witness_bytes: EncodedChunkStateWitness,
        chunk_validators: &[AccountId],
        signer: &ValidatorSigner,
        network_adapter: &PeerManagerAdapter,
        state_witness_tracker: &Arc<Mutex<ChunkStateWitnessTracker>>,
        protocol_version: ProtocolVersion,
    ) {
        let _span = tracing::debug_span!(
            target: "client",
            "send_state_witness_parts",
            chunk_hash = ?chunk_header.chunk_hash(),
            height = %chunk_header.height_created(),
            shard_id = %chunk_header.shard_id(),
            tag_witness_distribution = true,
        )
        .entered();

        // Capture these values first, as the sources are consumed before calling record_witness_sent.
        let chunk_hash = chunk_header.chunk_hash();
        let witness_size_in_bytes = witness_bytes.size_bytes();

        // Record time taken to encode the state witness parts.
        let shard_id_label = chunk_header.shard_id().to_string();
        let encode_timer = metrics::PARTIAL_WITNESS_ENCODE_TIME
            .with_label_values(&[shard_id_label.as_str()])
            .start_timer();
        let validator_witness_tuple = generate_state_witness_parts(
            encoder,
            epoch_id,
            chunk_header,
            witness_bytes,
            chunk_validators,
            signer,
            protocol_version,
        );
        encode_timer.observe_duration();

        // Count one emit per (chunk_validator, part) with the wire version
        // derived from the first entry — all parts generated in a single
        // distribution share the same variant.
        if let Some((_, first)) = validator_witness_tuple.first() {
            let version = match first {
                VersionedPartialEncodedStateWitness::V1(_) => "v1",
                VersionedPartialEncodedStateWitness::V2(_) => "v2",
            };
            metrics::PARTIAL_WITNESS_PART_MESSAGES_EMITTED_TOTAL
                .with_label_values(&[shard_id_label.as_str(), version])
                .inc_by(validator_witness_tuple.len() as u64);
        }

        // Record the witness in order to match the incoming acks for measuring round-trip times.
        // See process_chunk_state_witness_ack for the handling of the ack messages.
        state_witness_tracker.lock().record_witness_sent(
            chunk_hash.clone(),
            witness_size_in_bytes,
            validator_witness_tuple.len(),
        );

        // Send the parts to the corresponding chunk validator owners.
        network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::PartialEncodedStateWitness(validator_witness_tuple),
        ));
    }

    /// Function to handle receiving partial_encoded_state_witness message from chunk producer.
    fn handle_partial_encoded_state_witness(
        &self,
        partial_witness: VersionedPartialEncodedStateWitness,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(
            target: "client",
            "handle_partial_encoded_state_witness",
            height = partial_witness.chunk_production_key().height_created,
            shard_id = %partial_witness.chunk_production_key().shard_id,
            part_ord = partial_witness.part_ord(),
            tag_witness_distribution = true,
        )
        .entered();
        tracing::debug!(target: "client", ?partial_witness, "received partial encoded state witness message");
        let signer = self.my_validator_signer()?;
        let validator_account_id = signer.validator_id().clone();
        let epoch_manager = self.epoch_manager.clone();
        let runtime_adapter = self.runtime.clone();

        let ChunkProductionKey { shard_id, epoch_id, height_created } =
            partial_witness.chunk_production_key();

        let version_label = match &partial_witness {
            VersionedPartialEncodedStateWitness::V1(_) => "v1",
            VersionedPartialEncodedStateWitness::V2(_) => "v2",
        };
        metrics::PARTIAL_WITNESS_PART_MESSAGES_RECEIVED_TOTAL
            .with_label_values(&[shard_id.to_string().as_str(), version_label])
            .inc();

        // V1 witnesses resolve the chunk producer via the epoch-based sampler;
        // V2 witnesses carry `prev_block_hash` and use the DB-backed hash
        // lookup. On `ChunkProducerNotInDB` for V2, defer the witness: the
        // prev block hasn't populated `DBCol::ChunkProducers` yet.
        let chunk_producer_info = match &partial_witness {
            VersionedPartialEncodedStateWitness::V1(_) => {
                self.epoch_manager.get_chunk_producer_info(&ChunkProductionKey {
                    epoch_id,
                    height_created,
                    shard_id,
                })
            }
            VersionedPartialEncodedStateWitness::V2(v2) => {
                self.epoch_manager.get_chunk_producer_info_db(v2.prev_block_hash(), shard_id)
            }
        };
        let chunk_producer = match chunk_producer_info {
            Ok(info) => info.take_account_id(),
            Err(EpochError::ChunkProducerNotInDB(_, _)) => {
                self.defer_pending_v2_witness(partial_witness);
                return Ok(());
            }
            Err(err) => return Err(err.into()),
        };

        // Forward witness part to chunk validators except the validator that produced the chunk and witness.
        let target_chunk_validators = self
            .epoch_manager
            .get_chunk_validator_assignments(&epoch_id, shard_id, height_created)?
            .ordered_chunk_validators()
            .into_iter()
            .filter(|validator| validator != &chunk_producer)
            .collect_vec();

        let network_adapter = self.network_adapter.clone();
        let partial_witness_tracker = self.partial_witness_tracker.clone();
        let pending_v2_witnesses = self.pending_v2_witnesses.clone();

        self.partial_witness_spawner.spawn("handle_partial_encoded_state_witness", move || {
            // Validate the partial encoded state witness and forward the part to all the chunk validators.
            match validate_partial_encoded_state_witness(
                epoch_manager.as_ref(),
                &partial_witness,
                &validator_account_id,
                runtime_adapter.store(),
            ) {
                Ok(ChunkRelevance::Relevant) => {
                    // Forward to other validators (excluding ourselves to avoid duplicate processing).
                    let other_validators: Vec<_> = target_chunk_validators
                        .into_iter()
                        .filter(|validator| validator != &validator_account_id)
                        .collect();

                    if !other_validators.is_empty() {
                        network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                            NetworkRequests::PartialEncodedStateWitnessForward(
                                other_validators,
                                partial_witness.clone(),
                            ),
                        ));
                    }
                    // Store the part locally (as part owner) to avoid need for self-forwarding.
                    if let Err(err) = partial_witness_tracker.store_partial_encoded_state_witness(partial_witness) {
                        tracing::error!(target: "client", ?err, "failed to store partial encoded state witness");
                    }
                }
                Ok(_) => {
                    tracing::debug!(
                        target: "client",
                        chunk_production_key = ?partial_witness.chunk_production_key(),
                        "received irrelevant partial encoded state witness",
                    );
                }
                Err(Error::DBNotFoundErr(_)) => {
                    // V2 witness whose prev block hasn't populated
                    // `DBCol::ChunkProducers` yet. Defer and replay on
                    // block arrival. V1 witnesses can't reach this path
                    // because the epoch-based lookup is purely in-memory.
                    if let Some(prev_block_hash) = partial_witness.prev_block_hash().copied() {
                        let mut cache = pending_v2_witnesses.lock();
                        cache.insert(prev_block_hash, partial_witness);
                        metrics::PARTIAL_WITNESS_PENDING_CACHE_SIZE.set(cache.len() as i64);
                    } else {
                        tracing::warn!(
                            target: "client",
                            "DBNotFoundErr on V1 witness validation, unexpected",
                        );
                    }
                }
                Err(err) => {
                    // TODO: ban sending peer
                    tracing::warn!(
                        target: "client",
                        chunk_production_key = ?partial_witness.chunk_production_key(),
                        ?err,
                        "received invalid partial encoded state witness"
                    );
                }
            }
        });

        Ok(())
    }

    /// Insert a V2 witness into the pending pool keyed by its
    /// `prev_block_hash`. Called when `DBCol::ChunkProducers` doesn't yet
    /// contain an entry for the prev block. The witness is replayed from
    /// `handle_block_notification`.
    fn defer_pending_v2_witness(&self, partial_witness: VersionedPartialEncodedStateWitness) {
        let Some(prev_block_hash) = partial_witness.prev_block_hash().copied() else {
            // V1 witnesses never reach this path — the epoch-based sampler
            // is purely in-memory and can't return `ChunkProducerNotInDB`.
            tracing::warn!(
                target: "client",
                "attempted to defer a V1 witness; ignoring",
            );
            return;
        };
        tracing::debug!(
            target: "client",
            ?prev_block_hash,
            chunk_production_key = ?partial_witness.chunk_production_key(),
            "deferring V2 partial witness until prev block is processed",
        );
        let mut cache = self.pending_v2_witnesses.lock();
        cache.insert(prev_block_hash, partial_witness);
        metrics::PARTIAL_WITNESS_PENDING_CACHE_SIZE.set(cache.len() as i64);
    }

    /// Drain and replay any V2 witnesses deferred on this block's hash.
    fn handle_block_notification(&self, block_hash: &CryptoHash) {
        let pending = {
            let mut cache = self.pending_v2_witnesses.lock();
            let pending = cache.drain(block_hash);
            metrics::PARTIAL_WITNESS_PENDING_CACHE_SIZE.set(cache.len() as i64);
            pending
        };
        if pending.is_empty() {
            return;
        }
        tracing::debug!(
            target: "client",
            ?block_hash,
            count = pending.len(),
            "replaying deferred V2 partial witnesses",
        );
        for witness in pending {
            if let Err(err) = self.handle_partial_encoded_state_witness(witness) {
                tracing::warn!(
                    target: "client",
                    ?err,
                    ?block_hash,
                    "failed to replay deferred V2 partial witness",
                );
            }
        }
    }

    /// Function to handle receiving partial_encoded_state_witness_forward message from chunk producer.
    fn handle_partial_encoded_state_witness_forward(
        &self,
        partial_witness: VersionedPartialEncodedStateWitness,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(
            target: "client",
            "handle_partial_encoded_state_witness_forward",
            height = partial_witness.chunk_production_key().height_created,
            shard_id = %partial_witness.chunk_production_key().shard_id,
            part_ord = partial_witness.part_ord(),
            tag_witness_distribution = true,
        )
        .entered();
        tracing::debug!(target: "client", ?partial_witness, "received partial encoded state witness forward message");

        let version_label = match &partial_witness {
            VersionedPartialEncodedStateWitness::V1(_) => "v1",
            VersionedPartialEncodedStateWitness::V2(_) => "v2",
        };
        let shard_id_label = partial_witness.chunk_production_key().shard_id.to_string();
        metrics::PARTIAL_WITNESS_PART_MESSAGES_RECEIVED_TOTAL
            .with_label_values(&[shard_id_label.as_str(), version_label])
            .inc();

        let signer = self.my_validator_signer()?;
        let validator_account_id = signer.validator_id().clone();
        let partial_witness_tracker = self.partial_witness_tracker.clone();
        let epoch_manager = self.epoch_manager.clone();
        let runtime_adapter = self.runtime.clone();
        self.partial_witness_spawner.spawn(
            "handle_partial_encoded_state_witness_forward",
            move || {
                // Validate the partial encoded state witness and store the partial encoded state witness.
                match validate_partial_encoded_state_witness(
                    epoch_manager.as_ref(),
                    &partial_witness,
                    &validator_account_id,
                    runtime_adapter.store(),
                ) {
                    Ok(ChunkRelevance::Relevant) => {
                        if let Err(err) = partial_witness_tracker.store_partial_encoded_state_witness(partial_witness) {
                            tracing::error!(target: "client", ?err, "failed to store partial encoded state witness");
                        }
                    }
                    Ok(_) => {
                        tracing::debug!(
                            target: "client",
                            chunk_production_key = ?partial_witness.chunk_production_key(),
                            "received irrelevant partial encoded state witness",
                        );
                    }
                    Err(err) => {
                        // TODO: ban sending peer
                        tracing::warn!(
                            target: "client",
                            chunk_production_key = ?partial_witness.chunk_production_key(),
                            ?err,
                            "received invalid partial encoded state witness"
                        );
                    }
                }
            },
        );

        Ok(())
    }

    /// Handles partial contract deploy message received from a peer.
    ///
    /// This message may belong to one of two steps of distributing contract code. In the first step the code is compressed
    /// and encoded into parts using Reed Solomon encoding and each part is sent to one of the validators (part owner).
    /// See `send_chunk_contract_deploys_parts` for the code implementing this. In the second step each validator (part-owner)
    /// forwards the part it receives to other validators.
    fn handle_partial_encoded_contract_deploys(
        &mut self,
        partial_deploys: PartialEncodedContractDeploys,
    ) -> Result<(), Error> {
        tracing::debug!(target: "client", ?partial_deploys, "received partial encoded contract deploys");
        if !validate_partial_encoded_contract_deploys(
            self.epoch_manager.as_ref(),
            &partial_deploys,
            self.runtime.store(),
        )?
        .is_relevant()
        {
            return Ok(());
        }
        if self.partial_deploys_tracker.already_processed(&partial_deploys) {
            return Ok(());
        }
        let key = partial_deploys.chunk_production_key().clone();
        let part_owners = self.ordered_contract_deploys_part_owners(&key)?;
        if part_owners.is_empty() {
            // Note that target validators do not include the chunk producers, and thus in some case
            // (eg. tests or small networks) there may be no other validators to send the new contracts to.
            // In such case, the message we are handling here should not be sent in the first place,
            // unless there is a bug or adversarial behavior that sends the message.
            debug_assert!(false, "No target validators, we must not receive this message");
            return Ok(());
        }

        // Forward to other validators if the part received is my part
        let signer = self.my_validator_signer()?;
        let my_account_id = signer.validator_id();
        let Some(my_part_ord) = part_owners.iter().position(|validator| validator == my_account_id)
        else {
            tracing::warn!(
                target: "client",
                ?key,
                "validator is not a part of contract deploys distribution"
            );
            return Ok(());
        };
        if partial_deploys.part().part_ord == my_part_ord {
            let validators = self.ordered_contract_deploys_validators(&key)?;
            let other_validators = validators
                .iter()
                .filter(|&validator| validator != my_account_id)
                .cloned()
                .collect_vec();
            if !other_validators.is_empty() {
                self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::PartialEncodedContractDeploys(
                        other_validators,
                        partial_deploys.clone(),
                    ),
                ));
            }
        }

        // Store part
        let encoder = self.contract_deploys_encoder(part_owners.len());
        if let Some(deploys) = self
            .partial_deploys_tracker
            .store_partial_encoded_contract_deploys(partial_deploys, encoder)?
        {
            let contracts = match deploys.decompress_contracts() {
                Ok(contracts) => contracts,
                Err(err) => {
                    tracing::warn!(
                        target: "client",
                        ?err,
                        ?key,
                        "failed to decompress deployed contracts"
                    );
                    return Ok(());
                }
            };
            let contract_codes = contracts.into_iter().map(|contract| contract.into()).collect();
            let runtime = self.runtime.clone();
            self.compile_contracts_spawner.spawn("precompile_deployed_contracts", move || {
                if let Err(err) = runtime.precompile_contracts(&key.epoch_id, contract_codes) {
                    tracing::error!(
                        target: "client",
                        ?err,
                        ?key,
                        "failed to precompile deployed contracts"
                    );
                }
            });
        }

        Ok(())
    }

    /// Handles the state witness ack message from the chunk validator.
    /// It computes the round-trip time between sending the state witness and receiving
    /// the ack message and updates the corresponding metric with it.
    /// Currently we do not raise an error for handling of witness-ack messages,
    /// as it is used only for tracking some networking metrics.
    fn handle_chunk_state_witness_ack(&self, witness_ack: ChunkStateWitnessAck) {
        self.state_witness_tracker.lock().on_witness_ack_received(witness_ack);
    }

    /// Handles contract code accesses message from chunk producer.
    /// This is sent in parallel to a chunk state witness and contains the hashes
    /// of the contract code accessed when applying the previous chunk of the witness.
    fn handle_chunk_contract_accesses(&self, accesses: ChunkContractAccesses) -> Result<(), Error> {
        let signer = self.my_validator_signer()?;
        if !validate_chunk_contract_accesses(
            self.epoch_manager.as_ref(),
            &accesses,
            &signer,
            self.runtime.store(),
        )?
        .is_relevant()
        {
            return Ok(());
        }
        let key = accesses.chunk_production_key();
        let contracts_cache = self.runtime.compiled_contract_cache();
        let runtime_config = self
            .runtime
            .get_runtime_config(self.epoch_manager.get_epoch_protocol_version(&key.epoch_id)?);
        let missing_contract_hashes = HashSet::from_iter(
            accesses
                .contracts()
                .iter()
                .filter(|&hash| {
                    !contracts_cache_contains_contract(contracts_cache, hash, &runtime_config)
                })
                .cloned(),
        );
        if missing_contract_hashes.is_empty() {
            return Ok(());
        }
        self.partial_witness_tracker
            .store_accessed_contract_hashes(key.clone(), missing_contract_hashes.clone())?;
        let random_chunk_producer = {
            let mut chunk_producers = self
                .epoch_manager
                .get_epoch_chunk_producers_for_shard(&key.epoch_id, key.shard_id)?;
            chunk_producers.swap_remove(rand::thread_rng().gen_range(0..chunk_producers.len()))
        };
        let request = ContractCodeRequest::new(
            key.clone(),
            missing_contract_hashes,
            accesses.main_transition().clone(),
            &signer,
        );
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ContractCodeRequest(random_chunk_producer, request),
        ));
        Ok(())
    }

    /// Sends the contract accesses to the same chunk validators
    /// (except for the chunk producers that track the same shard),
    /// which will receive the state witness for the new chunk.
    fn send_contract_accesses_to_chunk_validators(
        &self,
        key: ChunkProductionKey,
        contract_accesses: HashSet<CodeHash>,
        main_transition: MainTransitionKey,
        chunk_validators: &[AccountId],
        my_signer: &ValidatorSigner,
    ) {
        let chunk_producers: HashSet<AccountId> = self
            .epoch_manager
            .get_epoch_chunk_producers_for_shard(&key.epoch_id, key.shard_id)
            .expect("Chunk producers must be defined")
            .into_iter()
            .collect();

        // Exclude chunk producers that track the same shard from the target list, since they track the state that contains the respective code.
        let target_chunk_validators = chunk_validators
            .iter()
            .filter(|validator| !chunk_producers.contains(*validator))
            .cloned()
            .collect();
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkContractAccesses(
                target_chunk_validators,
                ChunkContractAccesses::new(key, contract_accesses, main_transition, my_signer),
            ),
        ));
    }

    /// Retrieves the code for the given contract hashes and distributes them to validator in parts.
    ///
    /// This implements the first step of distributing contract code to validators where the contract codes
    /// are compressed and encoded into parts using Reed Solomon encoding, and then each part is sent to
    /// one of the validators (part-owner). Second step of the distribution, where each validator (part-owner)
    /// forwards the part it receives is implemented in `handle_partial_encoded_contract_deploys`.
    fn send_chunk_contract_deploys_parts(
        &mut self,
        key: ChunkProductionKey,
        contract_codes: Vec<ContractCode>,
    ) -> Result<(), Error> {
        let contracts = contract_codes.into_iter().map(|contract| contract.into()).collect();
        let compressed_deploys = ChunkContractDeploys::compress_contracts(&contracts)?;
        let validator_parts = self.generate_contract_deploys_parts(&key, compressed_deploys)?;
        for (part_owner, deploys_part) in validator_parts {
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::PartialEncodedContractDeploys(vec![part_owner], deploys_part),
            ));
        }
        Ok(())
    }

    /// Handles contract code requests message from chunk validators.
    /// As response to this message, sends the contract code requested to
    /// the requesting chunk validator for the given hashes of the contract code.
    fn handle_contract_code_request(&mut self, request: ContractCodeRequest) -> Result<(), Error> {
        if !validate_contract_code_request(
            self.epoch_manager.as_ref(),
            &request,
            self.runtime.store(),
        )?
        .is_relevant()
        {
            return Ok(());
        }

        let key = request.chunk_production_key();
        let processed_requests_key = (key.clone(), request.requester().clone());
        if self.processed_contract_code_requests.contains(&processed_requests_key) {
            tracing::warn!(
                target: "client",
                ?processed_requests_key,
                "contract code request from this account was already processed"
            );
            return Ok(());
        }
        self.processed_contract_code_requests.push(processed_requests_key, ());

        let _timer = near_chain::stateless_validation::metrics::PROCESS_CONTRACT_CODE_REQUEST_TIME
            .with_label_values(&[&key.shard_id.to_string()])
            .start_timer();

        let main_transition_key = request.main_transition();
        let Some(transition_data) = self.runtime.store().get_ser::<StoredChunkStateTransitionData>(
            DBCol::StateTransitionData,
            &near_primitives::utils::get_block_shard_id(
                &main_transition_key.block_hash,
                main_transition_key.shard_id,
            ),
        ) else {
            tracing::warn!(
                target: "client",
                ?key,
                ?main_transition_key,
                "missing state transition data"
            );
            return Ok(());
        };
        let valid_accesses: HashSet<CodeHash> =
            transition_data.contract_accesses().iter().cloned().collect();

        let storage = TrieDBStorage::new(
            TrieStoreAdapter::new(self.runtime.store().clone()),
            shard_id_to_uid(
                self.epoch_manager.as_ref(),
                main_transition_key.shard_id,
                &self.epoch_manager.get_epoch_id(&main_transition_key.block_hash)?,
            )?,
        );
        let mut contracts = Vec::new();
        for contract_hash in request.contracts() {
            if !valid_accesses.contains(contract_hash) {
                tracing::warn!(
                    target: "client",
                    ?key,
                    ?contract_hash,
                    "requested contract code was not accessed when applying the chunk"
                );
                return Ok(());
            }
            match storage.retrieve_raw_bytes(&contract_hash.0) {
                Ok(bytes) => contracts.push(CodeBytes(bytes)),
                Err(StorageError::MissingTrieValue(_)) => {
                    tracing::warn!(
                        target: "client",
                        ?contract_hash,
                        chunk_production_key = ?key,
                        "requested contract hash is not present in the storage"
                    );
                    return Ok(());
                }
                Err(err) => return Err(err.into()),
            }
        }
        let response = ContractCodeResponse::encode(key.clone(), &contracts)?;
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ContractCodeResponse(request.requester().clone(), response),
        ));
        Ok(())
    }

    /// Handles contract code responses message from chunk producer.
    fn handle_contract_code_response(&self, response: ContractCodeResponse) -> Result<(), Error> {
        let key = response.chunk_production_key().clone();
        let contracts = response.decompress_contracts()?;
        self.partial_witness_tracker.store_accessed_contract_codes(key, contracts)
    }

    fn my_validator_signer(&self) -> Result<Arc<ValidatorSigner>, Error> {
        self.my_signer.get().ok_or_else(|| Error::NotAValidator("not a validator".to_owned()))
    }

    fn contract_deploys_encoder(&mut self, validators_count: usize) -> Arc<ReedSolomonEncoder> {
        self.contract_deploys_encoders.entry(validators_count)
    }

    fn ordered_contract_deploys_part_owners(
        &self,
        key: &ChunkProductionKey,
    ) -> Result<Vec<AccountId>, Error> {
        let mut validators = self.ordered_contract_deploys_validators(key)?;
        validators.truncate(REED_SOLOMON_MAX_PARTS);
        Ok(validators)
    }

    fn ordered_contract_deploys_validators(
        &self,
        key: &ChunkProductionKey,
    ) -> Result<Vec<AccountId>, Error> {
        let chunk_producers = HashSet::<AccountId>::from_iter(
            self.epoch_manager.get_epoch_chunk_producers_for_shard(&key.epoch_id, key.shard_id)?,
        );
        let mut validators = self
            .epoch_manager
            .get_epoch_all_validators(&key.epoch_id)?
            .into_iter()
            .filter(|stake| !chunk_producers.contains(stake.account_id()))
            .map(|stake| stake.account_id().clone())
            .collect::<Vec<_>>();
        validators.sort();
        Ok(validators)
    }
}

// Function to generate the parts of the state witness and return them as a tuple of chunk_validator and part.
pub fn generate_state_witness_parts(
    encoder: Arc<ReedSolomonEncoder>,
    epoch_id: EpochId,
    chunk_header: &ShardChunkHeader,
    witness_bytes: EncodedChunkStateWitness,
    chunk_validators: &[AccountId],
    signer: &ValidatorSigner,
    protocol_version: ProtocolVersion,
) -> Vec<(AccountId, VersionedPartialEncodedStateWitness)> {
    let _span = tracing::debug_span!(
        target: "client",
        "generate_state_witness_parts",
        chunk_hash = ?chunk_header.chunk_hash(),
        height = %chunk_header.height_created(),
        shard_id = %chunk_header.shard_id(),
        chunk_validators_len = chunk_validators.len(),
        tag_witness_distribution = true,
    )
    .entered();

    // Break the state witness into parts using Reed Solomon encoding.
    let (parts, encoded_length) = encoder.encode(&witness_bytes);

    chunk_validators
        .par_iter()
        .zip_eq(parts)
        .enumerate()
        .map(|(part_ord, (chunk_validator, part))| {
            // It's fine to unwrap part here as we just constructed the parts above and we expect
            // all of them to be present.
            let partial_witness = VersionedPartialEncodedStateWitness::new(
                epoch_id,
                chunk_header.clone(),
                part_ord,
                part.unwrap().into_vec(),
                encoded_length,
                signer,
                protocol_version,
            );
            (chunk_validator.clone(), partial_witness)
        })
        .collect()
}

pub fn compress_witness(witness: &ChunkStateWitness) -> Result<EncodedChunkStateWitness, Error> {
    let _span = tracing::debug_span!(
        target: "client",
        "compress_witness",
        chunk_hash = ?witness.chunk_header().chunk_hash(),
        height = %witness.chunk_header().height_created(),
        shard_id = %witness.chunk_header().shard_id(),
        tag_witness_distribution=true,
    )
    .entered();

    let shard_id_label = witness.chunk_header().shard_id().to_string();
    let encode_timer = near_chain::stateless_validation::metrics::CHUNK_STATE_WITNESS_ENCODE_TIME
        .with_label_values(&[shard_id_label.as_str()])
        .start_timer();
    let (witness_bytes, raw_witness_size) = EncodedChunkStateWitness::encode(witness)?;
    encode_timer.observe_duration();

    near_chain::stateless_validation::metrics::record_witness_size_metrics(
        raw_witness_size,
        witness_bytes.size_bytes(),
        witness,
    );
    Ok(witness_bytes)
}

#[cfg(test)]
mod tests {
    use super::{PENDING_V2_WITNESS_CACHE_SIZE, PendingV2WitnessCache};
    use near_primitives::hash::CryptoHash;
    use near_primitives::stateless_validation::partial_witness::VersionedPartialEncodedStateWitness;
    use near_primitives::test_utils::{create_test_signer, test_chunk_header};
    use near_primitives::types::EpochId;
    use near_primitives::validator_signer::ValidatorSigner;
    use near_primitives::version::{ProtocolFeature, ProtocolVersion};

    fn post_kickout_version() -> ProtocolVersion {
        ProtocolFeature::EarlyKickout.protocol_version()
    }

    fn pre_kickout_version() -> ProtocolVersion {
        ProtocolFeature::EarlyKickout.protocol_version().checked_sub(1).unwrap()
    }

    fn make_witness(
        signer: &ValidatorSigner,
        prev_block_hash: CryptoHash,
        protocol_version: ProtocolVersion,
    ) -> VersionedPartialEncodedStateWitness {
        let chunk_header = test_chunk_header(prev_block_hash, signer, protocol_version);
        VersionedPartialEncodedStateWitness::new(
            EpochId(CryptoHash::default()),
            chunk_header,
            0,
            b"payload".to_vec(),
            7,
            signer,
            protocol_version,
        )
    }

    #[test]
    fn inserts_group_by_prev_block_hash() {
        let signer = create_test_signer("test_account");
        let block_a = CryptoHash::hash_bytes(b"block_a");
        let block_b = CryptoHash::hash_bytes(b"block_b");
        let mut cache = PendingV2WitnessCache::new();

        cache.insert(block_a, make_witness(&signer, block_a, post_kickout_version()));
        cache.insert(block_a, make_witness(&signer, block_a, post_kickout_version()));
        cache.insert(block_b, make_witness(&signer, block_b, post_kickout_version()));
        assert_eq!(cache.len(), 2);

        let drained_a = cache.drain(&block_a);
        assert_eq!(drained_a.len(), 2);
        assert!(
            drained_a.iter().all(|w| w.prev_block_hash() == Some(&block_a)),
            "drained witnesses must all point to block_a",
        );

        let drained_b = cache.drain(&block_b);
        assert_eq!(drained_b.len(), 1);
        assert_eq!(drained_b[0].prev_block_hash(), Some(&block_b));

        assert_eq!(cache.len(), 0);
        assert!(cache.drain(&block_a).is_empty(), "re-draining yields nothing");
    }

    #[test]
    fn drain_unknown_block_is_empty() {
        let mut cache = PendingV2WitnessCache::new();
        assert!(cache.drain(&CryptoHash::hash_bytes(b"absent")).is_empty());
    }

    #[test]
    fn capacity_cap_evicts_oldest_block() {
        let signer = create_test_signer("test_account");
        let mut cache = PendingV2WitnessCache::new();
        // Insert one entry per block hash until we overflow the cap.
        let total = PENDING_V2_WITNESS_CACHE_SIZE + 2;
        let hashes: Vec<CryptoHash> =
            (0..total).map(|i| CryptoHash::hash_bytes(format!("blk_{i}").as_bytes())).collect();
        for h in &hashes {
            cache.insert(*h, make_witness(&signer, *h, post_kickout_version()));
        }
        assert_eq!(cache.len(), PENDING_V2_WITNESS_CACHE_SIZE);

        // Oldest entries were evicted.
        for h in &hashes[..total - PENDING_V2_WITNESS_CACHE_SIZE] {
            assert!(cache.drain(h).is_empty(), "oldest block {h:?} should have been evicted",);
        }
        // Newer entries remain.
        for h in &hashes[total - PENDING_V2_WITNESS_CACHE_SIZE..] {
            assert_eq!(cache.drain(h).len(), 1, "newer block {h:?} must still be cached");
        }
    }

    /// V1 witnesses never carry a `prev_block_hash`, so they are never inserted
    /// into the pending pool. This test guards the invariant that V1
    /// discriminants can't accidentally slip through the cache if a caller
    /// mistakenly routes them here.
    #[test]
    fn prev_block_hash_absent_for_v1() {
        let signer = create_test_signer("test_account");
        let block = CryptoHash::hash_bytes(b"block");
        let v1 = make_witness(&signer, block, pre_kickout_version());
        assert!(v1.prev_block_hash().is_none(), "V1 witness must not carry prev_block_hash");
        let v2 = make_witness(&signer, block, post_kickout_version());
        assert_eq!(v2.prev_block_hash(), Some(&block));
    }
}
