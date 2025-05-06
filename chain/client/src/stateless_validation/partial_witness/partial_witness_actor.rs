use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use itertools::Itertools;
use lru::LruCache;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{Actor, CanSend, Handler, Sender};
use near_async::time::Clock;
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::Error;
use near_chain::types::RuntimeAdapter;
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_network::state_witness::{
    ChunkContractAccessesMessage, ChunkStateWitnessAckMessage, ContractCodeRequestMessage,
    ContractCodeResponseMessage, PartialEncodedContractDeploysMessage,
    PartialEncodedStateWitnessForwardMessage, PartialEncodedStateWitnessMessage,
};
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_parameters::RuntimeConfig;
use near_performance_metrics_macros::perf;
use near_primitives::reed_solomon::{ReedSolomonEncoder, ReedSolomonEncoderCache};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractAccesses, ChunkContractDeploys, CodeBytes, CodeHash, ContractCodeRequest,
    ContractCodeResponse, ContractUpdates, MainTransitionKey, PartialEncodedContractDeploys,
    PartialEncodedContractDeploysPart,
};
use near_primitives::stateless_validation::partial_witness::PartialEncodedStateWitness;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessAck, EncodedChunkStateWitness,
};
use near_primitives::stateless_validation::stored_chunk_state_transition_data::StoredChunkStateTransitionData;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::validator_signer::ValidatorSigner;
use near_store::adapter::trie_store::TrieStoreAdapter;
use near_store::{DBCol, StorageError, TrieDBStorage, TrieStorage};
use near_vm_runner::{ContractCode, ContractRuntimeCache, get_contract_cache_key};
use rand::Rng;

use crate::client_actor::ClientSenderForPartialWitness;
use crate::metrics;
use crate::stateless_validation::state_witness_tracker::ChunkStateWitnessTracker;
use crate::stateless_validation::validate::{
    ChunkRelevance, validate_chunk_contract_accesses, validate_contract_code_request,
    validate_partial_encoded_contract_deploys, validate_partial_encoded_state_witness,
};

use super::encoding::{CONTRACT_DEPLOYS_RATIO_DATA_PARTS, WITNESS_RATIO_DATA_PARTS};
use super::partial_deploys_tracker::PartialEncodedContractDeploysTracker;
use super::partial_witness_tracker::PartialEncodedStateWitnessTracker;
use near_primitives::utils::compression::CompressedData;

const PROCESSED_CONTRACT_CODE_REQUESTS_CACHE_SIZE: usize = 30;

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
    state_witness_tracker: ChunkStateWitnessTracker,
    /// Reed Solomon encoder for encoding state witness parts.
    /// We keep one wrapper for each length of chunk_validators to avoid re-creating the encoder.
    witness_encoders: ReedSolomonEncoderCache,
    /// Same as above for contract deploys.
    contract_deploys_encoders: ReedSolomonEncoderCache,
    compile_contracts_spawner: Arc<dyn AsyncComputationSpawner>,
    partial_witness_spawner: Arc<dyn AsyncComputationSpawner>,
    /// AccountId in the key corresponds to the requester (chunk validator).
    processed_contract_code_requests: LruCache<(ChunkProductionKey, AccountId), ()>,
}

impl Actor for PartialWitnessActor {}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct DistributeStateWitnessRequest {
    pub state_witness: ChunkStateWitness,
    pub contract_updates: ContractUpdates,
    pub main_transition_shard_id: ShardId,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct PartialWitnessSenderForClient {
    pub distribute_chunk_state_witness: Sender<DistributeStateWitnessRequest>,
}

impl Handler<DistributeStateWitnessRequest> for PartialWitnessActor {
    #[perf]
    fn handle(&mut self, msg: DistributeStateWitnessRequest) {
        if let Err(err) = self.handle_distribute_state_witness_request(msg) {
            tracing::error!(target: "client", ?err, "Failed to handle distribute chunk state witness request");
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
            tracing::error!(target: "client", ?err, "Failed to handle PartialEncodedStateWitnessMessage");
        }
    }
}

impl Handler<PartialEncodedStateWitnessForwardMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: PartialEncodedStateWitnessForwardMessage) {
        if let Err(err) = self.handle_partial_encoded_state_witness_forward(msg.0) {
            tracing::error!(target: "client", ?err, "Failed to handle PartialEncodedStateWitnessForwardMessage");
        }
    }
}

impl Handler<ChunkContractAccessesMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: ChunkContractAccessesMessage) {
        if let Err(err) = self.handle_chunk_contract_accesses(msg.0) {
            tracing::error!(target: "client", ?err, "Failed to handle ChunkContractAccessesMessage");
        }
    }
}

impl Handler<PartialEncodedContractDeploysMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: PartialEncodedContractDeploysMessage) {
        if let Err(err) = self.handle_partial_encoded_contract_deploys(msg.0) {
            tracing::error!(target: "client", ?err, "Failed to handle PartialEncodedContractDeploysMessage");
        }
    }
}

impl Handler<ContractCodeRequestMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: ContractCodeRequestMessage) {
        if let Err(err) = self.handle_contract_code_request(msg.0) {
            tracing::error!(target: "client", ?err, "Failed to handle ContractCodeRequestMessage");
        }
    }
}

impl Handler<ContractCodeResponseMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: ContractCodeResponseMessage) {
        if let Err(err) = self.handle_contract_code_response(msg.0) {
            tracing::error!(target: "client", ?err, "Failed to handle ContractCodeResponseMessage");
        }
    }
}

impl PartialWitnessActor {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        client_sender: ClientSenderForPartialWitness,
        my_signer: MutableValidatorSigner,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime: Arc<dyn RuntimeAdapter>,
        compile_contracts_spawner: Arc<dyn AsyncComputationSpawner>,
        partial_witness_spawner: Arc<dyn AsyncComputationSpawner>,
    ) -> Self {
        let partial_witness_tracker =
            Arc::new(PartialEncodedStateWitnessTracker::new(client_sender, epoch_manager.clone()));
        Self {
            network_adapter,
            my_signer,
            epoch_manager,
            partial_witness_tracker,
            partial_deploys_tracker: PartialEncodedContractDeploysTracker::new(),
            state_witness_tracker: ChunkStateWitnessTracker::new(clock),
            runtime,
            witness_encoders: ReedSolomonEncoderCache::new(WITNESS_RATIO_DATA_PARTS),
            contract_deploys_encoders: ReedSolomonEncoderCache::new(
                CONTRACT_DEPLOYS_RATIO_DATA_PARTS,
            ),
            compile_contracts_spawner,
            partial_witness_spawner,
            processed_contract_code_requests: LruCache::new(
                NonZeroUsize::new(PROCESSED_CONTRACT_CODE_REQUESTS_CACHE_SIZE).unwrap(),
            ),
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

        tracing::debug!(
            target: "client",
            chunk_hash=?state_witness.chunk_header().chunk_hash(),
            "distribute_chunk_state_witness",
        );

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

        let witness_bytes = compress_witness(&state_witness)?;
        self.send_state_witness_parts(
            key.epoch_id,
            state_witness.chunk_header(),
            witness_bytes,
            &chunk_validators,
            &signer,
        );

        if !contract_deploys.is_empty() {
            self.send_chunk_contract_deploys_parts(key, contract_deploys)?;
        }
        Ok(())
    }

    // Function to generate the parts of the state witness and return them as a tuple of chunk_validator and part.
    fn generate_state_witness_parts(
        &mut self,
        epoch_id: EpochId,
        chunk_header: &ShardChunkHeader,
        witness_bytes: EncodedChunkStateWitness,
        chunk_validators: &[AccountId],
        signer: &ValidatorSigner,
    ) -> Vec<(AccountId, PartialEncodedStateWitness)> {
        tracing::debug!(
            target: "client",
            chunk_hash=?chunk_header.chunk_hash(),
            ?chunk_validators,
            "generate_state_witness_parts",
        );

        // Break the state witness into parts using Reed Solomon encoding.
        let encoder = self.witness_encoders.entry(chunk_validators.len());
        let (parts, encoded_length) = encoder.encode(&witness_bytes);

        chunk_validators
            .iter()
            .zip_eq(parts)
            .enumerate()
            .map(|(part_ord, (chunk_validator, part))| {
                // It's fine to unwrap part here as we just constructed the parts above and we expect
                // all of them to be present.
                let partial_witness = PartialEncodedStateWitness::new(
                    epoch_id,
                    chunk_header.clone(),
                    part_ord,
                    part.unwrap().to_vec(),
                    encoded_length,
                    signer,
                );
                (chunk_validator.clone(), partial_witness)
            })
            .collect_vec()
    }

    fn generate_contract_deploys_parts(
        &mut self,
        key: &ChunkProductionKey,
        deploys: ChunkContractDeploys,
    ) -> Result<Vec<(AccountId, PartialEncodedContractDeploys)>, Error> {
        let validators = self.ordered_contract_deploys_validators(key)?;
        // Note that target validators do not include the chunk producers, and thus in some case
        // (eg. tests or small networks) there may be no other validators to send the new contracts to.
        if validators.is_empty() {
            return Ok(vec![]);
        }

        let encoder = self.contract_deploys_encoder(validators.len());
        let (parts, encoded_length) = encoder.encode(&deploys);
        let signer = self.my_validator_signer()?;

        Ok(validators
            .into_iter()
            .zip_eq(parts)
            .enumerate()
            .map(|(part_ord, (validator, part))| {
                let partial_deploys = PartialEncodedContractDeploys::new(
                    key.clone(),
                    PartialEncodedContractDeploysPart {
                        part_ord,
                        data: part.unwrap().to_vec().into_boxed_slice(),
                        encoded_length,
                    },
                    &signer,
                );
                (validator, partial_deploys)
            })
            .collect_vec())
    }

    // Break the state witness into parts and send each part to the corresponding chunk validator owner.
    // The chunk validator owner will then forward the part to all other chunk validators.
    // Each chunk validator would collect the parts and reconstruct the state witness.
    fn send_state_witness_parts(
        &mut self,
        epoch_id: EpochId,
        chunk_header: &ShardChunkHeader,
        witness_bytes: EncodedChunkStateWitness,
        chunk_validators: &[AccountId],
        signer: &ValidatorSigner,
    ) {
        // Capture these values first, as the sources are consumed before calling record_witness_sent.
        let chunk_hash = chunk_header.chunk_hash();
        let witness_size_in_bytes = witness_bytes.size_bytes();

        // Record time taken to encode the state witness parts.
        let shard_id_label = chunk_header.shard_id().to_string();
        let encode_timer = metrics::PARTIAL_WITNESS_ENCODE_TIME
            .with_label_values(&[shard_id_label.as_str()])
            .start_timer();
        let validator_witness_tuple = self.generate_state_witness_parts(
            epoch_id,
            chunk_header,
            witness_bytes,
            chunk_validators,
            signer,
        );
        encode_timer.observe_duration();

        // Record the witness in order to match the incoming acks for measuring round-trip times.
        // See process_chunk_state_witness_ack for the handling of the ack messages.
        self.state_witness_tracker.record_witness_sent(
            chunk_hash,
            witness_size_in_bytes,
            validator_witness_tuple.len(),
        );

        // Send the parts to the corresponding chunk validator owners.
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::PartialEncodedStateWitness(validator_witness_tuple),
        ));
    }

    /// Function to handle receiving partial_encoded_state_witness message from chunk producer.
    fn handle_partial_encoded_state_witness(
        &self,
        partial_witness: PartialEncodedStateWitness,
    ) -> Result<(), Error> {
        tracing::debug!(target: "client", ?partial_witness, "Receive PartialEncodedStateWitnessMessage");
        let signer = self.my_validator_signer()?;
        let validator_account_id = signer.validator_id().clone();
        let epoch_manager = self.epoch_manager.clone();
        let runtime_adapter = self.runtime.clone();

        let ChunkProductionKey { shard_id, epoch_id, height_created } =
            partial_witness.chunk_production_key();

        let chunk_producer = self
            .epoch_manager
            .get_chunk_producer_info(&ChunkProductionKey { epoch_id, height_created, shard_id })?
            .take_account_id();

        // Forward witness part to chunk validators except the validator that produced the chunk and witness.
        let target_chunk_validators = self
            .epoch_manager
            .get_chunk_validator_assignments(&epoch_id, shard_id, height_created)?
            .ordered_chunk_validators()
            .into_iter()
            .filter(|validator| validator != &chunk_producer)
            .collect();

        let network_adapter = self.network_adapter.clone();

        self.partial_witness_spawner.spawn("handle_partial_encoded_state_witness", move || {
            // Validate the partial encoded state witness and forward the part to all the chunk validators.
            match validate_partial_encoded_state_witness(
                epoch_manager.as_ref(),
                &partial_witness,
                &validator_account_id,
                runtime_adapter.store(),
            ) {
                Ok(ChunkRelevance::Relevant) => {
                    network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                        NetworkRequests::PartialEncodedStateWitnessForward(
                            target_chunk_validators,
                            partial_witness,
                        ),
                    ));
                }
                Ok(_) => {
                    tracing::debug!(
                        target: "client",
                        chunk_production_key = ?partial_witness.chunk_production_key(),
                        "Received irrelevant partial encoded state witness",
                    );
                }
                Err(err) => {
                    // TODO: ban sending peer
                    tracing::warn!(
                        target: "client",
                        chunk_production_key = ?partial_witness.chunk_production_key(),
                        "Received invalid partial encoded state witness: {}",
                        err
                    );
                }
            }
        });

        Ok(())
    }

    /// Function to handle receiving partial_encoded_state_witness_forward message from chunk producer.
    fn handle_partial_encoded_state_witness_forward(
        &self,
        partial_witness: PartialEncodedStateWitness,
    ) -> Result<(), Error> {
        tracing::debug!(target: "client", ?partial_witness, "Receive PartialEncodedStateWitnessForwardMessage");

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
                            tracing::error!(target: "client", "Failed to store partial encoded state witness: {}", err);
                        }
                    }
                    Ok(_) => {
                        tracing::debug!(
                            target: "client",
                            chunk_production_key = ?partial_witness.chunk_production_key(),
                            "Received irrelevant partial encoded state witness",
                        );
                    }
                    Err(err) => {
                        // TODO: ban sending peer
                        tracing::warn!(
                            target: "client",
                            chunk_production_key = ?partial_witness.chunk_production_key(),
                            "Received invalid partial encoded state witness: {}",
                            err
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
        tracing::debug!(target: "client", ?partial_deploys, "Receive PartialEncodedContractDeploys");
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
        let validators = self.ordered_contract_deploys_validators(&key)?;
        if validators.is_empty() {
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
        let Some(my_part_ord) = validators.iter().position(|validator| validator == my_account_id)
        else {
            tracing::warn!(
                target: "client",
                ?key,
                "Validator is not a part of contract deploys distribution"
            );
            return Ok(());
        };
        if partial_deploys.part().part_ord == my_part_ord {
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
        let encoder = self.contract_deploys_encoder(validators.len());
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
                        "Failed to decompress deployed contracts."
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
                        "Failed to precompile deployed contracts."
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
    fn handle_chunk_state_witness_ack(&mut self, witness_ack: ChunkStateWitnessAck) {
        self.state_witness_tracker.on_witness_ack_received(witness_ack);
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
                "Contract code request from this account was already processed"
            );
            return Ok(());
        }
        self.processed_contract_code_requests.push(processed_requests_key, ());

        let _timer = near_chain::stateless_validation::metrics::PROCESS_CONTRACT_CODE_REQUEST_TIME
            .with_label_values(&[&key.shard_id.to_string()])
            .start_timer();

        let main_transition_key = request.main_transition();
        let Some(transition_data) =
            self.runtime.store().get_ser::<StoredChunkStateTransitionData>(
                DBCol::StateTransitionData,
                &near_primitives::utils::get_block_shard_id(
                    &main_transition_key.block_hash,
                    main_transition_key.shard_id,
                ),
            )?
        else {
            tracing::warn!(
                target: "client",
                ?key,
                ?main_transition_key,
                "Missing state transition data"
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
                    "Requested contract code was not accessed when applying the chunk"
                );
                return Ok(());
            }
            match storage.retrieve_raw_bytes(&contract_hash.0) {
                Ok(bytes) => contracts.push(CodeBytes(bytes)),
                Err(StorageError::MissingTrieValue(_, _)) => {
                    tracing::warn!(
                        target: "client",
                        ?contract_hash,
                        chunk_production_key = ?key,
                        "Requested contract hash is not present in the storage"
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

fn compress_witness(witness: &ChunkStateWitness) -> Result<EncodedChunkStateWitness, Error> {
    let shard_id_label = witness.chunk_header().shard_id().to_string();
    let encode_timer = near_chain::stateless_validation::metrics::CHUNK_STATE_WITNESS_ENCODE_TIME
        .with_label_values(&[shard_id_label.as_str()])
        .start_timer();
    let (witness_bytes, raw_witness_size) = if let ChunkStateWitness::V1(witness_v1) = witness {
        // For V1 witness, we need to encode only the inner witness struct for backwards compatibility.
        EncodedChunkStateWitness::encode(witness_v1)?
    } else {
        EncodedChunkStateWitness::encode(witness)?
    };
    encode_timer.observe_duration();

    near_chain::stateless_validation::metrics::record_witness_size_metrics(
        raw_witness_size,
        witness_bytes.size_bytes(),
        witness,
    );
    Ok(witness_bytes)
}

fn contracts_cache_contains_contract(
    cache: &dyn ContractRuntimeCache,
    contract_hash: &CodeHash,
    runtime_config: &RuntimeConfig,
) -> bool {
    let cache_key = get_contract_cache_key(contract_hash.0, &runtime_config.wasm_config);
    cache.memory_cache().contains(cache_key) || cache.has(&cache_key).is_ok_and(|has| has)
}
