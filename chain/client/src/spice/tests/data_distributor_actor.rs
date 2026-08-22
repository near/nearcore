use crate::spice::chunk_executor_actor::{
    ExecutorIncomingUnverifiedReceipts, save_receipt_proof, save_witness_and_contract_accesses,
};
use crate::spice::chunk_validator_actor::SpiceChunkStateWitnessMessage;
use crate::spice::data_distributor_actor::{
    Error, FALLBACK_WITNESS_PULL_GRACE, FALLBACK_WITNESS_PUSH_LOOKAHEAD, MAX_REQUESTED_DATA_IDS,
    MAX_REQUESTED_PARTS, MalformedDataRequest, ReceiveDataError, SpiceDataDistributorActor,
    SpiceDistributorOutgoingReceipts, SpiceDistributorStateWitness,
};
use assert_matches::assert_matches;
use itertools::Itertools as _;
use near_async::messaging::Actor;
use near_async::messaging::{Handler, IntoAsyncSender, IntoSender, Sender, noop};
use near_async::test_utils::FakeDelayedActionRunner;
use near_async::time::Clock;
use near_chain::Block;
use near_chain::ChainStoreAccess;
use near_chain::spice::activation::SpiceMessageKind;
use near_chain::spice::all_stake_fallback::{
    SPICE_FALLBACK_CERTIFICATION_DELAY, fallback_endorsers, is_fallback_only_chunk,
};
use near_chain::spice::core::SpiceCoreReader;
use near_chain::spice::core_writer_actor::{ProcessedBlock, SpiceCoreWriterActor};
use near_chain::test_utils::{
    get_chain_with_genesis, get_fake_next_block_chunk_headers, process_block_sync,
};
use near_chain::types::Tip;
use near_chain::{BlockProcessingArtifact, Chain, Provenance};
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_chain_configs::{Genesis, MutableConfigValue, TrackedShardsConfig};
use near_crypto::{KeyType, Signature};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::recv_permit::RecvMessagePermit;
use near_network::spice::data_distribution::{
    SpiceContractCodeRequestMessage, SpiceDataRequest, SpiceDataRequestMessage,
    SpiceIncomingPartialData,
};
use near_network::types::{
    NetworkRequestWithPermit, NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_o11y::testonly::init_test_logger;
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::hash::hash;
use near_primitives::merkle::merklize;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ReceiptProof;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::sharding::ShardProof;
use near_primitives::spice::chunk_endorsement::{
    SpiceChunkEndorsement, SpiceEndorsementSignedData, testonly_create_endorsement_core_statement,
};
use near_primitives::spice::partial_data::{
    SpiceDataCommitment, SpiceDataIdentifier, SpiceDataPart, SpicePartialData,
    SpiceVerifiedPartialData, testonly_create_spice_partial_data,
};
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::{
    CodeHash, SpiceContractCodeRequest,
};
use near_primitives::test_utils::{
    TestBlockBuilder, create_test_signer, pre_spice_protocol_version,
};
use near_primitives::types::AccountId;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ChunkExecutionResult};
use near_primitives::types::{ShardId, SpiceChunkId};
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_store::ShardUId;
use near_store::adapter::StoreAdapter;
use near_store::adapter::StoreUpdateAdapter;
use near_store::adapter::trie_store::TrieStoreAdapter;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::num::NonZero;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

fn build_block(epoch_manager: &dyn EpochManagerAdapter, prev_block: &Block) -> Arc<Block> {
    build_block_with_core_statements(epoch_manager, prev_block, vec![])
}

fn build_block_with_core_statements(
    epoch_manager: &dyn EpochManagerAdapter,
    prev_block: &Block,
    spice_core_statements: Vec<SpiceCoreStatement>,
) -> Arc<Block> {
    let block_producer = epoch_manager
        .get_block_producer_info(prev_block.header().epoch_id(), prev_block.header().height() + 1)
        .unwrap();
    let signer = Arc::new(create_test_signer(block_producer.account_id().as_str()));
    TestBlockBuilder::from_prev_block(Clock::real(), prev_block, signer)
        .chunks(get_fake_next_block_chunk_headers(&prev_block, epoch_manager))
        .spice_core_statements(spice_core_statements)
        .build()
}

fn produce_block(chain: &mut Chain, prev_block: &Block) -> Arc<Block> {
    let block = build_block(chain.epoch_manager.as_ref(), &prev_block);
    process_block_sync(
        chain,
        block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    block
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq)]
enum OutgoingMessage {
    NetworkRequests { request: NetworkRequests },
    ExecutorIncomingUnverifiedReceipts(ExecutorIncomingUnverifiedReceipts),
    ChunkStateWitnessMessage(SpiceChunkStateWitnessMessage),
}

fn latest_block(chain: &Chain) -> Arc<Block> {
    let head = chain.chain_store.head().unwrap();
    let block_hash = &head.last_block_hash;
    chain.chain_store.get_block(block_hash).unwrap()
}

fn new_test_receipt_proof(block: &Block) -> ReceiptProof {
    let chunks = block.chunks();
    let from_shard_id = chunks[0].shard_id();
    let to_shard_id = chunks[1].shard_id();
    ReceiptProof(vec![], ShardProof { from_shard_id, to_shard_id, proof: vec![] })
}

fn new_test_witness_for_chunk(
    block: &Block,
    chunk_header: &ShardChunkHeader,
) -> SpiceChunkStateWitness {
    let pre_state = PartialState::TrieValues(vec![]);
    let receipt_proofs = HashMap::new();
    let receipts_hash = CryptoHash::default();
    let transactions = vec![];
    SpiceChunkStateWitness::new(
        near_primitives::types::SpiceChunkId {
            block_hash: *block.hash(),
            shard_id: chunk_header.shard_id(),
        },
        pre_state,
        receipt_proofs,
        receipts_hash,
        transactions,
        BTreeSet::new(),
        None,
    )
}

fn witness_shard_id(block: &Block) -> ShardId {
    let chunks = block.chunks();
    let chunk_header = &chunks[0];
    chunk_header.shard_id()
}

fn new_test_witness(block: &Block) -> SpiceChunkStateWitness {
    let chunks = block.chunks();
    let chunk_header = &chunks[0];
    new_test_witness_for_chunk(block, chunk_header)
}

fn setup(num_chunk_producers: usize, num_validators: usize) -> (Genesis, Chain) {
    setup_with_shard_layout(num_chunk_producers, num_validators, ShardLayout::multi_shard(2, 0))
}

fn setup_with_shard_layout(
    num_chunk_producers: usize,
    num_validators: usize,
    shard_layout: ShardLayout,
) -> (Genesis, Chain) {
    init_test_logger();

    let block_and_chunk_producers =
        (0..num_chunk_producers).map(|i| format!("test-producer-{i}")).collect_vec();
    let chunk_validators_only =
        (0..num_validators).map(|i| format!("test-validator-{i}")).collect_vec();
    let validators_spec = ValidatorsSpec::desired_roles(
        &block_and_chunk_producers.iter().map(String::as_str).collect_vec(),
        &chunk_validators_only.iter().map(String::as_str).collect_vec(),
    );

    let genesis = TestGenesisBuilder::new()
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .build();

    let mut chain = get_chain_with_genesis(Clock::real(), genesis.clone());
    let epoch_manager = chain.epoch_manager.as_ref();
    let genesis_block = chain.genesis_block();
    let first_block = build_block(epoch_manager, &genesis_block);
    let second_block = build_block(epoch_manager, &first_block);
    for block in [first_block, second_block] {
        process_block_sync(
            &mut chain,
            block.into(),
            Provenance::PRODUCED,
            &mut BlockProcessingArtifact::default(),
        )
        .unwrap();
    }
    (genesis, chain)
}

fn new_chain(chain: &Chain, genesis: &Genesis) -> Chain {
    let mut cloned_chain = get_chain_with_genesis(Clock::real(), genesis.clone());
    let mut blocks = Vec::new();
    let head = chain.chain_store.head().unwrap();
    let mut last_block = chain.chain_store.get_block(&head.last_block_hash).unwrap();
    while !last_block.header().is_genesis() {
        blocks.push(last_block.clone());
        last_block = chain.chain_store.get_block(last_block.header().prev_hash()).unwrap();
    }
    for block in blocks.into_iter().rev() {
        process_block_sync(
            &mut cloned_chain,
            block.into(),
            Provenance::PRODUCED,
            &mut BlockProcessingArtifact::default(),
        )
        .unwrap();
    }
    cloned_chain
}

struct ActorBuilder {
    validator: Option<AccountId>,
    tracked_shards_config: TrackedShardsConfig,
}

impl ActorBuilder {
    fn new(validator: Option<AccountId>) -> Self {
        Self { validator, tracked_shards_config: TrackedShardsConfig::NoShards }
    }

    fn tracked_shards_config(mut self, config: TrackedShardsConfig) -> Self {
        self.tracked_shards_config = config;
        self
    }

    fn build(
        self,
        outgoing_sc: UnboundedSender<OutgoingMessage>,
        chain: &Chain,
    ) -> SpiceDataDistributorActor {
        let signer =
            self.validator.map(|account_id| Arc::new(create_test_signer(account_id.as_str())));
        let validator_signer = MutableConfigValue::new(signer, "validator_signer");
        let epoch_manager = chain.epoch_manager.clone();
        let shard_tracker = ShardTracker::new(
            self.tracked_shards_config,
            chain.epoch_manager.clone(),
            validator_signer.clone(),
        );

        let network_adapter = PeerManagerAdapter {
            async_request_sender: noop().into_async_sender(),
            set_chain_info_sender: noop().into_sender(),
            state_sync_event_sender: noop().into_sender(),
            request_sender: Sender::from_fn({
                let outgoing_sc = outgoing_sc.clone();
                move |message: PeerManagerMessageRequest| {
                    let PeerManagerMessageRequest::NetworkRequests(request) = message else {
                        unreachable!()
                    };
                    outgoing_sc.send(OutgoingMessage::NetworkRequests { request }).unwrap();
                }
            }),
            request_with_permit_sender: Sender::from_fn({
                let outgoing_sc = outgoing_sc.clone();
                move |message: NetworkRequestWithPermit| {
                    // ignore the permit in tests
                    outgoing_sc
                        .send(OutgoingMessage::NetworkRequests { request: message.request })
                        .unwrap();
                }
            }),
        };
        SpiceDataDistributorActor::new(
            epoch_manager.clone(),
            chain.chain_store.store().chain_store(),
            validator_signer,
            shard_tracker,
            core_reader(chain),
            network_adapter,
            Sender::from_fn({
                let outgoing_sc = outgoing_sc.clone();
                move |message| {
                    outgoing_sc
                        .send(OutgoingMessage::ExecutorIncomingUnverifiedReceipts(message))
                        .unwrap();
                }
            }),
            Sender::from_fn({
                move |message: SpanWrapped<SpiceChunkStateWitnessMessage>| {
                    outgoing_sc
                        .send(OutgoingMessage::ChunkStateWitnessMessage(message.span_unwrap()))
                        .unwrap();
                }
            }),
            Sender::from_fn(|_| {}),
            Sender::from_fn(|_| {}),
        )
    }
}

fn core_reader(chain: &Chain) -> SpiceCoreReader {
    SpiceCoreReader::new(
        chain.chain_store.store().chain_store(),
        chain.epoch_manager.clone(),
        Gas::from_teragas(100),
    )
}

fn new_actor_for_account(
    outgoing_sc: UnboundedSender<OutgoingMessage>,
    chain: &Chain,
    account_id: &AccountId,
) -> SpiceDataDistributorActor {
    ActorBuilder::new(Some(account_id.clone())).build(outgoing_sc, chain)
}

fn witness_producer_accounts(
    chain: &Chain,
    block: &Block,
    witness: &SpiceChunkStateWitness,
) -> Vec<AccountId> {
    let chunk_id = witness.chunk_id();
    chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(block.header().epoch_id(), chunk_id.shard_id)
        .unwrap()
}

fn witness_chunk_height_created(block: &Block, witness: &SpiceChunkStateWitness) -> BlockHeight {
    block
        .chunks()
        .iter_raw()
        .find(|chunk| chunk.shard_id() == witness.chunk_id().shard_id)
        .unwrap()
        .height_created()
}

fn witness_validators(
    chain: &Chain,
    block: &Block,
    witness: &SpiceChunkStateWitness,
) -> Vec<AccountId> {
    let chunk_id = witness.chunk_id();
    let height_created = witness_chunk_height_created(block, witness);
    let validator_assignment = chain
        .epoch_manager
        .get_chunk_validator_assignments(
            block.header().epoch_id(),
            chunk_id.shard_id,
            height_created,
        )
        .unwrap();
    validator_assignment.assignments().iter().map(|(id, _)| id).cloned().collect()
}

fn witness_validator_account(chain: &Chain) -> AccountId {
    let block = latest_block(chain);
    let witness = new_test_witness(&block);
    witness_validators(chain, &block, &witness).into_iter().next().unwrap()
}

fn non_producer_witness_validator_account(chain: &Chain) -> AccountId {
    let block = latest_block(chain);
    let witness = new_test_witness(&block);
    let producers: HashSet<_> =
        witness_producer_accounts(chain, &block, &witness).into_iter().collect();
    witness_validators(chain, &block, &witness)
        .into_iter()
        .find(|validator| !producers.contains(validator))
        .unwrap()
}

fn producers_of_receipt_proof(
    chain: &Chain,
    block: &Block,
    receipt_proof: &ReceiptProof,
) -> Vec<AccountId> {
    let from_shard_id = receipt_proof.1.from_shard_id;
    chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(block.header().epoch_id(), from_shard_id)
        .unwrap()
}

fn recipients_of_receipt_proof(
    chain: &Chain,
    block: &Block,
    receipt_proof: &ReceiptProof,
) -> Vec<AccountId> {
    let to_shard_id = receipt_proof.1.to_shard_id;
    chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(block.header().epoch_id(), to_shard_id)
        .unwrap()
}

fn chunk_producer_for_shard(chain: &Chain, shard_id: ShardId) -> AccountId {
    let block = latest_block(chain);
    chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(block.header().epoch_id(), shard_id)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

fn save_final_execution_head(chain: &Chain, block: &Block) {
    let store = chain.chain_store.store();
    let mut store_update = store.store_update();
    store_update
        .chain_store_update()
        .set_spice_final_execution_head(&Tip::from_header(block.header()));
    store_update.commit();
}

struct SpicePartialDataBuilder {
    id: SpiceDataIdentifier,
    commitment: SpiceDataCommitment,
    parts: Vec<SpiceDataPart>,
    sender: AccountId,
}

macro_rules! builder_setter {
    ($field: ident, $type: ty) => {
        fn $field(mut self, value: $type) -> Self {
            self.$field = value;
            self
        }
    };
}

impl SpicePartialDataBuilder {
    builder_setter!(id, SpiceDataIdentifier);
    builder_setter!(commitment, SpiceDataCommitment);
    builder_setter!(parts, Vec<SpiceDataPart>);
    builder_setter!(sender, AccountId);

    fn from_default(default: SpicePartialData) -> Self {
        Self::from_verified(data_into_verified(default))
    }

    fn from_verified(
        SpiceVerifiedPartialData { id, commitment, parts, sender }: SpiceVerifiedPartialData,
    ) -> Self {
        Self { id, commitment, parts, sender }
    }

    fn build(self) -> SpicePartialData {
        SpicePartialData::new(
            self.id,
            self.commitment,
            self.parts,
            &create_test_signer(self.sender.as_str()),
        )
    }

    fn build_with_signature(self, signature: Signature) -> SpicePartialData {
        testonly_create_spice_partial_data(
            self.id,
            self.commitment,
            self.parts,
            signature,
            self.sender,
        )
    }
}

fn data_into_verified(data: SpicePartialData) -> SpiceVerifiedPartialData {
    let signer = create_test_signer(data.sender().as_str());
    data.into_verified(&signer.public_key()).unwrap()
}

fn test_witness_can_be_reconstructed_impl(num_chunk_producers: usize, num_validators: usize) {
    let (genesis, chain) = setup(num_chunk_producers, num_validators);

    let block = latest_block(&chain);
    let state_witness = new_test_witness(&block);

    let producer_accounts = &witness_producer_accounts(&chain, &block, &state_witness);
    let validator_accounts = witness_validators(&chain, &block, &state_witness);
    let mut recipient_accounts: HashSet<AccountId> =
        HashSet::from_iter(validator_accounts.into_iter());
    for producer in producer_accounts {
        recipient_accounts.remove(producer);
    }

    let (producers_messages_sc, mut producers_messages_rc) = unbounded_channel();
    let mut producers = producer_accounts
        .iter()
        .map(|producer| new_actor_for_account(producers_messages_sc.clone(), &chain, producer))
        .collect_vec();
    for producer in &mut producers {
        producer.handle(SpiceDistributorStateWitness {
            contract_accesses: HashSet::new(),
            state_witness: state_witness.clone(),
        })
    }

    let (receiver_messages_sc, mut receiver_messages_rc) = unbounded_channel();
    let validator = recipient_accounts.iter().next().unwrap();

    // Separate chain makes sure that receiver doesn't share storage with producers.
    let receiver_chain = new_chain(&chain, &genesis);
    let mut receiver = new_actor_for_account(receiver_messages_sc, &receiver_chain, validator);
    while let Ok(message) = producers_messages_rc.try_recv() {
        let OutgoingMessage::NetworkRequests {
            request: NetworkRequests::SpicePartialData { partial_data, recipients },
        } = message
        else {
            // allowed non-partial-data messages that can be received before the witness is fully reconstructed
            assert_matches!(
                message,
                OutgoingMessage::NetworkRequests {
                    request: NetworkRequests::SpiceChunkContractAccesses { .. }
                } | OutgoingMessage::NetworkRequests {
                    request: NetworkRequests::SpiceContractCodeResponse { .. }
                },
                "unexpected message type before witness is reconstructed: {message:?}"
            );

            continue;
        };
        assert!(recipients.contains(validator));
        receiver.handle(SpiceIncomingPartialData {
            data: partial_data.clone(),
            recv_permit: RecvMessagePermit::none(),
        });
    }
    let message = receiver_messages_rc.try_recv().unwrap();
    assert_matches!(receiver_messages_rc.try_recv(), Err(TryRecvError::Empty));
    let OutgoingMessage::ChunkStateWitnessMessage(SpiceChunkStateWitnessMessage {
        witness: reconstructed_witness,
        ..
    }) = message
    else {
        panic!();
    };
    assert_eq!(reconstructed_witness, state_witness);
}

fn test_witness_is_distributed_to_all_validators_impl(
    num_chunk_producers: usize,
    num_validators: usize,
) {
    let (_genesis, chain) = setup(num_chunk_producers, num_validators);

    let block = latest_block(&chain);
    let state_witness = new_test_witness(&block);

    let producer_accounts = &witness_producer_accounts(&chain, &block, &state_witness);
    let validator_accounts = witness_validators(&chain, &block, &state_witness);
    let mut recipient_accounts: HashSet<AccountId> =
        HashSet::from_iter(validator_accounts.into_iter());
    for producer in producer_accounts {
        recipient_accounts.remove(producer);
    }

    let (producers_messages_sc, mut producers_messages_rc) = unbounded_channel();
    let mut producers = producer_accounts
        .iter()
        .map(|producer| new_actor_for_account(producers_messages_sc.clone(), &chain, producer))
        .collect_vec();
    for producer in &mut producers {
        producer.handle(SpiceDistributorStateWitness {
            contract_accesses: HashSet::new(),
            state_witness: state_witness.clone(),
        })
    }

    while let Ok(message) = producers_messages_rc.try_recv() {
        let OutgoingMessage::NetworkRequests {
            request: NetworkRequests::SpicePartialData { recipients: message_recipients, .. },
            ..
        } = message
        else {
            // allowed non-partial-data messages that can be received before the witness is fully reconstructed
            assert_matches!(
                message,
                OutgoingMessage::NetworkRequests {
                    request: NetworkRequests::SpiceChunkContractAccesses { .. }
                } | OutgoingMessage::NetworkRequests {
                    request: NetworkRequests::SpiceContractCodeResponse { .. }
                },
                "Unexpected message type before witness is reconstructed: {message:?}"
            );

            continue;
        };
        assert_eq!(message_recipients.len(), recipient_accounts.len());
        let message_recipients = HashSet::from_iter(message_recipients.into_iter());
        assert_eq!(message_recipients, recipient_accounts);
    }
}

macro_rules! test_witness_distribution {
    ($($name:ident ( $num_producers:literal, $num_validators:literal ))+) => {
        mod test_witness_can_be_reconstructed {
            $(
                #[test]
                #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
                fn $name() {
                    super::test_witness_can_be_reconstructed_impl($num_producers, $num_validators);
                }
            )+
        }
        mod test_witness_is_distributed_to_all_validators {
            $(
                #[test]
                #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
                fn $name() {
                    super::test_witness_is_distributed_to_all_validators_impl($num_producers, $num_validators);
                }
            )+
        }
    }
}

test_witness_distribution! {
    with_1_producer_1_validator(1, 1)
    with_1_producer_10_validators(1, 10)
    with_1_producer_100_validators(1, 100)
    with_1_producer_1000_validators(1, 1000)
    with_1_producer_9999_validators(1, 9999)
    with_10_producers_1_validator(10, 1)
    with_10_producers_10_validators(10, 10)
    with_10_producers_100_validators(10, 100)
    with_10_producers_1000_validators(10, 1000)
    with_100_producers_1_validator(100, 1)
    with_100_producers_10_validators(100, 10)
    with_100_producers_100_validators(100, 100)
    with_100_producers_1000_validators(100, 1000)
}

fn test_receipts_can_be_reconstructed_impl(num_chunk_producers: usize) {
    let (genesis, chain) = setup(num_chunk_producers, 0);

    let block = latest_block(&chain);
    let receipt_proof = new_test_receipt_proof(&block);

    let producer_accounts = &producers_of_receipt_proof(&chain, &block, &receipt_proof);
    let recipient_accounts = &recipients_of_receipt_proof(&chain, &block, &receipt_proof);

    let (producers_messages_sc, mut producers_messages_rc) = unbounded_channel();
    let mut producers = producer_accounts
        .iter()
        .map(|producer| new_actor_for_account(producers_messages_sc.clone(), &chain, producer))
        .collect_vec();
    for producer in &mut producers {
        producer.handle(SpiceDistributorOutgoingReceipts {
            block_hash: *block.hash(),
            receipt_proofs: vec![receipt_proof.clone()],
        })
    }

    let (receiver_messages_sc, mut receiver_messages_rc) = unbounded_channel();
    let receiver_account = &recipient_accounts[0];

    // Separate chain makes sure that receiver doesn't share storage with producers.
    let receiver_chain = new_chain(&chain, &genesis);
    let mut receiver =
        new_actor_for_account(receiver_messages_sc, &receiver_chain, receiver_account);
    while let Ok(message) = producers_messages_rc.try_recv() {
        let OutgoingMessage::NetworkRequests {
            request: NetworkRequests::SpicePartialData { partial_data, recipients },
        } = message
        else {
            panic!()
        };
        assert!(recipients.contains(receiver_account));
        receiver.handle(SpiceIncomingPartialData {
            data: partial_data.clone(),
            recv_permit: RecvMessagePermit::none(),
        });
    }
    let message = receiver_messages_rc.try_recv().unwrap();
    assert_matches!(receiver_messages_rc.try_recv(), Err(TryRecvError::Empty));
    let OutgoingMessage::ExecutorIncomingUnverifiedReceipts(ExecutorIncomingUnverifiedReceipts {
        block_hash: reconstructed_block_hash,
        receipt_proof: reconstructed_receipt_proof,
    }) = message
    else {
        panic!();
    };
    assert_eq!(&reconstructed_block_hash, block.hash());
    assert_eq!(reconstructed_receipt_proof, receipt_proof);
}

fn test_receipts_are_distributed_to_all_validators_impl(num_chunk_producers: usize) {
    let (_genesis, chain) = setup(num_chunk_producers, 0);

    let block = latest_block(&chain);
    let receipt_proof = new_test_receipt_proof(&block);

    let producer_accounts = &producers_of_receipt_proof(&chain, &block, &receipt_proof);
    let recipient_accounts = &recipients_of_receipt_proof(&chain, &block, &receipt_proof);

    let (producers_messages_sc, mut producers_messages_rc) = unbounded_channel();
    let mut producers = producer_accounts
        .iter()
        .map(|producer| new_actor_for_account(producers_messages_sc.clone(), &chain, producer))
        .collect_vec();
    for producer in &mut producers {
        producer.handle(SpiceDistributorOutgoingReceipts {
            block_hash: *block.hash(),
            receipt_proofs: vec![receipt_proof.clone()],
        })
    }

    let recipients: HashSet<AccountId> = HashSet::from_iter(recipient_accounts.iter().cloned());
    while let Ok(message) = producers_messages_rc.try_recv() {
        let OutgoingMessage::NetworkRequests {
            request: NetworkRequests::SpicePartialData { recipients: message_recipients, .. },
            ..
        } = message
        else {
            panic!()
        };
        assert_eq!(message_recipients.len(), recipient_accounts.len());
        let message_recipients = HashSet::from_iter(message_recipients.into_iter());
        assert_eq!(message_recipients, recipients);
    }
}

macro_rules! test_receipts_distribution {
    ($($name:ident ( $num_producers:literal ))+) => {
        mod test_receipts_can_be_reconstructed {
            $(
                #[test]
                #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
                fn $name() {
                    super::test_receipts_can_be_reconstructed_impl($num_producers);
                }
            )+
        }
        mod test_receipts_are_distributed_to_all_validators {
            $(
                #[test]
                #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
                fn $name() {
                    super::test_receipts_are_distributed_to_all_validators_impl($num_producers);
                }
            )+
        }
    }
}

test_receipts_distribution! {
    with_2_producers(2)
    with_5_producers(5)
    with_10_producers(10)
    with_20_producers(20)
    with_50_producers(50)
    with_100_producers(100)
}

fn drain_outgoing_partial_data(
    outgoing_rc: &mut UnboundedReceiver<OutgoingMessage>,
) -> Vec<(SpicePartialData, HashSet<AccountId>)> {
    let mut requests = Vec::new();
    while let Ok(message) = outgoing_rc.try_recv() {
        let OutgoingMessage::NetworkRequests {
            request: NetworkRequests::SpicePartialData { partial_data, recipients },
        } = message
        else {
            continue;
        };
        requests.push((partial_data, recipients));
    }
    requests
}

/// Flattens each request into its `(data_id, requester)` pairs, since requests the actor
/// sends carry a single id.
fn drain_outgoing_data_requests(
    outgoing_rc: &mut UnboundedReceiver<OutgoingMessage>,
) -> Vec<(SpiceDataIdentifier, AccountId)> {
    let mut requests = Vec::new();
    while let Ok(message) = outgoing_rc.try_recv() {
        let OutgoingMessage::NetworkRequests {
            request: NetworkRequests::SpiceDataRequest { request, producer: _ },
        } = message
        else {
            continue;
        };
        let (wants, requester) = request.into_parts();
        assert_eq!(wants.len(), 1);
        let (data_id, _ordinals) = wants.into_iter().next().unwrap();
        requests.push((data_id, requester));
    }
    requests
}

/// Asks for every part of `data_id`, as the actor's own requests do.
fn want_all_parts(
    data_id: SpiceDataIdentifier,
    total_parts: usize,
) -> BTreeMap<SpiceDataIdentifier, BTreeSet<u64>> {
    BTreeMap::from([(data_id, (0..total_parts as u64).collect())])
}

fn drain_outgoing_witness_request_producers(
    outgoing_rc: &mut UnboundedReceiver<OutgoingMessage>,
    block_hash: &CryptoHash,
) -> Vec<AccountId> {
    let mut asked_producers = Vec::new();
    while let Ok(message) = outgoing_rc.try_recv() {
        let OutgoingMessage::NetworkRequests {
            request: NetworkRequests::SpiceDataRequest { request, producer },
        } = message
        else {
            continue;
        };
        let (wants, _requester) = request.into_parts();
        let Ok((SpiceDataIdentifier::Witness { block_hash: requested_block_hash, .. }, _ordinals)) =
            wants.into_iter().exactly_one()
        else {
            continue;
        };
        if &requested_block_hash == block_hash {
            asked_producers.push(producer);
        }
    }
    asked_producers
}

/// Uses the same assignment lookup as `start_waiting_on_data`, so the sets match.
fn witness_requesters(chain: &Chain, block: &Block, shard_id: ShardId) -> Vec<AccountId> {
    let epoch_id = block.header().epoch_id();
    let producers: HashSet<AccountId> = chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(&epoch_id, shard_id)
        .unwrap()
        .into_iter()
        .collect();
    chain
        .epoch_manager
        .get_chunk_validator_assignments(&epoch_id, shard_id, block.header().height())
        .unwrap()
        .assignments()
        .iter()
        .map(|(account_id, _)| account_id)
        .filter(|account_id| !producers.contains(*account_id))
        .cloned()
        .collect()
}

fn get_incoming_data<T>(
    producer: &AccountId,
    chain: &Chain,
    message: T,
) -> (SpiceIncomingPartialData, Option<AccountId>)
where
    T: Send + 'static,
    SpiceDataDistributorActor: Handler<T>,
{
    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, chain, producer);
    actor.handle(message);
    let (partial_data, recipients) = drain_outgoing_partial_data(&mut outgoing_rc).swap_remove(0);
    let recipient = recipients.into_iter().next();
    (
        SpiceIncomingPartialData { data: partial_data, recv_permit: RecvMessagePermit::none() },
        recipient,
    )
}

fn receipt_proof_incoming_data(
    chain: &Chain,
    block: &Block,
) -> (SpiceIncomingPartialData, AccountId) {
    let receipt_proof = new_test_receipt_proof(block);
    let producer = producers_of_receipt_proof(chain, block, &receipt_proof).swap_remove(0);
    let (data, recipient) = get_incoming_data(
        &producer,
        chain,
        SpiceDistributorOutgoingReceipts {
            block_hash: *block.hash(),
            receipt_proofs: vec![receipt_proof],
        },
    );
    (data, recipient.unwrap())
}

fn witness_incoming_data(chain: &Chain, block: &Block) -> (SpiceIncomingPartialData, AccountId) {
    let state_witness = new_test_witness(&block);
    let producer = witness_producer_accounts(chain, block, &state_witness).swap_remove(0);
    let (data, recipient) = get_incoming_data(
        &producer,
        chain,
        SpiceDistributorStateWitness { contract_accesses: HashSet::new(), state_witness },
    );
    (data, recipient.unwrap())
}

macro_rules! test_invalid_incoming_partial_data {
    ($($name:ident ( $error:pat,  $partial_data_func:ident, $default:ident , $build_block:block ) )+) => {
        mod test_invalid_incoming_partial_data {
            use super::*;
            $(
                #[test]
                #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
                fn $name() {
                    let (_genesis, chain) = setup(2, 0);
                    let block = latest_block(&chain);

                    let (incoming_data, recipient) = $partial_data_func(&chain, &block);

                    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
                    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient);
                    {
                        let $default = data_into_verified(incoming_data.data.clone());
                        let partial_data = $build_block;
                        let result = actor.receive_data(partial_data);
                        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
                        assert_matches!(result, Err(ReceiveDataError::ReceivingDataWithBlock($error)));
                    }
                    actor.handle(incoming_data);
                    assert_matches!(outgoing_rc.try_recv(), Ok(_));
                }
            )+
        }
    }
}

test_invalid_incoming_partial_data! {
    invalid_receipt_proof_from_shard_id(Error::InvalidReceiptFromShardId, receipt_proof_incoming_data, default, {
        let SpiceDataIdentifier::ReceiptProof { from_shard_id: _, block_hash, to_shard_id } =
            default.id
        else {
            panic!();
        };
        let from_shard_id = ShardId::new(42);
        SpicePartialDataBuilder::from_verified(default)
            .id(SpiceDataIdentifier::ReceiptProof { from_shard_id, block_hash, to_shard_id })
            .build()
    })
    invalid_receipt_proof_to_shard_id(Error::InvalidReceiptToShardId, receipt_proof_incoming_data, default, {
        let SpiceDataIdentifier::ReceiptProof { from_shard_id, block_hash, to_shard_id: _ } =
            default.id
        else {
            panic!();
        };
        let to_shard_id = ShardId::new(42);
        SpicePartialDataBuilder::from_verified(default)
            .id(SpiceDataIdentifier::ReceiptProof { from_shard_id, block_hash, to_shard_id })
            .build()
    })
    invalid_witness_shard_id(Error::InvalidWitnessShardId, witness_incoming_data, default, {
        let SpiceDataIdentifier::Witness { shard_id: _, block_hash } = default.id else {
            panic!();
        };
        let shard_id = ShardId::new(42);
        SpicePartialDataBuilder::from_verified(default)
            .id(SpiceDataIdentifier::Witness { block_hash, shard_id })
            .build()
    })
    sender_is_not_validator(Error::SenderIsNotValidator, receipt_proof_incoming_data, default, {
        SpicePartialDataBuilder::from_verified(default)
            .sender(AccountId::from_str("invalid-sender").unwrap())
            .build()
    })
    sender_is_not_producer(Error::SenderIsNotProducer, receipt_proof_incoming_data, default, {
        let SpiceDataIdentifier::ReceiptProof { from_shard_id: _, block_hash, to_shard_id }
            = default.id
        else {
            panic!();
        };
        let from_shard_id = to_shard_id;
        SpicePartialDataBuilder::from_verified(default)
            .id(SpiceDataIdentifier::ReceiptProof { from_shard_id, block_hash, to_shard_id })
            .build()
    })
    node_is_not_recipient(Error::DataIsIrrelevant(_), receipt_proof_incoming_data, default, {
        let SpiceDataIdentifier::ReceiptProof { from_shard_id, to_shard_id: _, block_hash } =
            default.id
        else {
                panic!();
        };
        let to_shard_id = from_shard_id;
        SpicePartialDataBuilder::from_verified(default)
            .id(SpiceDataIdentifier::ReceiptProof { from_shard_id, block_hash, to_shard_id })
            .build()
    })
    merkle_path_does_not_match_commitment_root(Error::InvalidCommitmentRoot, receipt_proof_incoming_data, default, {
        let mut commitment = default.commitment.clone();
        commitment.root = CryptoHash::default();
        SpicePartialDataBuilder::from_verified(default).commitment(commitment).build()
    })
    data_does_not_match_commitment_hash(Error::InvalidCommitmentHash, receipt_proof_incoming_data, default, {
        let mut commitment = default.commitment.clone();
        commitment.hash = CryptoHash::default();
        SpicePartialDataBuilder::from_verified(default).commitment(commitment).build()
    })
    invalid_part_ord(Error::InvalidCommitmentRoot, receipt_proof_incoming_data, default, {
        let mut parts = default.parts.clone();
        parts[0].part_ord = 42;
        SpicePartialDataBuilder::from_verified(default).parts(parts).build()
    })
    undecodable_part(Error::DecodeError(_), receipt_proof_incoming_data, default, {
        let data = "bad data";
        let parts = vec![borsh::to_vec(&data).unwrap()];
        let mut boxed_parts: Vec<Box<[u8]>> =
            parts.into_iter().map(|v| v.into_boxed_slice()).collect();
        let data_hash = hash(&borsh::to_vec(&data).unwrap());
        let (merkle_root, mut merkle_proofs) = merklize(&boxed_parts);
        assert_eq!(boxed_parts.len(), 1);
        assert_eq!(merkle_proofs.len(), 1);
        SpicePartialDataBuilder::from_verified(default)
            .commitment(SpiceDataCommitment {
                hash: data_hash,
                root: merkle_root,
                encoded_length: data.len() as u64,
            })
            .parts(vec![SpiceDataPart {
                part_ord: 0,
                part: boxed_parts.swap_remove(0),
                merkle_proof: merkle_proofs.swap_remove(0),
            }])
            .build()
    })
    invalid_signature(Error::InvalidPartialDataSignature, receipt_proof_incoming_data, default, {
        SpicePartialDataBuilder::from_verified(default)
            .build_with_signature(Signature::default())
    })
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_partial_data_is_already_decoded() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let (incoming_data, recipient) = receipt_proof_incoming_data(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient);
    let data = incoming_data.data.clone();
    actor.handle(incoming_data);
    assert_matches!(outgoing_rc.try_recv(), Ok(_));
    let result = actor.receive_data(data);
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_matches!(
        result,
        Err(ReceiveDataError::ReceivingDataWithBlock(Error::DataIsIrrelevant(_)))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_partial_data_for_already_known_receipts() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let receipt_proof = new_test_receipt_proof(&block);
    let mut store_update = chain.chain_store.store().store_update();
    save_receipt_proof(&mut store_update, block.hash(), &receipt_proof);
    store_update.commit();
    let (incoming_data, recipient) = receipt_proof_incoming_data(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient);
    let SpiceIncomingPartialData { data, .. } = incoming_data;
    let result = actor.receive_data(data);
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_matches!(
        result,
        Err(ReceiveDataError::ReceivingDataWithBlock(Error::DataIsIrrelevant(_)))
    );
}

fn record_endorsement(chain: &Chain, chunk_id: SpiceChunkId, validator: &AccountId) {
    let signer = create_test_signer(validator.as_str());
    let execution_result = ChunkExecutionResult {
        chunk_extra: ChunkExtra::new_with_only_state_root(&CryptoHash::default()),
        outgoing_receipts_root: CryptoHash::default(),
    };
    let mut core_writer_actor = SpiceCoreWriterActor::new(
        chain.runtime_adapter.store().chain_store(),
        chain.epoch_manager.clone(),
        MutableConfigValue::new(None, "validator_signer"),
        core_reader(chain),
        noop().into_sender(),
        noop().into_sender(),
    );
    core_writer_actor.handle(SpiceChunkEndorsementMessage(
        SpiceChunkEndorsement::new(chunk_id, execution_result, &signer),
        RecvMessagePermit::none(),
    ));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_partial_data_for_already_endorsed_witness() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let (incoming_data, recipient) = witness_incoming_data(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient);
    let witness = new_test_witness(&block);
    record_endorsement(&chain, witness.chunk_id().clone(), &recipient);

    let SpiceIncomingPartialData { data, .. } = incoming_data;
    let result = actor.receive_data(data);
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_matches!(
        result,
        Err(ReceiveDataError::ReceivingDataWithBlock(Error::DataIsIrrelevant(_)))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_partial_data_for_witness_with_receipt_id() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);
    let (incoming_data, recipient) = receipt_proof_incoming_data(&chain, &block);
    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient);
    {
        let (witness_partial_data, _) = witness_incoming_data(&chain, &block);
        let witness_partial_data = data_into_verified(witness_partial_data.data);

        let data = SpicePartialDataBuilder::from_default(incoming_data.data.clone())
            .commitment(witness_partial_data.commitment)
            .parts(witness_partial_data.parts)
            .build();
        let result = actor.receive_data(data);
        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(
            result,
            Err(ReceiveDataError::ReceivingDataWithBlock(Error::IdAndDataMismatch))
        );
    }
    actor.handle(incoming_data);
    assert_matches!(outgoing_rc.try_recv(), Ok(_));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_partial_data_for_receipts_with_non_matching_from_shard_id() {
    let (_genesis, chain) = setup(4, 0);
    let block = latest_block(&chain);
    let (incoming_data, recipient) = receipt_proof_incoming_data(&chain, &block);
    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient);
    {
        let mut receipt_proof = new_test_receipt_proof(&block);
        receipt_proof.1.from_shard_id = receipt_proof.1.to_shard_id;
        let producer = producers_of_receipt_proof(&chain, &block, &receipt_proof).swap_remove(0);
        let (different_incoming_data, _recipient) = get_incoming_data(
            &producer,
            &chain,
            SpiceDistributorOutgoingReceipts {
                block_hash: *block.hash(),
                receipt_proofs: vec![receipt_proof],
            },
        );
        let different_incoming_data = data_into_verified(different_incoming_data.data);

        let data = SpicePartialDataBuilder::from_default(incoming_data.data.clone())
            .commitment(different_incoming_data.commitment)
            .parts(different_incoming_data.parts)
            .build();
        let result = actor.receive_data(data);
        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(
            result,
            Err(ReceiveDataError::ReceivingDataWithBlock(Error::InvalidDecodedReceiptFromShardId))
        );
    }
    actor.handle(incoming_data);
    assert_matches!(outgoing_rc.try_recv(), Ok(_));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_partial_data_for_receipts_with_non_matching_to_shard_id() {
    let (_genesis, chain) = setup(4, 0);
    let block = latest_block(&chain);
    let (incoming_data, recipient) = receipt_proof_incoming_data(&chain, &block);
    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient);
    {
        let mut receipt_proof = new_test_receipt_proof(&block);
        receipt_proof.1.to_shard_id = receipt_proof.1.from_shard_id;
        let producer = producers_of_receipt_proof(&chain, &block, &receipt_proof).swap_remove(0);
        let (different_incoming_data, _recipient) = get_incoming_data(
            &producer,
            &chain,
            SpiceDistributorOutgoingReceipts {
                block_hash: *block.hash(),
                receipt_proofs: vec![receipt_proof],
            },
        );
        let different_incoming_data = data_into_verified(different_incoming_data.data);

        let data = SpicePartialDataBuilder::from_default(incoming_data.data.clone())
            .commitment(different_incoming_data.commitment)
            .parts(different_incoming_data.parts)
            .build();
        let result = actor.receive_data(data);
        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(
            result,
            Err(ReceiveDataError::ReceivingDataWithBlock(Error::InvalidDecodedReceiptToShardId))
        );
    }
    actor.handle(incoming_data);
    assert_matches!(outgoing_rc.try_recv(), Ok(_));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_partial_data_for_receipt_with_witness_id() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);
    let (incoming_data, recipient) = witness_incoming_data(&chain, &block);
    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient);
    {
        let (receipt_partial_data, _) = receipt_proof_incoming_data(&chain, &block);
        let receipt_partial_data = data_into_verified(receipt_partial_data.data);

        let data = SpicePartialDataBuilder::from_default(incoming_data.data.clone())
            .commitment(receipt_partial_data.commitment)
            .parts(receipt_partial_data.parts)
            .build();
        let result = actor.receive_data(data);
        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(
            result,
            Err(ReceiveDataError::ReceivingDataWithBlock(Error::IdAndDataMismatch))
        );
    }
    actor.handle(incoming_data);
    assert_matches!(outgoing_rc.try_recv(), Ok(_));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_partial_data_for_witness_with_wrong_shard_id() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let state_witness = new_test_witness(&block);
    let producer = witness_producer_accounts(&chain, &block, &state_witness).swap_remove(0);

    let (incoming_data, recipient) = get_incoming_data(
        &producer,
        &chain,
        SpiceDistributorStateWitness {
            contract_accesses: HashSet::new(),
            state_witness: state_witness.clone(),
        },
    );
    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient.unwrap());
    {
        let different_chunk_header = block
            .chunks()
            .iter_raw()
            .find(|chunk| chunk.shard_id() != state_witness.chunk_id().shard_id)
            .cloned()
            .unwrap();
        let witness_with_different_shard =
            new_test_witness_for_chunk(&block, &different_chunk_header);
        let producer =
            witness_producer_accounts(&chain, &block, &witness_with_different_shard).swap_remove(0);
        let (incoming_data_for_different_witness, _recipient) = get_incoming_data(
            &producer,
            &chain,
            SpiceDistributorStateWitness {
                contract_accesses: HashSet::new(),
                state_witness: witness_with_different_shard,
            },
        );
        let incoming_data_for_different_witness =
            data_into_verified(incoming_data_for_different_witness.data);
        let data = SpicePartialDataBuilder::from_default(incoming_data.data.clone())
            .commitment(incoming_data_for_different_witness.commitment)
            .parts(incoming_data_for_different_witness.parts)
            .build();
        let result = actor.receive_data(data);
        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(
            result,
            Err(ReceiveDataError::ReceivingDataWithBlock(Error::InvalidDecodedWitnessShardId))
        );
    }
    actor.handle(incoming_data);
    assert_matches!(outgoing_rc.try_recv(), Ok(_));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_partial_data_for_witness_with_wrong_block_hash() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let state_witness = new_test_witness(&block);
    let producer = witness_producer_accounts(&chain, &block, &state_witness).swap_remove(0);

    let (incoming_data, recipient) = get_incoming_data(
        &producer,
        &chain,
        SpiceDistributorStateWitness { contract_accesses: HashSet::new(), state_witness },
    );
    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient.unwrap());
    {
        let prev_block = chain.chain_store.get_block(block.header().prev_hash()).unwrap();
        let (incoming_data_for_different_witness, _recipient) =
            witness_incoming_data(&chain, &prev_block);
        let incoming_data_for_different_witness =
            data_into_verified(incoming_data_for_different_witness.data);
        let data = SpicePartialDataBuilder::from_default(incoming_data.data.clone())
            .commitment(incoming_data_for_different_witness.commitment)
            .parts(incoming_data_for_different_witness.parts)
            .build();
        let result = actor.receive_data(data);
        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(
            result,
            Err(ReceiveDataError::ReceivingDataWithBlock(Error::InvalidDecodedWitnessBlockHash))
        );
    }
    actor.handle(incoming_data);
    assert_matches!(outgoing_rc.try_recv(), Ok(_));
}

macro_rules! test_invalid_incoming_partial_data_without_block {
    ($($name:ident ( $error:pat, $partial_data_func:ident, $default:ident , $build_block:block ) )+) => {
        mod test_invalid_incoming_partial_data_without_block {
            use super::*;
            $(
                #[test]
                #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
                fn $name() {
                    let (genesis, mut chain) = setup(2, 0);
                    let block = latest_block(&chain);

                    let receiver_chain = new_chain(&chain, &genesis);

                    // We use next_block to get starting incoming data calling into data
                    // distribution.
                    let next_block = build_block(chain.epoch_manager.as_ref(), &block);
                    process_block_sync(
                        &mut chain,
                        next_block.clone().into(),
                        Provenance::PRODUCED,
                        &mut BlockProcessingArtifact::default(),
                    )
                    .unwrap();
                    let (incoming_data, recipient) = $partial_data_func(&chain, &next_block);

                    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
                    let mut actor = new_actor_for_account(outgoing_sc, &receiver_chain, &recipient);
                    {
                        let $default = data_into_verified(incoming_data.data.clone());
                        let partial_data = $build_block;
                        let result = actor.receive_data(partial_data);
                        assert_eq!(actor.pending_partial_data_size(), 0);
                        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
                        assert_matches!(result, Err(ReceiveDataError::ReceivingDataWithoutBlock($error)));
                    }
                }
            )+
        }
    }
}

test_invalid_incoming_partial_data_without_block! {
    invalid_sender_of_receipts(Error::SenderIsNotValidator, receipt_proof_incoming_data, default, {
        SpicePartialDataBuilder::from_verified(default)
            .sender(AccountId::from_str("invalid-sender").unwrap())
            .build()
    })
    invalid_sender_of_witness(Error::SenderIsNotValidator, witness_incoming_data, default, {
        SpicePartialDataBuilder::from_verified(default)
            .sender(AccountId::from_str("invalid-sender").unwrap())
            .build()
    })
    sender_is_not_producer(Error::SenderIsNotProducer, receipt_proof_incoming_data, default, {
        let SpiceDataIdentifier::ReceiptProof { from_shard_id: _, block_hash, to_shard_id } =
            default.id
        else {
            panic!();
        };
        let from_shard_id = to_shard_id;
        SpicePartialDataBuilder::from_verified(default)
            .id(SpiceDataIdentifier::ReceiptProof { from_shard_id, block_hash, to_shard_id })
            .build()
    })
    invalid_receipts_from_shard(Error::NearChainError(_), receipt_proof_incoming_data, default, {
        let SpiceDataIdentifier::ReceiptProof { from_shard_id: _, block_hash, to_shard_id } =
            default.id
        else {
                panic!();
        };
        let from_shard_id = ShardId::new(42);
        SpicePartialDataBuilder::from_verified(default)
            .id(SpiceDataIdentifier::ReceiptProof { from_shard_id, block_hash, to_shard_id })
            .build()
    })
    invalid_receipts_to_shard(Error::NodeIsNotRecipient, receipt_proof_incoming_data, default, {
        let SpiceDataIdentifier::ReceiptProof { to_shard_id: _, block_hash, from_shard_id } =
            default.id
        else {
                panic!();
        };
        let to_shard_id = ShardId::new(42);
        SpicePartialDataBuilder::from_verified(default)
            .id(SpiceDataIdentifier::ReceiptProof { from_shard_id, block_hash, to_shard_id })
            .build()
    })
    invalid_witness_shard_id(Error::NearChainError(_), witness_incoming_data, default, {
        let SpiceDataIdentifier::Witness { shard_id: _, block_hash } = default.id
        else {
            panic!();
        };
        let shard_id = ShardId::new(42);
        SpicePartialDataBuilder::from_verified(default)
            .id(SpiceDataIdentifier::Witness { block_hash, shard_id })
            .build()
    })
    empty_parts(Error::PartsIsEmpty, receipt_proof_incoming_data, default, {
        SpicePartialDataBuilder::from_verified(default).parts(vec![]).build()
    })
    invalid_signature(Error::InvalidPartialDataSignature, receipt_proof_incoming_data, default, {
        SpicePartialDataBuilder::from_verified(default)
            .build_with_signature(Signature::default())
    })
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_invalid_incoming_partial_data_without_block_node_is_not_recipient() {
    let (genesis, mut chain) = setup(2, 0);
    let block = latest_block(&chain);

    let receiver_chain = new_chain(&chain, &genesis);

    // We use next_block to get starting incoming data calling into data
    // distribution.
    let next_block = build_block(chain.epoch_manager.as_ref(), &block);
    process_block_sync(
        &mut chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    let (incoming_data, _recipient) = witness_incoming_data(&chain, &next_block);
    let verified = data_into_verified(incoming_data.data);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(
        outgoing_sc,
        &receiver_chain,
        &AccountId::from_str("non-validator").unwrap(),
    );
    let partial_data = SpicePartialDataBuilder::from_verified(verified).build();
    let result = actor.receive_data(partial_data);
    assert_eq!(actor.pending_partial_data_size(), 0);
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_matches!(
        result,
        Err(ReceiveDataError::ReceivingDataWithoutBlock(Error::NodeIsNotRecipient))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_data_is_processed_with_block_arriving_late() {
    let (genesis, mut chain) = setup(2, 0);
    let block = latest_block(&chain);

    let mut receiver_chain = new_chain(&chain, &genesis);

    // We use next_block to get starting incoming data calling into data
    // distribution.
    let next_block = build_block(chain.epoch_manager.as_ref(), &block);
    process_block_sync(
        &mut chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    let (incoming_data, recipient) = receipt_proof_incoming_data(&chain, &next_block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &receiver_chain, &recipient);

    actor.handle(incoming_data);
    assert_eq!(actor.pending_partial_data_size(), 1);
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));

    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });
    assert_matches!(outgoing_rc.try_recv(), Ok(_));
    assert_eq!(actor.pending_partial_data_size(), 0);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_witness_requests_from_different_validators_reach_different_producers() {
    let num_chunk_producers = 4;
    let (_genesis, mut chain) =
        setup_with_shard_layout(num_chunk_producers, 8, ShardLayout::single_shard());
    let block = latest_block(&chain);
    let next_block = produce_block(&mut chain, &block);
    save_final_execution_head(&chain, &block);

    let shard_id = witness_shard_id(&next_block);
    let requesters = witness_requesters(&chain, &next_block, shard_id);
    assert!(requesters.len() > 1);

    let asked_producers: HashSet<_> = requesters
        .iter()
        .map(|requester| {
            let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
            let mut actor = new_actor_for_account(outgoing_sc, &chain, requester);
            let mut fake_runner = FakeDelayedActionRunner::default();
            actor.start_actor(&mut fake_runner);

            let asked =
                drain_outgoing_witness_request_producers(&mut outgoing_rc, next_block.hash());
            assert_eq!(asked.len(), 1);
            asked.into_iter().next().unwrap()
        })
        .collect();

    assert!(
        asked_producers.len() > 1,
        "{} requesters all asked {asked_producers:?}",
        requesters.len()
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_witness_request_retries_cycle_through_all_producers() {
    let num_chunk_producers = 4;
    let (_genesis, mut chain) =
        setup_with_shard_layout(num_chunk_producers, 8, ShardLayout::single_shard());
    let block = latest_block(&chain);
    let next_block = produce_block(&mut chain, &block);
    save_final_execution_head(&chain, &block);

    let shard_id = witness_shard_id(&next_block);
    let requester = witness_requesters(&chain, &next_block, shard_id).into_iter().next().unwrap();
    let producers: HashSet<AccountId> = chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(&next_block.header().epoch_id(), shard_id)
        .unwrap()
        .into_iter()
        .collect();

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &requester);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);

    let mut asked_producers =
        drain_outgoing_witness_request_producers(&mut outgoing_rc, next_block.hash());
    for _ in 1..producers.len() {
        fake_runner.run_queued_actions(&mut actor);
        asked_producers
            .extend(drain_outgoing_witness_request_producers(&mut outgoing_rc, next_block.hash()));
    }

    assert_eq!(asked_producers.into_iter().collect::<HashSet<_>>(), producers);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_requesting_witnesses_from_forks_on_start() {
    let (_genesis, mut chain) = setup_with_shard_layout(1, 1, ShardLayout::single_shard());
    let block = latest_block(&chain);
    let shard_id = witness_shard_id(&block);
    let validator = non_producer_witness_validator_account(&chain);

    let next_block = produce_block(&mut chain, &block);
    let next_next_block = produce_block(&mut chain, &next_block);
    let fork_block = produce_block(&mut chain, &block);

    save_final_execution_head(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &validator);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    fake_runner.run_queued_actions(&mut actor);

    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    let requests: HashSet<_> = requests
        .into_iter()
        .filter_map(|(data_id, requester)| {
            assert_eq!(requester, validator);
            let SpiceDataIdentifier::Witness { block_hash, shard_id: request_shard_id } = data_id
            else {
                return None;
            };
            assert_eq!(request_shard_id, shard_id);
            Some(block_hash)
        })
        .collect();
    assert_eq!(
        requests,
        HashSet::from([*next_block.hash(), *next_next_block.hash(), *fork_block.hash()])
    )
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_requesting_witnesses_we_already_endorsed_on_start() {
    let (_genesis, mut chain) = setup(1, 1);
    let block = latest_block(&chain);
    let shard_id = witness_shard_id(&block);
    let validator = witness_validator_account(&chain);

    let next_block = produce_block(&mut chain, &block);

    save_final_execution_head(&chain, &block);
    record_endorsement(
        &chain,
        SpiceChunkId { block_hash: *next_block.hash(), shard_id },
        &validator,
    );

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &validator);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    fake_runner.run_queued_actions(&mut actor);

    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(
        !requests
            .into_iter()
            .map(|(data_id, _)| data_id)
            .contains(&SpiceDataIdentifier::Witness { block_hash: *next_block.hash(), shard_id })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_requesting_witnesses_we_produce_on_start() {
    let (_genesis, mut chain) = setup(1, 1);
    let block = latest_block(&chain);
    let shard_id = witness_shard_id(&block);
    let producer = chunk_producer_for_shard(&chain, shard_id);

    let next_block = produce_block(&mut chain, &block);

    save_final_execution_head(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    fake_runner.run_queued_actions(&mut actor);

    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(
        !requests
            .into_iter()
            .map(|(data_id, _)| data_id)
            .contains(&SpiceDataIdentifier::Witness { block_hash: *next_block.hash(), shard_id })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_requesting_receipts_without_final_execution_head_on_start() {
    let (genesis, chain) = setup(2, 0);
    let (from_shard_id, to_shard_id) =
        genesis.config.shard_layout.shard_ids().collect_tuple().unwrap();
    let recipient = chunk_producer_for_shard(&chain, to_shard_id);

    let mut last_block = latest_block(&chain);
    let mut blocks = HashSet::new();
    while !last_block.header().is_genesis() {
        blocks.insert(*last_block.hash());
        last_block = chain.get_block(last_block.header().prev_hash()).unwrap();
    }

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    fake_runner.run_queued_actions(&mut actor);

    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    let requests: HashSet<_> = requests
        .into_iter()
        .filter_map(|(data_id, requester)| {
            assert_eq!(requester, recipient);
            let SpiceDataIdentifier::ReceiptProof {
                block_hash,
                from_shard_id: request_from_shard_id,
                to_shard_id: request_to_shard_id,
            } = data_id
            else {
                return None;
            };
            assert_eq!(request_from_shard_id, from_shard_id);
            assert_eq!(request_to_shard_id, to_shard_id);
            Some(block_hash)
        })
        .collect();
    assert_eq!(requests, blocks)
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_requesting_receipts_from_forks_on_start() {
    let (genesis, mut chain) = setup(2, 0);
    let (from_shard_id, to_shard_id) =
        genesis.config.shard_layout.shard_ids().collect_tuple().unwrap();
    let recipient = chunk_producer_for_shard(&chain, to_shard_id);

    let block = latest_block(&chain);
    let next_block = produce_block(&mut chain, &block);
    let next_next_block = produce_block(&mut chain, &next_block);
    let fork_block = produce_block(&mut chain, &block);

    save_final_execution_head(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    fake_runner.run_queued_actions(&mut actor);

    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    let requests: HashSet<_> = requests
        .into_iter()
        .filter_map(|(data_id, requester)| {
            assert_eq!(requester, recipient);
            let SpiceDataIdentifier::ReceiptProof {
                block_hash,
                from_shard_id: request_from_shard_id,
                to_shard_id: request_to_shard_id,
            } = data_id
            else {
                return None;
            };
            assert_eq!(request_from_shard_id, from_shard_id);
            assert_eq!(request_to_shard_id, to_shard_id);
            Some(block_hash)
        })
        .collect();
    assert_eq!(
        requests,
        HashSet::from([*next_block.hash(), *next_next_block.hash(), *fork_block.hash()])
    )
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_requesting_receipts_we_already_have_on_start() {
    let (genesis, mut chain) = setup(2, 0);
    let (from_shard_id, to_shard_id) =
        genesis.config.shard_layout.shard_ids().collect_tuple().unwrap();
    let recipient = chunk_producer_for_shard(&chain, to_shard_id);

    let block = latest_block(&chain);
    let next_block = produce_block(&mut chain, &block);

    let mut store_update = chain.chain_store.store().store_update();
    save_receipt_proof(
        &mut store_update,
        next_block.hash(),
        &ReceiptProof(vec![], ShardProof { from_shard_id, to_shard_id, proof: vec![] }),
    );
    store_update.commit();

    save_final_execution_head(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    fake_runner.run_queued_actions(&mut actor);

    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.into_iter().map(|(data_id, _)| data_id).contains(
        &SpiceDataIdentifier::ReceiptProof {
            block_hash: *next_block.hash(),
            from_shard_id,
            to_shard_id
        }
    ));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_requesting_receipts_we_produce_on_start() {
    let (genesis, mut chain) = setup(2, 0);
    let (from_shard_id, to_shard_id) =
        genesis.config.shard_layout.shard_ids().collect_tuple().unwrap();
    let producer = chunk_producer_for_shard(&chain, from_shard_id);

    let block = latest_block(&chain);
    let next_block = produce_block(&mut chain, &block);

    save_final_execution_head(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    fake_runner.run_queued_actions(&mut actor);

    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.into_iter().map(|(data_id, _)| data_id).contains(
        &SpiceDataIdentifier::ReceiptProof {
            block_hash: *next_block.hash(),
            from_shard_id,
            to_shard_id
        }
    ));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_requesting_witness_for_new_block_when_validator() {
    let (genesis, mut chain) = setup(2, 0);
    let block = latest_block(&chain);
    let mut receiver_chain = new_chain(&chain, &genesis);

    let next_block = build_block(chain.epoch_manager.as_ref(), &block);
    process_block_sync(
        &mut chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();

    let (incoming_witness_data, witness_recipient) = witness_incoming_data(&chain, &next_block);
    let data_id = data_into_verified(incoming_witness_data.data).id;

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &receiver_chain, &witness_recipient);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    fake_runner.run_queued_actions(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(requests.contains(&(data_id, witness_recipient)));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_requesting_witness_for_new_block_when_not_validator() {
    let (genesis, mut chain) = setup(2, 0);
    let block = latest_block(&chain);
    let mut receiver_chain = new_chain(&chain, &genesis);

    let next_block = build_block(chain.epoch_manager.as_ref(), &block);
    process_block_sync(
        &mut chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();

    let (incoming_witness_data, _witness_recipient) = witness_incoming_data(&chain, &next_block);
    let data_id = data_into_verified(incoming_witness_data.data).id;

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(
        outgoing_sc,
        &receiver_chain,
        &AccountId::from_str("not-validator").unwrap(),
    );
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    fake_runner.run_queued_actions(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.into_iter().map(|(data_id, _)| data_id).contains(&data_id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_requesting_witness_for_new_block_without_signer() {
    let (genesis, mut chain) = setup(2, 0);
    let block = latest_block(&chain);
    let mut receiver_chain = new_chain(&chain, &genesis);

    let next_block = build_block(chain.epoch_manager.as_ref(), &block);
    process_block_sync(
        &mut chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();

    let (incoming_witness_data, _witness_recipient) = witness_incoming_data(&chain, &next_block);
    let data_id = data_into_verified(incoming_witness_data.data).id;

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = ActorBuilder::new(None).build(outgoing_sc, &receiver_chain);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    fake_runner.run_queued_actions(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.into_iter().map(|(data_id, _)| data_id).contains(&data_id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_requesting_receipts_we_do_not_produce_for_new_block() {
    let (genesis, mut chain) = setup(2, 0);
    let block = latest_block(&chain);
    let mut receiver_chain = new_chain(&chain, &genesis);

    let next_block = build_block(chain.epoch_manager.as_ref(), &block);
    process_block_sync(
        &mut chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();

    let (incoming_receipts_data, receipts_recipient) =
        receipt_proof_incoming_data(&chain, &next_block);
    let data_id = data_into_verified(incoming_receipts_data.data).id;

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &receiver_chain, &receipts_recipient);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    fake_runner.run_queued_actions(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(requests.contains(&(data_id, receipts_recipient)));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_requesting_receipts_we_produce_for_new_block() {
    let (genesis, mut chain) = setup(2, 0);
    let block = latest_block(&chain);
    let mut receiver_chain = new_chain(&chain, &genesis);

    let next_block = build_block(chain.epoch_manager.as_ref(), &block);
    process_block_sync(
        &mut chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();

    let (incoming_receipts_data, receipts_recipient) =
        receipt_proof_incoming_data(&chain, &next_block);
    let data_id = data_into_verified(incoming_receipts_data.data).id;

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = ActorBuilder::new(Some(receipts_recipient))
        .tracked_shards_config(TrackedShardsConfig::AllShards)
        .build(outgoing_sc, &receiver_chain);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    fake_runner.run_queued_actions(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.into_iter().map(|(data_id, _)| data_id).contains(&data_id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_requesting_witnesses_we_produce_for_new_block() {
    let (genesis, mut chain) = setup(2, 0);
    let block = latest_block(&chain);
    let mut receiver_chain = new_chain(&chain, &genesis);

    let next_block = build_block(chain.epoch_manager.as_ref(), &block);
    process_block_sync(
        &mut chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();

    let (incoming_witness_data, witness_recipient) =
        receipt_proof_incoming_data(&chain, &next_block);
    let data_id = data_into_verified(incoming_witness_data.data).id;

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = ActorBuilder::new(Some(witness_recipient))
        .tracked_shards_config(TrackedShardsConfig::AllShards)
        .build(outgoing_sc, &receiver_chain);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    fake_runner.run_queued_actions(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.into_iter().map(|(data_id, _)| data_id).contains(&data_id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_requesting_data_we_already_received() {
    let (genesis, mut chain) = setup(2, 0);
    let block = latest_block(&chain);
    let mut receiver_chain = new_chain(&chain, &genesis);

    let next_block = build_block(chain.epoch_manager.as_ref(), &block);
    process_block_sync(
        &mut chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();

    let (incoming_data, recipient) = receipt_proof_incoming_data(&chain, &next_block);
    let data_id = data_into_verified(incoming_data.data.clone()).id;

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = ActorBuilder::new(Some(recipient)).build(outgoing_sc, &receiver_chain);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });
    actor.handle(incoming_data);

    fake_runner.run_queued_actions(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.into_iter().map(|(data_id, _)| data_id).contains(&data_id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_requesting_data_we_already_received_before_block() {
    let (genesis, mut chain) = setup(2, 0);
    let block = latest_block(&chain);
    let mut receiver_chain = new_chain(&chain, &genesis);

    let next_block = build_block(chain.epoch_manager.as_ref(), &block);
    process_block_sync(
        &mut chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();

    let (incoming_data, recipient) = receipt_proof_incoming_data(&chain, &next_block);
    let data_id = data_into_verified(incoming_data.data.clone()).id;

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = ActorBuilder::new(Some(recipient)).build(outgoing_sc, &receiver_chain);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    actor.handle(incoming_data);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    fake_runner.run_queued_actions(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.iter().map(|(data_id, _)| data_id).contains(&&data_id),);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_data_request_with_receipts_in_store() {
    let (genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);
    let recipient_chain = new_chain(&chain, &genesis);

    let receipt_proof = new_test_receipt_proof(&block);
    let mut store_update = chain.chain_store.store().store_update();
    save_receipt_proof(&mut store_update, block.hash(), &receipt_proof);
    store_update.commit();
    let mut producers = producers_of_receipt_proof(&chain, &block, &receipt_proof);
    let total_parts = producers.len();
    let producer = producers.swap_remove(0);
    let (_incoming_data, recipient) = receipt_proof_incoming_data(&chain, &block);
    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);

    let data_id = SpiceDataIdentifier::ReceiptProof {
        block_hash: *block.hash(),
        from_shard_id: receipt_proof.1.from_shard_id,
        to_shard_id: receipt_proof.1.to_shard_id,
    };
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(want_all_parts(data_id, total_parts), recipient.clone()),
        recv_permit: RecvMessagePermit::none(),
    });
    let (partial_data, recipients) = drain_outgoing_partial_data(&mut outgoing_rc).swap_remove(0);
    assert_eq!(recipients, HashSet::from([recipient.clone()]));

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &recipient_chain, &recipient);
    actor.handle(SpiceIncomingPartialData {
        data: partial_data,
        recv_permit: RecvMessagePermit::none(),
    });

    let message = outgoing_rc.try_recv().unwrap();
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    let OutgoingMessage::ExecutorIncomingUnverifiedReceipts(ExecutorIncomingUnverifiedReceipts {
        receipt_proof: reconstructed_receipt_proof,
        ..
    }) = message
    else {
        panic!();
    };
    assert_eq!(reconstructed_receipt_proof, receipt_proof);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_data_request_with_witness_in_store() {
    let (genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);
    let recipient_chain = new_chain(&chain, &genesis);

    let state_witness = new_test_witness(&block);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &HashSet::new(),
    );

    let mut producers = witness_producer_accounts(&chain, &block, &state_witness);
    let total_parts = producers.len();
    let producer = producers.swap_remove(0);
    let (_incoming_data, recipient) = witness_incoming_data(&chain, &block);
    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);

    let data_id = SpiceDataIdentifier::Witness {
        block_hash: *block.hash(),
        shard_id: state_witness.chunk_id().shard_id,
    };
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(want_all_parts(data_id, total_parts), recipient.clone()),
        recv_permit: RecvMessagePermit::none(),
    });
    let (partial_data, recipients) = drain_outgoing_partial_data(&mut outgoing_rc).swap_remove(0);
    assert_eq!(recipients, HashSet::from([recipient.clone()]));

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &recipient_chain, &recipient);
    actor.handle(SpiceIncomingPartialData {
        data: partial_data,
        recv_permit: RecvMessagePermit::none(),
    });

    let message = outgoing_rc.try_recv().unwrap();
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    let OutgoingMessage::ChunkStateWitnessMessage(SpiceChunkStateWitnessMessage {
        witness: reconstructed_witness,
        ..
    }) = message
    else {
        panic!();
    };
    assert_eq!(reconstructed_witness, state_witness);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_data_request_when_not_producer() {
    let (_genesis, chain) = setup(2, 1);
    let block = latest_block(&chain);
    let state_witness = new_test_witness(&block);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &HashSet::new(),
    );
    let (_incoming_data, recipient) = witness_incoming_data(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient);

    let data_id = SpiceDataIdentifier::Witness {
        block_hash: state_witness.chunk_id().block_hash,
        shard_id: state_witness.chunk_id().shard_id,
    };
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(want_all_parts(data_id, 1), recipient),
        recv_permit: RecvMessagePermit::none(),
    });
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_batched_data_request() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let receipt_proof = new_test_receipt_proof(&block);
    let mut store_update = chain.chain_store.store().store_update();
    save_receipt_proof(&mut store_update, block.hash(), &receipt_proof);
    store_update.commit();

    let state_witness = new_test_witness(&block);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &HashSet::new(),
    );

    // The witness shard is the receipt proof's source shard, so one producer holds both.
    let receipts_producers = producers_of_receipt_proof(&chain, &block, &receipt_proof);
    let witness_producers = witness_producer_accounts(&chain, &block, &state_witness);
    let producer = receipts_producers[0].clone();
    assert!(witness_producers.contains(&producer));

    let witness_id = SpiceDataIdentifier::Witness {
        block_hash: *block.hash(),
        shard_id: state_witness.chunk_id().shard_id,
    };
    let receipts_id = SpiceDataIdentifier::ReceiptProof {
        block_hash: *block.hash(),
        from_shard_id: receipt_proof.1.from_shard_id,
        to_shard_id: receipt_proof.1.to_shard_id,
    };
    let (_incoming_data, requester) = receipt_proof_incoming_data(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(
            BTreeMap::from([
                (witness_id.clone(), (0..witness_producers.len() as u64).collect()),
                (receipts_id.clone(), (0..receipts_producers.len() as u64).collect()),
            ]),
            requester.clone(),
        ),
        recv_permit: RecvMessagePermit::none(),
    });

    let served: HashSet<_> = drain_outgoing_partial_data(&mut outgoing_rc)
        .into_iter()
        .map(|(partial_data, recipients)| {
            assert_eq!(recipients, HashSet::from([requester.clone()]));
            data_into_verified(partial_data).id
        })
        .collect();
    assert_eq!(served, HashSet::from([witness_id, receipts_id]));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_data_request_serves_only_requested_ordinals() {
    let (_genesis, chain) = setup(4, 0);
    let block = latest_block(&chain);

    let state_witness = new_test_witness(&block);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &HashSet::new(),
    );

    let mut producers = witness_producer_accounts(&chain, &block, &state_witness);
    let total_parts = producers.len();
    assert!(total_parts > 1, "requesting a subset needs more than one part");
    let producer = producers.swap_remove(0);
    let (_incoming_data, requester) = witness_incoming_data(&chain, &block);

    let data_id = SpiceDataIdentifier::Witness {
        block_hash: *block.hash(),
        shard_id: state_witness.chunk_id().shard_id,
    };
    let wanted = BTreeSet::from([total_parts as u64 - 1]);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(BTreeMap::from([(data_id, wanted.clone())]), requester),
        recv_permit: RecvMessagePermit::none(),
    });

    let (partial_data, _recipients) = drain_outgoing_partial_data(&mut outgoing_rc).swap_remove(0);
    let served: BTreeSet<u64> =
        data_into_verified(partial_data).parts.iter().map(|part| part.part_ord).collect();
    assert_eq!(served, wanted);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_data_request_with_ordinal_outside_producer_set() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let state_witness = new_test_witness(&block);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &HashSet::new(),
    );

    let mut producers = witness_producer_accounts(&chain, &block, &state_witness);
    let total_parts = producers.len();
    let producer = producers.swap_remove(0);
    let (_incoming_data, requester) = witness_incoming_data(&chain, &block);

    let data_id = SpiceDataIdentifier::Witness {
        block_hash: *block.hash(),
        shard_id: state_witness.chunk_id().shard_id,
    };

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(
            BTreeMap::from([(data_id, BTreeSet::from([total_parts as u64]))]),
            requester,
        ),
        recv_permit: RecvMessagePermit::none(),
    });
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(
        actor.malformed_data_request_count(MalformedDataRequest::OrdinalOutsideProducerSet),
        1
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_batched_data_request_serves_available_entries() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    // Never saved, so this entry has nothing to serve while the witness entry does.
    let receipt_proof = new_test_receipt_proof(&block);

    let state_witness = new_test_witness(&block);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &HashSet::new(),
    );

    let receipts_producers = producers_of_receipt_proof(&chain, &block, &receipt_proof);
    let witness_producers = witness_producer_accounts(&chain, &block, &state_witness);
    let producer = receipts_producers[0].clone();
    assert!(witness_producers.contains(&producer));

    let witness_id = SpiceDataIdentifier::Witness {
        block_hash: *block.hash(),
        shard_id: state_witness.chunk_id().shard_id,
    };
    let receipts_id = SpiceDataIdentifier::ReceiptProof {
        block_hash: *block.hash(),
        from_shard_id: receipt_proof.1.from_shard_id,
        to_shard_id: receipt_proof.1.to_shard_id,
    };
    let (_incoming_data, requester) = receipt_proof_incoming_data(&chain, &block);
    // Entries are served in key order, so the one with nothing to serve has to come first for the
    // served witness to show the batch continued past it.
    assert!(receipts_id < witness_id);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(
            BTreeMap::from([
                (witness_id.clone(), (0..witness_producers.len() as u64).collect()),
                (receipts_id, (0..receipts_producers.len() as u64).collect()),
            ]),
            requester,
        ),
        recv_permit: RecvMessagePermit::none(),
    });

    let served: HashSet<_> = drain_outgoing_partial_data(&mut outgoing_rc)
        .into_iter()
        .map(|(partial_data, _recipients)| data_into_verified(partial_data).id)
        .collect();
    assert_eq!(served, HashSet::from([witness_id]));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_batched_data_request_continues_after_failing_entry() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let receipt_proof = new_test_receipt_proof(&block);
    let mut store_update = chain.chain_store.store().store_update();
    save_receipt_proof(&mut store_update, block.hash(), &receipt_proof);
    store_update.commit();

    let state_witness = new_test_witness(&block);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &HashSet::new(),
    );

    let receipts_producers = producers_of_receipt_proof(&chain, &block, &receipt_proof);
    let witness_producers = witness_producer_accounts(&chain, &block, &state_witness);
    let producer = receipts_producers[0].clone();
    assert!(witness_producers.contains(&producer));

    let witness_id = SpiceDataIdentifier::Witness {
        block_hash: *block.hash(),
        shard_id: state_witness.chunk_id().shard_id,
    };
    let receipts_id = SpiceDataIdentifier::ReceiptProof {
        block_hash: *block.hash(),
        from_shard_id: receipt_proof.1.from_shard_id,
        to_shard_id: receipt_proof.1.to_shard_id,
    };
    let (_incoming_data, requester) = receipt_proof_incoming_data(&chain, &block);
    // Entries are served in key order, so the failing one has to come first for the witness to
    // prove the error did not end the batch.
    assert!(receipts_id < witness_id);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(
            BTreeMap::from([
                (witness_id.clone(), (0..witness_producers.len() as u64).collect()),
                // Outside the producer set, so serving this entry errors.
                (receipts_id, BTreeSet::from([receipts_producers.len() as u64])),
            ]),
            requester,
        ),
        recv_permit: RecvMessagePermit::none(),
    });

    let served: HashSet<_> = drain_outgoing_partial_data(&mut outgoing_rc)
        .into_iter()
        .map(|(partial_data, _recipients)| data_into_verified(partial_data).id)
        .collect();
    assert_eq!(served, HashSet::from([witness_id]));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_data_request_with_too_many_entries() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let state_witness = new_test_witness(&block);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &HashSet::new(),
    );
    let mut producers = witness_producer_accounts(&chain, &block, &state_witness);
    let total_parts = producers.len();
    let producer = producers.swap_remove(0);
    let (_incoming_data, requester) = witness_incoming_data(&chain, &block);

    let data_id = SpiceDataIdentifier::Witness {
        block_hash: *block.hash(),
        shard_id: state_witness.chunk_id().shard_id,
    };
    // The witness entry alone would be served, so nothing coming back shows the whole request
    // was rejected rather than the padding entries individually failing.
    let mut wants = want_all_parts(data_id, total_parts);
    for shard_id in 0..MAX_REQUESTED_DATA_IDS as u64 {
        wants.insert(
            SpiceDataIdentifier::ReceiptProof {
                block_hash: *block.hash(),
                from_shard_id: ShardId::new(shard_id),
                to_shard_id: ShardId::new(shard_id),
            },
            BTreeSet::from([0]),
        );
    }
    assert_eq!(wants.len(), MAX_REQUESTED_DATA_IDS + 1);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(wants, requester),
        recv_permit: RecvMessagePermit::none(),
    });
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(actor.malformed_data_request_count(MalformedDataRequest::TooManyEntries), 1);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_data_request_with_too_many_ordinals() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let state_witness = new_test_witness(&block);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &HashSet::new(),
    );
    let producer = witness_producer_accounts(&chain, &block, &state_witness).swap_remove(0);
    let (_incoming_data, requester) = witness_incoming_data(&chain, &block);

    let data_id = SpiceDataIdentifier::Witness {
        block_hash: *block.hash(),
        shard_id: state_witness.chunk_id().shard_id,
    };
    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(want_all_parts(data_id, MAX_REQUESTED_PARTS + 1), requester),
        recv_permit: RecvMessagePermit::none(),
    });
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(actor.malformed_data_request_count(MalformedDataRequest::TooManyOrdinals), 1);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_data_request_with_no_entries() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let state_witness = new_test_witness(&block);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &HashSet::new(),
    );
    let producer = witness_producer_accounts(&chain, &block, &state_witness).swap_remove(0);
    let (_incoming_data, requester) = witness_incoming_data(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(BTreeMap::new(), requester),
        recv_permit: RecvMessagePermit::none(),
    });
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(actor.malformed_data_request_count(MalformedDataRequest::NoEntries), 1);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_data_request_with_entry_without_ordinals() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let state_witness = new_test_witness(&block);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &HashSet::new(),
    );
    let producer = witness_producer_accounts(&chain, &block, &state_witness).swap_remove(0);
    let (_incoming_data, requester) = witness_incoming_data(&chain, &block);

    let data_id = SpiceDataIdentifier::Witness {
        block_hash: *block.hash(),
        shard_id: state_witness.chunk_id().shard_id,
    };
    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(BTreeMap::from([(data_id, BTreeSet::new())]), requester),
        recv_permit: RecvMessagePermit::none(),
    });
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(actor.malformed_data_request_count(MalformedDataRequest::EntryWithoutOrdinals), 1);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_data_request_with_unknown_shard() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let state_witness = new_test_witness(&block);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &HashSet::new(),
    );
    let mut producers = witness_producer_accounts(&chain, &block, &state_witness);
    let total_parts = producers.len();
    let producer = producers.swap_remove(0);
    let (_incoming_data, requester) = witness_incoming_data(&chain, &block);

    let shard_layout = chain.epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();
    let unknown_shard_id = ShardId::new(u64::MAX);
    assert!(!shard_layout.shard_ids().contains(&unknown_shard_id));
    let data_id =
        SpiceDataIdentifier::Witness { block_hash: *block.hash(), shard_id: unknown_shard_id };

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(want_all_parts(data_id, total_parts), requester),
        recv_permit: RecvMessagePermit::none(),
    });
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(actor.malformed_data_request_count(MalformedDataRequest::UnknownShard), 1);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_requesting_receipts_when_not_validator() {
    let (genesis, chain) = setup(2, 1);
    let requester_chain = new_chain(&chain, &genesis);

    let block = latest_block(&chain);
    let receipt_proof = new_test_receipt_proof(&block);
    let mut store_update = chain.chain_store.store().store_update();
    save_receipt_proof(&mut store_update, block.hash(), &receipt_proof);
    store_update.commit();

    let mut producers = producers_of_receipt_proof(&chain, &block, &receipt_proof);
    let total_parts = producers.len();
    let producer = producers.swap_remove(0);
    let data_id = SpiceDataIdentifier::ReceiptProof {
        block_hash: *block.hash(),
        from_shard_id: receipt_proof.1.from_shard_id,
        to_shard_id: receipt_proof.1.to_shard_id,
    };
    let to_shard_id = receipt_proof.1.to_shard_id;

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);

    let requester = AccountId::from_str("not-validator").unwrap();
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(want_all_parts(data_id, total_parts), requester.clone()),
        recv_permit: RecvMessagePermit::none(),
    });
    let (partial_data, recipients) = drain_outgoing_partial_data(&mut outgoing_rc).swap_remove(0);
    assert_eq!(recipients, HashSet::from([requester.clone()]));

    let to_shard_uid = {
        let shard_layout = chain.epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();
        ShardUId::from_shard_id_and_layout(to_shard_id, &shard_layout)
    };

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = ActorBuilder::new(Some(requester))
        .tracked_shards_config(TrackedShardsConfig::Shards(vec![to_shard_uid]))
        .build(outgoing_sc, &requester_chain);
    actor.handle(SpiceIncomingPartialData {
        data: partial_data,
        recv_permit: RecvMessagePermit::none(),
    });

    let message = outgoing_rc.try_recv().unwrap();
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    let OutgoingMessage::ExecutorIncomingUnverifiedReceipts(ExecutorIncomingUnverifiedReceipts {
        block_hash: reconstructed_block_hash,
        receipt_proof: reconstructed_receipt_proof,
    }) = message
    else {
        panic!();
    };
    assert_eq!(block.hash(), &reconstructed_block_hash);
    assert_eq!(receipt_proof, reconstructed_receipt_proof);
}

/// Verifies that contract accesses are served from the persistent store during catch-up
/// when the in-memory LRU cache has been evicted.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_contract_accesses_served_from_store_on_catchup() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let state_witness = new_test_witness(&block);

    // Persist witness and contract accesses to the store (simulating what the executor does),
    // but do NOT send them through the actor (so the in-memory cache stays empty).
    let contract_accesses: HashSet<CodeHash> =
        HashSet::from([CodeHash(hash(&[1])), CodeHash(hash(&[2]))]);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &contract_accesses,
    );

    let mut producers = witness_producer_accounts(&chain, &block, &state_witness);
    let total_parts = producers.len();
    let producer = producers.swap_remove(0);
    let (_incoming_data, recipient) = witness_incoming_data(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    // Create a fresh actor — its cache is empty, so accesses must come from the store.
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);

    let data_id = SpiceDataIdentifier::Witness {
        block_hash: *block.hash(),
        shard_id: state_witness.chunk_id().shard_id,
    };
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(want_all_parts(data_id, total_parts), recipient.clone()),
        recv_permit: RecvMessagePermit::none(),
    });

    // Collect all outgoing messages and find the contract accesses message.
    let mut found_accesses = false;
    while let Ok(message) = outgoing_rc.try_recv() {
        if let OutgoingMessage::NetworkRequests {
            request: NetworkRequests::SpiceChunkContractAccesses(targets, accesses_msg),
        } = message
        {
            assert_eq!(targets, vec![recipient.clone()]);
            let received: HashSet<CodeHash> = accesses_msg.contracts().iter().cloned().collect();
            assert_eq!(received, contract_accesses);
            found_accesses = true;
        }
    }
    assert!(found_accesses, "expected contract accesses message to be sent from store fallback");
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_duplicate_contract_code_request_is_dropped() {
    let (_genesis, chain) = setup(1, 1);
    let block = latest_block(&chain);
    let state_witness = new_test_witness(&block);
    let chunk_id = state_witness.chunk_id().clone();
    let shard_id = chunk_id.shard_id;

    // Save witness and empty contract accesses so the request passes the access check.
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        shard_id,
        &state_witness,
        &HashSet::new(),
    );

    let producer = witness_producer_accounts(&chain, &block, &state_witness).swap_remove(0);
    let validator = witness_validators(&chain, &block, &state_witness)
        .into_iter()
        .find(|v| v != &producer)
        .unwrap();
    let validator_signer = create_test_signer(validator.as_str());

    let request = SpiceContractCodeRequest::new(chunk_id, HashSet::new(), &validator_signer);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);

    // First request should produce a response.
    actor.handle(SpiceContractCodeRequestMessage(request.clone(), RecvMessagePermit::none()));
    assert_matches!(
        outgoing_rc.try_recv(),
        Ok(OutgoingMessage::NetworkRequests {
            request: NetworkRequests::SpiceContractCodeResponse(_, _),
        })
    );
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));

    // Second identical request should be deduplicated — no response.
    actor.handle(SpiceContractCodeRequestMessage(request, RecvMessagePermit::none()));
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_contract_code_request_invalid_signature_rejected() {
    let (_genesis, chain) = setup(1, 1);
    let block = latest_block(&chain);
    let state_witness = new_test_witness(&block);
    let chunk_id = state_witness.chunk_id().clone();

    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        chunk_id.shard_id,
        &state_witness,
        &HashSet::new(),
    );

    let producer = witness_producer_accounts(&chain, &block, &state_witness).swap_remove(0);
    let validator = witness_validators(&chain, &block, &state_witness)
        .into_iter()
        .find(|v| v != &producer)
        .unwrap();

    // Sign with a wrong key so signature verification fails.
    // Use the correct validator account ID but a different seed to produce a wrong key.
    let wrong_signer =
        InMemoryValidatorSigner::from_seed(validator, KeyType::ED25519, "wrong_seed");
    let tampered_request = SpiceContractCodeRequest::new(chunk_id, HashSet::new(), &wrong_signer);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);

    actor.handle(SpiceContractCodeRequestMessage(tampered_request, RecvMessagePermit::none()));
    // Invalid signature — no response should be sent.
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_contract_code_request_invalid_contract_hash_rejected() {
    let (_genesis, chain) = setup(1, 1);
    let block = latest_block(&chain);
    let state_witness = new_test_witness(&block);
    let chunk_id = state_witness.chunk_id().clone();

    // Save witness and contract accesses with only hash_a.
    let hash_a = CodeHash(hash(&[1]));
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        chunk_id.shard_id,
        &state_witness,
        &HashSet::from([hash_a.clone()]),
    );

    let producer = witness_producer_accounts(&chain, &block, &state_witness).swap_remove(0);
    let validator = witness_validators(&chain, &block, &state_witness)
        .into_iter()
        .find(|v| v != &producer)
        .unwrap();
    let validator_signer = create_test_signer(validator.as_str());

    // Request a contract hash that was NOT accessed in the chunk.
    let hash_b = CodeHash(hash(&[2]));
    let request = SpiceContractCodeRequest::new(
        chunk_id.clone(),
        HashSet::from([hash_b.clone()]),
        &validator_signer,
    );

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);

    actor.handle(SpiceContractCodeRequestMessage(request, RecvMessagePermit::none()));
    // Contract not in valid accesses — no response should be sent.
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));

    // Request both hash_a (valid) and hash_b (invalid) ()using a fresh actor to
    // bypass dedup). Should still be rejected.
    let request =
        SpiceContractCodeRequest::new(chunk_id, HashSet::from([hash_a, hash_b]), &validator_signer);
    let (outgoing_sc2, mut outgoing_rc2) = unbounded_channel();
    let mut actor2 = new_actor_for_account(outgoing_sc2, &chain, &producer);
    actor2.handle(SpiceContractCodeRequestMessage(request, RecvMessagePermit::none()));
    assert_matches!(outgoing_rc2.try_recv(), Err(TryRecvError::Empty));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_contract_code_request_happy_path() {
    let (_genesis, chain) = setup(1, 1);
    let block = latest_block(&chain);
    let state_witness = new_test_witness(&block);
    let chunk_id = state_witness.chunk_id().clone();

    let contract_bytes = b"fake-contract-wasm-bytes";
    let contract_hash = CodeHash(hash(contract_bytes));

    // Save witness and the contract hash as an accessed contract for this chunk.
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        chunk_id.shard_id,
        &state_witness,
        &HashSet::from([contract_hash.clone()]),
    );

    // Store the contract bytes in trie storage so TrieDBStorage can find them.
    let epoch_id = block.header().epoch_id();
    let shard_layout = chain.epoch_manager.get_shard_layout(epoch_id).unwrap();
    let shard_uid = ShardUId::from_shard_id_and_layout(chunk_id.shard_id, &shard_layout);
    let trie_store = TrieStoreAdapter::new(chain.chain_store.store());
    {
        let mut update = trie_store.store_update();
        update.increment_refcount_by(
            shard_uid,
            &contract_hash.0,
            contract_bytes,
            NonZero::new(1).unwrap(),
        );
        update.commit();
    }

    let producer = witness_producer_accounts(&chain, &block, &state_witness).swap_remove(0);
    let validator = witness_validators(&chain, &block, &state_witness)
        .into_iter()
        .find(|v| v != &producer)
        .unwrap();
    let validator_signer = create_test_signer(validator.as_str());

    let request = SpiceContractCodeRequest::new(
        chunk_id.clone(),
        HashSet::from([contract_hash]),
        &validator_signer,
    );

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);

    actor.handle(SpiceContractCodeRequestMessage(request, RecvMessagePermit::none()));

    let message = outgoing_rc.try_recv().unwrap();
    let OutgoingMessage::NetworkRequests {
        request: NetworkRequests::SpiceContractCodeResponse(target, response),
    } = message
    else {
        panic!("expected SpiceContractCodeResponse, got {message:?}");
    };
    assert_eq!(target, validator);
    assert_eq!(response.chunk_id(), &chunk_id);
    let decoded_contracts = response.decompress_contracts().unwrap();
    assert_eq!(decoded_contracts.len(), 1);
    assert_eq!(&*decoded_contracts[0].0, contract_bytes.as_slice());
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
}

fn grow_chain_until_fallback_opens(chain: &mut Chain, shard_id: ShardId) -> SpiceChunkId {
    grow_chain_toward_fallback_opening(chain, shard_id, 0)
}

/// Grows the chain until the all-stake fallback is `blocks_short` blocks away from opening for a
/// chunk of `shard_id`, and returns it. Takes the shard's first uncertified chunk, the oldest one,
/// since every block appends its own.
fn grow_chain_toward_fallback_opening(
    chain: &mut Chain,
    shard_id: ShardId,
    blocks_short: BlockHeight,
) -> SpiceChunkId {
    let head = chain.chain_store.head().unwrap();
    let chunk_info = chain
        .spice_core_reader
        .get_uncertified_chunks(&head.last_block_hash)
        .unwrap()
        .into_iter()
        .find(|chunk_info| chunk_info.chunk_id.shard_id == shard_id)
        .expect("no uncertified chunk for the shard");
    let certifiable_since =
        chunk_info.certifiable_since_height.expect("the oldest chunk is not certifiable yet");

    while chain.chain_store.head().unwrap().height + 1 + blocks_short
        < certifiable_since + SPICE_FALLBACK_CERTIFICATION_DELAY
    {
        produce_block(chain, &latest_block(chain));
    }
    chunk_info.chunk_id
}

fn broadcast_endorsement_chunk_ids(
    outgoing_rc: &mut UnboundedReceiver<OutgoingMessage>,
) -> HashSet<SpiceChunkId> {
    let mut chunk_ids = HashSet::new();
    while let Ok(message) = outgoing_rc.try_recv() {
        let OutgoingMessage::NetworkRequests {
            request: NetworkRequests::SpiceChunkEndorsement(_target, endorsement),
        } = message
        else {
            continue;
        };
        chunk_ids.insert(SpiceChunkId {
            block_hash: *endorsement.block_hash(),
            shard_id: endorsement.shard_id(),
        });
    }
    chunk_ids
}

/// Produces a block whose core statements carry `validator`'s stored endorsement of `chunk_id`.
fn produce_block_carrying_endorsement(
    chain: &mut Chain,
    prev_block: &Block,
    chunk_id: &SpiceChunkId,
    validator: &AccountId,
) -> Arc<Block> {
    let stored = chain
        .spice_core_reader
        .get_endorsement(&chunk_id.block_hash, chunk_id.shard_id, validator)
        .unwrap();
    let core_statement =
        SpiceCoreStatement::Endorsement(testonly_create_endorsement_core_statement(
            validator.clone(),
            stored.signature.clone(),
            SpiceEndorsementSignedData {
                execution_result_hash: stored.execution_result_hash,
                chunk_id: chunk_id.clone(),
            },
        ));
    let block = build_block_with_core_statements(
        chain.epoch_manager.as_ref(),
        prev_block,
        vec![core_statement],
    );
    process_block_sync(
        chain,
        block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    block
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_fallback_endorsement_is_broadcast_on_every_block_until_it_is_on_chain() {
    // More validators than mandates per shard, so some are outside every chunk's designated set.
    let (_genesis, mut chain) = setup(2, 100);
    let shard_id = witness_shard_id(&latest_block(&chain));
    let chunk_id = grow_chain_until_fallback_opens(&mut chain, shard_id);

    let chunk_block = chain.chain_store.get_block(&chunk_id.block_hash).unwrap();
    let validator = fallback_endorsers(
        chain.epoch_manager.as_ref(),
        chunk_block.header().epoch_id(),
        chunk_id.shard_id,
        chunk_block.header().height(),
    )
    .unwrap()
    .into_iter()
    .next()
    .expect("no non-designated validator; increase the validator count");
    record_endorsement(&chain, chunk_id.clone(), &validator);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &validator);

    let mut block = latest_block(&chain);
    for _ in 0..3 {
        actor.handle(ProcessedBlock { block_hash: *block.hash() });
        assert!(broadcast_endorsement_chunk_ids(&mut outgoing_rc).contains(&chunk_id));
        block = produce_block(&mut chain, &block);
    }

    let carrying_block =
        produce_block_carrying_endorsement(&mut chain, &block, &chunk_id, &validator);
    actor.handle(ProcessedBlock { block_hash: *carrying_block.hash() });
    assert!(!broadcast_endorsement_chunk_ids(&mut outgoing_rc).contains(&chunk_id));
}

fn save_test_witness_for_chunk(chain: &Chain, chunk_id: &SpiceChunkId) -> SpiceChunkStateWitness {
    let block = chain.chain_store.get_block(&chunk_id.block_hash).unwrap();
    let chunks = block.chunks();
    let chunk_header =
        chunks.iter_raw().find(|chunk| chunk.shard_id() == chunk_id.shard_id).unwrap();
    let witness = new_test_witness_for_chunk(&block, chunk_header);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        chunk_id.shard_id,
        &witness,
        &HashSet::new(),
    );
    witness
}

/// The chain's first chunk that the schedule marks fallback-only.
fn grow_chain_to_fallback_only_chunk(chain: &mut Chain) -> SpiceChunkId {
    for _ in 0..100 {
        let block = produce_block(chain, &latest_block(chain));
        let chunks = block.chunks();
        let scheduled = chunks.iter_raw().find(|chunk| {
            is_fallback_only_chunk(chain.epoch_manager.as_ref(), block.header(), chunk.shard_id())
                .unwrap()
        });
        if let Some(chunk) = scheduled {
            return SpiceChunkId { block_hash: *block.hash(), shard_id: chunk.shard_id() };
        }
    }
    panic!("no fallback-only chunk within 100 blocks");
}

/// What the push targets: the fallback endorsers of `chunk_id`, less its producers, who already
/// hold the witness.
fn fallback_witness_recipients(chain: &Chain, chunk_id: &SpiceChunkId) -> HashSet<AccountId> {
    let block = chain.chain_store.get_block(&chunk_id.block_hash).unwrap();
    let epoch_id = block.header().epoch_id();
    let producers: HashSet<AccountId> = chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(epoch_id, chunk_id.shard_id)
        .unwrap()
        .into_iter()
        .collect();
    fallback_endorsers(
        chain.epoch_manager.as_ref(),
        epoch_id,
        chunk_id.shard_id,
        block.header().height(),
    )
    .unwrap()
    .into_iter()
    .filter(|account_id| !producers.contains(account_id))
    .collect()
}

fn requested_witness_chunk_ids(
    outgoing_rc: &mut UnboundedReceiver<OutgoingMessage>,
) -> HashSet<SpiceChunkId> {
    drain_outgoing_data_requests(outgoing_rc)
        .into_iter()
        .filter_map(|(data_id, _requester)| match data_id {
            SpiceDataIdentifier::Witness { block_hash, shard_id } => {
                Some(SpiceChunkId { block_hash, shard_id })
            }
            SpiceDataIdentifier::ReceiptProof { .. } => None,
        })
        .collect()
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_witness_is_pushed_when_fallback_opens() {
    // More validators than mandates per shard, so some are outside every chunk's designated set.
    let (_genesis, mut chain) = setup(2, 100);
    let shard_id = witness_shard_id(&latest_block(&chain));
    let chunk_id = grow_chain_until_fallback_opens(&mut chain, shard_id);
    let witness = save_test_witness_for_chunk(&chain, &chunk_id);

    let head_block = latest_block(&chain);
    let producer = chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(head_block.header().epoch_id(), shard_id)
        .unwrap()
        .swap_remove(0);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(ProcessedBlock { block_hash: *head_block.hash() });

    let mut pushes = drain_outgoing_partial_data(&mut outgoing_rc);
    assert_eq!(pushes.len(), 1);
    let (partial_data, recipients) = pushes.swap_remove(0);
    assert_eq!(partial_data.block_hash(), &witness.chunk_id().block_hash);
    assert_eq!(recipients, fallback_witness_recipients(&chain, &chunk_id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_witness_is_pushed_only_once() {
    // More validators than mandates per shard, so some are outside every chunk's designated set.
    let (_genesis, mut chain) = setup(2, 100);
    let shard_id = witness_shard_id(&latest_block(&chain));
    let chunk_id = grow_chain_until_fallback_opens(&mut chain, shard_id);
    save_test_witness_for_chunk(&chain, &chunk_id);

    let head_block = latest_block(&chain);
    let producer = chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(head_block.header().epoch_id(), shard_id)
        .unwrap()
        .swap_remove(0);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(ProcessedBlock { block_hash: *head_block.hash() });
    assert_eq!(drain_outgoing_partial_data(&mut outgoing_rc).len(), 1);

    let next_block = produce_block(&mut chain, &head_block);
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });
    assert!(drain_outgoing_partial_data(&mut outgoing_rc).is_empty());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_fallback_only_witness_is_pushed_without_waiting_for_the_delay() {
    // More validators than mandates per shard, so some are outside every chunk's designated set.
    let (_genesis, mut chain) = setup(2, 100);
    let chunk_id = grow_chain_to_fallback_only_chunk(&mut chain);
    let witness = save_test_witness_for_chunk(&chain, &chunk_id);

    let head_block = latest_block(&chain);
    let producer = chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(head_block.header().epoch_id(), chunk_id.shard_id)
        .unwrap()
        .swap_remove(0);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(ProcessedBlock { block_hash: *head_block.hash() });

    let mut pushes = drain_outgoing_partial_data(&mut outgoing_rc);
    assert_eq!(pushes.len(), 1, "the scheduled chunk's witness was not pushed on its own block");
    let (partial_data, recipients) = pushes.swap_remove(0);
    assert_eq!(partial_data.block_hash(), &witness.chunk_id().block_hash);
    assert_eq!(recipients, fallback_witness_recipients(&chain, &chunk_id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_fallback_witness_is_requested_only_after_the_pull_grace() {
    // More validators than mandates per shard, so some are outside every chunk's designated set.
    let (_genesis, mut chain) = setup(2, 100);
    let shard_id = witness_shard_id(&latest_block(&chain));
    let chunk_id = grow_chain_until_fallback_opens(&mut chain, shard_id);

    let head_block = latest_block(&chain);
    save_final_execution_head(&chain, &head_block);
    let validator =
        fallback_witness_recipients(&chain, &chunk_id).into_iter().sorted().next().unwrap();

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &validator);
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut fake_runner);
    actor.handle(ProcessedBlock { block_hash: *head_block.hash() });

    fake_runner.run_queued_actions(&mut actor);
    assert!(!requested_witness_chunk_ids(&mut outgoing_rc).contains(&chunk_id));

    let mut block = head_block;
    for _ in 0..FALLBACK_WITNESS_PULL_GRACE {
        block = produce_block(&mut chain, &block);
    }
    fake_runner.run_queued_actions(&mut actor);
    assert!(requested_witness_chunk_ids(&mut outgoing_rc).contains(&chunk_id));
}

/// A producer's own part of `chunk_id`'s witness, the same content a fallback push carries.
fn pushed_witness_data(chain: &Chain, chunk_id: &SpiceChunkId) -> SpicePartialData {
    let block = chain.chain_store.get_block(&chunk_id.block_hash).unwrap();
    let chunks = block.chunks();
    let chunk_header =
        chunks.iter_raw().find(|chunk| chunk.shard_id() == chunk_id.shard_id).unwrap();
    let state_witness = new_test_witness_for_chunk(&block, chunk_header);
    let producer = chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(block.header().epoch_id(), chunk_id.shard_id)
        .unwrap()
        .swap_remove(0);
    let (incoming, _) = get_incoming_data(
        &producer,
        chain,
        SpiceDistributorStateWitness { contract_accesses: HashSet::new(), state_witness },
    );
    incoming.data
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_pushed_fallback_witness_is_kept_when_head_is_within_the_lookahead() {
    // More validators than mandates per shard, so some are outside every chunk's designated set.
    let (_genesis, mut chain) = setup(2, 100);
    let shard_id = witness_shard_id(&latest_block(&chain));
    // The producer pushes as soon as it sees the fallback open. This receiver is still short of
    // that height, so it has no entry waiting for the witness.
    let chunk_id =
        grow_chain_toward_fallback_opening(&mut chain, shard_id, FALLBACK_WITNESS_PUSH_LOOKAHEAD);
    let data = pushed_witness_data(&chain, &chunk_id);
    let validator = fallback_witness_recipients(&chain, &chunk_id)
        .into_iter()
        .sorted()
        .next()
        .expect("no non-designated validator; increase the validator count");

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &validator);
    actor.handle(SpiceIncomingPartialData { data, recv_permit: RecvMessagePermit::none() });

    let message = outgoing_rc.try_recv().unwrap();
    let OutgoingMessage::ChunkStateWitnessMessage(SpiceChunkStateWitnessMessage {
        witness, ..
    }) = message
    else {
        panic!("expected the pushed witness to be reassembled");
    };
    assert_eq!(witness.chunk_id(), &chunk_id);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_pushed_fallback_witness_is_dropped_when_head_is_beyond_the_lookahead() {
    // More validators than mandates per shard, so some are outside every chunk's designated set.
    let (_genesis, mut chain) = setup(2, 100);
    let shard_id = witness_shard_id(&latest_block(&chain));
    let chunk_id = grow_chain_toward_fallback_opening(
        &mut chain,
        shard_id,
        FALLBACK_WITNESS_PUSH_LOOKAHEAD + 1,
    );
    let data = pushed_witness_data(&chain, &chunk_id);
    let validator = fallback_witness_recipients(&chain, &chunk_id)
        .into_iter()
        .sorted()
        .next()
        .expect("no non-designated validator; increase the validator count");

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &validator);
    let result = actor.receive_data(data);

    assert_matches!(
        result,
        Err(ReceiveDataError::ReceivingDataWithBlock(Error::DataIsIrrelevant(_)))
    );
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_batched_data_request_gates_entries_separately() {
    let (_genesis, mut chain) = setup(2, 0);
    let block = latest_block(&chain);

    let state_witness = new_test_witness(&block);
    save_witness_and_contract_accesses(
        &chain.chain_store,
        block.hash(),
        state_witness.chunk_id().shard_id,
        &state_witness,
        &HashSet::new(),
    );

    // A block header from the pre-spice side of activation, taken from a pre-spice genesis. Only
    // the header is saved, since that is all the gate reads.
    let pre_spice_header = {
        let pre_spice_genesis = TestGenesisBuilder::new()
            .protocol_version(pre_spice_protocol_version())
            .validators_spec(ValidatorsSpec::desired_roles(&["test-producer-0"], &[]))
            .build();
        let pre_spice_chain = get_chain_with_genesis(Clock::real(), pre_spice_genesis);
        pre_spice_chain.genesis_block().header().clone()
    };
    assert!(!pre_spice_header.is_spice());
    let pre_spice_block_hash = *pre_spice_header.hash();
    let mut store_update = chain.mut_chain_store().store_update();
    store_update.save_block_header(pre_spice_header).unwrap();
    store_update.commit().unwrap();

    let mut producers = witness_producer_accounts(&chain, &block, &state_witness);
    let total_parts = producers.len();
    let producer = producers.swap_remove(0);
    let (_incoming_data, requester) = witness_incoming_data(&chain, &block);

    let witness_id = SpiceDataIdentifier::Witness {
        block_hash: *block.hash(),
        shard_id: state_witness.chunk_id().shard_id,
    };
    let pre_spice_id = SpiceDataIdentifier::Witness {
        block_hash: pre_spice_block_hash,
        shard_id: state_witness.chunk_id().shard_id,
    };

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(SpiceDataRequestMessage {
        request: SpiceDataRequest::new(
            BTreeMap::from([
                (witness_id.clone(), (0..total_parts as u64).collect()),
                (pre_spice_id, (0..total_parts as u64).collect()),
            ]),
            requester,
        ),
        recv_permit: RecvMessagePermit::none(),
    });

    let served: HashSet<_> = drain_outgoing_partial_data(&mut outgoing_rc)
        .into_iter()
        .map(|(partial_data, _recipients)| data_into_verified(partial_data).id)
        .collect();
    assert_eq!(served, HashSet::from([witness_id]));
    assert_eq!(actor.spice_dropped_count(SpiceMessageKind::DataRequest), 1);
}
