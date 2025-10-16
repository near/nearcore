use near_async::futures::DelayedActionRunner;
use near_async::messaging::Actor;
use near_chain::ChainStoreAccess;
use near_chain::spice_core_writer_actor::{ProcessedBlock, SpiceCoreWriterActor};
use near_crypto::Signature;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::client::SpiceChunkEndorsementMessage;
use near_primitives::spice_partial_data::{
    SpiceDataCommitment, SpiceDataIdentifier, SpiceDataPart, SpicePartialData,
    SpiceVerifiedPartialData, testonly_create_spice_partial_data,
};
use near_primitives::stateless_validation::spice_chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::stateless_validation::spice_state_witness::{
    SpiceChunkStateTransition, SpiceChunkStateWitness,
};
use near_store::ShardUId;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use assert_matches::assert_matches;
use itertools::Itertools as _;
use near_async::messaging::{Handler, IntoAsyncSender, IntoSender, Sender, noop};
use near_async::time::Clock;
use near_chain::Block;
use near_chain::test_utils::{
    get_chain_with_genesis, get_fake_next_block_chunk_headers, process_block_sync,
};
use near_chain::{BlockProcessingArtifact, Chain, Provenance};
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_chain_configs::{Genesis, MutableConfigValue, TrackedShardsConfig};
use near_epoch_manager::EpochManagerAdapter;
use near_network::spice_data_distribution::{SpiceIncomingPartialData, SpicePartialDataRequest};
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::hash::hash;
use near_primitives::merkle::merklize;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ReceiptProof;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::sharding::ShardProof;
use near_primitives::state::PartialState;
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::ShardId;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, ChunkExecutionResultHash};
use near_primitives::types::{BlockHeight, ChunkExecutionResult};
use near_store::adapter::StoreAdapter;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::chunk_executor_actor::{ExecutorIncomingUnverifiedReceipts, save_receipt_proof};
use crate::spice_chunk_validator_actor::SpiceChunkStateWitnessMessage;
use crate::spice_data_distributor_actor::{
    DataIsKnownError, Error, ReceiveDataError, SpiceDataDistributorActor,
    SpiceDistributorOutgoingReceipts, SpiceDistributorStateWitness,
};

fn build_block(epoch_manager: &dyn EpochManagerAdapter, prev_block: &Block) -> Arc<Block> {
    let block_producer = epoch_manager
        .get_block_producer_info(prev_block.header().epoch_id(), prev_block.header().height() + 1)
        .unwrap();
    let signer = Arc::new(create_test_signer(block_producer.account_id().as_str()));
    TestBlockBuilder::new(Clock::real(), prev_block, signer)
        .chunks(get_fake_next_block_chunk_headers(&prev_block, epoch_manager))
        .spice_core_statements(vec![])
        .build()
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
    let state_transition = SpiceChunkStateTransition {
        base_state: PartialState::TrieValues(vec![]),
        post_state_root: CryptoHash::default(),
    };
    let receipt_proofs = HashMap::new();
    let receipts_hash = CryptoHash::default();
    let transactions = vec![];
    SpiceChunkStateWitness::new(
        near_primitives::types::SpiceChunkId {
            block_hash: *block.hash(),
            shard_id: chunk_header.shard_id(),
        },
        state_transition,
        receipt_proofs,
        receipts_hash,
        transactions,
        ChunkExecutionResultHash(CryptoHash::default()),
    )
}

fn new_test_witness(block: &Block) -> SpiceChunkStateWitness {
    let chunks = block.chunks();
    let chunk_header = &chunks[0];
    new_test_witness_for_chunk(block, chunk_header)
}

fn setup(num_chunk_producers: usize, num_validators: usize) -> (Genesis, Chain) {
    init_test_logger();

    let num_shards = 2;
    let shard_layout = ShardLayout::multi_shard(num_shards, 0);

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
        };
        SpiceDataDistributorActor::new(
            epoch_manager,
            chain.chain_store.store().chain_store(),
            validator_signer,
            shard_tracker,
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
        )
    }
}

fn new_actor_for_account(
    outgoing_sc: UnboundedSender<OutgoingMessage>,
    chain: &Chain,
    account_id: &AccountId,
) -> SpiceDataDistributorActor {
    ActorBuilder::new(Some(account_id.clone())).build(outgoing_sc, chain)
}

type FakeActionTask = Box<
    dyn FnOnce(
            &mut SpiceDataDistributorActor,
            &mut dyn DelayedActionRunner<SpiceDataDistributorActor>,
        ) + Send
        + 'static,
>;

#[derive(Default)]
struct FakeActionRunner {
    tasks: Vec<FakeActionTask>,
}

impl DelayedActionRunner<SpiceDataDistributorActor> for FakeActionRunner {
    fn run_later_boxed(
        &mut self,
        _name: &'static str,
        _dur: near_async::time::Duration,
        f: FakeActionTask,
    ) {
        self.tasks.push(f);
    }
}

impl FakeActionRunner {
    fn trigger(&mut self, actor: &mut SpiceDataDistributorActor) {
        let tasks = std::mem::take(&mut self.tasks);
        for task in tasks {
            task(actor, self);
        }
    }
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

fn receipt_producer_accounts(
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

fn receipt_recipients_accounts(
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
        producer.handle(SpiceDistributorStateWitness { state_witness: state_witness.clone() })
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
            panic!()
        };
        assert!(recipients.contains(validator));
        receiver.handle(SpiceIncomingPartialData { data: partial_data.clone() });
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
        producer.handle(SpiceDistributorStateWitness { state_witness: state_witness.clone() })
    }

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
    with_5000_producers_1_validator(5000, 1)
    with_5000_producers_10_validators(5000, 10)
    with_5000_producers_100_validators(5000, 100)
    with_5000_producers_1000_validators(5000, 1000)
    with_5000_producers_4000_validators(5000, 4000)
}

fn test_receipts_can_be_reconstructed_impl(num_chunk_producers: usize) {
    let (genesis, chain) = setup(num_chunk_producers, 0);

    let block = latest_block(&chain);
    let receipt_proof = new_test_receipt_proof(&block);

    let producer_accounts = &receipt_producer_accounts(&chain, &block, &receipt_proof);
    let recipient_accounts = &receipt_recipients_accounts(&chain, &block, &receipt_proof);

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
        receiver.handle(SpiceIncomingPartialData { data: partial_data.clone() });
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

    let producer_accounts = &receipt_producer_accounts(&chain, &block, &receipt_proof);
    let recipient_accounts = &receipt_recipients_accounts(&chain, &block, &receipt_proof);

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
    with_500_producers(500)
    with_5000_producers(5000)
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

fn drain_outgoing_data_requests(
    outgoing_rc: &mut UnboundedReceiver<OutgoingMessage>,
) -> Vec<SpicePartialDataRequest> {
    let mut requests = Vec::new();
    while let Ok(message) = outgoing_rc.try_recv() {
        let OutgoingMessage::NetworkRequests {
            request: NetworkRequests::SpicePartialDataRequest { request, producer: _ },
        } = message
        else {
            continue;
        };
        requests.push(request);
    }
    requests
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
    (SpiceIncomingPartialData { data: partial_data }, recipient)
}

fn receipt_proof_incoming_data(
    chain: &Chain,
    block: &Block,
) -> (SpiceIncomingPartialData, AccountId) {
    let receipt_proof = new_test_receipt_proof(block);
    let producer = receipt_producer_accounts(chain, block, &receipt_proof).swap_remove(0);
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
    let (data, recipient) =
        get_incoming_data(&producer, chain, SpiceDistributorStateWitness { state_witness });
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
    node_is_not_recipient(Error::NodeIsNotRecipient, receipt_proof_incoming_data, default, {
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
    actor.handle(incoming_data.clone());
    assert_matches!(outgoing_rc.try_recv(), Ok(_));
    let SpiceIncomingPartialData { data } = incoming_data;
    let result = actor.receive_data(data);
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_matches!(
        result,
        Err(ReceiveDataError::ReceivingDataWithBlock(Error::DataIsKnown(
            DataIsKnownError::ReceiptsDecoded { .. }
        )))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_partial_data_for_already_known_receipts() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let receipt_proof = new_test_receipt_proof(&block);
    let mut store_update = chain.chain_store.store().store_update();
    save_receipt_proof(&mut store_update, block.hash(), &receipt_proof).unwrap();
    store_update.commit().unwrap();
    let (incoming_data, recipient) = receipt_proof_incoming_data(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &recipient);
    let SpiceIncomingPartialData { data } = incoming_data;
    let result = actor.receive_data(data);
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_matches!(
        result,
        Err(ReceiveDataError::ReceivingDataWithBlock(Error::DataIsKnown(
            DataIsKnownError::ReceiptsKnown { .. }
        )))
    );
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
    let signer = create_test_signer(recipient.as_str());
    let execution_result = ChunkExecutionResult {
        chunk_extra: ChunkExtra::new_with_only_state_root(&CryptoHash::default()),
        outgoing_receipts_root: CryptoHash::default(),
    };
    let mut core_writer_actor = SpiceCoreWriterActor::new(
        chain.runtime_adapter.store().chain_store(),
        chain.epoch_manager.clone(),
        noop().into_sender(),
        noop().into_sender(),
    );
    core_writer_actor.handle(SpiceChunkEndorsementMessage(SpiceChunkEndorsement::new(
        witness.chunk_id().clone(),
        execution_result,
        &signer,
    )));

    let SpiceIncomingPartialData { data } = incoming_data;
    let result = actor.receive_data(data);
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_matches!(
        result,
        Err(ReceiveDataError::ReceivingDataWithBlock(Error::DataIsKnown(
            DataIsKnownError::WitnessValidated(..)
        )))
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
        let producer = receipt_producer_accounts(&chain, &block, &receipt_proof).swap_remove(0);
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
        let producer = receipt_producer_accounts(&chain, &block, &receipt_proof).swap_remove(0);
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
        SpiceDistributorStateWitness { state_witness: state_witness.clone() },
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
            SpiceDistributorStateWitness { state_witness: witness_with_different_shard },
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

    let (incoming_data, recipient) =
        get_incoming_data(&producer, &chain, SpiceDistributorStateWitness { state_witness });
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
    let mut fake_runner = FakeActionRunner::default();
    actor.start_actor(&mut fake_runner);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    fake_runner.trigger(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(requests.contains(&SpicePartialDataRequest { data_id, requester: witness_recipient }));
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
    let mut fake_runner = FakeActionRunner::default();
    actor.start_actor(&mut fake_runner);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    fake_runner.trigger(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.into_iter().map(|r| r.data_id).contains(&data_id));
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
    let mut fake_runner = FakeActionRunner::default();
    actor.start_actor(&mut fake_runner);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    fake_runner.trigger(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.into_iter().map(|r| r.data_id).contains(&data_id));
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
    let mut fake_runner = FakeActionRunner::default();
    actor.start_actor(&mut fake_runner);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    fake_runner.trigger(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(requests.contains(&SpicePartialDataRequest { data_id, requester: receipts_recipient }));
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
    let mut fake_runner = FakeActionRunner::default();
    actor.start_actor(&mut fake_runner);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    fake_runner.trigger(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.into_iter().map(|r| r.data_id).contains(&data_id));
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
    let mut fake_runner = FakeActionRunner::default();
    actor.start_actor(&mut fake_runner);
    process_block_sync(
        &mut receiver_chain,
        next_block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    fake_runner.trigger(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.into_iter().map(|r| r.data_id).contains(&data_id));
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
    let mut fake_runner = FakeActionRunner::default();
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

    fake_runner.trigger(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.into_iter().map(|r| r.data_id).contains(&data_id));
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
    let mut fake_runner = FakeActionRunner::default();
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

    fake_runner.trigger(&mut actor);
    let requests = drain_outgoing_data_requests(&mut outgoing_rc);
    assert!(!requests.iter().map(|r| &r.data_id).contains(&data_id),);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handling_partial_data_request_with_data_available() {
    let (_genesis, chain) = setup(2, 1);
    let block = latest_block(&chain);
    let state_witness = new_test_witness(&block);
    let producer = witness_producer_accounts(&chain, &block, &state_witness).swap_remove(0);
    let data_id = SpiceDataIdentifier::Witness {
        block_hash: state_witness.chunk_id().block_hash,
        shard_id: state_witness.chunk_id().shard_id,
    };

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(SpiceDistributorStateWitness { state_witness: state_witness.clone() });
    let (_, recipients) = drain_outgoing_partial_data(&mut outgoing_rc).swap_remove(0);

    let requester = recipients.into_iter().next().unwrap();
    actor.handle(SpicePartialDataRequest { data_id, requester: requester.clone() });
    let (partial_data, recipients) = drain_outgoing_partial_data(&mut outgoing_rc).swap_remove(0);
    assert_eq!(recipients, HashSet::from([requester.clone()]));

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &requester);
    actor.handle(SpiceIncomingPartialData { data: partial_data });

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
fn test_requesting_receipts_when_not_validator() {
    let (_genesis, chain) = setup(2, 1);
    let block = latest_block(&chain);
    let receipt_proof = new_test_receipt_proof(&block);
    let producer = receipt_producer_accounts(&chain, &block, &receipt_proof).swap_remove(0);
    let data_id = SpiceDataIdentifier::ReceiptProof {
        block_hash: *block.hash(),
        from_shard_id: receipt_proof.1.from_shard_id,
        to_shard_id: receipt_proof.1.to_shard_id,
    };
    let to_shard_id = receipt_proof.1.to_shard_id;

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor_for_account(outgoing_sc, &chain, &producer);
    actor.handle(SpiceDistributorOutgoingReceipts {
        block_hash: *block.hash(),
        receipt_proofs: vec![receipt_proof.clone()],
    });
    drain_outgoing_partial_data(&mut outgoing_rc).swap_remove(0);

    let requester = AccountId::from_str("not-validator").unwrap();
    actor.handle(SpicePartialDataRequest { data_id, requester: requester.clone() });
    let (partial_data, recipients) = drain_outgoing_partial_data(&mut outgoing_rc).swap_remove(0);
    assert_eq!(recipients, HashSet::from([requester.clone()]));

    let to_shard_uid = {
        let shard_layout = chain.epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();
        ShardUId::from_shard_id_and_layout(to_shard_id, &shard_layout)
    };

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = ActorBuilder::new(Some(requester))
        .tracked_shards_config(TrackedShardsConfig::Shards(vec![to_shard_uid]))
        .build(outgoing_sc, &chain);
    actor.handle(SpiceIncomingPartialData { data: partial_data });

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
