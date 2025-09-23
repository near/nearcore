use near_chain::ChainStoreAccess;
use near_chain::spice_core::CoreStatementsProcessor;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use assert_matches::assert_matches;
use itertools::Itertools as _;
use near_async::messaging::Handler;
use near_async::messaging::Sender;
use near_async::messaging::{IntoSender as _, noop};
use near_async::time::Clock;
use near_chain::Block;
use near_chain::chain::ChunkStateWitnessMessage;
use near_chain::test_utils::{
    get_chain_with_genesis, get_fake_next_block_chunk_headers, process_block_sync,
};
use near_chain::{BlockProcessingArtifact, Chain, Provenance};
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_chain_configs::{Genesis, MutableConfigValue};
use near_epoch_manager::EpochManagerAdapter;
use near_network::spice_data_distribution::SpiceDataCommitment;
use near_network::spice_data_distribution::SpiceDataIdentifier;
use near_network::spice_data_distribution::SpiceDataPart;
use near_network::spice_data_distribution::SpiceIncomingPartialData;
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
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::state_witness::ChunkStateTransition;
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::AccountId;
use near_primitives::types::ChunkExecutionResult;
use near_primitives::types::ShardId;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::adapter::StoreAdapter;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::chunk_executor_actor::{
    ExecutorIncomingUnverifiedReceipts, ProcessedBlock, save_receipt_proof,
};
use crate::spice_data_distributor_actor::{
    Error, SpiceDataDistributorActor, SpiceDistributorOutgoingReceipts,
    SpiceDistributorStateWitness,
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
#[derive(Debug)]
enum OutgoingMessage {
    NetworkRequests { request: NetworkRequests },
    ExecutorIncomingUnverifiedReceipts(ExecutorIncomingUnverifiedReceipts),
    ChunkStateWitnessMessage(ChunkStateWitnessMessage),
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

fn new_test_witness_for_chunk(block: &Block, chunk_header: &ShardChunkHeader) -> ChunkStateWitness {
    let epoch_id = block.header().epoch_id();
    let state_transition = ChunkStateTransition {
        block_hash: *block.hash(),
        base_state: PartialState::TrieValues(vec![]),
        post_state_root: CryptoHash::default(),
    };
    let receipt_proofs = HashMap::new();
    let receipts_hash = CryptoHash::default();
    let transactions = vec![];
    let implicit_transitions = vec![];
    let new_transactions = vec![];
    ChunkStateWitness::new(
        AccountId::from_str("unused").unwrap(),
        *epoch_id,
        chunk_header.clone(),
        state_transition,
        receipt_proofs,
        receipts_hash,
        transactions,
        implicit_transitions,
        new_transactions,
        PROTOCOL_VERSION,
    )
}

fn new_test_witness(block: &Block) -> ChunkStateWitness {
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

fn new_actor(
    outgoing_sc: UnboundedSender<OutgoingMessage>,
    chain: &Chain,
    account_id: &AccountId,
) -> SpiceDataDistributorActor {
    let signer = Arc::new(create_test_signer(account_id.as_str()));
    let validator_signer = MutableConfigValue::new(Some(signer), "validator_signer");
    let epoch_manager = chain.epoch_manager.clone();
    let core_processor = CoreStatementsProcessor::new_with_noop_senders(
        chain.chain_store.store().chain_store(),
        epoch_manager.clone(),
    );

    let network_adapter = PeerManagerAdapter {
        async_request_sender: noop().into_sender(),
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
        core_processor,
        validator_signer,
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
            move |message: SpanWrapped<ChunkStateWitnessMessage>| {
                outgoing_sc
                    .send(OutgoingMessage::ChunkStateWitnessMessage(message.span_unwrap()))
                    .unwrap();
            }
        }),
    )
}

fn witness_producer_accounts(
    chain: &Chain,
    block: &Block,
    witness: &ChunkStateWitness,
) -> Vec<AccountId> {
    let key = witness.chunk_production_key();
    chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(block.header().epoch_id(), key.shard_id)
        .unwrap()
}

fn witness_validators(chain: &Chain, block: &Block, witness: &ChunkStateWitness) -> Vec<AccountId> {
    let key = witness.chunk_production_key();
    let validator_assignment = chain
        .epoch_manager
        .get_chunk_validator_assignments(
            block.header().epoch_id(),
            key.shard_id,
            key.height_created,
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
        .map(|producer| new_actor(producers_messages_sc.clone(), &chain, producer))
        .collect_vec();
    for producer in &mut producers {
        producer.handle(SpiceDistributorStateWitness { state_witness: state_witness.clone() })
    }

    let (receiver_messages_sc, mut receiver_messages_rc) = unbounded_channel();
    let validator = recipient_accounts.iter().next().unwrap();

    // Separate chain makes sure that receiver doesn't share storage with producers.
    let receiver_chain = new_chain(&chain, &genesis);
    let mut receiver = new_actor(receiver_messages_sc, &receiver_chain, validator);
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
    let OutgoingMessage::ChunkStateWitnessMessage(ChunkStateWitnessMessage {
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
        .map(|producer| new_actor(producers_messages_sc.clone(), &chain, producer))
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
        .map(|producer| new_actor(producers_messages_sc.clone(), &chain, producer))
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
    let mut receiver = new_actor(receiver_messages_sc, &receiver_chain, receiver_account);
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
        .map(|producer| new_actor(producers_messages_sc.clone(), &chain, producer))
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

fn get_incoming_data<T>(
    producer: &AccountId,
    chain: &Chain,
    message: T,
) -> (SpiceIncomingPartialData, Option<AccountId>)
where
    T: actix::Message,
    SpiceDataDistributorActor: Handler<T>,
{
    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor(outgoing_sc, chain, producer);
    actor.handle(message);
    let OutgoingMessage::NetworkRequests {
        request: NetworkRequests::SpicePartialData { partial_data, recipients },
    } = outgoing_rc.try_recv().unwrap()
    else {
        panic!();
    };
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
    ($($name:ident ( $error:pat,  $partial_data_func:ident, $incoming_data:ident , $update_block:block ) )+) => {
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
                    let mut actor = new_actor(outgoing_sc, &chain, &recipient);
                    {
                        let mut $incoming_data = incoming_data.clone();
                        $update_block
                        let SpiceIncomingPartialData { data } = $incoming_data;
                        let result = actor.receive_data(data);
                        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
                        assert_matches!(result, Err($error));
                    }
                    actor.handle(incoming_data);
                    assert_matches!(outgoing_rc.try_recv(), Ok(_));
                }
            )+
        }
    }
}

test_invalid_incoming_partial_data! {
    invalid_receipt_proof_from_shard_id(Error::InvalidReceiptFromShardId, receipt_proof_incoming_data, incoming_data, {
        let SpiceDataIdentifier::ReceiptProof { from_shard_id, .. } = &mut incoming_data.data.id else {
            panic!();
        };
        *from_shard_id = ShardId::new(42);
    })
    invalid_receipt_proof_to_shard_id(Error::InvalidReceiptToShardId, receipt_proof_incoming_data, incoming_data, {
        let SpiceDataIdentifier::ReceiptProof { to_shard_id, .. } = &mut incoming_data.data.id else {
            panic!();
        };
        *to_shard_id = ShardId::new(42);
    })
    invalid_witness_shard_id(Error::InvalidWitnessShardId, witness_incoming_data, incoming_data, {
        let SpiceDataIdentifier::Witness { shard_id, .. } = &mut incoming_data.data.id else {
            panic!();
        };
        *shard_id = ShardId::new(42);
    })
    sender_is_not_producer(Error::SenderIsNotProducer, receipt_proof_incoming_data, incoming_data, {
        incoming_data.data.sender = AccountId::from_str("invalid-sender").unwrap();
    })
    node_is_not_recipient(Error::NodeIsNotRecipient, receipt_proof_incoming_data, incoming_data, {
        let SpiceDataIdentifier::ReceiptProof { from_shard_id, to_shard_id, .. } =
            &mut incoming_data.data.id else {
            panic!();
        };
        *to_shard_id = *from_shard_id;
    })
    merkle_path_does_not_match_commitment_root(Error::InvalidCommitmentRoot, receipt_proof_incoming_data, incoming_data, {
        incoming_data.data.commitment.root = CryptoHash::default();
    })
    data_does_not_match_commitment_hash(Error::InvalidCommitmentHash, receipt_proof_incoming_data, incoming_data, {
        incoming_data.data.commitment.hash = CryptoHash::default();
    })
    invalid_part_ord(Error::InvalidCommitmentRoot, receipt_proof_incoming_data, incoming_data, {
        incoming_data.data.parts[0].part_ord = 42;
    })
    undecodable_part(Error::DecodeError(_), receipt_proof_incoming_data, incoming_data, {
        let data = "bad data";
        let parts = vec![borsh::to_vec(&data).unwrap()];
        let mut boxed_parts: Vec<Box<[u8]>> =
            parts.into_iter().map(|v| v.into_boxed_slice()).collect();
        let data_hash = hash(&borsh::to_vec(&data).unwrap());
        let (merkle_root, mut merkle_proofs) = merklize(&boxed_parts);
        assert_eq!(boxed_parts.len(), 1);
        assert_eq!(merkle_proofs.len(), 1);
        incoming_data.data.commitment = SpiceDataCommitment {
            hash: data_hash,
            root: merkle_root,
            encoded_length: data.len() as u64,
        };
        incoming_data.data.parts = vec![SpiceDataPart {
            part_ord: 0,
            part: boxed_parts.swap_remove(0),
            merkle_proof: merkle_proofs.swap_remove(0),
        }];
    })

}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_partial_data_is_already_decoded() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let (incoming_data, recipient) = receipt_proof_incoming_data(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor(outgoing_sc, &chain, &recipient);
    actor.handle(incoming_data.clone());
    assert_matches!(outgoing_rc.try_recv(), Ok(_));
    let SpiceIncomingPartialData { data } = incoming_data;
    let result = actor.receive_data(data);
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_matches!(result, Err(Error::DataIsAlreadyDecoded));
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
    let mut actor = new_actor(outgoing_sc, &chain, &recipient);
    let SpiceIncomingPartialData { data } = incoming_data;
    let result = actor.receive_data(data);
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_matches!(result, Err(Error::ReceiptsAreKnown));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_partial_data_for_already_endorsed_witness() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);

    let (incoming_data, recipient) = witness_incoming_data(&chain, &block);

    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor(outgoing_sc, &chain, &recipient);
    let witness = new_test_witness(&block);
    let signer = create_test_signer(recipient.as_str());
    let execution_result = ChunkExecutionResult {
        chunk_extra: ChunkExtra::new_with_only_state_root(&CryptoHash::default()),
        outgoing_receipts_root: CryptoHash::default(),
    };
    actor
        .core_processor
        .record_chunk_endorsement(ChunkEndorsement::new_with_execution_result(
            *block.header().epoch_id(),
            execution_result,
            *block.hash(),
            witness.chunk_header(),
            &signer,
        ))
        .unwrap();

    let SpiceIncomingPartialData { data } = incoming_data;
    let result = actor.receive_data(data);
    assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
    assert_matches!(result, Err(Error::WitnessAlreadyValidated));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_incoming_partial_data_for_witness_with_receipt_id() {
    let (_genesis, chain) = setup(2, 0);
    let block = latest_block(&chain);
    let (incoming_data, recipient) = receipt_proof_incoming_data(&chain, &block);
    let (outgoing_sc, mut outgoing_rc) = unbounded_channel();
    let mut actor = new_actor(outgoing_sc, &chain, &recipient);
    {
        let mut incoming_data = incoming_data.clone();
        let (witness_partial_data, _) = witness_incoming_data(&chain, &block);
        incoming_data.data.commitment = witness_partial_data.data.commitment;
        incoming_data.data.parts = witness_partial_data.data.parts;
        let SpiceIncomingPartialData { data } = incoming_data;
        let result = actor.receive_data(data);
        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(result, Err(Error::IdAndDataMismatch));
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
    let mut actor = new_actor(outgoing_sc, &chain, &recipient);
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
        let mut incoming_data = incoming_data.clone();
        incoming_data.data.commitment = different_incoming_data.data.commitment;
        incoming_data.data.parts = different_incoming_data.data.parts;
        let SpiceIncomingPartialData { data } = incoming_data;
        let result = actor.receive_data(data);
        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(result, Err(Error::InvalidDecodedReceiptFromShardId));
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
    let mut actor = new_actor(outgoing_sc, &chain, &recipient);
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
        let mut incoming_data = incoming_data.clone();
        incoming_data.data.commitment = different_incoming_data.data.commitment;
        incoming_data.data.parts = different_incoming_data.data.parts;
        let SpiceIncomingPartialData { data } = incoming_data;
        let result = actor.receive_data(data);
        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(result, Err(Error::InvalidDecodedReceiptToShardId));
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
    let mut actor = new_actor(outgoing_sc, &chain, &recipient);
    {
        let mut incoming_data = incoming_data.clone();
        let (receipt_partial_data, _) = receipt_proof_incoming_data(&chain, &block);
        incoming_data.data.commitment = receipt_partial_data.data.commitment;
        incoming_data.data.parts = receipt_partial_data.data.parts;
        let SpiceIncomingPartialData { data } = incoming_data;
        let result = actor.receive_data(data);
        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(result, Err(Error::IdAndDataMismatch));
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
    let mut actor = new_actor(outgoing_sc, &chain, &recipient.unwrap());
    {
        let different_chunk_header = block
            .chunks()
            .iter_raw()
            .find(|chunk| chunk != &state_witness.chunk_header())
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
        let mut incoming_data = incoming_data.clone();
        incoming_data.data.commitment = incoming_data_for_different_witness.data.commitment;
        incoming_data.data.parts = incoming_data_for_different_witness.data.parts;
        let SpiceIncomingPartialData { data } = incoming_data;
        let result = actor.receive_data(data);
        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(result, Err(Error::InvalidDecodedWitnessShardId));
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
    let mut actor = new_actor(outgoing_sc, &chain, &recipient.unwrap());
    {
        let prev_block = chain.chain_store.get_block(block.header().prev_hash()).unwrap();
        let (incoming_data_for_different_witness, _recipient) =
            witness_incoming_data(&chain, &prev_block);
        let mut incoming_data = incoming_data.clone();
        incoming_data.data.commitment = incoming_data_for_different_witness.data.commitment;
        incoming_data.data.parts = incoming_data_for_different_witness.data.parts;
        let SpiceIncomingPartialData { data } = incoming_data;
        let result = actor.receive_data(data);
        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(result, Err(Error::InvalidDecodedWitnessBlockHash));
    }
    actor.handle(incoming_data);
    assert_matches!(outgoing_rc.try_recv(), Ok(_));
}

macro_rules! test_invalid_incoming_partial_data_without_block {
    ($($name:ident ( $error:pat, $partial_data_func:ident, $incoming_data:ident , $update_block:block ) )+) => {
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
                    let mut actor = new_actor(outgoing_sc, &receiver_chain, &recipient);
                    {
                        let mut $incoming_data = incoming_data.clone();
                        $update_block
                        let SpiceIncomingPartialData { data } = $incoming_data;
                        let result = actor.receive_data(data);
                        assert_eq!(actor.pending_partial_data_size(), 0);
                        assert_matches!(outgoing_rc.try_recv(), Err(TryRecvError::Empty));
                        assert_matches!(result, Err($error));
                    }
                }
            )+
        }
    }
}

test_invalid_incoming_partial_data_without_block! {
    invalid_sender_of_receipts(Error::SenderIsNotProducer, receipt_proof_incoming_data, incoming_data, {
        incoming_data.data.sender = AccountId::from_str("invalid-sender").unwrap();
    })
    invalid_sender_of_witness(Error::SenderIsNotProducer, witness_incoming_data, incoming_data, {
        incoming_data.data.sender = AccountId::from_str("invalid-sender").unwrap();
    })
    node_is_not_recipient(Error::NodeIsNotRecipient, receipt_proof_incoming_data, incoming_data, {
        let SpiceDataIdentifier::ReceiptProof { from_shard_id, to_shard_id, .. } =
            &mut incoming_data.data.id else {
                panic!();
        };
        *to_shard_id = *from_shard_id;
    })
    invalid_receipts_from_shard(Error::NearChainError(_), receipt_proof_incoming_data, incoming_data, {
        let SpiceDataIdentifier::ReceiptProof { from_shard_id, .. } =
            &mut incoming_data.data.id else {
                panic!();
        };
        *from_shard_id = ShardId::new(42);
    })
    invalid_receipts_to_shard(Error::NearChainError(_), receipt_proof_incoming_data, incoming_data, {
        let SpiceDataIdentifier::ReceiptProof { to_shard_id, .. } =
            &mut incoming_data.data.id else {
                panic!();
        };
        *to_shard_id = ShardId::new(42);
    })
    invalid_witness_shard_id(Error::NearChainError(_), witness_incoming_data, incoming_data, {
        let SpiceDataIdentifier::Witness { shard_id, .. } = &mut incoming_data.data.id else {
            panic!();
        };
        *shard_id = ShardId::new(42);
    })
    empty_parts(Error::PartsIsEmpty, receipt_proof_incoming_data, incoming_data, {
        incoming_data.data.parts = vec![];
    })
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
    let mut actor = new_actor(outgoing_sc, &receiver_chain, &recipient);

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
