use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use actix::System;
use assert_matches::assert_matches;
use futures::{future, FutureExt};
use near_primitives::num_rational::Rational;

use near_actix_test_utils::run_actix;
use near_chain::chain::ApplyStatePartsRequest;
use near_chain::types::LatestKnown;
use near_chain::validate::validate_chunk_with_chunk_extra;
use near_chain::{
    Block, ChainGenesis, ChainStore, ChainStoreAccess, Error, Provenance, RuntimeAdapter,
};
use near_chain_configs::{ClientConfig, Genesis, DEFAULT_GC_NUM_EPOCHS_TO_KEEP};
use near_chunks::{ChunkStatus, ShardsManager};
use near_client::test_utils::{
    create_chunk_on_height, run_catchup, setup_client, setup_mock, setup_mock_all_validators,
    TestEnv,
};
use near_client::{Client, GetBlock, GetBlockWithMerkleTree};
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signature, Signer};
use near_logger_utils::{init_integration_logger, init_test_logger};
use near_network::test_utils::{wait_or_panic, MockPeerManagerAdapter};
use near_network::types::{
    FullPeerInfo, NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses,
};
use near_network::types::{NetworkInfo, PeerManagerMessageRequest, PeerManagerMessageResponse};
use near_network_primitives::types::{PeerChainInfoV2, PeerInfo, ReasonForBan};
use near_primitives::block::{Approval, ApprovalInner};
use near_primitives::block_header::BlockHeader;
use near_primitives::epoch_manager::RngSeed;
use near_primitives::errors::TxExecutionError;
use near_primitives::errors::{ActionErrorKind, InvalidTxError};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{verify_hash, PartialMerkleTree};
use near_primitives::receipt::DelayedReceiptIndices;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::{
    EncodedShardChunk, ReedSolomonWrapper, ShardChunkHeader, ShardChunkHeaderInner,
    ShardChunkHeaderV3,
};
use near_primitives::state_part::PartId;
use near_primitives::syncing::{get_num_state_parts, ShardStateSyncResponseHeader, StatePartKey};
use near_primitives::transaction::{
    Action, DeployContractAction, ExecutionStatus, FunctionCallAction, SignedTransaction,
    Transaction,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, BlockHeight, EpochId, NumBlocks, ProtocolVersion};
use near_primitives::utils::to_timestamp;
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::version::ProtocolFeature;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{
    BlockHeaderView, FinalExecutionStatus, QueryRequest, QueryResponseKind,
};
use near_store::test_utils::create_test_store;
use near_store::{get, DBCol};
use near_vm_errors::{CompilationError, FunctionCallErrorSer, PrepareError};
use nearcore::config::{GenesisExt, TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use nearcore::{TrackedConfig, NEAR_BASE};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

pub fn set_block_protocol_version(
    block: &mut Block,
    block_producer: AccountId,
    protocol_version: ProtocolVersion,
) {
    let validator_signer = InMemoryValidatorSigner::from_seed(
        block_producer.clone(),
        KeyType::ED25519,
        block_producer.as_ref(),
    );
    block.mut_header().set_lastest_protocol_version(protocol_version);
    block.mut_header().resign(&validator_signer);
}

pub fn create_nightshade_runtimes(genesis: &Genesis, n: usize) -> Vec<Arc<dyn RuntimeAdapter>> {
    (0..n)
        .map(|_| {
            Arc::new(nearcore::NightshadeRuntime::test(
                Path::new("../../../.."),
                create_test_store(),
                genesis,
            )) as Arc<dyn RuntimeAdapter>
        })
        .collect()
}

/// Produce `blocks_number` block in the given environment, starting from the given height.
/// Returns the first unoccupied height in the chain after this operation.
fn produce_blocks_from_height(
    env: &mut TestEnv,
    blocks_number: u64,
    height: BlockHeight,
) -> BlockHeight {
    let next_height = height + blocks_number;
    for i in height..next_height {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        for j in 1..env.clients.len() {
            env.process_block(j, block.clone(), Provenance::NONE);
        }
    }
    next_height
}

/// Try to process tx in the next blocks, check that tx and all generated receipts succeed.
/// Return height of the next block.
fn check_tx_processing(
    env: &mut TestEnv,
    tx: SignedTransaction,
    height: BlockHeight,
    blocks_number: u64,
) -> BlockHeight {
    let tx_hash = tx.get_hash();
    env.clients[0].process_tx(tx, false, false);
    let next_height = produce_blocks_from_height(env, blocks_number, height);
    let final_outcome = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
    assert_matches!(final_outcome.status, FinalExecutionStatus::SuccessValue(_));
    next_height
}

fn deploy_test_contract(
    env: &mut TestEnv,
    account_id: AccountId,
    wasm_code: &[u8],
    epoch_length: u64,
    height: BlockHeight,
) -> BlockHeight {
    let block = env.clients[0].chain.get_block_by_height(height - 1).unwrap();
    let signer =
        InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, account_id.as_ref());

    let tx = SignedTransaction::from_actions(
        height,
        account_id.clone(),
        account_id,
        &signer,
        vec![Action::DeployContract(DeployContractAction { code: wasm_code.to_vec() })],
        *block.hash(),
    );
    env.clients[0].process_tx(tx, false, false);
    produce_blocks_from_height(env, epoch_length, height)
}

/// Create environment and set of transactions which cause congestion on the chain.
fn prepare_env_with_congestion(
    protocol_version: ProtocolVersion,
    gas_price_adjustment_rate: Option<Rational>,
    number_of_transactions: u64,
) -> (TestEnv, Vec<CryptoHash>) {
    init_test_logger();
    let epoch_length = 100;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.protocol_version = protocol_version;
    genesis.config.epoch_length = epoch_length;
    genesis.config.gas_limit = 10_000_000_000_000;
    if let Some(gas_price_adjustment_rate) = gas_price_adjustment_rate {
        genesis.config.gas_price_adjustment_rate = gas_price_adjustment_rate;
    }
    let chain_genesis = ChainGenesis::from(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");

    // Deploy contract to test0.
    let tx = SignedTransaction::from_actions(
        1,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::base_rs_contract().to_vec(),
        })],
        *genesis_block.hash(),
    );
    env.clients[0].process_tx(tx, false, false);
    for i in 1..3 {
        env.produce_block(0, i);
    }

    // Create function call transactions that generate promises.
    let gas_1 = 9_000_000_000_000;
    let gas_2 = gas_1 / 3;
    let mut tx_hashes = vec![];

    for i in 0..number_of_transactions {
        let data = serde_json::json!([
            {"create": {
            "account_id": "test0",
            "method_name": "call_promise",
            "arguments": [],
            "amount": "0",
            "gas": gas_2,
            }, "id": 0 }
        ]);

        let signed_transaction = SignedTransaction::from_actions(
            i + 10,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "call_promise".to_string(),
                args: serde_json::to_vec(&data).unwrap(),
                gas: gas_1,
                deposit: 0,
            })],
            *genesis_block.hash(),
        );
        tx_hashes.push(signed_transaction.get_hash());
        env.clients[0].process_tx(signed_transaction, false, false);
    }

    (env, tx_hashes)
}

/// Runs block producing client and stops after network mock received two blocks.
#[test]
fn produce_two_blocks() {
    init_test_logger();
    run_actix(async {
        let count = Arc::new(AtomicUsize::new(0));
        setup_mock(
            vec!["test".parse().unwrap()],
            "test".parse().unwrap(),
            true,
            false,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::Block { .. } = msg.as_network_requests_ref() {
                    count.fetch_add(1, Ordering::Relaxed);
                    if count.load(Ordering::Relaxed) >= 2 {
                        System::current().stop();
                    }
                }
                PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
            }),
        );
        near_network::test_utils::wait_or_panic(5000);
    });
}

/// Runs block producing client and sends it a transaction.
#[test]
// TODO: figure out how to re-enable it correctly
#[ignore]
fn produce_blocks_with_tx() {
    let mut encoded_chunks: Vec<EncodedShardChunk> = vec![];
    init_test_logger();
    run_actix(async {
        let (client, view_client) = setup_mock(
            vec!["test".parse().unwrap()],
            "test".parse().unwrap(),
            true,
            false,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::PartialEncodedChunkMessage {
                    account_id: _,
                    partial_encoded_chunk,
                } = msg.as_network_requests_ref()
                {
                    let header = partial_encoded_chunk.header.clone();
                    let height = header.height_created() as usize;
                    assert!(encoded_chunks.len() + 2 >= height);

                    // the following two lines must match data_parts and total_parts in KeyValueRuntimeAdapter
                    let data_parts = 12 + 2 * (((height - 1) as usize) % 4);
                    let total_parts = 1 + data_parts * (1 + ((height - 1) as usize) % 3);
                    if encoded_chunks.len() + 2 == height {
                        encoded_chunks.push(EncodedShardChunk::from_header(
                            header,
                            total_parts,
                            PROTOCOL_VERSION,
                        ));
                    }
                    for part in partial_encoded_chunk.parts.iter() {
                        encoded_chunks[height - 2].content_mut().parts[part.part_ord as usize] =
                            Some(part.part.clone());
                    }

                    let parity_parts = total_parts - data_parts;
                    let mut rs = ReedSolomonWrapper::new(data_parts, parity_parts);

                    if let ChunkStatus::Complete(_) = ShardsManager::check_chunk_complete(
                        &mut encoded_chunks[height - 2],
                        &mut rs,
                    ) {
                        let chunk = encoded_chunks[height - 2].decode_chunk(data_parts).unwrap();
                        if !chunk.transactions().is_empty() {
                            System::current().stop();
                        }
                    }
                }
                PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
            }),
        );
        near_network::test_utils::wait_or_panic(5000);
        actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
            let block_hash = res.unwrap().unwrap().header.hash;
            client.do_send(NetworkClientMessages::Transaction {
                transaction: SignedTransaction::empty(block_hash),
                is_forwarded: false,
                check_only: false,
            });
            future::ready(())
        }))
    });
}

/// Runs client that receives a block from network and announces header to the network with approval.
/// Need 3 block producers, to receive approval.
#[test]
fn receive_network_block() {
    init_test_logger();
    run_actix(async {
        // The first header announce will be when the block is received. We don't immediately endorse
        // it. The second header announce will happen with the endorsement a little later.
        let first_header_announce = Arc::new(RwLock::new(true));
        let (client, view_client) = setup_mock(
            vec!["test2".parse().unwrap(), "test1".parse().unwrap(), "test3".parse().unwrap()],
            "test2".parse().unwrap(),
            true,
            false,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::Approval { .. } = msg.as_network_requests_ref() {
                    let mut first_header_announce = first_header_announce.write().unwrap();
                    if *first_header_announce {
                        *first_header_announce = false;
                    } else {
                        System::current().stop();
                    }
                }
                PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
            }),
        );
        actix::spawn(view_client.send(GetBlockWithMerkleTree::latest()).then(move |res| {
            let (last_block, block_merkle_tree) = res.unwrap().unwrap();
            let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
            block_merkle_tree.insert(last_block.header.hash);
            let signer = InMemoryValidatorSigner::from_seed(
                "test1".parse().unwrap(),
                KeyType::ED25519,
                "test1",
            );
            let next_block_ordinal = last_block.header.block_ordinal.unwrap() + 1;
            let block = Block::produce(
                PROTOCOL_VERSION,
                PROTOCOL_VERSION,
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                next_block_ordinal,
                last_block.chunks.into_iter().map(Into::into).collect(),
                EpochId::default(),
                if last_block.header.prev_hash == CryptoHash::default() {
                    EpochId(last_block.header.hash)
                } else {
                    EpochId(last_block.header.next_epoch_id)
                },
                None,
                vec![],
                Rational::from_integer(0),
                0,
                100,
                None,
                vec![],
                vec![],
                &signer,
                last_block.header.next_bp_hash,
                block_merkle_tree.root(),
                None,
            );
            client.do_send(NetworkClientMessages::Block(block, PeerInfo::random().id, false));
            future::ready(())
        }));
        near_network::test_utils::wait_or_panic(5000);
    });
}

/// Include approvals to the next block in newly produced block.
#[test]
fn produce_block_with_approvals() {
    init_test_logger();
    let validators: Vec<_> =
        (1..=10).map(|i| AccountId::try_from(format!("test{}", i)).unwrap()).collect();
    run_actix(async {
        let (client, view_client) = setup_mock(
            validators.clone(),
            "test1".parse().unwrap(),
            true,
            false,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::Block { block } = msg.as_network_requests_ref() {
                    // Below we send approvals from all the block producers except for test1 and test2
                    // test1 will only create their approval for height 10 after their doomslug timer
                    // runs 10 iterations, which is way further in the future than them producing the
                    // block
                    if block.header().num_approvals() == validators.len() as u64 - 2 {
                        System::current().stop();
                    } else if block.header().height() == 10 {
                        println!("{}", block.header().height());
                        println!(
                            "{} != {} -2 (height: {})",
                            block.header().num_approvals(),
                            validators.len(),
                            block.header().height()
                        );

                        assert!(false);
                    }
                }
                PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
            }),
        );
        actix::spawn(view_client.send(GetBlockWithMerkleTree::latest()).then(move |res| {
            let (last_block, block_merkle_tree) = res.unwrap().unwrap();
            let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
            block_merkle_tree.insert(last_block.header.hash);
            let signer1 = InMemoryValidatorSigner::from_seed(
                "test2".parse().unwrap(),
                KeyType::ED25519,
                "test2",
            );
            let next_block_ordinal = last_block.header.block_ordinal.unwrap() + 1;
            let block = Block::produce(
                PROTOCOL_VERSION,
                PROTOCOL_VERSION,
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                next_block_ordinal,
                last_block.chunks.into_iter().map(Into::into).collect(),
                EpochId::default(),
                if last_block.header.prev_hash == CryptoHash::default() {
                    EpochId(last_block.header.hash)
                } else {
                    EpochId(last_block.header.next_epoch_id)
                },
                None,
                vec![],
                Rational::from_integer(0),
                0,
                100,
                Some(0),
                vec![],
                vec![],
                &signer1,
                last_block.header.next_bp_hash,
                block_merkle_tree.root(),
                None,
            );
            client.do_send(NetworkClientMessages::Block(
                block.clone(),
                PeerInfo::random().id,
                false,
            ));

            for i in 3..11 {
                let s = AccountId::try_from(if i > 10 {
                    "test1".to_string()
                } else {
                    format!("test{}", i)
                })
                .unwrap();
                let signer =
                    InMemoryValidatorSigner::from_seed(s.clone(), KeyType::ED25519, s.as_ref());
                let approval = Approval::new(
                    *block.hash(),
                    block.header().height(),
                    10, // the height at which "test1" is producing
                    &signer,
                );
                client
                    .do_send(NetworkClientMessages::BlockApproval(approval, PeerInfo::random().id));
            }

            future::ready(())
        }));
        near_network::test_utils::wait_or_panic(5000);
    });
}

/// When approvals arrive early, they should be properly cached.
#[test]
fn produce_block_with_approvals_arrived_early() {
    init_test_logger();
    let validators = vec![vec![
        "test1".parse().unwrap(),
        "test2".parse().unwrap(),
        "test3".parse().unwrap(),
        "test4".parse().unwrap(),
    ]];
    let key_pairs =
        vec![PeerInfo::random(), PeerInfo::random(), PeerInfo::random(), PeerInfo::random()];
    let block_holder: Arc<RwLock<Option<Block>>> = Arc::new(RwLock::new(None));
    run_actix(async move {
        let mut approval_counter = 0;
        let network_mock: Arc<
            RwLock<
                Box<
                    dyn FnMut(
                        AccountId,
                        &PeerManagerMessageRequest,
                    ) -> (PeerManagerMessageResponse, bool),
                >,
            >,
        > = Arc::new(RwLock::new(Box::new(|_: _, _: &PeerManagerMessageRequest| {
            (PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse), true)
        })));
        let (_, conns, _) = setup_mock_all_validators(
            validators.clone(),
            key_pairs,
            1,
            true,
            2000,
            false,
            false,
            100,
            true,
            vec![false; validators.iter().map(|x| x.len()).sum()],
            vec![true; validators.iter().map(|x| x.len()).sum()],
            false,
            network_mock.clone(),
        );
        *network_mock.write().unwrap() = Box::new(
            move |_: _, msg: &PeerManagerMessageRequest| -> (PeerManagerMessageResponse, bool) {
                let msg = msg.as_network_requests_ref();
                match msg {
                    NetworkRequests::Block { block } => {
                        if block.header().height() == 3 {
                            for (i, (client, _)) in conns.clone().into_iter().enumerate() {
                                if i > 0 {
                                    client.do_send(NetworkClientMessages::Block(
                                        block.clone(),
                                        PeerInfo::random().id,
                                        false,
                                    ))
                                }
                            }
                            *block_holder.write().unwrap() = Some(block.clone());
                            return (NetworkResponses::NoResponse.into(), false);
                        } else if block.header().height() == 4 {
                            System::current().stop();
                        }
                        (NetworkResponses::NoResponse.into(), true)
                    }
                    NetworkRequests::Approval { approval_message } => {
                        if approval_message.target.as_ref() == "test1"
                            && approval_message.approval.target_height == 4
                        {
                            approval_counter += 1;
                        }
                        if approval_counter == 3 {
                            let block = block_holder.read().unwrap().clone().unwrap();
                            conns[0].0.do_send(NetworkClientMessages::Block(
                                block,
                                PeerInfo::random().id,
                                false,
                            ));
                        }
                        (NetworkResponses::NoResponse.into(), true)
                    }
                    _ => (NetworkResponses::NoResponse.into(), true),
                }
            },
        );

        near_network::test_utils::wait_or_panic(10000);
    });
}

/// Sends one invalid block followed by one valid block, and checks that client announces only valid block.
/// and that the node bans the peer for invalid block header.
fn invalid_blocks_common(is_requested: bool) {
    init_test_logger();
    run_actix(async move {
        let mut ban_counter = 0;
        let (client, view_client) = setup_mock(
            vec!["test".parse().unwrap()],
            "other".parse().unwrap(),
            true,
            false,
            Box::new(move |msg, _ctx, _client_actor| {
                match msg.as_network_requests_ref() {
                    NetworkRequests::Block { block } => {
                        if is_requested {
                            panic!("rebroadcasting requested block");
                        } else {
                            assert_eq!(block.header().height(), 1);
                            assert_eq!(block.header().chunk_mask().len(), 1);
                            assert_eq!(ban_counter, 2);
                            System::current().stop();
                        }
                    }
                    NetworkRequests::BanPeer { ban_reason, .. } => {
                        assert_eq!(ban_reason, &ReasonForBan::BadBlockHeader);
                        ban_counter += 1;
                        if ban_counter == 3 && is_requested {
                            System::current().stop();
                        }
                    }
                    _ => {}
                };
                PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
            }),
        );
        actix::spawn(view_client.send(GetBlockWithMerkleTree::latest()).then(move |res| {
            let (last_block, block_merkle_tree) = res.unwrap().unwrap();
            let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
            block_merkle_tree.insert(last_block.header.hash);
            let signer = InMemoryValidatorSigner::from_seed(
                "test".parse().unwrap(),
                KeyType::ED25519,
                "test",
            );
            let next_block_ordinal = last_block.header.block_ordinal.unwrap() + 1;
            let valid_block = Block::produce(
                PROTOCOL_VERSION,
                PROTOCOL_VERSION,
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                next_block_ordinal,
                last_block.chunks.iter().cloned().map(Into::into).collect(),
                EpochId::default(),
                if last_block.header.prev_hash == CryptoHash::default() {
                    EpochId(last_block.header.hash)
                } else {
                    EpochId(last_block.header.next_epoch_id)
                },
                None,
                vec![],
                Rational::from_integer(0),
                0,
                100,
                Some(0),
                vec![],
                vec![],
                &signer,
                last_block.header.next_bp_hash,
                block_merkle_tree.root(),
                None,
            );
            // Send block with invalid chunk mask
            let mut block = valid_block.clone();
            block.mut_header().get_mut().inner_rest.chunk_mask = vec![];
            block.mut_header().get_mut().init();
            client.do_send(NetworkClientMessages::Block(
                block.clone(),
                PeerInfo::random().id,
                is_requested,
            ));

            // Send block with invalid chunk signature
            let mut block = valid_block.clone();
            let mut chunks: Vec<_> = block.chunks().iter().cloned().collect();
            let some_signature = Signature::from_parts(KeyType::ED25519, &[1; 64]).unwrap();
            match &mut chunks[0] {
                ShardChunkHeader::V1(chunk) => {
                    chunk.signature = some_signature;
                }
                ShardChunkHeader::V2(chunk) => {
                    chunk.signature = some_signature;
                }
                ShardChunkHeader::V3(chunk) => {
                    chunk.signature = some_signature;
                }
            };
            block.set_chunks(chunks);
            client.do_send(NetworkClientMessages::Block(
                block.clone(),
                PeerInfo::random().id,
                is_requested,
            ));

            // Send proper block.
            let block2 = valid_block;
            client.do_send(NetworkClientMessages::Block(
                block2.clone(),
                PeerInfo::random().id,
                is_requested,
            ));
            if is_requested {
                let mut block3 = block2;
                block3.mut_header().get_mut().inner_rest.chunk_headers_root = hash(&[1]);
                block3.mut_header().get_mut().init();
                client.do_send(NetworkClientMessages::Block(
                    block3.clone(),
                    PeerInfo::random().id,
                    is_requested,
                ));
            }
            future::ready(())
        }));
        near_network::test_utils::wait_or_panic(5000);
    });
}

const TEST_SEED: RngSeed = [3; 32];

#[test]
fn test_invalid_blocks_not_requested() {
    invalid_blocks_common(false);
}

#[test]
fn test_invalid_blocks_requested() {
    invalid_blocks_common(true);
}

enum InvalidBlockMode {
    /// Header is invalid
    InvalidHeader,
    /// Block is ill-formed (roots check fail)
    IllFormed,
    /// Block is invalid for other reasons
    InvalidBlock,
}

fn ban_peer_for_invalid_block_common(mode: InvalidBlockMode) {
    init_test_logger();
    let validators = vec![vec![
        "test1".parse().unwrap(),
        "test2".parse().unwrap(),
        "test3".parse().unwrap(),
        "test4".parse().unwrap(),
    ]];
    let key_pairs =
        vec![PeerInfo::random(), PeerInfo::random(), PeerInfo::random(), PeerInfo::random()];
    run_actix(async move {
        let mut ban_counter = 0;
        let peer_manager_mock: Arc<
            RwLock<
                Box<
                    dyn FnMut(
                        AccountId,
                        &PeerManagerMessageRequest,
                    ) -> (PeerManagerMessageResponse, bool),
                >,
            >,
        > = Arc::new(RwLock::new(Box::new(|_: _, _: &PeerManagerMessageRequest| {
            (PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse), true)
        })));

        let (_, conns, _) = setup_mock_all_validators(
            validators.clone(),
            key_pairs,
            1,
            true,
            100,
            false,
            false,
            100,
            true,
            vec![false; validators.iter().map(|x| x.len()).sum()],
            vec![true; validators.iter().map(|x| x.len()).sum()],
            false,
            peer_manager_mock.clone(),
        );
        let mut sent_bad_blocks = false;
        *peer_manager_mock.write().unwrap() = Box::new(
            move |_: _, msg: &PeerManagerMessageRequest| -> (PeerManagerMessageResponse, bool) {
                match msg.as_network_requests_ref() {
                    NetworkRequests::Block { block } => {
                        if block.header().height() >= 4 && !sent_bad_blocks {
                            let block_producer_idx =
                                block.header().height() as usize % validators[0].len();
                            let block_producer = &validators[0][block_producer_idx];
                            let validator_signer1 = InMemoryValidatorSigner::from_seed(
                                block_producer.clone(),
                                KeyType::ED25519,
                                block_producer.as_ref(),
                            );
                            sent_bad_blocks = true;
                            let mut block_mut = block.clone();
                            match mode {
                                InvalidBlockMode::InvalidHeader => {
                                    // produce an invalid block with invalid header.
                                    block_mut.mut_header().get_mut().inner_rest.chunk_mask = vec![];
                                    block_mut.mut_header().resign(&validator_signer1);
                                }
                                InvalidBlockMode::IllFormed => {
                                    // produce an ill-formed block
                                    block_mut
                                        .mut_header()
                                        .get_mut()
                                        .inner_rest
                                        .chunk_headers_root = hash(&[1]);
                                    block_mut.mut_header().resign(&validator_signer1);
                                }
                                InvalidBlockMode::InvalidBlock => {
                                    // produce an invalid block whose invalidity cannot be verified by just
                                    // having its header.
                                    #[cfg(feature = "protocol_feature_chunk_only_producers")]
                                    let proposals = vec![ValidatorStake::new(
                                        "test1".parse().unwrap(),
                                        PublicKey::empty(KeyType::ED25519),
                                        0,
                                        false,
                                    )];
                                    #[cfg(not(feature = "protocol_feature_chunk_only_producers"))]
                                    let proposals = vec![ValidatorStake::new(
                                        "test1".parse().unwrap(),
                                        PublicKey::empty(KeyType::ED25519),
                                        0,
                                    )];

                                    block_mut
                                        .mut_header()
                                        .get_mut()
                                        .inner_rest
                                        .validator_proposals = proposals;
                                    block_mut.mut_header().resign(&validator_signer1);
                                }
                            }

                            for (i, (client, _)) in conns.clone().into_iter().enumerate() {
                                if i != block_producer_idx {
                                    client.do_send(NetworkClientMessages::Block(
                                        block_mut.clone(),
                                        PeerInfo::random().id,
                                        false,
                                    ))
                                }
                            }

                            return (
                                PeerManagerMessageResponse::NetworkResponses(
                                    NetworkResponses::NoResponse,
                                ),
                                false,
                            );
                        }
                        if block.header().height() > 20 {
                            match mode {
                                InvalidBlockMode::InvalidHeader | InvalidBlockMode::IllFormed => {
                                    assert_eq!(ban_counter, 3);
                                }
                                _ => {}
                            }
                            System::current().stop();
                        }
                        (
                            PeerManagerMessageResponse::NetworkResponses(
                                NetworkResponses::NoResponse,
                            ),
                            true,
                        )
                    }
                    NetworkRequests::BanPeer { peer_id, ban_reason } => match mode {
                        InvalidBlockMode::InvalidHeader | InvalidBlockMode::IllFormed => {
                            assert_eq!(ban_reason, &ReasonForBan::BadBlockHeader);
                            ban_counter += 1;
                            if ban_counter > 3 {
                                panic!("more bans than expected");
                            }
                            (
                                PeerManagerMessageResponse::NetworkResponses(
                                    NetworkResponses::NoResponse,
                                ),
                                true,
                            )
                        }
                        InvalidBlockMode::InvalidBlock => {
                            panic!("banning peer {:?} unexpectedly for {:?}", peer_id, ban_reason);
                        }
                    },
                    _ => (
                        PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse),
                        true,
                    ),
                }
            },
        );

        near_network::test_utils::wait_or_panic(20000);
    });
}

/// If a peer sends a block whose header is valid and passes basic validation, the peer is not banned.
#[test]
fn test_not_ban_peer_for_invalid_block() {
    ban_peer_for_invalid_block_common(InvalidBlockMode::InvalidBlock);
}

/// If a peer sends a block whose header is invalid, we should ban them and do not forward the block
#[test]
fn test_ban_peer_for_invalid_block_header() {
    ban_peer_for_invalid_block_common(InvalidBlockMode::InvalidHeader);
}

/// If a peer sends a block that is ill-formed, we should ban them and do not forward the block
#[test]
fn test_ban_peer_for_ill_formed_block() {
    ban_peer_for_invalid_block_common(InvalidBlockMode::IllFormed);
}

/// Runs two validators runtime with only one validator online.
/// Present validator produces blocks on it's height after deadline.
#[test]
fn skip_block_production() {
    init_test_logger();
    run_actix(async {
        setup_mock(
            vec!["test1".parse().unwrap(), "test2".parse().unwrap()],
            "test2".parse().unwrap(),
            true,
            false,
            Box::new(move |msg, _ctx, _client_actor| {
                match msg.as_network_requests_ref() {
                    NetworkRequests::Block { block } => {
                        if block.header().height() > 3 {
                            System::current().stop();
                        }
                    }
                    _ => {}
                };
                PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
            }),
        );
        wait_or_panic(10000);
    });
}

/// Runs client that requests syncing headers from peers.
#[test]
fn client_sync_headers() {
    init_test_logger();
    run_actix(async {
        let peer_info1 = PeerInfo::random();
        let peer_info2 = peer_info1.clone();
        let (client, _) = setup_mock(
            vec!["test".parse().unwrap()],
            "other".parse().unwrap(),
            false,
            false,
            Box::new(move |msg, _ctx, _client_actor| match msg.as_network_requests_ref() {
                NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                    assert_eq!(*peer_id, peer_info1.id);
                    assert_eq!(hashes.len(), 1);
                    // TODO: check it requests correct hashes.
                    System::current().stop();

                    PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
                }
                _ => PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse),
            }),
        );
        client.do_send(NetworkClientMessages::NetworkInfo(NetworkInfo {
            connected_peers: vec![FullPeerInfo {
                peer_info: peer_info2.clone(),
                chain_info: PeerChainInfoV2 {
                    genesis_id: Default::default(),
                    height: 5,
                    tracked_shards: vec![],
                    archival: false,
                },
                partial_edge_info: near_network_primitives::types::PartialEdgeInfo::default(),
            }],
            num_connected_peers: 1,
            peer_max_count: 1,
            highest_height_peers: vec![FullPeerInfo {
                peer_info: peer_info2,
                chain_info: PeerChainInfoV2 {
                    genesis_id: Default::default(),
                    height: 5,
                    tracked_shards: vec![],
                    archival: false,
                },
                partial_edge_info: near_network_primitives::types::PartialEdgeInfo::default(),
            }],
            sent_bytes_per_sec: 0,
            received_bytes_per_sec: 0,
            known_producers: vec![],
            peer_counter: 0,
        }));
        wait_or_panic(2000);
    });
}

fn produce_blocks(client: &mut Client, num: u64) {
    for i in 1..num {
        let b = client.produce_block(i).unwrap().unwrap();
        let (mut accepted_blocks, _) = client.process_block(b.into(), Provenance::PRODUCED);
        let more_accepted_blocks = run_catchup(client, &vec![]).unwrap();
        accepted_blocks.extend(more_accepted_blocks);
        for accepted_block in accepted_blocks {
            client.on_block_accepted(
                accepted_block.hash,
                accepted_block.status,
                accepted_block.provenance,
            );
        }
    }
}

#[test]
fn test_process_invalid_tx() {
    init_test_logger();
    let store = create_test_store();
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.transaction_validity_period = 10;
    let mut client = setup_client(
        store,
        vec![vec!["test1".parse().unwrap()]],
        1,
        1,
        Some("test1".parse().unwrap()),
        false,
        network_adapter,
        chain_genesis,
        TEST_SEED,
    );
    let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
    let tx = SignedTransaction::new(
        Signature::empty(KeyType::ED25519),
        Transaction {
            signer_id: "test".parse().unwrap(),
            public_key: signer.public_key(),
            nonce: 0,
            receiver_id: "test".parse().unwrap(),
            block_hash: *client.chain.genesis().hash(),
            actions: vec![],
        },
    );
    produce_blocks(&mut client, 12);
    assert_eq!(
        client.process_tx(tx, false, false),
        NetworkClientResponses::InvalidTx(InvalidTxError::Expired)
    );
    let tx2 = SignedTransaction::new(
        Signature::empty(KeyType::ED25519),
        Transaction {
            signer_id: "test".parse().unwrap(),
            public_key: signer.public_key(),
            nonce: 0,
            receiver_id: "test".parse().unwrap(),
            block_hash: hash(&[1]),
            actions: vec![],
        },
    );
    assert_eq!(
        client.process_tx(tx2, false, false),
        NetworkClientResponses::InvalidTx(InvalidTxError::Expired)
    );
}

/// If someone produce a block with Utc::now() + 1 min, we should produce a block with valid timestamp
#[test]
fn test_time_attack() {
    init_test_logger();
    let store = create_test_store();
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let chain_genesis = ChainGenesis::test();
    let mut client = setup_client(
        store,
        vec![vec!["test1".parse().unwrap()]],
        1,
        1,
        Some("test1".parse().unwrap()),
        false,
        network_adapter,
        chain_genesis,
        TEST_SEED,
    );
    let signer =
        InMemoryValidatorSigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
    let genesis = client.chain.get_block_by_height(0).unwrap();
    let mut b1 = Block::empty_with_height(&genesis, 1, &signer);
    b1.mut_header().get_mut().inner_lite.timestamp =
        to_timestamp(b1.header().timestamp() + chrono::Duration::seconds(60));
    b1.mut_header().resign(&signer);

    let _ = client.process_block(b1.into(), Provenance::NONE);

    let b2 = client.produce_block(2).unwrap().unwrap();
    assert!(client.process_block(b2.into(), Provenance::PRODUCED).1.is_ok());
}

// TODO: use real runtime for this test
#[test]
#[ignore]
fn test_invalid_approvals() {
    init_test_logger();
    let store = create_test_store();
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let chain_genesis = ChainGenesis::test();
    let mut client = setup_client(
        store,
        vec![vec!["test1".parse().unwrap()]],
        1,
        1,
        Some("test1".parse().unwrap()),
        false,
        network_adapter,
        chain_genesis,
        TEST_SEED,
    );
    let signer =
        InMemoryValidatorSigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
    let genesis = client.chain.get_block_by_height(0).unwrap();
    let mut b1 = Block::empty_with_height(&genesis, 1, &signer);
    b1.mut_header().get_mut().inner_rest.approvals = (0..100)
        .map(|i| {
            let account_id = AccountId::try_from(format!("test{}", i)).unwrap();
            Some(
                InMemoryValidatorSigner::from_seed(
                    account_id.clone(),
                    KeyType::ED25519,
                    account_id.as_ref(),
                )
                .sign_approval(&ApprovalInner::Endorsement(*genesis.hash()), 1),
            )
        })
        .collect();
    b1.mut_header().resign(&signer);

    let (_, tip) = client.process_block(b1.into(), Provenance::NONE);
    match tip {
        Err(e) => match e {
            Error::InvalidApprovals => {}
            _ => assert!(false, "wrong error: {}", e),
        },
        _ => assert!(false, "succeeded, tip: {:?}", tip),
    }
}

#[test]
fn test_no_double_sign() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    let _ = env.clients[0].produce_block(1).unwrap().unwrap();
    // Second time producing with the same height should fail.
    assert_eq!(env.clients[0].produce_block(1).unwrap(), None);
}

#[test]
fn test_invalid_gas_price() {
    init_test_logger();
    let store = create_test_store();
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.min_gas_price = 100;
    let mut client = setup_client(
        store,
        vec![vec!["test1".parse().unwrap()]],
        1,
        1,
        Some("test1".parse().unwrap()),
        false,
        network_adapter,
        chain_genesis,
        TEST_SEED,
    );
    let signer =
        InMemoryValidatorSigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
    let genesis = client.chain.get_block_by_height(0).unwrap();
    let mut b1 = Block::empty_with_height(&genesis, 1, &signer);
    b1.mut_header().get_mut().inner_rest.gas_price = 0;
    b1.mut_header().resign(&signer);

    let (_, result) = client.process_block(b1.into(), Provenance::NONE);
    match result {
        Err(e) => match e {
            Error::InvalidGasPrice => {}
            _ => assert!(false, "wrong error: {}", e),
        },
        _ => assert!(false, "succeeded, tip: {:?}", result),
    }
}

#[test]
fn test_invalid_height_too_large() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    let b1 = env.clients[0].produce_block(1).unwrap().unwrap();
    let _ = env.clients[0].process_block(b1.clone().into(), Provenance::PRODUCED);
    let signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let b2 = Block::empty_with_height(&b1, u64::MAX, &signer);
    let (_, res) = env.clients[0].process_block(b2.into(), Provenance::NONE);
    assert_matches!(res.unwrap_err(), Error::InvalidBlockHeight(_));
}

#[test]
fn test_invalid_height_too_old() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    for i in 1..4 {
        env.produce_block(0, i);
    }
    let block = env.clients[0].produce_block(4).unwrap().unwrap();
    for i in 5..30 {
        env.produce_block(0, i);
    }
    let (_, res) = env.clients[0].process_block(block.into(), Provenance::NONE);
    assert_matches!(res.unwrap_err(), Error::InvalidBlockHeight(_));
}

#[test]
fn test_bad_orphan() {
    let mut genesis = ChainGenesis::test();
    genesis.epoch_length = 100;
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    for i in 1..4 {
        env.produce_block(0, i);
    }
    let block = env.clients[0].produce_block(5).unwrap().unwrap();
    let signer = env.clients[0].validator_signer.as_ref().unwrap().clone();
    {
        // Orphan block with unknown epoch
        let mut block = env.clients[0].produce_block(6).unwrap().unwrap();
        block.mut_header().get_mut().inner_lite.epoch_id = EpochId(CryptoHash([1; 32]));
        block.mut_header().get_mut().prev_hash = CryptoHash([1; 32]);
        block.mut_header().resign(&*signer);
        let (_, res) = env.clients[0].process_block(block.clone().into(), Provenance::NONE);
        match res {
            Err(Error::EpochOutOfBounds(epoch_id)) => {
                assert_eq!(&epoch_id, block.header().epoch_id())
            }
            _ => panic!("expected EpochOutOfBounds error, got {res:?}"),
        }
    }
    {
        // Orphan block with invalid signature
        let mut block = env.clients[0].produce_block(7).unwrap().unwrap();
        block.mut_header().get_mut().prev_hash = CryptoHash([1; 32]);
        block.mut_header().get_mut().init();
        let (_, res) = env.clients[0].process_block(block.into(), Provenance::NONE);
        assert_matches!(res.unwrap_err(), Error::InvalidSignature);
    }
    {
        // Orphan block with a valid header, but garbage in body
        let mut block = env.clients[0].produce_block(8).unwrap().unwrap();
        {
            // Change the chunk in any way, chunk_headers_root won't match
            let body = match &mut block {
                Block::BlockV1(_) => unreachable!(),
                Block::BlockV2(body) => Arc::make_mut(body),
            };
            let chunk = match &mut body.chunks[0] {
                ShardChunkHeader::V1(_) => unreachable!(),
                ShardChunkHeader::V2(_) => unreachable!(),
                ShardChunkHeader::V3(chunk) => chunk,
            };
            match &mut chunk.inner {
                ShardChunkHeaderInner::V1(inner) => inner.outcome_root = CryptoHash([1; 32]),
                ShardChunkHeaderInner::V2(inner) => inner.outcome_root = CryptoHash([1; 32]),
            }
            chunk.hash = ShardChunkHeaderV3::compute_hash(&chunk.inner);
        }
        block.mut_header().get_mut().prev_hash = CryptoHash([3; 32]);
        block.mut_header().resign(&*signer);
        let (_, res) = env.clients[0].process_block(block.into(), Provenance::NONE);
        assert_matches!(res.unwrap_err(), Error::InvalidChunkHeadersRoot);
    }
    {
        // Orphan block with invalid approvals. Allowed for now.
        let mut block = env.clients[0].produce_block(9).unwrap().unwrap();
        let some_signature = Signature::from_parts(KeyType::ED25519, &[1; 64]).unwrap();
        block.mut_header().get_mut().inner_rest.approvals = vec![Some(some_signature)];
        block.mut_header().get_mut().prev_hash = CryptoHash([3; 32]);
        block.mut_header().resign(&*signer);
        let (_, res) = env.clients[0].process_block(block.into(), Provenance::NONE);

        assert_matches!(res.unwrap_err(), Error::Orphan);
    }
    {
        // Orphan block with no chunk signatures. Allowed for now.
        let mut block = env.clients[0].produce_block(10).unwrap().unwrap();
        let some_signature = Signature::from_parts(KeyType::ED25519, &[1; 64]).unwrap();
        {
            let body = match &mut block {
                Block::BlockV1(_) => unreachable!(),
                Block::BlockV2(body) => Arc::make_mut(body),
            };
            let chunk = match &mut body.chunks[0] {
                ShardChunkHeader::V1(_) => unreachable!(),
                ShardChunkHeader::V2(_) => unreachable!(),
                ShardChunkHeader::V3(chunk) => chunk,
            };
            chunk.signature = some_signature;
            chunk.hash = ShardChunkHeaderV3::compute_hash(&chunk.inner);
        }
        block.mut_header().get_mut().prev_hash = CryptoHash([4; 32]);
        block.mut_header().resign(&*signer);
        let (_, res) = env.clients[0].process_block(block.into(), Provenance::NONE);
        assert_matches!(res.unwrap_err(), Error::Orphan);
    }
    {
        // Orphan block that's too far ahead: 20 * epoch_length
        let mut block = block.clone();
        block.mut_header().get_mut().prev_hash = CryptoHash([3; 32]);
        block.mut_header().get_mut().inner_lite.height += 2000;
        block.mut_header().resign(&*signer);
        let (_, res) = env.clients[0].process_block(block.into(), Provenance::NONE);
        assert_matches!(res.unwrap_err(), Error::InvalidBlockHeight(_));
    }
    let (_, res) = env.clients[0].process_block(block.into(), Provenance::NONE);
    assert!(res.is_ok());
}

#[test]
fn test_bad_chunk_mask() {
    init_test_logger();
    let chain_genesis = ChainGenesis::test();
    let validators = vec!["test0".parse().unwrap(), "test1".parse().unwrap()];
    let mut clients: Vec<Client> = validators
        .iter()
        .map(|account_id| {
            setup_client(
                create_test_store(),
                vec![validators.clone()],
                1,
                2,
                Some(account_id.clone()),
                false,
                Arc::new(MockPeerManagerAdapter::default()),
                chain_genesis.clone(),
                TEST_SEED,
            )
        })
        .collect();
    for height in 1..5 {
        let block_producer = (height % 2) as usize;
        let chunk_producer = ((height + 1) % 2) as usize;

        let (encoded_chunk, merkle_paths, receipts) =
            create_chunk_on_height(&mut clients[chunk_producer], height);
        for client in clients.iter_mut() {
            let mut chain_store =
                ChainStore::new(client.chain.store().store().clone(), chain_genesis.height, true);
            client
                .shards_mgr
                .distribute_encoded_chunk(
                    encoded_chunk.clone(),
                    merkle_paths.clone(),
                    receipts.clone(),
                    &mut chain_store,
                )
                .unwrap();
        }

        let mut block = clients[block_producer].produce_block(height).unwrap().unwrap();
        {
            let mut chunk_header = encoded_chunk.cloned_header();
            *chunk_header.height_included_mut() = height;
            let mut chunk_headers: Vec<_> = block.chunks().iter().cloned().collect();
            chunk_headers[0] = chunk_header;
            block.set_chunks(chunk_headers.clone());
            block.mut_header().get_mut().inner_rest.chunk_headers_root =
                Block::compute_chunk_headers_root(&chunk_headers).0;
            block.mut_header().get_mut().inner_rest.chunk_tx_root =
                Block::compute_chunk_tx_root(&chunk_headers);
            block.mut_header().get_mut().inner_rest.chunk_receipts_root =
                Block::compute_chunk_receipts_root(&chunk_headers);
            block.mut_header().get_mut().inner_lite.prev_state_root =
                Block::compute_state_root(&chunk_headers);
            block.mut_header().get_mut().inner_rest.chunk_mask = vec![true, false];
            let mess_with_chunk_mask = height == 4;
            if mess_with_chunk_mask {
                block.mut_header().get_mut().inner_rest.chunk_mask = vec![false, true];
            }
            block
                .mut_header()
                .resign(&*clients[block_producer].validator_signer.as_ref().unwrap().clone());
            let (_, res1) =
                clients[chunk_producer].process_block(block.clone().into(), Provenance::NONE);
            let (_, res2) =
                clients[block_producer].process_block(block.clone().into(), Provenance::NONE);
            assert_eq!(res1.is_err(), mess_with_chunk_mask);
            assert_eq!(res2.is_err(), mess_with_chunk_mask);
        }
    }
}

#[test]
fn test_minimum_gas_price() {
    let min_gas_price = 100;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.min_gas_price = min_gas_price;
    chain_genesis.gas_price_adjustment_rate = Rational::new(1, 10);
    let mut env = TestEnv::builder(chain_genesis).build();
    for i in 1..=100 {
        env.produce_block(0, i);
    }
    let block = env.clients[0].chain.get_block_by_height(100).unwrap();
    assert!(block.header().gas_price() >= min_gas_price);
}

fn test_gc_with_epoch_length_common(epoch_length: NumBlocks) {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let mut blocks = vec![];
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    blocks.push(genesis_block);
    for i in 1..=epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 1) {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        blocks.push(block);
    }
    for i in 0..=epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 1) {
        println!("height = {}", i);
        if i < epoch_length {
            let block_hash = *blocks[i as usize].hash();
            assert_matches!(
                env.clients[0].chain.get_block(&block_hash).unwrap_err(),
                Error::DBNotFoundErr(missing_block_hash) if missing_block_hash == "BLOCK: ".to_owned() + &block_hash.to_string()
            );
            assert_matches!(
                env.clients[0].chain.get_block_by_height(i).unwrap_err(),
                Error::DBNotFoundErr(missing_block_hash) if missing_block_hash == "BLOCK: ".to_owned() + &block_hash.to_string()
            );
            assert!(env.clients[0]
                .chain
                .mut_store()
                .get_all_block_hashes_by_height(i as BlockHeight)
                .is_err());
        } else {
            assert!(env.clients[0].chain.get_block(blocks[i as usize].hash()).is_ok());
            assert!(env.clients[0].chain.get_block_by_height(i).is_ok());
            assert!(env.clients[0]
                .chain
                .mut_store()
                .get_all_block_hashes_by_height(i as BlockHeight)
                .is_ok());
        }
    }
    assert_eq!(env.clients[0].chain.store().chunk_tail().unwrap(), epoch_length - 1);
}

#[test]
fn test_gc_with_epoch_length() {
    for i in 3..20 {
        test_gc_with_epoch_length_common(i);
    }
}

/// When an epoch is very long there should not be anything garbage collected unexpectedly
#[test]
fn test_gc_long_epoch() {
    test_gc_with_epoch_length_common(200);
}

#[test]
fn test_gc_block_skips() {
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = 5;
    let mut env = TestEnv::builder(chain_genesis.clone()).build();
    for i in 1..=1000 {
        if i % 2 == 0 {
            env.produce_block(0, i);
        }
    }
    let mut env = TestEnv::builder(chain_genesis.clone()).build();
    for i in 1..=1000 {
        if i % 2 == 1 {
            env.produce_block(0, i);
        }
    }
    // Epoch skips
    let mut env = TestEnv::builder(chain_genesis).build();
    for i in 1..=1000 {
        if i % 9 == 7 {
            env.produce_block(0, i);
        }
    }
}

#[test]
fn test_gc_chunk_tail() {
    let mut chain_genesis = ChainGenesis::test();
    let epoch_length = 100;
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis).build();
    let mut chunk_tail = 0;
    for i in (1..10).chain(101..epoch_length * 6) {
        env.produce_block(0, i);
        let cur_chunk_tail = env.clients[0].chain.store().chunk_tail().unwrap();
        assert!(cur_chunk_tail >= chunk_tail);
        chunk_tail = cur_chunk_tail;
    }
}

#[test]
fn test_gc_execution_outcome() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let tx = SignedTransaction::send_money(
        1,
        "test0".parse().unwrap(),
        "test1".parse().unwrap(),
        &signer,
        100,
        genesis_hash,
    );
    let tx_hash = tx.get_hash();

    env.clients[0].process_tx(tx, false, false);
    for i in 1..epoch_length {
        env.produce_block(0, i);
    }
    assert!(env.clients[0].chain.get_final_transaction_result(&tx_hash).is_ok());

    for i in epoch_length..=epoch_length * 6 + 1 {
        env.produce_block(0, i);
    }
    assert!(env.clients[0].chain.get_final_transaction_result(&tx_hash).is_err());
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_gc_after_state_sync() {
    let epoch_length = 1024;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .clients_count(2)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 2))
        .build();
    for i in 1..epoch_length * 4 + 2 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        env.process_block(1, block, Provenance::NONE);
    }
    let sync_height = epoch_length * 4 + 1;
    let sync_block = env.clients[0].chain.get_block_by_height(sync_height).unwrap();
    let sync_hash = *sync_block.hash();
    let prev_block_hash = *sync_block.header().prev_hash();
    // reset cache
    for i in epoch_length * 3 - 1..sync_height - 1 {
        let block_hash = *env.clients[0].chain.get_block_by_height(i).unwrap().hash();
        assert!(env.clients[1].chain.runtime_adapter.get_epoch_start_height(&block_hash).is_ok());
    }
    env.clients[1].chain.reset_data_pre_state_sync(sync_hash).unwrap();
    assert_eq!(env.clients[1].runtime_adapter.get_gc_stop_height(&sync_hash), 0);
    // mimic what we do in possible_targets
    assert!(env.clients[1].runtime_adapter.get_epoch_id_from_prev_block(&prev_block_hash).is_ok());
    let tries = env.clients[1].runtime_adapter.get_tries();
    env.clients[1].chain.clear_data(tries, &Default::default()).unwrap();
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_process_block_after_state_sync() {
    let epoch_length = 1024;
    // test with shard_version > 0
    let mut genesis = Genesis::test_sharded_new_version(
        vec!["test0".parse().unwrap(), "test1".parse().unwrap()],
        1,
        vec![1],
    );
    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    for i in 1..epoch_length * 4 + 2 {
        env.produce_block(0, i);
    }
    let sync_height = epoch_length * 4 + 1;
    let sync_block = env.clients[0].chain.get_block_by_height(sync_height).unwrap();
    let sync_hash = *sync_block.hash();
    let chunk_extra = env.clients[0]
        .chain
        .get_chunk_extra(&sync_hash, &ShardUId { version: 1, shard_id: 0 })
        .unwrap();
    let state_part = env.clients[0]
        .runtime_adapter
        .obtain_state_part(0, &sync_hash, chunk_extra.state_root(), PartId::new(0, 1))
        .unwrap();
    // reset cache
    for i in epoch_length * 3 - 1..sync_height - 1 {
        let block_hash = *env.clients[0].chain.get_block_by_height(i).unwrap().hash();
        assert!(env.clients[0].chain.runtime_adapter.get_epoch_start_height(&block_hash).is_ok());
    }
    env.clients[0].chain.reset_data_pre_state_sync(sync_hash).unwrap();
    let epoch_id = env.clients[0].chain.get_block_header(&sync_hash).unwrap().epoch_id().clone();
    env.clients[0]
        .runtime_adapter
        .apply_state_part(0, chunk_extra.state_root(), PartId::new(0, 1), &state_part, &epoch_id)
        .unwrap();
    let block = env.clients[0].produce_block(sync_height + 1).unwrap().unwrap();
    let (_, res) = env.clients[0].process_block(block.into(), Provenance::PRODUCED);
    assert!(res.is_ok());
}

#[test]
fn test_gc_fork_tail() {
    let epoch_length = 101;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .clients_count(2)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 2))
        .build();
    let b1 = env.clients[0].produce_block(1).unwrap().unwrap();
    for i in 0..2 {
        env.process_block(i, b1.clone(), Provenance::NONE);
    }
    // create 100 forks
    for i in 2..102 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(1, block, Provenance::NONE);
    }

    let mut second_epoch_start = None;
    for i in 102..epoch_length * DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 5 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        for j in 0..2 {
            env.process_block(j, block.clone(), Provenance::NONE);
        }
        if second_epoch_start.is_none() && block.header().epoch_id() != &EpochId::default() {
            second_epoch_start = Some(i);
        }
    }
    let head = env.clients[1].chain.head().unwrap();
    assert!(
        env.clients[1].runtime_adapter.get_gc_stop_height(&head.last_block_hash) > epoch_length
    );
    let tail = env.clients[1].chain.store().tail().unwrap();
    let fork_tail = env.clients[1].chain.store().fork_tail().unwrap();
    assert!(tail <= fork_tail && fork_tail < second_epoch_start.unwrap());
}

#[test]
fn test_tx_forwarding() {
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = 100;
    let mut env = TestEnv::builder(chain_genesis).clients_count(50).validator_seats(50).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();
    // forward to 2 chunk producers
    env.clients[0].process_tx(SignedTransaction::empty(genesis_hash), false, false);
    assert_eq!(env.network_adapters[0].requests.read().unwrap().len(), 4);
}

#[test]
fn test_tx_forwarding_no_double_forwarding() {
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = 100;
    let mut env = TestEnv::builder(chain_genesis).clients_count(50).validator_seats(50).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();
    env.clients[0].process_tx(SignedTransaction::empty(genesis_hash), true, false);
    assert!(env.network_adapters[0].requests.read().unwrap().is_empty());
}

#[test]
fn test_tx_forward_around_epoch_boundary() {
    let epoch_length = 4;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.num_block_producer_seats = 2;
    genesis.config.num_block_producer_seats_per_shard = vec![2];
    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    chain_genesis.gas_limit = genesis.config.gas_limit;
    let mut env = TestEnv::builder(chain_genesis)
        .clients_count(3)
        .validator_seats(2)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 3))
        .build();
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
    let tx = SignedTransaction::stake(
        1,
        "test1".parse().unwrap(),
        &signer,
        TESTING_INIT_STAKE,
        signer.public_key.clone(),
        genesis_hash,
    );
    env.clients[0].process_tx(tx, false, false);

    for i in 1..epoch_length * 2 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        for j in 0..3 {
            if j != 1 {
                let provenance = if j == 0 { Provenance::PRODUCED } else { Provenance::NONE };
                env.process_block(j, block.clone(), provenance);
            }
        }
    }
    let tx = SignedTransaction::send_money(
        1,
        "test1".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        1,
        genesis_hash,
    );
    env.clients[2].process_tx(tx, false, false);
    let mut accounts_to_forward = HashSet::new();
    for request in env.network_adapters[2].requests.read().unwrap().iter() {
        if let PeerManagerMessageRequest::NetworkRequests(NetworkRequests::ForwardTx(
            account_id,
            _,
        )) = request
        {
            accounts_to_forward.insert(account_id.clone());
        }
    }
    assert_eq!(
        accounts_to_forward,
        HashSet::from_iter(vec!["test0".parse().unwrap(), "test1".parse().unwrap()])
    );
}

/// Blocks that have already been gc'ed should not be accepted again.
#[test]
fn test_not_resync_old_blocks() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let epoch_length = 5;
    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let mut blocks = vec![];
    for i in 1..=epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 1) {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        blocks.push(block);
    }
    for i in 2..epoch_length {
        let block = blocks[i as usize - 1].clone();
        assert!(env.clients[0].chain.get_block(block.hash()).is_err());
        let (_, res) = env.clients[0].process_block(block.into(), Provenance::NONE);
        assert_matches!(res, Err(x) if matches!(x, Error::Orphan));
        assert_eq!(env.clients[0].chain.orphans_len(), 0);
    }
}

#[test]
fn test_gc_tail_update() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let epoch_length = 2;
    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .clients_count(2)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 2))
        .build();
    let mut blocks = vec![];
    for i in 1..=epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 1) {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        blocks.push(block);
    }
    let headers = blocks.iter().map(|b| b.header().clone()).collect::<Vec<_>>();
    env.clients[1].sync_block_headers(headers).unwrap();
    // simulate save sync hash block
    let prev_sync_block = blocks[blocks.len() - 3].clone();
    let prev_sync_hash = *prev_sync_block.hash();
    let prev_sync_height = prev_sync_block.header().height();
    let sync_block = blocks[blocks.len() - 2].clone();
    env.clients[1].chain.reset_data_pre_state_sync(*sync_block.hash()).unwrap();
    env.clients[1].chain.save_block(prev_sync_block.into()).unwrap();
    let mut store_update = env.clients[1].chain.mut_store().store_update();
    store_update.inc_block_refcount(&prev_sync_hash).unwrap();
    store_update.save_block(sync_block.clone());
    store_update.commit().unwrap();
    env.clients[1]
        .chain
        .reset_heads_post_state_sync(
            &None,
            *sync_block.hash(),
            &mut |_| {},
            &mut |_| {},
            &mut |_| {},
            &mut |_| {},
        )
        .unwrap();
    env.process_block(1, blocks.pop().unwrap(), Provenance::NONE);
    assert_eq!(env.clients[1].chain.store().tail().unwrap(), prev_sync_height);
}

/// Test that transaction does not become invalid when there is some gas price change.
#[test]
fn test_gas_price_change() {
    init_test_logger();
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let target_num_tokens_left = NEAR_BASE / 10 + 1;
    let transaction_costs = RuntimeConfig::test().transaction_costs;

    let send_money_total_gas =
        transaction_costs.action_creation_config.transfer_cost.send_fee(false)
            + transaction_costs.action_receipt_creation_config.send_fee(false)
            + transaction_costs.action_creation_config.transfer_cost.exec_fee()
            + transaction_costs.action_receipt_creation_config.exec_fee();
    let min_gas_price = target_num_tokens_left / send_money_total_gas as u128;
    let gas_limit = 1000000000000;
    let gas_price_adjustment_rate = Rational::new(1, 10);

    genesis.config.min_gas_price = min_gas_price;
    genesis.config.gas_limit = gas_limit;
    genesis.config.gas_price_adjustment_rate = gas_price_adjustment_rate;
    let chain_genesis = ChainGenesis::from(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();
    let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
    let tx = SignedTransaction::send_money(
        1,
        "test1".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        TESTING_INIT_BALANCE
            - target_num_tokens_left
            - send_money_total_gas as u128 * min_gas_price,
        genesis_hash,
    );
    env.clients[0].process_tx(tx, false, false);
    env.produce_block(0, 1);
    let tx = SignedTransaction::send_money(
        2,
        "test1".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        1,
        genesis_hash,
    );
    env.clients[0].process_tx(tx, false, false);
    for i in 2..=4 {
        env.produce_block(0, i);
    }
}

#[test]
fn test_gas_price_overflow() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let min_gas_price = 1000000;
    let max_gas_price = 10_u128.pow(20);
    let gas_limit = 450000000000;
    let gas_price_adjustment_rate = Rational::from_integer(1);
    genesis.config.min_gas_price = min_gas_price;
    genesis.config.gas_limit = gas_limit;
    genesis.config.gas_price_adjustment_rate = gas_price_adjustment_rate;
    genesis.config.transaction_validity_period = 100000;
    genesis.config.epoch_length = 43200;
    genesis.config.max_gas_price = max_gas_price;

    let chain_genesis = ChainGenesis::from(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();
    let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
    for i in 1..100 {
        let tx = SignedTransaction::send_money(
            i,
            "test1".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            1,
            genesis_hash,
        );
        env.clients[0].process_tx(tx, false, false);
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        assert!(block.header().gas_price() <= max_gas_price);
        env.process_block(0, block, Provenance::PRODUCED);
    }
}

#[test]
fn test_invalid_block_root() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    let mut b1 = env.clients[0].produce_block(1).unwrap().unwrap();
    let signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    b1.mut_header().get_mut().inner_lite.block_merkle_root = CryptoHash::default();
    b1.mut_header().resign(&signer);
    let (_, tip) = env.clients[0].process_block(b1.into(), Provenance::NONE);
    match tip {
        Err(e) => match e {
            Error::InvalidBlockMerkleRoot => {}
            _ => assert!(false, "wrong error: {}", e),
        },
        _ => assert!(false, "succeeded, tip: {:?}", tip),
    }
}

#[test]
fn test_incorrect_validator_key_produce_block() {
    let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 2);
    let chain_genesis = ChainGenesis::from(&genesis);
    let runtime_adapter: Arc<dyn RuntimeAdapter> = Arc::new(nearcore::NightshadeRuntime::test(
        Path::new("../../../.."),
        create_test_store(),
        &genesis,
    ));
    let signer = Arc::new(InMemoryValidatorSigner::from_seed(
        "test0".parse().unwrap(),
        KeyType::ED25519,
        "seed",
    ));
    let mut config = ClientConfig::test(true, 10, 20, 2, false, true);
    config.epoch_length = chain_genesis.epoch_length;
    let mut client = Client::new(
        config,
        chain_genesis,
        runtime_adapter,
        Arc::new(MockPeerManagerAdapter::default()),
        Some(signer),
        false,
        TEST_SEED,
    )
    .unwrap();
    let res = client.produce_block(1);
    assert_matches!(res, Ok(None));
}

fn test_block_merkle_proof_with_len(n: NumBlocks, rng: &mut StdRng) {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let mut blocks = vec![genesis_block.clone()];
    let mut cur_height = genesis_block.header().height() + 1;
    while cur_height < n {
        let should_fork = rng.gen_bool(0.5);
        if should_fork {
            let block = env.clients[0].produce_block(cur_height).unwrap().unwrap();
            let fork_block = env.clients[0].produce_block(cur_height + 1).unwrap().unwrap();
            env.process_block(0, block.clone(), Provenance::PRODUCED);
            let next_block = env.clients[0].produce_block(cur_height + 2).unwrap().unwrap();
            assert_eq!(next_block.header().prev_hash(), block.hash());
            // simulate blocks arriving in random order
            if rng.gen_bool(0.5) {
                env.process_block(0, fork_block, Provenance::PRODUCED);
                env.process_block(0, next_block.clone(), Provenance::PRODUCED);
            } else {
                env.process_block(0, next_block.clone(), Provenance::PRODUCED);
                env.process_block(0, fork_block, Provenance::PRODUCED);
            }
            blocks.push(block);
            blocks.push(next_block);
            cur_height += 3;
        } else {
            let block = env.clients[0].produce_block(cur_height).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block, Provenance::PRODUCED);
            cur_height += 1;
        }
    }

    let head = blocks.pop().unwrap();
    let root = head.header().block_merkle_root();
    // verify that the mapping from block ordinal to block hash is correct
    for h in 0..head.header().height() {
        if let Ok(block) = env.clients[0].chain.get_block_by_height(h) {
            let block_hash = *block.hash();
            let block_ordinal =
                env.clients[0].chain.mut_store().get_block_merkle_tree(&block_hash).unwrap().size();
            let block_hash1 = env.clients[0]
                .chain
                .mut_store()
                .get_block_hash_from_ordinal(block_ordinal)
                .unwrap();
            assert_eq!(block_hash, block_hash1);
        }
    }
    for block in blocks {
        let proof = env.clients[0].chain.get_block_proof(block.hash(), head.hash()).unwrap();
        assert!(verify_hash(*root, &proof, *block.hash()));
    }
}

#[test]
fn test_block_merkle_proof() {
    let mut rng = StdRng::seed_from_u64(0);
    for i in 0..50 {
        test_block_merkle_proof_with_len(i, &mut rng);
    }
}

#[test]
fn test_block_merkle_proof_same_hash() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let proof =
        env.clients[0].chain.get_block_proof(genesis_block.hash(), genesis_block.hash()).unwrap();
    assert!(proof.is_empty());
}

#[test]
fn test_data_reset_before_state_sync() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let epoch_length = 5;
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();
    let tx = SignedTransaction::create_account(
        1,
        "test0".parse().unwrap(),
        "test_account".parse().unwrap(),
        NEAR_BASE,
        signer.public_key(),
        &signer,
        genesis_hash,
    );
    env.clients[0].process_tx(tx, false, false);
    for i in 1..5 {
        env.produce_block(0, i);
    }
    // check that the new account exists
    let head = env.clients[0].chain.head().unwrap();
    let head_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
    let response = env.clients[0]
        .runtime_adapter
        .query(
            ShardUId::single_shard(),
            &head_block.chunks()[0].prev_state_root(),
            head.height,
            0,
            &head.prev_block_hash,
            &head.last_block_hash,
            head_block.header().epoch_id(),
            &QueryRequest::ViewAccount { account_id: "test_account".parse().unwrap() },
        )
        .unwrap();
    assert_matches!(response.kind, QueryResponseKind::ViewAccount(_));
    env.clients[0].chain.reset_data_pre_state_sync(*head_block.hash()).unwrap();
    // account should not exist after clearing state
    let response = env.clients[0].runtime_adapter.query(
        ShardUId::single_shard(),
        &head_block.chunks()[0].prev_state_root(),
        head.height,
        0,
        &head.prev_block_hash,
        &head.last_block_hash,
        head_block.header().epoch_id(),
        &QueryRequest::ViewAccount { account_id: "test_account".parse().unwrap() },
    );
    // TODO(#3742): ViewClient still has data in cache by current design.
    assert!(response.is_ok());
}

#[test]
fn test_sync_hash_validity() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    for i in 1..19 {
        env.produce_block(0, i);
    }
    for i in 0..19 {
        let block_hash = *env.clients[0].chain.get_header_by_height(i).unwrap().hash();
        let res = env.clients[0].chain.check_sync_hash_validity(&block_hash);
        println!("height {:?} -> {:?}", i, res);
        if i == 11 || i == 16 {
            assert!(res.unwrap())
        } else {
            assert!(!res.unwrap())
        }
    }
    let bad_hash = CryptoHash::from_str("7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t").unwrap();
    let res = env.clients[0].chain.check_sync_hash_validity(&bad_hash);
    println!("bad hash -> {:?}", res.is_ok());
    match res {
        Ok(_) => assert!(false),
        Err(e) => match e {
            Error::DBNotFoundErr(_) => { /* the only expected error */ }
            _ => assert!(false),
        },
    }
}

/// Only process one block per height
#[test]
fn test_not_process_height_twice() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    let block = env.clients[0].produce_block(1).unwrap().unwrap();
    let mut invalid_block = block.clone();
    env.process_block(0, block, Provenance::PRODUCED);
    let validator_signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    #[cfg(feature = "protocol_feature_chunk_only_producers")]
    let proposals = vec![ValidatorStake::new(
        "test1".parse().unwrap(),
        PublicKey::empty(KeyType::ED25519),
        0,
        false,
    )];
    #[cfg(not(feature = "protocol_feature_chunk_only_producers"))]
    let proposals =
        vec![ValidatorStake::new("test1".parse().unwrap(), PublicKey::empty(KeyType::ED25519), 0)];
    invalid_block.mut_header().get_mut().inner_rest.validator_proposals = proposals;
    invalid_block.mut_header().resign(&validator_signer);
    let (accepted_blocks, res) =
        env.clients[0].process_block(invalid_block.into(), Provenance::NONE);
    assert!(accepted_blocks.is_empty());
    assert_matches!(res, Ok(None));
}

#[test]
fn test_block_height_processed_orphan() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    let block = env.clients[0].produce_block(1).unwrap().unwrap();
    let mut orphan_block = block;
    let validator_signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    orphan_block.mut_header().get_mut().prev_hash = hash(&[1]);
    orphan_block.mut_header().resign(&validator_signer);
    let block_height = orphan_block.header().height();
    let (_, tip) = env.clients[0].process_block(orphan_block.into(), Provenance::NONE);
    assert_matches!(tip.unwrap_err(), Error::Orphan);
    assert!(env.clients[0].chain.mut_store().is_height_processed(block_height).unwrap());
}

#[test]
fn test_validate_chunk_extra() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_height = genesis_block.header().height();

    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let tx = SignedTransaction::from_actions(
        1,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
        *genesis_block.hash(),
    );
    env.clients[0].process_tx(tx, false, false);
    let mut last_block = genesis_block;
    for i in 1..3 {
        last_block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, last_block.clone(), Provenance::PRODUCED);
    }

    // Construct a chunk that such when the receipts generated by this chunk are included
    // in blocks of different heights, the state transitions are different.
    let function_call_tx = SignedTransaction::from_actions(
        2,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "write_block_height".to_string(),
            args: vec![],
            gas: 100000000000000,
            deposit: 0,
        })],
        *last_block.hash(),
    );
    env.clients[0].process_tx(function_call_tx, false, false);
    for i in 3..5 {
        last_block = env.clients[0].produce_block(i).unwrap().unwrap();
        if i == 3 {
            env.process_block(0, last_block.clone(), Provenance::PRODUCED);
        } else {
            let (_, res) =
                env.clients[0].process_block(last_block.clone().into(), Provenance::NONE);
            assert_matches!(res, Ok(Some(_)));
        }
    }

    // Construct two blocks that contain the same chunk and make the chunk unavailable.
    let validator_signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let next_height = last_block.header().height() + 1;
    let (encoded_chunk, merkle_paths, receipts) =
        create_chunk_on_height(&mut env.clients[0], next_height);
    let mut block1 = env.clients[0].produce_block(next_height).unwrap().unwrap();
    let mut block2 = env.clients[0].produce_block(next_height + 1).unwrap().unwrap();

    // Process two blocks on two different forks that contain the same chunk.
    for (i, block) in vec![&mut block1, &mut block2].into_iter().enumerate() {
        let mut chunk_header = encoded_chunk.cloned_header();
        *chunk_header.height_included_mut() = i as BlockHeight + next_height;
        let chunk_headers = vec![chunk_header];
        block.set_chunks(chunk_headers.clone());
        block.mut_header().get_mut().inner_rest.chunk_headers_root =
            Block::compute_chunk_headers_root(&chunk_headers).0;
        block.mut_header().get_mut().inner_rest.chunk_tx_root =
            Block::compute_chunk_tx_root(&chunk_headers);
        block.mut_header().get_mut().inner_rest.chunk_receipts_root =
            Block::compute_chunk_receipts_root(&chunk_headers);
        block.mut_header().get_mut().inner_lite.prev_state_root =
            Block::compute_state_root(&chunk_headers);
        block.mut_header().get_mut().inner_rest.chunk_mask = vec![true];
        block.mut_header().resign(&validator_signer);
        let (_, res) = env.clients[0].process_block(block.clone().into(), Provenance::NONE);
        assert_matches!(res.unwrap_err(), near_chain::Error::ChunksMissing(_));
    }

    // Process the previously unavailable chunk. This causes two blocks to be accepted.
    let mut chain_store =
        ChainStore::new(env.clients[0].chain.store().store().clone(), genesis_height, true);
    let chunk_header = encoded_chunk.cloned_header();
    env.clients[0]
        .shards_mgr
        .distribute_encoded_chunk(encoded_chunk, merkle_paths, receipts, &mut chain_store)
        .unwrap();
    env.clients[0].chain.blocks_with_missing_chunks.accept_chunk(&chunk_header.chunk_hash());
    let accepted_blocks = env.clients[0].process_blocks_with_missing_chunks();
    assert_eq!(accepted_blocks.len(), 2);
    for (i, accepted_block) in accepted_blocks.into_iter().enumerate() {
        if i == 0 {
            assert_eq!(&accepted_block.hash, block1.hash());
            env.clients[0].on_block_accepted(
                accepted_block.hash,
                accepted_block.status,
                accepted_block.provenance,
            );
        }
    }

    // About to produce a block on top of block1. Validate that this chunk is legit.
    let chunks = env.clients[0].shards_mgr.prepare_chunks(block1.hash());
    let chunk_extra =
        env.clients[0].chain.get_chunk_extra(block1.hash(), &ShardUId::single_shard()).unwrap();
    assert!(validate_chunk_with_chunk_extra(
        &mut chain_store,
        &*env.clients[0].runtime_adapter,
        block1.hash(),
        &chunk_extra,
        block1.chunks()[0].height_included(),
        &chunks.get(&0).cloned().unwrap(),
    )
    .is_ok());
}

/// Change protocol version back and forth and make sure that we do not produce invalid blocks
/// TODO (#3759): re-enable the test when we have the ability to mutate `PROTOCOL_VERSION`
#[test]
#[ignore]
fn test_gas_price_change_no_chunk() {
    let epoch_length = 5;
    let min_gas_price = 5000;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let genesis_protocol_version = PROTOCOL_VERSION - 1;
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = genesis_protocol_version;
    genesis.config.min_gas_price = min_gas_price;
    let chain_genesis = ChainGenesis::from(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let validator_signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    for i in 1..=20 {
        let mut block = env.clients[0].produce_block(i).unwrap().unwrap();
        if i <= 5 || (i > 10 && i <= 15) {
            block.mut_header().get_mut().inner_rest.latest_protocol_version =
                genesis_protocol_version;
            block.mut_header().resign(&validator_signer);
        }
        env.process_block(0, block, Provenance::NONE);
    }
    env.clients[0].produce_block(21).unwrap().unwrap();
    let block = env.clients[0].produce_block(22).unwrap().unwrap();
    let (_, res) = env.clients[0].process_block(block.into(), Provenance::NONE);
    assert!(res.is_ok());
}

#[test]
fn test_catchup_gas_price_change() {
    init_test_logger();
    let epoch_length = 5;
    let min_gas_price = 10000;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.min_gas_price = min_gas_price;
    genesis.config.gas_limit = 1000000000000;
    let chain_genesis = ChainGenesis::from(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .clients_count(2)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 2))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let mut blocks = vec![];
    for i in 1..3 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        env.process_block(1, block, Provenance::NONE);
    }
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    for i in 0..3 {
        let tx = SignedTransaction::send_money(
            i + 1,
            "test0".parse().unwrap(),
            "test1".parse().unwrap(),
            &signer,
            1,
            *genesis_block.hash(),
        );
        env.clients[0].process_tx(tx, false, false);
    }
    for i in 3..=6 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        env.process_block(1, block, Provenance::NONE);
    }

    assert_ne!(blocks[3].header().gas_price(), blocks[4].header().gas_price());
    assert!(env.clients[1]
        .chain
        .get_chunk_extra(blocks[4].hash(), &ShardUId::single_shard())
        .is_err());

    // Simulate state sync
    let sync_hash = *blocks[5].hash();
    assert!(env.clients[0].chain.check_sync_hash_validity(&sync_hash).unwrap());
    let state_sync_header = env.clients[0].chain.get_state_response_header(0, sync_hash).unwrap();
    let state_root = match &state_sync_header {
        ShardStateSyncResponseHeader::V1(header) => header.chunk.header.inner.prev_state_root,
        ShardStateSyncResponseHeader::V2(header) => {
            *header.chunk.cloned_header().take_inner().prev_state_root()
        }
    };
    //let state_root = state_sync_header.chunk.header.inner.prev_state_root;
    let state_root_node =
        env.clients[0].runtime_adapter.get_state_root_node(0, &sync_hash, &state_root).unwrap();
    let num_parts = get_num_state_parts(state_root_node.memory_usage);
    let state_sync_parts = (0..num_parts)
        .map(|i| env.clients[0].chain.get_state_response_part(0, i, sync_hash).unwrap())
        .collect::<Vec<_>>();

    env.clients[1].chain.set_state_header(0, sync_hash, state_sync_header).unwrap();
    for i in 0..num_parts {
        env.clients[1]
            .chain
            .set_state_part(0, sync_hash, PartId::new(i, num_parts), &state_sync_parts[i as usize])
            .unwrap();
    }
    let rt = Arc::clone(&env.clients[1].runtime_adapter);
    let f = move |msg: ApplyStatePartsRequest| {
        use borsh::BorshSerialize;
        let store = rt.get_store();

        for part_id in 0..msg.num_parts {
            let key = StatePartKey(msg.sync_hash, msg.shard_id, part_id).try_to_vec().unwrap();
            let part = store.get(DBCol::StateParts, &key).unwrap().unwrap();

            rt.apply_state_part(
                msg.shard_id,
                &msg.state_root,
                PartId::new(part_id, msg.num_parts),
                &part,
                &msg.epoch_id,
            )
            .unwrap();
        }
    };
    env.clients[1].chain.schedule_apply_state_parts(0, sync_hash, num_parts, &f).unwrap();
    env.clients[1].chain.set_state_finalize(0, sync_hash, Ok(())).unwrap();
    let chunk_extra_after_sync =
        env.clients[1].chain.get_chunk_extra(blocks[4].hash(), &ShardUId::single_shard()).unwrap();
    let expected_chunk_extra =
        env.clients[0].chain.get_chunk_extra(blocks[4].hash(), &ShardUId::single_shard()).unwrap();
    // The chunk extra of the prev block of sync block should be the same as the node that it is syncing from
    assert_eq!(chunk_extra_after_sync, expected_chunk_extra);
}

#[test]
fn test_block_execution_outcomes() {
    let epoch_length = 5;
    let min_gas_price = 10000;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.min_gas_price = min_gas_price;
    genesis.config.gas_limit = 1000000000000;
    let chain_genesis = ChainGenesis::from(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let mut tx_hashes = vec![];
    for i in 0..3 {
        // send transaction to the same account to generate local receipts
        let tx = SignedTransaction::send_money(
            i + 1,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            1,
            *genesis_block.hash(),
        );
        tx_hashes.push(tx.get_hash());
        env.clients[0].process_tx(tx, false, false);
    }
    for i in 1..4 {
        env.produce_block(0, i);
    }

    let mut expected_outcome_ids = HashSet::new();
    let mut delayed_receipt_id = vec![];
    // Due to gas limit, the first two transaactions will create local receipts and they get executed
    // in the same block. The last local receipt will become delayed receipt
    for (i, id) in tx_hashes.into_iter().enumerate() {
        let execution_outcome = env.clients[0].chain.get_execution_outcome(&id).unwrap();
        assert_eq!(execution_outcome.outcome_with_id.outcome.receipt_ids.len(), 1);
        expected_outcome_ids.insert(id);
        if i < 2 {
            expected_outcome_ids.insert(execution_outcome.outcome_with_id.outcome.receipt_ids[0]);
        } else {
            delayed_receipt_id.push(execution_outcome.outcome_with_id.outcome.receipt_ids[0])
        }
    }
    let block = env.clients[0].chain.get_block_by_height(2).unwrap();
    let chunk = env.clients[0].chain.get_chunk(&block.chunks()[0].chunk_hash()).unwrap();
    assert_eq!(chunk.transactions().len(), 3);
    let execution_outcomes_from_block = env.clients[0]
        .chain
        .get_block_execution_outcomes(block.hash())
        .unwrap()
        .remove(&0)
        .unwrap();
    assert_eq!(execution_outcomes_from_block.len(), 5);
    assert_eq!(
        execution_outcomes_from_block
            .into_iter()
            .map(|execution_outcome| execution_outcome.outcome_with_id.id)
            .collect::<HashSet<_>>(),
        expected_outcome_ids
    );

    // Make sure the chunk outcomes contain the outcome from the delayed receipt.
    let next_block = env.clients[0].chain.get_block_by_height(3).unwrap();
    let next_chunk = env.clients[0].chain.get_chunk(&next_block.chunks()[0].chunk_hash()).unwrap();
    assert!(next_chunk.transactions().is_empty());
    assert!(next_chunk.receipts().is_empty());
    let execution_outcomes_from_block = env.clients[0]
        .chain
        .get_block_execution_outcomes(next_block.hash())
        .unwrap()
        .remove(&0)
        .unwrap();
    assert_eq!(execution_outcomes_from_block.len(), 1);
    assert!(execution_outcomes_from_block[0].outcome_with_id.id == delayed_receipt_id[0]);
}

#[test]
fn test_refund_receipts_processing() {
    init_test_logger();

    let epoch_length = 5;
    let min_gas_price = 10000;
    let mut genesis = Genesis::test_sharded_new_version(
        vec!["test0".parse().unwrap(), "test1".parse().unwrap()],
        1,
        vec![1],
    );
    genesis.config.epoch_length = epoch_length;
    genesis.config.min_gas_price = min_gas_price;
    // set gas limit to be small
    genesis.config.gas_limit = 1_000_000;
    let chain_genesis = ChainGenesis::from(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let mut tx_hashes = vec![];
    // send transactions to a non-existing account to generate refund
    for i in 0..3 {
        // send transaction to the same account to generate local receipts
        let tx = SignedTransaction::send_money(
            i + 1,
            "test0".parse().unwrap(),
            "random_account".parse().unwrap(),
            &signer,
            1,
            *genesis_block.hash(),
        );
        tx_hashes.push(tx.get_hash());
        env.clients[0].process_tx(tx, false, false);
    }

    env.produce_block(0, 3);
    env.produce_block(0, 4);
    let mut block_height = 5;
    let test_shard_uid = ShardUId { version: 1, shard_id: 0 };
    loop {
        env.produce_block(0, block_height);
        let block = env.clients[0].chain.get_block_by_height(block_height).unwrap().clone();
        let prev_block =
            env.clients[0].chain.get_block_by_height(block_height - 1).unwrap().clone();
        let chunk_extra = env.clients[0]
            .chain
            .get_chunk_extra(prev_block.hash(), &test_shard_uid)
            .unwrap()
            .clone();
        let state_update = env.clients[0]
            .runtime_adapter
            .get_tries()
            .new_trie_update(test_shard_uid, *chunk_extra.state_root());
        let delayed_indices =
            get::<DelayedReceiptIndices>(&state_update, &TrieKey::DelayedReceiptIndices).unwrap();
        let finished_all_delayed_receipts = match delayed_indices {
            None => false,
            Some(delayed_indices) => {
                delayed_indices.next_available_index > 0
                    && delayed_indices.first_index == delayed_indices.next_available_index
            }
        };
        let chunk =
            env.clients[0].chain.get_chunk(&block.chunks()[0].chunk_hash()).unwrap().clone();
        if chunk.receipts().is_empty()
            && chunk.transactions().is_empty()
            && finished_all_delayed_receipts
        {
            break;
        }
        block_height += 1;
    }

    let mut refund_receipt_ids = HashSet::new();
    for (_, id) in tx_hashes.into_iter().enumerate() {
        let execution_outcome = env.clients[0].chain.get_execution_outcome(&id).unwrap();
        assert_eq!(execution_outcome.outcome_with_id.outcome.receipt_ids.len(), 1);
        match execution_outcome.outcome_with_id.outcome.status {
            ExecutionStatus::SuccessReceiptId(id) => {
                let receipt_outcome = env.clients[0].chain.get_execution_outcome(&id).unwrap();
                assert_matches!(
                    receipt_outcome.outcome_with_id.outcome.status,
                    ExecutionStatus::Failure(TxExecutionError::ActionError(_))
                );
                receipt_outcome.outcome_with_id.outcome.receipt_ids.iter().for_each(|id| {
                    refund_receipt_ids.insert(*id);
                });
            }
            _ => assert!(false),
        };
    }

    let ending_block_height = block_height - 1;
    let begin_block_height = ending_block_height - refund_receipt_ids.len() as u64 + 1;
    let mut processed_refund_receipt_ids = HashSet::new();
    for i in begin_block_height..=ending_block_height {
        let block = env.clients[0].chain.get_block_by_height(i).unwrap().clone();
        let execution_outcomes_from_block = env.clients[0]
            .chain
            .get_block_execution_outcomes(block.hash())
            .unwrap()
            .remove(&0)
            .unwrap();
        execution_outcomes_from_block.iter().for_each(|outcome| {
            processed_refund_receipt_ids.insert(outcome.outcome_with_id.id);
        });
        let chunk_extra =
            env.clients[0].chain.get_chunk_extra(block.hash(), &test_shard_uid).unwrap().clone();
        assert_eq!(execution_outcomes_from_block.len(), 1);
        assert!(chunk_extra.gas_used() >= chunk_extra.gas_limit());
    }
    assert_eq!(processed_refund_receipt_ids, refund_receipt_ids);
}

#[test]
fn test_wasmer2_upgrade() {
    let mut capture = near_logger_utils::TracingCapture::enable();

    let old_protocol_version =
        near_primitives::version::ProtocolFeature::Wasmer2.protocol_version() - 1;
    let new_protocol_version = old_protocol_version + 1;

    // Prepare TestEnv with a contract at the old protocol version.
    let mut env = {
        let epoch_length = 5;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = old_protocol_version;
        let chain_genesis = ChainGenesis::from(&genesis);
        let mut env = TestEnv::builder(chain_genesis)
            .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
            .build();

        deploy_test_contract(
            &mut env,
            "test0".parse().unwrap(),
            near_test_contracts::rs_contract(),
            epoch_length,
            1,
        );
        env
    };

    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let tx = Transaction {
        signer_id: "test0".parse().unwrap(),
        receiver_id: "test0".parse().unwrap(),
        public_key: signer.public_key(),
        actions: vec![Action::FunctionCall(FunctionCallAction {
            method_name: "log_something".to_string(),
            args: Vec::new(),
            gas: 100_000_000_000_000,
            deposit: 0,
        })],

        nonce: 0,
        block_hash: CryptoHash::default(),
    };

    // Run the transaction & collect the logs.
    let logs_at_old_version = {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_transaction =
            Transaction { nonce: 10, block_hash: tip.last_block_hash, ..tx.clone() }.sign(&signer);
        env.clients[0].process_tx(signed_transaction, false, false);
        for i in 0..3 {
            env.produce_block(0, tip.height + i + 1);
        }
        capture.drain()
    };

    // Move to the new protocol version.
    {
        let tip = env.clients[0].chain.head().unwrap();
        let epoch_id = env.clients[0]
            .runtime_adapter
            .get_epoch_id_from_prev_block(&tip.last_block_hash)
            .unwrap();
        let block_producer =
            env.clients[0].runtime_adapter.get_block_producer(&epoch_id, tip.height).unwrap();
        let mut block = env.clients[0].produce_block(tip.height + 1).unwrap().unwrap();
        set_block_protocol_version(&mut block, block_producer, new_protocol_version);
        let (_, res) = env.clients[0].process_block(block.clone().into(), Provenance::NONE);
        assert!(res.is_ok());
    }

    // Re-run the transaction.
    let logs_at_new_version = {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_transaction =
            Transaction { nonce: 11, block_hash: tip.last_block_hash, ..tx }.sign(&signer);
        env.clients[0].process_tx(signed_transaction, false, false);
        for i in 0..3 {
            env.produce_block(0, tip.height + i + 1);
        }
        capture.drain()
    };

    assert!(logs_at_old_version.iter().any(|l| l.contains(&"run_wasmer0")));
    assert!(logs_at_new_version.iter().any(|l| l.contains(&"run_wasmer2")));
}

#[test]
fn test_execution_metadata() {
    // Prepare TestEnv with a very simple WASM contract.
    let wasm_code = wat::parse_str(
        r#"
(module
    (import "env" "block_index" (func $block_index (result i64)))
    (func (export "main")
        (call $block_index)
        drop
    )
)"#,
    )
    .unwrap();

    let mut env = {
        let epoch_length = 5;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        let chain_genesis = ChainGenesis::from(&genesis);
        let mut env = TestEnv::builder(chain_genesis)
            .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
            .build();

        deploy_test_contract(&mut env, "test0".parse().unwrap(), &wasm_code, epoch_length, 1);
        env
    };

    // Call the contract and get the execution outcome.
    let execution_outcome = {
        let tip = env.clients[0].chain.head().unwrap();
        let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        let tx = Transaction {
            signer_id: "test0".parse().unwrap(),
            receiver_id: "test0".parse().unwrap(),
            public_key: signer.public_key(),
            actions: vec![Action::FunctionCall(FunctionCallAction {
                method_name: "main".to_string(),
                args: Vec::new(),
                gas: 100_000_000_000_000,
                deposit: 0,
            })],

            nonce: 10,
            block_hash: tip.last_block_hash,
        }
        .sign(&signer);
        let tx_hash = tx.get_hash();

        env.clients[0].process_tx(tx, false, false);
        for i in 0..3 {
            env.produce_block(0, tip.height + i + 1);
        }
        env.query_transaction_status(&tx_hash)
    };

    // Now, let's assert that we get the cost breakdown we expect.
    let config = RuntimeConfigStore::test().get_config(PROTOCOL_VERSION).clone();

    // Total costs for creating a function call receipt.
    let expected_receipt_cost = config.transaction_costs.action_receipt_creation_config.execution
        + config.transaction_costs.action_creation_config.function_call_cost.exec_fee()
        + config.transaction_costs.action_creation_config.function_call_cost_per_byte.exec_fee()
            * "main".len() as u64;

    // Profile for what's happening *inside* wasm vm during function call.
    let expected_profile = serde_json::json!([
      // Inside the contract, we called one host function.
      {
        "cost_category": "WASM_HOST_COST",
        "cost": "BASE",
        "gas_used": config.wasm_config.ext_costs.base.to_string()
      },
      // We include compilation costs into running the function.
      {
        "cost_category": "WASM_HOST_COST",
        "cost": "CONTRACT_LOADING_BASE",
        "gas_used": config.wasm_config.ext_costs.contract_loading_base.to_string()
      },
      {
        "cost_category": "WASM_HOST_COST",
        "cost": "CONTRACT_LOADING_BYTES",
        "gas_used": "18423750"
      },
      // We spend two wasm instructions (call & drop).
      {
        "cost_category": "WASM_HOST_COST",
        "cost": "WASM_INSTRUCTION",
        "gas_used": (config.wasm_config.regular_op_cost as u64 * 2).to_string()
      }
    ]);
    let outcome = &execution_outcome.receipts_outcome[0].outcome;
    let metadata = &outcome.metadata;

    let actual_profile = serde_json::to_value(&metadata.gas_profile).unwrap();
    assert_eq!(expected_profile, actual_profile);

    let actual_receipt_cost = outcome.gas_burnt
        - metadata
            .gas_profile
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|it| it.gas_used)
            .sum::<u64>();

    assert_eq!(expected_receipt_cost, actual_receipt_cost)
}

#[test]
fn test_epoch_protocol_version_change() {
    init_test_logger();
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 2);
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = PROTOCOL_VERSION;
    let genesis_height = genesis.config.genesis_height;
    let chain_genesis = ChainGenesis::from(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .clients_count(2)
        .validator_seats(2)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 2))
        .build();
    for i in 1..=16 {
        let head = env.clients[0].chain.head().unwrap();
        let epoch_id = env.clients[0]
            .runtime_adapter
            .get_epoch_id_from_prev_block(&head.last_block_hash)
            .unwrap();
        let chunk_producer =
            env.clients[0].runtime_adapter.get_chunk_producer(&epoch_id, i, 0).unwrap();
        let index = if chunk_producer.as_ref() == "test0" { 0 } else { 1 };
        let (encoded_chunk, merkle_paths, receipts) =
            create_chunk_on_height(&mut env.clients[index], i);

        for j in 0..2 {
            let mut chain_store =
                ChainStore::new(env.clients[j].chain.store().store().clone(), genesis_height, true);
            env.clients[j]
                .shards_mgr
                .distribute_encoded_chunk(
                    encoded_chunk.clone(),
                    merkle_paths.clone(),
                    receipts.clone(),
                    &mut chain_store,
                )
                .unwrap();
        }

        let epoch_id = env.clients[0]
            .runtime_adapter
            .get_epoch_id_from_prev_block(&head.last_block_hash)
            .unwrap();
        let block_producer =
            env.clients[0].runtime_adapter.get_block_producer(&epoch_id, i).unwrap();
        let index = if block_producer.as_ref() == "test0" { 0 } else { 1 };
        let mut block = env.clients[index].produce_block(i).unwrap().unwrap();
        // upgrade to new protocol version but in the second epoch one node vote for the old version.
        if i != 10 {
            set_block_protocol_version(&mut block, block_producer.clone(), PROTOCOL_VERSION + 1);
        }
        for j in 0..2 {
            let (_, res) = env.clients[j].process_block(block.clone().into(), Provenance::NONE);
            res.unwrap();
            run_catchup(&mut env.clients[j], &vec![]).unwrap();
        }
    }
    let last_block = env.clients[0].chain.get_block_by_height(16).unwrap();
    let protocol_version = env.clients[0]
        .runtime_adapter
        .get_epoch_protocol_version(last_block.header().epoch_id())
        .unwrap();
    assert_eq!(protocol_version, PROTOCOL_VERSION + 1);
}

/// Final state should be consistent when a node switches between forks in the following scenario
///                      /-----------h+2
/// h-2 ---- h-1 ------ h
///                      \------h+1
/// even though from the perspective of h+2 the last final block is h-2.
#[test]
fn test_query_final_state() {
    let epoch_length = 10;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;

    let chain_genesis = ChainGenesis::from(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let tx = SignedTransaction::send_money(
        1,
        "test0".parse().unwrap(),
        "test1".parse().unwrap(),
        &signer,
        100,
        *genesis_block.hash(),
    );
    env.clients[0].process_tx(tx, false, false);

    let mut blocks = vec![];

    for i in 1..5 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block.clone(), Provenance::PRODUCED);
    }

    let query_final_state = |chain: &mut near_chain::Chain,
                             runtime_adapter: Arc<dyn RuntimeAdapter>,
                             account_id: AccountId| {
        let final_head = chain.store().final_head().unwrap();
        let last_final_block = chain.get_block(&final_head.last_block_hash).unwrap();
        let response = runtime_adapter
            .query(
                ShardUId::single_shard(),
                &last_final_block.chunks()[0].prev_state_root(),
                last_final_block.header().height(),
                last_final_block.header().raw_timestamp(),
                &final_head.prev_block_hash,
                last_final_block.hash(),
                last_final_block.header().epoch_id(),
                &QueryRequest::ViewAccount { account_id },
            )
            .unwrap();
        match response.kind {
            QueryResponseKind::ViewAccount(account_view) => account_view,
            _ => panic!("Wrong return value"),
        }
    };

    let fork1_block = env.clients[0].produce_block(5).unwrap().unwrap();
    env.clients[0]
        .chain
        .mut_store()
        .save_latest_known(LatestKnown {
            height: blocks.last().unwrap().header().height(),
            seen: blocks.last().unwrap().header().raw_timestamp(),
        })
        .unwrap();
    let fork2_block = env.clients[0].produce_block(6).unwrap().unwrap();
    assert_eq!(fork1_block.header().prev_hash(), fork2_block.header().prev_hash());
    env.process_block(0, fork1_block, Provenance::NONE);
    assert_eq!(env.clients[0].chain.head().unwrap().height, 5);

    let runtime_adapter = env.clients[0].runtime_adapter.clone();
    let account_state1 = query_final_state(
        &mut env.clients[0].chain,
        runtime_adapter.clone(),
        "test0".parse().unwrap(),
    );

    env.process_block(0, fork2_block, Provenance::NONE);
    assert_eq!(env.clients[0].chain.head().unwrap().height, 6);

    let runtime_adapter = env.clients[0].runtime_adapter.clone();
    let account_state2 = query_final_state(
        &mut env.clients[0].chain,
        runtime_adapter.clone(),
        "test0".parse().unwrap(),
    );

    assert_eq!(account_state1, account_state2);
    assert!(account_state1.amount < TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
}

#[test]
fn test_fork_receipt_ids() {
    let (mut env, tx_hash) = prepare_env_with_transaction();

    let produced_block = env.clients[0].produce_block(1).unwrap().unwrap();
    env.process_block(0, produced_block.clone(), Provenance::PRODUCED);

    // Construct two blocks that contain the same chunk and make the chunk unavailable.
    let validator_signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let next_height = produced_block.header().height() + 1;
    let (encoded_chunk, _, _) = create_chunk_on_height(&mut env.clients[0], next_height);
    let mut block1 = env.clients[0].produce_block(next_height).unwrap().unwrap();
    let mut block2 = env.clients[0].produce_block(next_height + 1).unwrap().unwrap();

    // Process two blocks on two different forks that contain the same chunk.
    for (i, block) in vec![&mut block2, &mut block1].into_iter().enumerate() {
        let mut chunk_header = encoded_chunk.cloned_header();
        *chunk_header.height_included_mut() = next_height - i as BlockHeight + 1;
        let chunk_headers = vec![chunk_header];
        block.set_chunks(chunk_headers.clone());
        block.mut_header().get_mut().inner_rest.chunk_headers_root =
            Block::compute_chunk_headers_root(&chunk_headers).0;
        block.mut_header().get_mut().inner_rest.chunk_tx_root =
            Block::compute_chunk_tx_root(&chunk_headers);
        block.mut_header().get_mut().inner_rest.chunk_receipts_root =
            Block::compute_chunk_receipts_root(&chunk_headers);
        block.mut_header().get_mut().inner_lite.prev_state_root =
            Block::compute_state_root(&chunk_headers);
        block.mut_header().get_mut().inner_rest.chunk_mask = vec![true];
        block.mut_header().resign(&validator_signer);
        let (_, res) = env.clients[0].process_block(block.clone().into(), Provenance::NONE);
        assert!(res.is_ok());
    }

    let transaction_execution_outcome =
        env.clients[0].chain.mut_store().get_outcomes_by_id(&tx_hash).unwrap();
    assert_eq!(transaction_execution_outcome.len(), 2);
    let receipt_id0 = transaction_execution_outcome[0].outcome_with_id.outcome.receipt_ids[0];
    let receipt_id1 = transaction_execution_outcome[1].outcome_with_id.outcome.receipt_ids[0];
    assert_ne!(receipt_id0, receipt_id1);
}

#[test]
fn test_fork_execution_outcome() {
    let (mut env, tx_hash) = prepare_env_with_transaction();

    let mut last_height = 0;
    for i in 1..3 {
        let last_block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, last_block.clone(), Provenance::PRODUCED);
        last_height = last_block.header().height();
    }

    // Construct two blocks that contain the same chunk and make the chunk unavailable.
    let validator_signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let next_height = last_height + 1;
    let (encoded_chunk, _, _) = create_chunk_on_height(&mut env.clients[0], next_height);
    let mut block1 = env.clients[0].produce_block(next_height).unwrap().unwrap();
    let mut block2 = env.clients[0].produce_block(next_height + 1).unwrap().unwrap();

    // Process two blocks on two different forks that contain the same chunk.
    for (i, block) in vec![&mut block2, &mut block1].into_iter().enumerate() {
        let mut chunk_header = encoded_chunk.cloned_header();
        *chunk_header.height_included_mut() = next_height - i as BlockHeight + 1;
        let chunk_headers = vec![chunk_header];
        block.set_chunks(chunk_headers.clone());
        block.mut_header().get_mut().inner_rest.chunk_headers_root =
            Block::compute_chunk_headers_root(&chunk_headers).0;
        block.mut_header().get_mut().inner_rest.chunk_tx_root =
            Block::compute_chunk_tx_root(&chunk_headers);
        block.mut_header().get_mut().inner_rest.chunk_receipts_root =
            Block::compute_chunk_receipts_root(&chunk_headers);
        block.mut_header().get_mut().inner_lite.prev_state_root =
            Block::compute_state_root(&chunk_headers);
        block.mut_header().get_mut().inner_rest.chunk_mask = vec![true];
        block.mut_header().resign(&validator_signer);
        let (_, res) = env.clients[0].process_block(block.clone().into(), Provenance::NONE);
        assert!(res.is_ok());
    }

    let transaction_execution_outcome =
        env.clients[0].chain.mut_store().get_outcomes_by_id(&tx_hash).unwrap();
    assert_eq!(transaction_execution_outcome.len(), 1);
    let receipt_id = transaction_execution_outcome[0].outcome_with_id.outcome.receipt_ids[0];
    let receipt_execution_outcomes =
        env.clients[0].chain.mut_store().get_outcomes_by_id(&receipt_id).unwrap();
    assert_eq!(receipt_execution_outcomes.len(), 2);
    let canonical_chain_outcome = env.clients[0].chain.get_execution_outcome(&receipt_id).unwrap();
    assert_eq!(canonical_chain_outcome.block_hash, *block2.hash());

    // make sure gc works properly
    for i in 5..32 {
        env.produce_block(0, i);
    }
    let transaction_execution_outcome =
        env.clients[0].chain.store().get_outcomes_by_id(&tx_hash).unwrap();
    assert!(transaction_execution_outcome.is_empty());
    let receipt_execution_outcomes =
        env.clients[0].chain.store().get_outcomes_by_id(&receipt_id).unwrap();
    assert!(receipt_execution_outcomes.is_empty());
}

fn prepare_env_with_transaction() -> (TestEnv, CryptoHash) {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let tx = SignedTransaction::send_money(
        1,
        "test0".parse().unwrap(),
        "test1".parse().unwrap(),
        &signer,
        100,
        *genesis_block.hash(),
    );
    let tx_hash = tx.get_hash();
    env.clients[0].process_tx(tx, false, false);
    (env, tx_hash)
}

#[test]
fn test_not_broadcast_block_on_accept() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let mut env = TestEnv::builder(ChainGenesis::test())
        .clients_count(2)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 2))
        .network_adapters(vec![
            Arc::new(MockPeerManagerAdapter::default()),
            network_adapter.clone(),
        ])
        .build();
    let b1 = env.clients[0].produce_block(1).unwrap().unwrap();
    for i in 0..2 {
        env.process_block(i, b1.clone(), Provenance::NONE);
    }
    assert!(network_adapter.requests.read().unwrap().is_empty());
}

#[test]
#[should_panic]
// TODO (#3729): reject header version downgrade
fn test_header_version_downgrade() {
    use borsh::ser::BorshSerialize;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = 5;
    let chain_genesis = ChainGenesis::from(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let validator_signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    for i in 1..10 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block, Provenance::NONE);
    }
    let block = {
        let mut block = env.clients[0].produce_block(10).unwrap().unwrap();
        // Convert header to BlockHeaderV1
        let mut header_view: BlockHeaderView = block.header().clone().into();
        header_view.latest_protocol_version = 1;
        let mut header = header_view.into();

        // BlockHeaderV1, but protocol version is newest
        match &mut header {
            BlockHeader::BlockHeaderV1(header) => {
                let header = Arc::make_mut(header);
                header.inner_rest.latest_protocol_version = PROTOCOL_VERSION;
                let (hash, signature) = validator_signer.sign_block_header_parts(
                    header.prev_hash,
                    &header.inner_lite.try_to_vec().expect("Failed to serialize"),
                    &header.inner_rest.try_to_vec().expect("Failed to serialize"),
                );
                header.hash = hash;
                header.signature = signature;
            }
            _ => {
                unreachable!();
            }
        }
        *block.mut_header() = header;
        block
    };
    let (_, res) = env.clients[0].process_block(block.into(), Provenance::NONE);
    assert!(!res.is_ok());
}

#[test]
#[should_panic(
    expected = "The client protocol version is older than the protocol version of the network"
)]
fn test_node_shutdown_with_old_protocol_version() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let validator_signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    for i in 1..=5 {
        let mut block = env.clients[0].produce_block(i).unwrap().unwrap();
        block.mut_header().get_mut().inner_rest.latest_protocol_version = PROTOCOL_VERSION + 1;
        block.mut_header().resign(&validator_signer);
        env.process_block(0, block, Provenance::NONE);
    }
    for i in 6..=10 {
        env.produce_block(0, i);
    }
    env.produce_block(0, 11);
}

#[test]
fn test_block_ordinal() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    assert_eq!(genesis_block.header().block_ordinal(), 1);
    let mut ordinal = 1;

    // Test no skips
    for i in 1..=5 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        ordinal += 1;
        assert_eq!(block.header().block_ordinal(), ordinal);
    }

    // Test skips
    for i in 1..=5 {
        let block = env.clients[0].produce_block(i * 10).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        ordinal += 1;
        assert_eq!(block.header().block_ordinal(), ordinal);
    }

    // Test forks
    let last_block = env.clients[0].produce_block(99).unwrap().unwrap();
    env.process_block(0, last_block.clone(), Provenance::PRODUCED);
    ordinal += 1;
    assert_eq!(last_block.header().block_ordinal(), ordinal);
    let fork1_block = env.clients[0].produce_block(100).unwrap().unwrap();
    env.clients[0]
        .chain
        .mut_store()
        .save_latest_known(LatestKnown {
            height: last_block.header().height(),
            seen: last_block.header().raw_timestamp(),
        })
        .unwrap();
    let fork2_block = env.clients[0].produce_block(101).unwrap().unwrap();
    assert_eq!(fork1_block.header().prev_hash(), fork2_block.header().prev_hash());
    env.process_block(0, fork1_block.clone(), Provenance::NONE);
    let next_block = env.clients[0].produce_block(102).unwrap().unwrap();
    assert_eq!(next_block.header().prev_hash(), fork1_block.header().hash());
    env.process_block(0, fork2_block.clone(), Provenance::NONE);
    ordinal += 1;
    let fork_ordinal = ordinal - 1;
    assert_eq!(fork1_block.header().block_ordinal(), ordinal);
    assert_eq!(fork2_block.header().block_ordinal(), ordinal);
    assert_eq!(env.clients[0].chain.head().unwrap().height, fork2_block.header().height());
    // Next block on top of fork
    env.process_block(0, next_block.clone(), Provenance::PRODUCED);
    ordinal += 1;
    assert_eq!(env.clients[0].chain.head().unwrap().height, next_block.header().height());
    assert_eq!(next_block.header().block_ordinal(), ordinal);

    // make sure that the old ordinal maps to what is on the canonical chain
    let fork_ordinal_block_hash =
        env.clients[0].chain.mut_store().get_block_hash_from_ordinal(fork_ordinal).unwrap();
    assert_eq!(fork_ordinal_block_hash, *fork1_block.hash());
}

#[test]
fn test_congestion_receipt_execution() {
    let (mut env, tx_hashes) = prepare_env_with_congestion(PROTOCOL_VERSION, None, 3);

    // Produce block with no new chunk.
    env.produce_block(0, 3);
    let height = 4;
    env.produce_block(0, height);
    let prev_block = env.clients[0].chain.get_block_by_height(height).unwrap();
    let chunk_extra =
        env.clients[0].chain.get_chunk_extra(prev_block.hash(), &ShardUId::single_shard()).unwrap();
    assert!(chunk_extra.gas_used() >= chunk_extra.gas_limit());
    let state_update = env.clients[0]
        .runtime_adapter
        .get_tries()
        .new_trie_update(ShardUId::single_shard(), *chunk_extra.state_root());
    let delayed_indices =
        get::<DelayedReceiptIndices>(&state_update, &TrieKey::DelayedReceiptIndices)
            .unwrap()
            .unwrap();
    assert!(delayed_indices.next_available_index > 0);
    let mut block = env.clients[0].produce_block(height + 1).unwrap().unwrap();
    testlib::process_blocks::set_no_chunk_in_block(&mut block, &prev_block);
    env.process_block(0, block.clone(), Provenance::NONE);

    // let all receipts finish
    for i in height + 2..height + 7 {
        env.produce_block(0, i);
    }

    for tx_hash in &tx_hashes {
        let final_outcome = env.clients[0].chain.get_final_transaction_result(tx_hash).unwrap();
        assert_matches!(final_outcome.status, FinalExecutionStatus::SuccessValue(_));

        // Check that all receipt ids have corresponding execution outcomes. This means that all receipts generated are executed.
        let transaction_outcome = env.clients[0].chain.get_execution_outcome(tx_hash).unwrap();
        let mut receipt_ids: VecDeque<_> =
            transaction_outcome.outcome_with_id.outcome.receipt_ids.into();
        while !receipt_ids.is_empty() {
            let receipt_id = receipt_ids.pop_front().unwrap();
            let receipt_outcome = env.clients[0].chain.get_execution_outcome(&receipt_id).unwrap();
            match receipt_outcome.outcome_with_id.outcome.status {
                ExecutionStatus::SuccessValue(_) | ExecutionStatus::SuccessReceiptId(_) => {}
                ExecutionStatus::Failure(_) | ExecutionStatus::Unknown => {
                    panic!("unexpected receipt execution outcome")
                }
            }
            receipt_ids.extend(receipt_outcome.outcome_with_id.outcome.receipt_ids);
        }
    }
}

#[test]
fn test_validator_stake_host_function() {
    init_test_logger();
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let block_height = deploy_test_contract(
        &mut env,
        "test0".parse().unwrap(),
        near_test_contracts::rs_contract(),
        epoch_length,
        1,
    );
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let signed_transaction = SignedTransaction::from_actions(
        10,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "ext_validator_stake".to_string(),
            args: b"test0".to_vec(),
            gas: 100_000_000_000_000,
            deposit: 0,
        })],
        *genesis_block.hash(),
    );
    env.clients[0].process_tx(signed_transaction, false, false);
    for i in 0..3 {
        env.produce_block(0, block_height + i);
    }
}

fn verify_contract_limits_upgrade(
    feature: ProtocolFeature,
    function_limit: u32,
    local_limit: u32,
    expected_prepare_err: PrepareError,
) {
    let old_protocol_version = feature.protocol_version() - 1;
    let new_protocol_version = feature.protocol_version();

    let epoch_length = 5;
    // Prepare TestEnv with a contract at the old protocol version.
    let mut env = {
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = old_protocol_version;
        let chain_genesis = ChainGenesis::from(&genesis);
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> =
            vec![Arc::new(nearcore::NightshadeRuntime::test_with_runtime_config_store(
                Path::new("../../../.."),
                create_test_store(),
                &genesis,
                TrackedConfig::new_empty(),
                RuntimeConfigStore::new(None),
            ))];
        let mut env = TestEnv::builder(chain_genesis).runtime_adapters(runtimes).build();

        deploy_test_contract(
            &mut env,
            "test0".parse().unwrap(),
            &near_test_contracts::LargeContract {
                functions: function_limit + 1,
                locals_per_function: local_limit + 1,
                ..Default::default()
            }
            .make(),
            epoch_length,
            1,
        );
        env
    };

    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let tx = Transaction {
        signer_id: "test0".parse().unwrap(),
        receiver_id: "test0".parse().unwrap(),
        public_key: signer.public_key(),
        actions: vec![Action::FunctionCall(FunctionCallAction {
            method_name: "main".to_string(),
            args: Vec::new(),
            gas: 100_000_000_000_000,
            deposit: 0,
        })],

        nonce: 0,
        block_hash: CryptoHash::default(),
    };

    // Run the transaction & get tx outcome.
    let old_outcome = {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_transaction =
            Transaction { nonce: 10, block_hash: tip.last_block_hash, ..tx.clone() }.sign(&signer);
        let tx_hash = signed_transaction.get_hash();
        env.clients[0].process_tx(signed_transaction, false, false);
        for i in 0..3 {
            env.produce_block(0, tip.height + i + 1);
        }
        env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap()
    };

    // Move to the new protocol version.
    {
        let tip = env.clients[0].chain.head().unwrap();
        let mut last_block_hash = tip.last_block_hash;
        for i in 0..2 * epoch_length {
            let height = tip.height + i + 1;
            let mut block = env.clients[0].produce_block(height).unwrap().unwrap();

            let epoch_id = env.clients[0]
                .runtime_adapter
                .get_epoch_id_from_prev_block(&last_block_hash)
                .unwrap();
            let block_producer =
                env.clients[0].runtime_adapter.get_block_producer(&epoch_id, height).unwrap();
            set_block_protocol_version(&mut block, block_producer, new_protocol_version);

            last_block_hash = *block.header().hash();
            env.process_block(0, block, Provenance::PRODUCED);
        }
    }

    // Re-run the transaction & get tx outcome.
    let new_outcome = {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_transaction =
            Transaction { nonce: 11, block_hash: tip.last_block_hash, ..tx }.sign(&signer);
        let tx_hash = signed_transaction.get_hash();
        env.clients[0].process_tx(signed_transaction, false, false);
        for i in 0..3 {
            env.produce_block(0, tip.height + i + 1);
        }
        env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap()
    };

    assert_matches!(old_outcome.status, FinalExecutionStatus::SuccessValue(_));
    let e = match new_outcome.status {
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(e)) => e,
        status => panic!("expected transaction to fail, got {:?}", status),
    };
    match e.kind {
        ActionErrorKind::FunctionCallError(FunctionCallErrorSer::CompilationError(
            CompilationError::PrepareError(e),
        )) if e == expected_prepare_err => (),
        kind => panic!("got unexpected action error kind: {:?}", kind),
    }
}

// Check that we can't call a contract exceeding functions number limit after upgrade.
#[test]
fn test_function_limit_change() {
    verify_contract_limits_upgrade(
        ProtocolFeature::LimitContractFunctionsNumber,
        100_000,
        0,
        PrepareError::TooManyFunctions,
    );
}

// Check that we can't call a contract exceeding functions number limit after upgrade.
#[test]
fn test_local_limit_change() {
    verify_contract_limits_upgrade(
        ProtocolFeature::LimitContractLocals,
        64,
        15625,
        PrepareError::TooManyLocals,
    );
}

#[test]
/// Test that if a node's shard assignment will not change in the next epoch, the node
/// does not need to catch up.
fn test_catchup_no_sharding_change() {
    init_integration_logger();
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    genesis.config.epoch_length = 5;
    let chain_genesis = ChainGenesis::from(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .clients_count(1)
        .validator_seats(1)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    // run the chain to a few epochs and make sure no catch up is triggered and the chain still
    // functions
    for h in 1..20 {
        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        let (_, res) = env.clients[0].process_block(block.clone().into(), Provenance::PRODUCED);
        res.unwrap();
        assert_eq!(env.clients[0].chain.store().iterate_state_sync_infos(), vec![]);
        assert_eq!(
            env.clients[0].chain.store().get_blocks_to_catchup(block.header().prev_hash()).unwrap(),
            vec![]
        );
    }
}

/// Tests if the cost of deployment is higher after the protocol update 53
#[test]
fn test_deploy_cost_increased() {
    let new_protocol_version = ProtocolFeature::IncreaseDeploymentCost.protocol_version();
    let old_protocol_version = new_protocol_version - 1;

    let contract_size = 1024 * 1024;
    let test_contract = near_test_contracts::sized_contract(contract_size);

    // Prepare TestEnv with a contract at the old protocol version.
    let epoch_length = 5;
    let mut env = {
        let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = old_protocol_version;
        let chain_genesis = ChainGenesis::from(&genesis);
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> =
            vec![Arc::new(nearcore::NightshadeRuntime::test_with_runtime_config_store(
                Path::new("../../../.."),
                create_test_store(),
                &genesis,
                TrackedConfig::new_empty(),
                RuntimeConfigStore::new(None),
            ))];
        TestEnv::builder(chain_genesis).runtime_adapters(runtimes).build()
    };

    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let tx = Transaction {
        signer_id: "test0".parse().unwrap(),
        receiver_id: "test0".parse().unwrap(),
        public_key: signer.public_key(),
        actions: vec![Action::DeployContract(DeployContractAction { code: test_contract })],
        nonce: 0,
        block_hash: CryptoHash::default(),
    };

    // Run the transaction & get tx outcome in a closure.
    let deploy_contract = |env: &mut TestEnv, nonce: u64| {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_transaction =
            Transaction { nonce, block_hash: tip.last_block_hash, ..tx.clone() }.sign(&signer);
        let tx_hash = signed_transaction.get_hash();
        env.clients[0].process_tx(signed_transaction, false, false);
        for i in 0..epoch_length {
            env.produce_block(0, tip.height + i + 1);
        }
        env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap()
    };

    let old_outcome = deploy_contract(&mut env, 10);

    // Move to the new protocol version.
    {
        let tip = env.clients[0].chain.head().unwrap();
        let mut last_block_hash = tip.last_block_hash;
        for i in 0..2 * epoch_length {
            let height = tip.height + i + 1;
            let mut block = env.clients[0].produce_block(height).unwrap().unwrap();

            let epoch_id = env.clients[0]
                .runtime_adapter
                .get_epoch_id_from_prev_block(&last_block_hash)
                .unwrap();
            let block_producer =
                env.clients[0].runtime_adapter.get_block_producer(&epoch_id, height).unwrap();
            set_block_protocol_version(&mut block, block_producer, new_protocol_version);

            last_block_hash = *block.header().hash();
            env.process_block(0, block, Provenance::PRODUCED);
        }
    }

    let new_outcome = deploy_contract(&mut env, 11);

    assert_matches!(old_outcome.status, FinalExecutionStatus::SuccessValue(_));
    assert_matches!(new_outcome.status, FinalExecutionStatus::SuccessValue(_));

    let old_deploy_gas = old_outcome.receipts_outcome[0].outcome.gas_burnt;
    let new_deploy_gas = new_outcome.receipts_outcome[0].outcome.gas_burnt;
    assert!(new_deploy_gas > old_deploy_gas);
    assert_eq!(new_deploy_gas - old_deploy_gas, contract_size as u64 * (64_572_944 - 6_812_999));
}

mod access_key_nonce_range_tests {
    use super::*;
    use near_chain::chain::NUM_ORPHAN_ANCESTORS_CHECK;
    use near_client::test_utils::create_chunk_with_transactions;
    use near_network::types::PeerManagerAdapter;
    use near_primitives::account::AccessKey;
    use near_primitives::shard_layout::ShardLayout;
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    /// Test that duplicate transactions are properly rejected.
    #[test]
    fn test_transaction_hash_collision() {
        let epoch_length = 5;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        let mut env = TestEnv::builder(ChainGenesis::test())
            .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
            .build();
        let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

        let signer0 =
            InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        let signer1 =
            InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let send_money_tx = SignedTransaction::send_money(
            1,
            "test1".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer1,
            100,
            *genesis_block.hash(),
        );
        let delete_account_tx = SignedTransaction::delete_account(
            2,
            "test1".parse().unwrap(),
            "test1".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer1,
            *genesis_block.hash(),
        );

        env.clients[0].process_tx(send_money_tx.clone(), false, false);
        env.clients[0].process_tx(delete_account_tx, false, false);

        for i in 1..4 {
            env.produce_block(0, i);
        }

        let create_account_tx = SignedTransaction::create_account(
            1,
            "test0".parse().unwrap(),
            "test1".parse().unwrap(),
            NEAR_BASE,
            signer1.public_key(),
            &signer0,
            *genesis_block.hash(),
        );
        let res = env.clients[0].process_tx(create_account_tx, false, false);
        assert_matches!(res, NetworkClientResponses::ValidTx);
        for i in 4..8 {
            env.produce_block(0, i);
        }

        let res = env.clients[0].process_tx(send_money_tx, false, false);
        assert_matches!(res, NetworkClientResponses::InvalidTx(_));
    }

    /// Helper for checking that duplicate transactions from implicit accounts are properly rejected.
    /// It creates implicit account, deletes it and creates again, so that nonce of the access
    /// key is updated. Then it tries to send tx from implicit account with invalid nonce, which
    /// should fail since the protocol upgrade.
    fn get_status_of_tx_hash_collision_for_implicit_account(
        protocol_version: ProtocolVersion,
    ) -> NetworkClientResponses {
        let epoch_length = 100;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = protocol_version;
        let mut env = TestEnv::builder(ChainGenesis::test())
            .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
            .build();
        let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

        let signer1 =
            InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");

        let public_key = signer1.public_key.clone();
        let raw_public_key = public_key.unwrap_as_ed25519().0.to_vec();
        let implicit_account_id = AccountId::try_from(hex::encode(&raw_public_key)).unwrap();
        let implicit_account_signer = InMemorySigner::from_secret_key(
            implicit_account_id.clone(),
            signer1.secret_key.clone(),
        );
        let deposit_for_account_creation = 10u128.pow(23);
        let mut height = 1;
        let blocks_number = 5;

        // Send money to implicit account, invoking its creation.
        let send_money_tx = SignedTransaction::send_money(
            1,
            "test1".parse().unwrap(),
            implicit_account_id.clone(),
            &signer1,
            deposit_for_account_creation,
            *genesis_block.hash(),
        );
        height = check_tx_processing(&mut env, send_money_tx, height, blocks_number);
        let block = env.clients[0].chain.get_block_by_height(height - 1).unwrap();

        // Delete implicit account.
        let delete_account_tx = SignedTransaction::delete_account(
            // Because AccessKeyNonceRange is enabled, correctness of this nonce is guaranteed.
            (height - 1) * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER,
            implicit_account_id.clone(),
            implicit_account_id.clone(),
            "test0".parse().unwrap(),
            &implicit_account_signer,
            *block.hash(),
        );
        height = check_tx_processing(&mut env, delete_account_tx, height, blocks_number);
        let block = env.clients[0].chain.get_block_by_height(height - 1).unwrap();

        // Send money to implicit account again, invoking its second creation.
        let send_money_again_tx = SignedTransaction::send_money(
            2,
            "test1".parse().unwrap(),
            implicit_account_id.clone(),
            &signer1,
            deposit_for_account_creation,
            *block.hash(),
        );
        height = check_tx_processing(&mut env, send_money_again_tx, height, blocks_number);
        let block = env.clients[0].chain.get_block_by_height(height - 1).unwrap();

        // Send money from implicit account with incorrect nonce.
        let send_money_from_implicit_account_tx = SignedTransaction::send_money(
            1,
            implicit_account_id.clone(),
            "test0".parse().unwrap(),
            &implicit_account_signer,
            100,
            *block.hash(),
        );
        let status = env.clients[0].process_tx(send_money_from_implicit_account_tx, false, false);

        // Check that sending money from implicit account with correct nonce is still valid.
        let send_money_from_implicit_account_tx = SignedTransaction::send_money(
            (height - 1) * AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER,
            implicit_account_id,
            "test0".parse().unwrap(),
            &implicit_account_signer,
            100,
            *block.hash(),
        );
        check_tx_processing(&mut env, send_money_from_implicit_account_tx, height, blocks_number);

        status
    }

    /// Test that duplicate transactions from implicit accounts are properly rejected.
    #[test]
    fn test_transaction_hash_collision_for_implicit_account_fail() {
        let protocol_version =
            ProtocolFeature::AccessKeyNonceForImplicitAccounts.protocol_version();
        assert_matches!(
            get_status_of_tx_hash_collision_for_implicit_account(protocol_version),
            NetworkClientResponses::InvalidTx(InvalidTxError::InvalidNonce { .. })
        );
    }

    /// Test that duplicate transactions from implicit accounts are not rejected until protocol upgrade.
    #[test]
    fn test_transaction_hash_collision_for_implicit_account_ok() {
        let protocol_version =
            ProtocolFeature::AccessKeyNonceForImplicitAccounts.protocol_version() - 1;
        assert_matches!(
            get_status_of_tx_hash_collision_for_implicit_account(protocol_version),
            NetworkClientResponses::ValidTx
        );
    }

    /// Test that chunks with transactions that have expired are considered invalid.
    #[test]
    fn test_chunk_transaction_validity() {
        let epoch_length = 5;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        let mut env = TestEnv::builder(ChainGenesis::test())
            .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
            .build();
        let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
        let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        let tx = SignedTransaction::send_money(
            1,
            "test1".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            100,
            *genesis_block.hash(),
        );
        for i in 1..200 {
            env.produce_block(0, i);
        }
        let (encoded_shard_chunk, merkle_path, receipts, block) =
            create_chunk_with_transactions(&mut env.clients[0], vec![tx]);
        let mut chain_store = ChainStore::new(
            env.clients[0].chain.store().store().clone(),
            genesis_block.header().height(),
            true,
        );
        env.clients[0]
            .shards_mgr
            .distribute_encoded_chunk(encoded_shard_chunk, merkle_path, receipts, &mut chain_store)
            .unwrap();
        let (_, res) = env.clients[0].process_block(block.into(), Provenance::NONE);
        assert_matches!(res.unwrap_err(), Error::InvalidTransactions);
    }

    #[test]
    fn test_transaction_nonce_too_large() {
        let epoch_length = 5;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        let mut env = TestEnv::builder(ChainGenesis::test())
            .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
            .build();
        let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
        let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        let large_nonce = AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER + 1;
        let tx = SignedTransaction::send_money(
            large_nonce,
            "test1".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            100,
            *genesis_block.hash(),
        );
        let res = env.clients[0].process_tx(tx, false, false);
        assert_matches!(
            res,
            NetworkClientResponses::InvalidTx(InvalidTxError::InvalidAccessKeyError(_))
        );
    }

    #[test]
    /// This test tests the logic regarding requesting chunks for orphan.
    /// The test tests the following scenario, there is one validator(test0) and one non-validator node(test1)
    /// test0 produces and processes 20 blocks and test1 processes these blocks with some delays. We
    /// want to test that test1 requests missing chunks for orphans ahead of time.
    /// Note: this test assumes NUM_ORPHAN_ANCESTORS_CHECK <= 5 and >= 2
    ///
    /// - test1 processes blocks 1, 2 successfully
    /// - test1 processes blocks 3, 4, ..., 20, but it doesn't have chunks for these blocks, so block 3
    ///         will be put to the missing chunks pool while block 4 - 20 will be orphaned
    /// - check that test1 sends missing chunk requests for block 4 - 2 + NUM_ORPHAN_ANCESTORS_CHECK
    /// - test1 processes partial chunk responses for block 4 - 2 + NUM_ORPHAN_ANCESTORS_CHECK
    /// - test1 processes partial chunk responses for block 3
    /// - check that block 3 - 2 + NUM_ORPHAN_ANCESTORS_CHECK are accepted, this confirms that the missing chunk requests are sent
    ///   and processed successfully for block 4 - 2 + NUM_ORPHAN_ANCESTORS_CHECK
    /// - process until block 8 and check that the node sends missing chunk requests for the new orphans
    ///   add unlocked
    /// - check that test1 does not send missing chunk requests for block 10, because it breaks
    ///   the requirement that the block must be in the same epoch as the next block after its accepted ancestor
    /// - test1 processes partial chunk responses for block 8 and 9
    /// - check that test1 sends missing chunk requests for block 11 to 10+NUM_ORPHAN_ANCESTORS+CHECK,
    ///   since now they satisfy the the requirements for requesting chunks for orphans
    /// - process the rest of blocks
    fn test_request_chunks_for_orphan() {
        init_test_logger();

        // Skip the test if NUM_ORPHAN_ANCESTORS_CHECK is 1, which effectively disables
        // fetching chunks for orphan
        if NUM_ORPHAN_ANCESTORS_CHECK == 1 {
            return;
        }

        let num_clients = 2;
        let num_validators = 1;
        let epoch_length = 10;

        let accounts: Vec<AccountId> =
            (0..num_clients).map(|i| format!("test{}", i).parse().unwrap()).collect();
        let mut genesis = Genesis::test(accounts, num_validators);
        genesis.config.epoch_length = epoch_length;
        // make the blockchain to 4 shards
        genesis.config.shard_layout = ShardLayout::v1_test();
        genesis.config.num_block_producer_seats_per_shard =
            vec![num_validators, num_validators, num_validators, num_validators];
        let chain_genesis = ChainGenesis::from(&genesis);
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> = (0..2)
            .map(|_| {
                Arc::new(nearcore::NightshadeRuntime::test_with_runtime_config_store(
                    Path::new("."),
                    create_test_store(),
                    &genesis,
                    TrackedConfig::AllShards,
                    RuntimeConfigStore::test(),
                )) as Arc<dyn RuntimeAdapter>
            })
            .collect();
        let mut env = TestEnv::builder(chain_genesis)
            .clients_count(num_clients)
            .validator_seats(num_validators as usize)
            .runtime_adapters(runtimes)
            .build();

        let mut blocks = vec![];
        // produce 20 blocks
        for i in 1..=20 {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block, Provenance::PRODUCED);
        }

        env.clients[1].process_block(blocks[0].clone().into(), Provenance::NONE).1.unwrap();
        // process blocks 1, 2 successfully
        for i in 1..3 {
            let (_, res) = env.clients[1].process_block(blocks[i].clone().into(), Provenance::NONE);
            run_catchup(&mut env.clients[1], &vec![]).unwrap();
            assert_matches!(res, Err(e) => {
                assert_matches!(e, near_chain::Error::ChunksMissing(_));
            });
            env.process_partial_encoded_chunks_requests(1);
        }

        // process blocks 3 to 15 without processing missing chunks
        // block 3 will be put into the blocks_with_missing_chunks pool
        let (_, res) = env.clients[1].process_block(blocks[3].clone().into(), Provenance::NONE);
        assert_matches!(res, Err(e) => {
            assert_matches!(e, near_chain::Error::ChunksMissing(_));
        });
        // remove the missing chunk request from the network queue because we want to process it later
        let missing_chunk_request = env.network_adapters[1].pop().unwrap();
        // block 4-20 will be put to the orphan pool
        for i in 4..20 {
            let (_, res) = env.clients[1].process_block(blocks[i].clone().into(), Provenance::NONE);
            assert_matches!(res, Err(e) => {
                assert_matches!(e, near_chain::Error::Orphan);
            });
        }
        // check that block 4-2+NUM_ORPHAN_ANCESTORS_CHECK requested partial encoded chunks already
        for i in 4..3 + NUM_ORPHAN_ANCESTORS_CHECK {
            assert!(
                env.clients[1]
                    .chain
                    .check_orphan_partial_chunks_requested(blocks[i as usize].hash()),
                "{}",
                i
            );
        }
        assert!(!env.clients[1].chain.check_orphan_partial_chunks_requested(
            blocks[3 + NUM_ORPHAN_ANCESTORS_CHECK as usize].hash()
        ));
        assert!(!env.clients[1].chain.check_orphan_partial_chunks_requested(
            blocks[4 + NUM_ORPHAN_ANCESTORS_CHECK as usize].hash()
        ));
        // process all the partial encoded chunk requests for block 4 - 2 + NUM_ORPHAN_ANCESTORS_CHECK
        env.process_partial_encoded_chunks_requests(1);

        // process partial encoded chunk request for block 3, which will unlock block 4 - 2 + NUM_ORPHAN_ANCESTORS_CHECK
        env.process_partial_encoded_chunk_request(1, missing_chunk_request);
        assert_eq!(
            &env.clients[1].chain.head().unwrap().last_block_hash,
            blocks[2 + NUM_ORPHAN_ANCESTORS_CHECK as usize].hash()
        );

        // check that `check_orphans` will request PartialChunks for new orphans as new blocks are processed
        // keep processing the partial encoded chunk requests in the queue, which will process
        // block 3+NUM_ORPHAN_ANCESTORS to 8.
        for i in 4 + NUM_ORPHAN_ANCESTORS_CHECK..10 {
            assert!(env.clients[1]
                .chain
                .check_orphan_partial_chunks_requested(blocks[i as usize].hash()));
            for _ in 0..4 {
                let request = env.network_adapters[1].pop().unwrap();
                env.process_partial_encoded_chunk_request(1, request);
            }
        }
        assert_eq!(&env.clients[1].chain.head().unwrap().last_block_hash, blocks[8].hash());
        // blocks[10] is at the new epoch, so we can't request partial chunks for it yet
        assert!(!env.clients[1].chain.check_orphan_partial_chunks_requested(blocks[10].hash()));

        // process missing chunks for block 9, which has 4 chunks, so there are 4 requests in total
        for _ in 0..4 {
            let request = env.network_adapters[1].pop().unwrap();
            env.process_partial_encoded_chunk_request(1, request);
        }
        assert_eq!(&env.clients[1].chain.head().unwrap().last_block_hash, blocks[9].hash());

        for i in 11..10 + NUM_ORPHAN_ANCESTORS_CHECK {
            assert!(env.clients[1]
                .chain
                .check_orphan_partial_chunks_requested(blocks[i as usize].hash()));
        }

        // process the rest of blocks
        for i in 10..20 {
            // process missing chunk requests for the 4 chunks in each block
            for _ in 0..4 {
                let request = env.network_adapters[1].pop().unwrap();
                env.process_partial_encoded_chunk_request(1, request);
            }
            assert_eq!(&env.clients[1].chain.head().unwrap().last_block_hash, blocks[i].hash());
        }
    }

    /// This test tests that if a node's requests for chunks are eventually answered,
    /// it can process blocks, which also means chunks and parts and processed correctly.
    /// It can be seen as a sanity test for the logic in processing chunks,
    /// while abstracting away the logic for requesting chunks by assuming chunks requests are
    /// always answered (it does test for delayed response).
    ///
    /// This test tests the following scenario: there is one validator(test0) and one non-validator node(test1)
    /// test0 produces and processes 21 blocks and test1 processes these blocks.
    /// test1 processes the blocks in some random order, to simulate in production, a node may not
    /// receive blocks in order. All of test1's requests for chunks are eventually answered, but
    /// with some delays. In the end, we check that test1 processes all 21 blocks, and it only
    /// requests for each chunk once
    #[test]
    fn test_processing_chunks_sanity() {
        init_test_logger();

        let num_clients = 2;
        let num_validators = 1;
        let epoch_length = 10;

        let accounts: Vec<AccountId> =
            (0..num_clients).map(|i| format!("test{}", i).parse().unwrap()).collect();
        let mut genesis = Genesis::test(accounts, num_validators);
        genesis.config.epoch_length = epoch_length;
        // make the blockchain to 4 shards
        genesis.config.shard_layout = ShardLayout::v1_test();
        genesis.config.num_block_producer_seats_per_shard =
            vec![num_validators, num_validators, num_validators, num_validators];
        let chain_genesis = ChainGenesis::from(&genesis);
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> = (0..2)
            .map(|_| {
                Arc::new(nearcore::NightshadeRuntime::test_with_runtime_config_store(
                    Path::new("."),
                    create_test_store(),
                    &genesis,
                    TrackedConfig::AllShards,
                    RuntimeConfigStore::test(),
                )) as Arc<dyn RuntimeAdapter>
            })
            .collect();
        let mut env = TestEnv::builder(chain_genesis)
            .clients_count(num_clients)
            .validator_seats(num_validators as usize)
            .runtime_adapters(runtimes)
            .build();

        let mut blocks = vec![];
        // produce 21 blocks
        for i in 1..=21 {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block, Provenance::PRODUCED);
        }

        // make test1 process these blocks, while grouping blocks to groups of three
        // and process blocks in each group in a random order.
        // Verify that it can process the blocks successfully if all its requests for missing
        // chunks are answered
        let mut rng = thread_rng();
        let mut num_requests = 0;
        for i in 0..=6 {
            let mut next_blocks: Vec<_> = (3 * i..3 * i + 3).collect();
            next_blocks.shuffle(&mut rng);
            for ind in next_blocks {
                let (_, _) =
                    env.clients[1].process_block(blocks[ind].clone().into(), Provenance::NONE);
                run_catchup(&mut env.clients[1], &vec![]).unwrap();
                while let Some(request) = env.network_adapters[1].pop() {
                    // process the chunk request some times, otherwise keep it in the queue
                    // this is to simulate delays in the network
                    if rng.gen_bool(0.7) {
                        env.process_partial_encoded_chunk_request(1, request);
                        num_requests += 1;
                    } else {
                        env.network_adapters[1].do_send(request);
                    }
                }
            }
        }
        // process the remaining chunk requests
        while let Some(request) = env.network_adapters[1].pop() {
            env.process_partial_encoded_chunk_request(1, request);
            num_requests += 1;
        }

        assert_eq!(env.clients[1].chain.head().unwrap().height, 21);
        // Check each chunk is only requested once.
        // There are 21 blocks in total, but the first block has no chunks,
        assert_eq!(num_requests, 4 * 20);
    }
}

mod protocol_feature_restore_receipts_after_fix_tests {
    use super::*;
    use near_primitives::runtime::migration_data::MigrationData;
    use near_primitives::version::ProtocolFeature;
    use nearcore::migrations::load_migration_data;

    const EPOCH_LENGTH: u64 = 5;
    const HEIGHT_TIMEOUT: u64 = 10;

    fn run_test(
        chain_id: &str,
        low_height_with_no_chunk: BlockHeight,
        high_height_with_no_chunk: BlockHeight,
        should_be_restored: bool,
    ) {
        init_test_logger();

        let protocol_version =
            ProtocolFeature::RestoreReceiptsAfterFixApplyChunks.protocol_version() - 1;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.chain_id = String::from(chain_id);
        genesis.config.epoch_length = EPOCH_LENGTH;
        genesis.config.protocol_version = protocol_version;
        let chain_genesis = ChainGenesis::from(&genesis);
        let runtime = nearcore::NightshadeRuntime::test(
            Path::new("../../../.."),
            create_test_store(),
            &genesis,
        );
        // TODO #4305: get directly from NightshadeRuntime
        let migration_data = load_migration_data(&genesis.config.chain_id);

        let mut env = TestEnv::builder(chain_genesis)
            .runtime_adapters(vec![Arc::new(runtime) as Arc<dyn RuntimeAdapter>])
            .build();

        let get_restored_receipt_hashes = |migration_data: &MigrationData| -> HashSet<CryptoHash> {
            HashSet::from_iter(
                migration_data
                    .restored_receipts
                    .get(&0u64)
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .map(|receipt| receipt.receipt_id),
            )
        };

        let mut receipt_hashes_to_restore = get_restored_receipt_hashes(&migration_data);
        let mut height: BlockHeight = 1;
        let mut last_update_height: BlockHeight = 0;

        // Simulate several blocks to guarantee that they are produced successfully.
        // Stop block production if all receipts were restored. Or, if some receipts are still not
        // applied, upgrade already happened, and no new receipt was applied in some last blocks,
        // consider the process stuck to avoid any possibility of infinite loop.
        while height < 15
            || (!receipt_hashes_to_restore.is_empty()
                && height - last_update_height < HEIGHT_TIMEOUT)
        {
            let mut block = env.clients[0].produce_block(height).unwrap().unwrap();
            if low_height_with_no_chunk <= height && height < high_height_with_no_chunk {
                let prev_block =
                    env.clients[0].chain.get_block_by_height(height - 1).unwrap().clone();
                testlib::process_blocks::set_no_chunk_in_block(&mut block, &prev_block);
            }
            set_block_protocol_version(
                &mut block,
                "test0".parse().unwrap(),
                ProtocolFeature::RestoreReceiptsAfterFixApplyChunks.protocol_version(),
            );

            env.process_block(0, block, Provenance::PRODUCED);

            let last_block = env.clients[0].chain.get_block_by_height(height).unwrap().clone();
            let protocol_version = env.clients[0]
                .runtime_adapter
                .get_epoch_protocol_version(last_block.header().epoch_id())
                .unwrap();

            for receipt_id in receipt_hashes_to_restore.clone().iter() {
                if env.clients[0].chain.get_execution_outcome(receipt_id).is_ok() {
                    assert!(
                        protocol_version
                            >= ProtocolFeature::RestoreReceiptsAfterFixApplyChunks
                                .protocol_version(),
                        "Restored receipt {} was executed before protocol upgrade",
                        receipt_id
                    );
                    receipt_hashes_to_restore.remove(receipt_id);
                    last_update_height = height;
                };
            }

            // Update last updated height anyway if upgrade did not happen
            if protocol_version
                < ProtocolFeature::RestoreReceiptsAfterFixApplyChunks.protocol_version()
            {
                last_update_height = height;
            }
            height += 1;
        }

        if should_be_restored {
            assert!(
                receipt_hashes_to_restore.is_empty(),
                "Some of receipts were not executed, hashes: {:?}",
                receipt_hashes_to_restore
            );
        } else {
            assert_eq!(
            receipt_hashes_to_restore,
            get_restored_receipt_hashes(&migration_data),
            "If accidentally there are no chunks in first epoch with new protocol version, receipts should not be introduced"
        );
        }
    }

    #[test]
    fn test_no_chunks_missing() {
        // If there are no chunks missing, all receipts should be applied
        run_test("mainnet", 1, 0, true);
    }

    #[test]
    fn test_first_chunk_in_epoch_missing() {
        // If the first chunk in the first epoch with needed protocol version is missing,
        // all receipts should still be applied
        run_test("mainnet", 8, 12, true);
    }

    #[test]
    fn test_all_chunks_in_epoch_missing() {
        // If all chunks are missing in the first epoch, no receipts should be applied
        run_test("mainnet", 11, 11 + EPOCH_LENGTH, false);
    }

    #[test]
    fn test_run_for_testnet() {
        // Run the same process for chain other than mainnet to ensure that blocks are produced
        // successfully during the protocol upgrade.
        run_test("testnet", 1, 0, true);
    }
}

mod storage_usage_fix_tests {
    use super::*;
    use borsh::BorshDeserialize;
    use near_primitives::types::AccountId;
    use near_primitives::version::ProtocolFeature;
    use near_store::TrieUpdate;
    use std::rc::Rc;

    fn process_blocks_with_storage_usage_fix(
        chain_id: String,
        check_storage_usage: fn(AccountId, u64, u64),
    ) {
        let epoch_length = 5;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.chain_id = chain_id;
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = ProtocolFeature::FixStorageUsage.protocol_version() - 1;
        let chain_genesis = ChainGenesis::from(&genesis);
        let mut env = TestEnv::builder(chain_genesis)
            .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
            .build();
        for i in 1..=16 {
            // We cannot just use TestEnv::produce_block as we are updating protocol version
            let mut block = env.clients[0].produce_block(i).unwrap().unwrap();
            set_block_protocol_version(
                &mut block,
                "test0".parse().unwrap(),
                ProtocolFeature::FixStorageUsage.protocol_version(),
            );

            let (_, res) = env.clients[0].process_block(block.clone().into(), Provenance::NONE);
            assert!(res.is_ok());
            run_catchup(&mut env.clients[0], &vec![]).unwrap();

            let root = *env.clients[0]
                .chain
                .get_chunk_extra(block.hash(), &ShardUId::single_shard())
                .unwrap()
                .state_root();
            let trie = Rc::new(
                env.clients[0]
                    .runtime_adapter
                    .get_trie_for_shard(0, block.header().prev_hash())
                    .unwrap(),
            );
            let state_update = TrieUpdate::new(trie.clone(), root);
            use near_primitives::account::Account;
            let mut account_test1_raw = state_update
                .get(&TrieKey::Account { account_id: "test1".parse().unwrap() })
                .unwrap()
                .unwrap()
                .clone();
            let account_test1 = Account::try_from_slice(&mut account_test1_raw).unwrap();
            let mut account_test0_raw = state_update
                .get(&TrieKey::Account { account_id: "test0".parse().unwrap() })
                .unwrap()
                .unwrap()
                .clone();
            let account_test0 = Account::try_from_slice(&mut account_test0_raw).unwrap();
            check_storage_usage("test1".parse().unwrap(), i, account_test1.storage_usage());
            check_storage_usage("test0".parse().unwrap(), i, account_test0.storage_usage());
        }
    }

    #[test]
    fn test_fix_storage_usage_migration() {
        init_test_logger();
        process_blocks_with_storage_usage_fix(
            "mainnet".to_string(),
            |account_id: AccountId, block_height: u64, storage_usage: u64| {
                if account_id.as_ref() == "test0" || account_id.as_ref() == "test1" {
                    assert_eq!(storage_usage, 182);
                } else if block_height >= 11 {
                    assert_eq!(storage_usage, 4560);
                } else {
                    assert_eq!(storage_usage, 364);
                }
            },
        );
        process_blocks_with_storage_usage_fix(
            "testnet".to_string(),
            |account_id: AccountId, _: u64, storage_usage: u64| {
                if account_id.as_ref() == "test0" || account_id.as_ref() == "test1" {
                    assert_eq!(storage_usage, 182);
                } else {
                    assert_eq!(storage_usage, 364);
                }
            },
        );
    }
}

#[cfg(test)]
mod cap_max_gas_price_tests {
    use super::*;
    use near_primitives::version::ProtocolFeature;

    fn does_gas_price_exceed_limit(protocol_version: ProtocolVersion) -> bool {
        let mut env =
            prepare_env_with_congestion(protocol_version, Some(Rational::new_raw(2, 1)), 7).0;
        let mut was_congested = false;
        let mut price_exceeded_limit = false;

        for i in 3..20 {
            env.produce_block(0, i);
            let block = env.clients[0].chain.get_block_by_height(i).unwrap().clone();
            let protocol_version = env.clients[0]
                .runtime_adapter
                .get_epoch_protocol_version(block.header().epoch_id())
                .unwrap();
            let min_gas_price =
                env.clients[0].chain.block_economics_config.min_gas_price(protocol_version);
            was_congested |= block.chunks()[0].gas_used() >= block.chunks()[0].gas_limit();
            price_exceeded_limit |= block.header().gas_price() > 20 * min_gas_price;
        }

        assert!(was_congested);
        price_exceeded_limit
    }

    #[test]
    fn test_not_capped_gas_price() {
        assert!(does_gas_price_exceed_limit(
            ProtocolFeature::CapMaxGasPrice.protocol_version() - 1
        ));
    }

    #[test]
    fn test_capped_gas_price() {
        assert!(!does_gas_price_exceed_limit(ProtocolFeature::CapMaxGasPrice.protocol_version()));
    }
}

mod contract_precompilation_tests {
    use super::*;
    use near_primitives::contract::ContractCode;
    use near_primitives::test_utils::MockEpochInfoProvider;
    use near_primitives::types::CompiledContractCache;
    use near_primitives::views::ViewApplyState;
    use near_store::{Store, StoreCompiledContractCache, TrieUpdate};
    use near_vm_runner::get_contract_cache_key;
    use near_vm_runner::internal::VMKind;
    use node_runtime::state_viewer::TrieViewer;
    use std::rc::Rc;

    const EPOCH_LENGTH: u64 = 5;

    fn state_sync_on_height(env: &mut TestEnv, height: BlockHeight) {
        let sync_block = env.clients[0].chain.get_block_by_height(height).unwrap();
        let sync_hash = *sync_block.hash();
        let chunk_extra =
            env.clients[0].chain.get_chunk_extra(&sync_hash, &ShardUId::single_shard()).unwrap();
        let epoch_id =
            env.clients[0].chain.get_block_header(&sync_hash).unwrap().epoch_id().clone();
        let state_part = env.clients[0]
            .runtime_adapter
            .obtain_state_part(0, &sync_hash, chunk_extra.state_root(), PartId::new(0, 1))
            .unwrap();
        env.clients[1]
            .runtime_adapter
            .apply_state_part(
                0,
                chunk_extra.state_root(),
                PartId::new(0, 1),
                &state_part,
                &epoch_id,
            )
            .unwrap();
    }

    #[test]
    fn test_sync_and_call_cached_contract() {
        let num_clients = 2;
        let stores: Vec<Store> = (0..num_clients).map(|_| create_test_store()).collect();
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = EPOCH_LENGTH;
        let runtime_adapters = stores
            .iter()
            .map(|store| {
                Arc::new(nearcore::NightshadeRuntime::test(
                    Path::new("../../../.."),
                    store.clone(),
                    &genesis,
                )) as Arc<dyn RuntimeAdapter>
            })
            .collect();

        let mut env = TestEnv::builder(ChainGenesis::test())
            .clients_count(num_clients)
            .runtime_adapters(runtime_adapters)
            .build();
        let start_height = 1;

        // Process test contract deployment on the first client.
        let wasm_code = near_test_contracts::rs_contract().to_vec();
        let height = deploy_test_contract(
            &mut env,
            "test0".parse().unwrap(),
            &wasm_code,
            EPOCH_LENGTH,
            start_height,
        );

        // Perform state sync for the second client.
        state_sync_on_height(&mut env, height - 1);

        // Check existence of contract in both caches.
        let caches: Vec<Arc<StoreCompiledContractCache>> = stores
            .iter()
            .map(|s| Arc::new(StoreCompiledContractCache { store: s.clone() }))
            .collect();
        let contract_code = ContractCode::new(wasm_code.clone(), None);
        let vm_kind = VMKind::for_protocol_version(PROTOCOL_VERSION);
        let epoch_id = env.clients[0]
            .chain
            .get_block_by_height(height - 1)
            .unwrap()
            .header()
            .epoch_id()
            .clone();
        let runtime_config = env.get_runtime_config(0, epoch_id);
        let key = get_contract_cache_key(&contract_code, vm_kind, &runtime_config.wasm_config);
        for i in 0..num_clients {
            caches[i]
                .get(&key.0)
                .unwrap_or_else(|_| panic!("Failed to get cached result for client {}", i))
                .unwrap_or_else(|| {
                    panic!("Compilation result should be non-empty for client {}", i)
                });
        }

        // Check that contract function may be successfully called on the second client.
        // Note that we can't test that behaviour is the same on two clients, because
        // compile_module_cached_wasmer0 is cached by contract key via macro.
        let block = env.clients[0].chain.get_block_by_height(EPOCH_LENGTH).unwrap();
        let chunk_extra =
            env.clients[0].chain.get_chunk_extra(block.hash(), &ShardUId::single_shard()).unwrap();
        let state_root = *chunk_extra.state_root();

        let viewer = TrieViewer::default();
        let trie = Rc::new(
            env.clients[1]
                .runtime_adapter
                .get_trie_for_shard(0, block.header().prev_hash())
                .unwrap(),
        );
        let state_update = TrieUpdate::new(trie, state_root);

        let mut logs = vec![];
        let view_state = ViewApplyState {
            block_height: EPOCH_LENGTH,
            prev_block_hash: *block.header().prev_hash(),
            block_hash: *block.hash(),
            epoch_id: block.header().epoch_id().clone(),
            epoch_height: 1,
            block_timestamp: block.header().raw_timestamp(),
            current_protocol_version: PROTOCOL_VERSION,
            cache: Some(caches[1].clone()),
        };
        viewer
            .call_function(
                state_update,
                view_state,
                &"test0".parse().unwrap(),
                "log_something",
                &[],
                &mut logs,
                &MockEpochInfoProvider::default(),
            )
            .unwrap();
    }

    #[test]
    fn test_two_deployments() {
        let num_clients = 2;
        let stores: Vec<Store> = (0..num_clients).map(|_| create_test_store()).collect();
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = EPOCH_LENGTH;
        let runtime_adapters = stores
            .iter()
            .map(|store| {
                Arc::new(nearcore::NightshadeRuntime::test(
                    Path::new("../../../.."),
                    store.clone(),
                    &genesis,
                )) as Arc<dyn RuntimeAdapter>
            })
            .collect();

        let mut env = TestEnv::builder(ChainGenesis::test())
            .clients_count(num_clients)
            .runtime_adapters(runtime_adapters)
            .build();
        let mut height = 1;

        // Process tiny contract deployment on the first client.
        let tiny_wasm_code = near_test_contracts::trivial_contract().to_vec();
        height = deploy_test_contract(
            &mut env,
            "test0".parse().unwrap(),
            &tiny_wasm_code,
            EPOCH_LENGTH,
            height,
        );

        // Wait 3 epochs.
        height = produce_blocks_from_height(&mut env, 3 * EPOCH_LENGTH, height);

        // Process test contract deployment on the first client.
        let wasm_code = near_test_contracts::rs_contract().to_vec();
        height = deploy_test_contract(
            &mut env,
            "test0".parse().unwrap(),
            &wasm_code,
            EPOCH_LENGTH,
            height,
        );

        // Perform state sync for the second client on the last produced height.
        state_sync_on_height(&mut env, height - 1);

        let caches: Vec<Arc<StoreCompiledContractCache>> = stores
            .iter()
            .map(|s| Arc::new(StoreCompiledContractCache { store: s.clone() }))
            .collect();
        let vm_kind = VMKind::for_protocol_version(PROTOCOL_VERSION);
        let epoch_id = env.clients[0]
            .chain
            .get_block_by_height(height - 1)
            .unwrap()
            .header()
            .epoch_id()
            .clone();
        let runtime_config = env.get_runtime_config(0, epoch_id);
        let tiny_contract_key = get_contract_cache_key(
            &ContractCode::new(tiny_wasm_code.clone(), None),
            vm_kind,
            &runtime_config.wasm_config,
        );
        let test_contract_key = get_contract_cache_key(
            &ContractCode::new(wasm_code.clone(), None),
            vm_kind,
            &runtime_config.wasm_config,
        );

        // Check that both deployed contracts are presented in cache for client 0.
        assert!(caches[0].get(&tiny_contract_key.0).unwrap().is_some());
        assert!(caches[0].get(&test_contract_key.0).unwrap().is_some());

        // Check that only last contract is presented in cache for client 1.
        assert!(caches[1].get(&tiny_contract_key.0).unwrap().is_none());
        assert!(caches[1].get(&test_contract_key.0).unwrap().is_some());
    }

    #[test]
    fn test_sync_after_delete_account() {
        let num_clients = 3;
        let stores: Vec<Store> = (0..num_clients).map(|_| create_test_store()).collect();
        let mut genesis = Genesis::test(
            vec!["test0".parse().unwrap(), "test1".parse().unwrap(), "test2".parse().unwrap()],
            1,
        );
        genesis.config.epoch_length = EPOCH_LENGTH;
        let runtime_adapters = stores
            .iter()
            .map(|store| {
                Arc::new(nearcore::NightshadeRuntime::test(
                    Path::new("../../../.."),
                    store.clone(),
                    &genesis,
                )) as Arc<dyn RuntimeAdapter>
            })
            .collect();

        let mut env = TestEnv::builder(ChainGenesis::test())
            .clients_count(num_clients)
            .runtime_adapters(runtime_adapters)
            .build();
        let mut height = 1;

        // Process test contract deployment on the first client.
        let wasm_code = near_test_contracts::rs_contract().to_vec();
        height = deploy_test_contract(
            &mut env,
            "test2".parse().unwrap(),
            &wasm_code,
            EPOCH_LENGTH,
            height,
        );

        // Delete account on which test contract is stored.
        let block = env.clients[0].chain.get_block_by_height(height - 1).unwrap();
        let signer = InMemorySigner::from_seed("test2".parse().unwrap(), KeyType::ED25519, "test2");
        let delete_account_tx = SignedTransaction::delete_account(
            2,
            "test2".parse().unwrap(),
            "test2".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            *block.hash(),
        );
        env.clients[0].process_tx(delete_account_tx, false, false);
        height = produce_blocks_from_height(&mut env, EPOCH_LENGTH, height);

        // Perform state sync for the second client.
        state_sync_on_height(&mut env, height - 1);

        let caches: Vec<Arc<StoreCompiledContractCache>> = stores
            .iter()
            .map(|s| Arc::new(StoreCompiledContractCache { store: s.clone() }))
            .collect();

        let epoch_id = env.clients[0]
            .chain
            .get_block_by_height(height - 1)
            .unwrap()
            .header()
            .epoch_id()
            .clone();
        let runtime_config = env.get_runtime_config(0, epoch_id);
        let vm_kind = VMKind::for_protocol_version(PROTOCOL_VERSION);
        let contract_key = get_contract_cache_key(
            &ContractCode::new(wasm_code.clone(), None),
            vm_kind,
            &runtime_config.wasm_config,
        );

        // Check that contract is cached for client 0 despite account deletion.
        assert!(caches[0].get(&contract_key.0).unwrap().is_some());

        // Check that contract is not cached for client 1 because of late state sync.
        assert!(caches[1].get(&contract_key.0).unwrap().is_none());
    }
}

mod chunk_nodes_cache_test {
    use super::*;
    use near_primitives::config::ExtCosts;
    use near_primitives::test_utils::encode;
    use near_primitives::transaction::ExecutionMetadata;
    use near_primitives::types::{BlockHeightDelta, Gas, TrieNodesCount};

    fn process_transaction(
        env: &mut TestEnv,
        signer: &dyn Signer,
        num_blocks: BlockHeightDelta,
        protocol_version: ProtocolVersion,
    ) -> CryptoHash {
        let tip = env.clients[0].chain.head().unwrap();
        let epoch_id = env.clients[0]
            .runtime_adapter
            .get_epoch_id_from_prev_block(&tip.last_block_hash)
            .unwrap();
        let block_producer =
            env.clients[0].runtime_adapter.get_block_producer(&epoch_id, tip.height).unwrap();
        let last_block_hash =
            env.clients[0].chain.get_block_by_height(tip.height).unwrap().hash().clone();
        let next_height = tip.height + 1;
        let gas = 20_000_000_000_000;
        let tx = SignedTransaction::from_actions(
            next_height,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            signer,
            vec![
                Action::FunctionCall(FunctionCallAction {
                    args: encode(&[0u64, 10u64]),
                    method_name: "write_key_value".to_string(),
                    gas,
                    deposit: 0,
                }),
                Action::FunctionCall(FunctionCallAction {
                    args: encode(&[1u64, 20u64]),
                    method_name: "write_key_value".to_string(),
                    gas,
                    deposit: 0,
                }),
            ],
            last_block_hash,
        );
        let tx_hash = tx.get_hash().clone();
        env.clients[0].process_tx(tx, false, false);

        for i in next_height..next_height + num_blocks {
            let mut block = env.clients[0].produce_block(i).unwrap().unwrap();
            set_block_protocol_version(&mut block, block_producer.clone(), protocol_version);
            env.process_block(0, block.clone(), Provenance::PRODUCED);
        }
        tx_hash
    }

    /// Compare charged node accesses before and after protocol upgrade to the protocol version of `ChunkNodesCache`.
    /// This upgrade during chunk processing saves each node for which we charge touching trie node cost to a special
    /// chunk cache, and such cost is charged only once on the first access. This effect doesn't persist across chunks.
    ///
    /// We run the same transaction 4 times and compare resulting costs. This transaction writes two different key-value
    /// pairs to the contract storage.
    /// 1st run establishes the trie structure. For our needs, the structure is:
    ///
    ///                                                    --> (Leaf) -> (Value 1)
    /// (Extension) -> (Branch) -> (Extension) -> (Branch) |
    ///                                                    --> (Leaf) -> (Value 2)
    ///
    /// 2nd run should count 12 regular db reads - for 6 nodes per each value, because protocol is not upgraded yet.
    /// 3nd run follows the upgraded protocol and it should count 8 db and 4 memory reads, which comes from 6 db reads
    /// for `Value 1` and only 2 db reads for `Value 2`, because first 4 nodes were already put into the chunk cache.
    /// 4nd run should give the same results, because caching must not affect different chunks.
    #[test]
    fn compare_node_counts() {
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        let epoch_length = 10;
        let num_blocks = 5;

        let old_protocol_version = ProtocolFeature::ChunkNodesCache.protocol_version() - 1;
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = old_protocol_version;
        let chain_genesis = ChainGenesis::from(&genesis);
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> =
            vec![Arc::new(nearcore::NightshadeRuntime::test_with_runtime_config_store(
                Path::new("../../../.."),
                create_test_store(),
                &genesis,
                TrackedConfig::new_empty(),
                RuntimeConfigStore::new(None),
            ))];
        let mut env = TestEnv::builder(chain_genesis).runtime_adapters(runtimes).build();

        deploy_test_contract(
            &mut env,
            "test0".parse().unwrap(),
            near_test_contracts::base_rs_contract(),
            num_blocks,
            1,
        );

        let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        let tx_node_counts: Vec<TrieNodesCount> = (0..4)
            .map(|i| {
                let touching_trie_node_cost: Gas = 16_101_955_926;
                let read_cached_trie_node_cost: Gas = 2_280_000_000;

                let tx_hash = if i < 1 {
                    process_transaction(&mut env, &signer, num_blocks, old_protocol_version)
                } else {
                    process_transaction(
                        &mut env,
                        &signer,
                        2 * epoch_length,
                        old_protocol_version + 1,
                    )
                };

                let final_result =
                    env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
                assert_matches!(final_result.status, FinalExecutionStatus::SuccessValue(_));
                let transaction_outcome =
                    env.clients[0].chain.get_execution_outcome(&tx_hash).unwrap();
                let receipt_ids = transaction_outcome.outcome_with_id.outcome.receipt_ids;
                assert_eq!(receipt_ids.len(), 1);
                let receipt_execution_outcome =
                    env.clients[0].chain.get_execution_outcome(&receipt_ids[0]).unwrap();
                let metadata = receipt_execution_outcome.outcome_with_id.outcome.metadata;
                match metadata {
                    ExecutionMetadata::V1 => panic!("ExecutionMetadata cannot be empty"),
                    ExecutionMetadata::V2(profile_data) => TrieNodesCount {
                        db_reads: {
                            let cost = profile_data.get_ext_cost(ExtCosts::touching_trie_node);
                            assert_eq!(cost % touching_trie_node_cost, 0);
                            cost / touching_trie_node_cost
                        },
                        mem_reads: {
                            let cost = profile_data.get_ext_cost(ExtCosts::read_cached_trie_node);
                            assert_eq!(cost % read_cached_trie_node_cost, 0);
                            cost / read_cached_trie_node_cost
                        },
                    },
                }
            })
            .collect();

        assert_eq!(tx_node_counts[0], TrieNodesCount { db_reads: 4, mem_reads: 0 });
        assert_eq!(tx_node_counts[1], TrieNodesCount { db_reads: 12, mem_reads: 0 });
        assert_eq!(tx_node_counts[2], TrieNodesCount { db_reads: 8, mem_reads: 4 });
        assert_eq!(tx_node_counts[3], TrieNodesCount { db_reads: 8, mem_reads: 4 });
    }
}

mod lower_storage_key_limit_test {
    use super::*;

    /// Check correctness of the protocol upgrade and ability to write 2 KB keys.
    #[test]
    fn protocol_upgrade() {
        let old_protocol_version =
            near_primitives::version::ProtocolFeature::LowerStorageKeyLimit.protocol_version() - 1;
        let new_protocol_version = old_protocol_version + 1;
        let new_storage_key_limit = 2usize.pow(11); // 2 KB
        let args: Vec<u8> = vec![1u8; new_storage_key_limit + 1]
            .into_iter()
            .chain(near_primitives::test_utils::encode(&[10u64]).into_iter())
            .collect();
        let epoch_length: BlockHeight = 5;

        // Prepare TestEnv with a contract at the old protocol version.
        let mut env = {
            let mut genesis =
                Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
            genesis.config.epoch_length = epoch_length;
            genesis.config.protocol_version = old_protocol_version;
            let chain_genesis = ChainGenesis::from(&genesis);
            let runtimes: Vec<Arc<dyn RuntimeAdapter>> =
                vec![Arc::new(nearcore::NightshadeRuntime::test_with_runtime_config_store(
                    Path::new("."),
                    create_test_store(),
                    &genesis,
                    TrackedConfig::AllShards,
                    RuntimeConfigStore::new(None),
                )) as Arc<dyn RuntimeAdapter>];
            let mut env = TestEnv::builder(chain_genesis).runtime_adapters(runtimes).build();

            deploy_test_contract(
                &mut env,
                "test0".parse().unwrap(),
                near_test_contracts::base_rs_contract(),
                epoch_length.clone(),
                1,
            );
            env
        };

        let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        let tx = Transaction {
            signer_id: "test0".parse().unwrap(),
            receiver_id: "test0".parse().unwrap(),
            public_key: signer.public_key(),
            actions: vec![Action::FunctionCall(FunctionCallAction {
                method_name: "write_key_value".to_string(),
                args,
                gas: 10u64.pow(14),
                deposit: 0,
            })],

            nonce: 0,
            block_hash: CryptoHash::default(),
        };

        // Run transaction writing storage key exceeding the limit. Check that execution succeeds.
        {
            let tip = env.clients[0].chain.head().unwrap();
            let signed_tx = Transaction {
                nonce: tip.height + 1,
                block_hash: tip.last_block_hash,
                ..tx.clone()
            }
            .sign(&signer);
            let tx_hash = signed_tx.get_hash().clone();
            env.clients[0].process_tx(signed_tx, false, false);
            for i in 0..epoch_length {
                let block = env.clients[0].produce_block(tip.height + i + 1).unwrap().unwrap();
                env.process_block(0, block.clone(), Provenance::PRODUCED);
            }
            let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
            assert_matches!(final_result.status, FinalExecutionStatus::SuccessValue(_));
        }

        // Move to the new protocol version.
        {
            let tip = env.clients[0].chain.head().unwrap();
            let mut last_block_hash = tip.last_block_hash;
            for i in 0..2 * epoch_length {
                let height = tip.height + i + 1;
                let mut block = env.clients[0].produce_block(height).unwrap().unwrap();

                let epoch_id = env.clients[0]
                    .runtime_adapter
                    .get_epoch_id_from_prev_block(&last_block_hash)
                    .unwrap();
                let block_producer =
                    env.clients[0].runtime_adapter.get_block_producer(&epoch_id, height).unwrap();
                set_block_protocol_version(&mut block, block_producer, new_protocol_version);

                last_block_hash = *block.header().hash();
                env.process_block(0, block, Provenance::PRODUCED);
            }
        }

        // Re-run the transaction, check that execution fails.
        {
            let tip = env.clients[0].chain.head().unwrap();
            let signed_tx =
                Transaction { nonce: tip.height + 1, block_hash: tip.last_block_hash, ..tx }
                    .sign(&signer);
            let tx_hash = signed_tx.get_hash().clone();
            env.clients[0].process_tx(signed_tx, false, false);
            for i in 0..epoch_length {
                let block = env.clients[0].produce_block(tip.height + i + 1).unwrap().unwrap();
                env.process_block(0, block.clone(), Provenance::PRODUCED);
            }
            let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
            assert_matches!(
                final_result.status,
                FinalExecutionStatus::Failure(TxExecutionError::ActionError(_))
            );
        }

        // Run transaction where storage key exactly fits the new limit, check that execution succeeds.
        {
            let args: Vec<u8> = vec![1u8; new_storage_key_limit]
                .into_iter()
                .chain(near_primitives::test_utils::encode(&[20u64]).into_iter())
                .collect();
            let tx = Transaction {
                signer_id: "test0".parse().unwrap(),
                receiver_id: "test0".parse().unwrap(),
                public_key: signer.public_key(),
                actions: vec![Action::FunctionCall(FunctionCallAction {
                    method_name: "write_key_value".to_string(),
                    args,
                    gas: 10u64.pow(14),
                    deposit: 0,
                })],

                nonce: 0,
                block_hash: CryptoHash::default(),
            };
            let tip = env.clients[0].chain.head().unwrap();
            let signed_tx =
                Transaction { nonce: tip.height + 1, block_hash: tip.last_block_hash, ..tx }
                    .sign(&signer);
            let tx_hash = signed_tx.get_hash().clone();
            env.clients[0].process_tx(signed_tx, false, false);
            for i in 0..epoch_length {
                let block = env.clients[0].produce_block(tip.height + i + 1).unwrap().unwrap();
                env.process_block(0, block.clone(), Provenance::PRODUCED);
            }
            let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
            assert_matches!(final_result.status, FinalExecutionStatus::SuccessValue(_));
        }
    }
}
