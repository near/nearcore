use crate::block_processing_utils::BlockNotInPoolError;
use crate::chain::Chain;
use crate::runtime::NightshadeRuntime;
use crate::store::ChainStoreAccess;
use crate::types::{AcceptedBlock, ChainConfig, ChainGenesis};
use crate::{ApplyChunksSpawner, DoomslugThresholdMode};
use crate::{BlockProcessingArtifact, Provenance};
use near_async::futures::RayonAsyncComputationSpawner;
use near_async::messaging::{IntoMultiSender, noop};
use near_async::time::Clock;
use near_chain_configs::test_genesis::{
    TestEpochConfigBuilder, TestGenesisBuilder, ValidatorsSpec,
};
use near_chain_configs::{Genesis, MutableConfigValue};
use near_chain_primitives::Error;
use near_crypto::KeyType;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::block::Block;
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::optimistic_block::BlockToApply;
use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderV3};
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, ChunkExecutionResult, EpochId, Gas, NumBlocks, NumShards,
    ProtocolVersion, ShardId, SpiceChunkId,
};
use near_primitives::utils::MaybeValidated;
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::DBCol;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use num_rational::Ratio;
use std::cmp::Ordering;
use std::mem;
use std::sync::Arc;

pub fn get_chain(clock: Clock) -> Chain {
    get_chain_with_epoch_length_and_num_shards(clock, 10, 1)
}

pub fn get_chain_with_num_shards(clock: Clock, num_shards: NumShards) -> Chain {
    get_chain_with_epoch_length_and_num_shards(clock, 10, num_shards)
}

pub fn get_chain_with_epoch_length(clock: Clock, epoch_length: NumBlocks) -> Chain {
    get_chain_with_epoch_length_and_num_shards(clock, epoch_length, 1)
}

pub fn get_chain_with_epoch_length_and_num_shards(
    clock: Clock,
    epoch_length: NumBlocks,
    num_shards: NumShards,
) -> Chain {
    let mut genesis = Genesis::test_sharded(
        clock.clone(),
        vec!["test1".parse::<AccountId>().unwrap()],
        1,
        num_shards,
    );
    genesis.config.epoch_length = epoch_length;
    genesis.config.transaction_validity_period = epoch_length * 2;
    get_chain_with_genesis(clock, genesis)
}

pub fn get_chain_with_genesis(clock: Clock, genesis: Genesis) -> Chain {
    let store = create_test_store();
    let tempdir = tempfile::tempdir().unwrap();
    initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
    let chain_genesis = ChainGenesis::new(&genesis.config);
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let epoch_manager = EpochManager::new_arc_handle_from_epoch_config_store(
        store.clone(),
        &genesis.config,
        epoch_config_store,
    );
    let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
    let runtime =
        NightshadeRuntime::test(tempdir.path(), store, &genesis.config, epoch_manager.clone());
    Chain::new(
        clock,
        epoch_manager,
        shard_tracker,
        runtime,
        &chain_genesis,
        DoomslugThresholdMode::NoApprovals,
        ChainConfig::test(),
        None,
        ApplyChunksSpawner::Custom(Arc::new(RayonAsyncComputationSpawner)),
        Default::default(),
        MutableConfigValue::new(None, "validator_signer"),
        noop().into_multi_sender(),
        None,
    )
    .unwrap()
}

/// Wait for all blocks that started processing to be ready for postprocessing
/// Returns true if there are new blocks that are ready
pub fn wait_for_all_blocks_in_processing(chain: &Chain) -> bool {
    chain.blocks_in_processing.wait_for_all_blocks()
}

pub fn is_block_in_processing(chain: &Chain, block_hash: &CryptoHash) -> bool {
    chain.blocks_in_processing.contains(&BlockToApply::Normal(*block_hash))
}

pub fn is_optimistic_block_in_processing(chain: &Chain, block_height: u64) -> bool {
    chain.blocks_in_processing.contains(&BlockToApply::Optimistic(block_height))
}

pub fn wait_for_block_in_processing(
    chain: &Chain,
    hash: &CryptoHash,
) -> Result<(), BlockNotInPoolError> {
    chain.blocks_in_processing.wait_for_block(hash)
}

/// Unlike Chain::start_process_block_async, this function blocks until the processing of this block
/// finishes
pub fn process_block_sync(
    chain: &mut Chain,
    block: MaybeValidated<Arc<Block>>,
    provenance: Provenance,
    block_processing_artifacts: &mut BlockProcessingArtifact,
) -> Result<Vec<AcceptedBlock>, Error> {
    let block_hash = *block.hash();
    chain.start_process_block_async(block, provenance, block_processing_artifacts, None)?;
    wait_for_block_in_processing(chain, &block_hash).unwrap();
    let (accepted_blocks, errors) =
        chain.postprocess_ready_blocks(block_processing_artifacts, None);
    // This is in test, we should never get errors when postprocessing blocks
    debug_assert!(errors.is_empty());
    Ok(accepted_blocks)
}

// TODO(#8190) Improve this testing API.
pub fn setup(
    clock: Clock,
) -> (Chain, Arc<EpochManagerHandle>, Arc<NightshadeRuntime>, Arc<ValidatorSigner>) {
    setup_with_tx_validity_period(clock, 100, 1000)
}

pub fn setup_with_tx_validity_period(
    clock: Clock,
    tx_validity_period: NumBlocks,
    epoch_length: u64,
) -> (Chain, Arc<EpochManagerHandle>, Arc<NightshadeRuntime>, Arc<ValidatorSigner>) {
    setup_with_tx_validity_period_at_version(
        clock,
        tx_validity_period,
        epoch_length,
        PROTOCOL_VERSION,
    )
}

/// `setup_with_tx_validity_period` with the genesis protocol version pinned to
/// `protocol_version` instead of `PROTOCOL_VERSION`, so features stabilized above it stay
/// disabled. Only holds while the chain stays inside the genesis epoch: blocks built by
/// `TestBlockBuilder` vote for `PROTOCOL_VERSION`.
pub fn setup_with_tx_validity_period_at_version(
    clock: Clock,
    tx_validity_period: NumBlocks,
    epoch_length: u64,
    protocol_version: ProtocolVersion,
) -> (Chain, Arc<EpochManagerHandle>, Arc<NightshadeRuntime>, Arc<ValidatorSigner>) {
    let store = create_test_store();
    let mut genesis =
        Genesis::test_sharded(clock.clone(), vec!["test".parse::<AccountId>().unwrap()], 1, 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.transaction_validity_period = tx_validity_period;
    genesis.config.gas_limit = Gas::from_gas(1_000_000);
    genesis.config.min_gas_price = Balance::from_yoctonear(100);
    genesis.config.max_gas_price = Balance::from_yoctonear(1_000_000_000);
    genesis.config.total_supply = Balance::from_yoctonear(1_000_000_000);
    genesis.config.gas_price_adjustment_rate = Ratio::from_integer(0);
    genesis.config.protocol_version = protocol_version;
    let tempdir = tempfile::tempdir().unwrap();
    initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
    let runtime =
        NightshadeRuntime::test(tempdir.path(), store, &genesis.config, epoch_manager.clone());
    let chain = Chain::new(
        clock,
        epoch_manager.clone(),
        shard_tracker,
        runtime.clone(),
        &ChainGenesis::new(&genesis.config),
        DoomslugThresholdMode::NoApprovals,
        ChainConfig::test(),
        None,
        ApplyChunksSpawner::Custom(Arc::new(RayonAsyncComputationSpawner)),
        Default::default(),
        MutableConfigValue::new(None, "validator_signer"),
        noop().into_multi_sender(),
        None,
    )
    .unwrap();
    chain.init_flat_storage().unwrap();
    let signer = Arc::new(create_test_signer("test"));
    (chain, epoch_manager, runtime, signer)
}

pub fn format_hash(hash: CryptoHash) -> String {
    let mut hash = hash.to_string();
    hash.truncate(6);
    hash
}

/// Displays chain from given store.
pub fn display_chain(me: &Option<AccountId>, chain: &mut Chain, tail: bool) {
    let epoch_manager = chain.epoch_manager.clone();
    let chain_store = chain.mut_chain_store();
    let head = chain_store.head().unwrap();
    tracing::debug!(
        ?me,
        mode = if tail { "tail" } else { "full" },
        height = %head.height,
        last_block_hash = %head.last_block_hash,
        "chain head"
    );
    let mut headers = vec![];
    for (key, _) in chain_store.store().iter(DBCol::BlockHeader) {
        let header = chain_store
            .get_block_header(&CryptoHash::try_from(key.as_ref()).unwrap())
            .unwrap()
            .clone();
        if !tail || header.height() + 10 > head.height {
            headers.push(header);
        }
    }
    headers.sort_by(|h_left, h_right| {
        if h_left.height() > h_right.height() { Ordering::Greater } else { Ordering::Less }
    });
    for header in headers {
        if header.is_genesis() {
            // Genesis block.
            tracing::debug!(height = %header.height(), hash = %format_hash(*header.hash()));
        } else {
            let parent_header = chain_store.get_block_header(header.prev_hash()).unwrap().clone();
            let maybe_block = chain_store.get_block(header.hash()).ok();
            let epoch_id = epoch_manager.get_epoch_id_from_prev_block(header.prev_hash()).unwrap();
            let block_producer =
                epoch_manager.get_block_producer(&epoch_id, header.height()).unwrap();
            tracing::debug!(
                height = %header.height(),
                hash = %format_hash(*header.hash()),
                %block_producer,
                parent_height = %parent_header.height(),
                parent_hash = %format_hash(*parent_header.hash()),
                chunks = %if let Some(block) = &maybe_block {
                    block.chunks().len().to_string()
                } else {
                    "-".to_string()
                },
                "block"
            );
            if let Some(block) = maybe_block {
                for chunk_header in block.chunks().iter() {
                    let chunk_producer = epoch_manager
                        .get_chunk_producer_info(&ChunkProductionKey {
                            epoch_id,
                            height_created: chunk_header.height_created(),
                            shard_id: chunk_header.shard_id(),
                        })
                        .unwrap()
                        .take_account_id();
                    if let Ok(chunk) = chain_store.get_chunk(&chunk_header.chunk_hash()) {
                        tracing::debug!(
                            height = %chunk_header.height_created(),
                            hash = %format_hash(chunk_header.chunk_hash().0),
                            shard_id = %chunk_header.shard_id(),
                            %chunk_producer,
                            tx_count = %chunk.to_transactions().len(),
                            receipts_count = %chunk.prev_outgoing_receipts().len(),
                        );
                    } else if let Ok(partial_chunk) =
                        chain_store.get_partial_chunk(&chunk_header.chunk_hash())
                    {
                        tracing::debug!(
                            height = %chunk_header.height_created(),
                            hash = %format_hash(chunk_header.chunk_hash().0),
                            shard_id = %chunk_header.shard_id(),
                            %chunk_producer,
                            parts = ?partial_chunk.parts().iter().map(|x| x.part_ord).collect::<Vec<_>>(),
                            receipts = ?partial_chunk
                                .prev_outgoing_receipts()
                                .iter()
                                .map(|x| format!("{} => {}", x.0.len(), x.1.to_shard_id))
                                .collect::<Vec<_>>(),
                            "partial chunk",
                        );
                    }
                }
            }
        }
    }
}

/// A spice chain whose only producer rotates its validator key: it signs with
/// `create_test_signer` until epoch `rotation_epoch_id` and with `new_signer` from then on.
pub struct SpiceKeyRotationSetup {
    pub genesis: Genesis,
    /// Processed up to the last block before `rotation_epoch_id`.
    pub chain: Chain,
    pub producer: AccountId,
    pub new_signer: Arc<ValidatorSigner>,
    pub rotation_epoch_id: EpochId,
    /// The first block of `rotation_epoch_id`, built on the head of `chain` but not processed.
    pub first_rotated_block: Arc<Block>,
}

/// Builds a spice chain where the producer proposes a new key in the first epoch, which takes
/// effect two epochs later. Every block certifies the chunks of its parent, so each epoch can end.
pub fn setup_spice_key_rotation() -> SpiceKeyRotationSetup {
    let producer: AccountId = "test-producer".parse().unwrap();
    let epoch_length = 5;
    let genesis = TestGenesisBuilder::new()
        .epoch_length(epoch_length)
        // Leaves the total supply unchanged at epoch boundaries, which test blocks don't account
        // for.
        .max_inflation_rate(Ratio::from_integer(0))
        .validators_spec(ValidatorsSpec::desired_roles(
            &[producer.as_str()],
            &["test-validator-0", "test-validator-1"],
        ))
        .build();
    let mut chain = get_chain_with_genesis(Clock::real(), genesis.clone());
    let epoch_manager = chain.epoch_manager.clone();
    let new_signer: Arc<ValidatorSigner> = Arc::new(
        InMemoryValidatorSigner::from_seed(producer.clone(), KeyType::ED25519, "rotated").into(),
    );
    let signers = [new_signer.clone()];

    let genesis_block = chain.genesis_block();
    let mut head = build_spice_test_block(&chain, &genesis_block, vec![], &signers);
    process_block_sync(
        &mut chain,
        head.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    let stake =
        epoch_manager.get_validator_by_account_id(head.header().epoch_id(), &producer).unwrap();
    let mut proposals =
        vec![ValidatorStake::new(producer.clone(), new_signer.public_key(), stake.stake())];
    loop {
        assert!(
            head.header().height() < genesis.config.genesis_height + 3 * epoch_length,
            "the new key should take effect two epochs after it is proposed"
        );
        let statements = spice_core_statements_certifying_block(
            epoch_manager.as_ref(),
            &head,
            mem::take(&mut proposals),
            &signers,
        );
        let block = build_spice_test_block(&chain, &head, statements, &signers);
        let block_key = epoch_manager
            .get_validator_by_account_id(block.header().epoch_id(), &producer)
            .unwrap()
            .take_public_key();
        if block_key == new_signer.public_key() {
            return SpiceKeyRotationSetup {
                genesis,
                chain,
                producer,
                new_signer,
                rotation_epoch_id: *block.header().epoch_id(),
                first_rotated_block: block,
            };
        }
        process_block_sync(
            &mut chain,
            block.clone().into(),
            Provenance::PRODUCED,
            &mut BlockProcessingArtifact::default(),
        )
        .unwrap();
        head = block;
    }
}

/// Returns the signer from `signers` holding `validator`'s key, or `create_test_signer` for it.
fn test_signer_with_key(
    validator: &ValidatorStake,
    signers: &[Arc<ValidatorSigner>],
) -> Arc<ValidatorSigner> {
    signers
        .iter()
        .find(|signer| {
            signer.validator_id() == validator.account_id()
                && &signer.public_key() == validator.public_key()
        })
        .cloned()
        .unwrap_or_else(|| Arc::new(create_test_signer(validator.account_id().as_str())))
}

/// Builds a spice block with a new chunk for every shard on top of `prev_block`, also when it
/// starts a new epoch. The block and its chunks are signed by the producers the epoch manager
/// picks, each with the signer from `signers` holding its key in the block's epoch, or with
/// `create_test_signer` if there is none, so a test can rotate a producer's key.
fn build_spice_test_block(
    chain: &Chain,
    prev_block: &Block,
    spice_core_statements: Vec<SpiceCoreStatement>,
    signers: &[Arc<ValidatorSigner>],
) -> Arc<Block> {
    let epoch_manager = chain.epoch_manager.as_ref();
    let prev_hash = prev_block.hash();
    let height = prev_block.header().height() + 1;
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_hash).unwrap();
    let next_epoch_id = epoch_manager.get_next_epoch_id_from_prev_block(prev_hash).unwrap();
    let next_bp_hash = if &next_epoch_id == prev_block.header().next_epoch_id() {
        *prev_block.header().next_bp_hash()
    } else {
        Chain::compute_bp_hash(epoch_manager, next_epoch_id).unwrap()
    };

    let mut chunks = Vec::new();
    for prev_chunk in prev_block.chunks().iter_raw() {
        let shard_id = prev_chunk.shard_id();
        let chunk_producer = epoch_manager
            .get_chunk_producer_info(&ChunkProductionKey {
                shard_id,
                epoch_id,
                height_created: height,
            })
            .unwrap();
        let mut chunk = ShardChunkHeader::V3(ShardChunkHeaderV3::new_for_spice(
            *prev_hash,
            Default::default(),
            Default::default(),
            height,
            shard_id,
            Default::default(),
            Default::default(),
            test_signer_with_key(&chunk_producer, signers).as_ref(),
        ));
        *chunk.height_included_mut() = height;
        chunks.push(chunk);
    }

    let block_producer = epoch_manager.get_block_producer_info(&epoch_id, height).unwrap();
    let spice_chunk_endorsement_stats = chain
        .spice_core_reader
        .spice_chunk_endorsement_stats_for_next_block(prev_block.header(), height)
        .unwrap();
    let prev_last_certified_block_epoch_id =
        chain.spice_core_reader.prev_last_certified_block_epoch_id(prev_hash).unwrap();
    let epoch_sync_data_hash = epoch_manager.compute_epoch_sync_data_hash(prev_hash).unwrap();
    TestBlockBuilder::from_prev_block(
        Clock::real(),
        prev_block,
        test_signer_with_key(&block_producer, signers),
    )
    .epoch_id(epoch_id)
    .next_epoch_id(next_epoch_id)
    .next_bp_hash(next_bp_hash)
    .chunks(chunks)
    .spice_core_statements(spice_core_statements)
    .spice_chunk_endorsement_stats(spice_chunk_endorsement_stats)
    .prev_last_certified_block_epoch_id(prev_last_certified_block_epoch_id)
    .epoch_sync_data_hash(epoch_sync_data_hash)
    .build()
}

/// Returns spice core statements certifying every chunk of `block`: an endorsement from each of
/// the chunk's validators and an execution result. The chunk extra of the first chunk carries
/// `validator_proposals`, so including the statements in a block feeds each proposal to the epoch
/// manager. Validators sign as in `build_spice_test_block`.
fn spice_core_statements_certifying_block(
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
    mut validator_proposals: Vec<ValidatorStake>,
    signers: &[Arc<ValidatorSigner>],
) -> Vec<SpiceCoreStatement> {
    let epoch_id = block.header().epoch_id();
    let mut statements = Vec::new();
    for chunk_header in block.chunks().iter_raw() {
        let chunk_id =
            SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() };
        let execution_result = ChunkExecutionResult {
            chunk_extra: ChunkExtra::new(
                &chunk_header.chunk_hash().0,
                CryptoHash::default(),
                mem::take(&mut validator_proposals),
                Gas::ZERO,
                Gas::ZERO,
                Balance::ZERO,
                Some(CongestionInfo::default()),
                BandwidthRequests::empty(),
                None,
            ),
            outgoing_receipts_root: CryptoHash::default(),
        };
        let validators = epoch_manager
            .get_chunk_validator_assignments(
                epoch_id,
                chunk_header.shard_id(),
                chunk_header.height_created(),
            )
            .unwrap()
            .ordered_chunk_validators();
        for account_id in validators {
            let validator =
                epoch_manager.get_validator_by_account_id(epoch_id, &account_id).unwrap();
            let signer = test_signer_with_key(&validator, signers);
            let verified =
                SpiceChunkEndorsement::new(chunk_id.clone(), execution_result.clone(), &signer)
                    .into_verified(&[signer.public_key()])
                    .unwrap();
            statements.push(verified.to_stored().into_core_statement(chunk_id.clone(), account_id));
        }
        statements.push(SpiceCoreStatement::ChunkExecutionResult { chunk_id, execution_result });
    }
    statements
}

pub fn get_fake_next_block_chunk_headers(
    block: &Block,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Vec<ShardChunkHeader> {
    fn chunk_header(
        height: BlockHeight,
        shard_id: ShardId,
        prev_block_hash: CryptoHash,
        signer: &ValidatorSigner,
        is_spice_block: bool,
    ) -> ShardChunkHeader {
        if is_spice_block {
            ShardChunkHeader::V3(ShardChunkHeaderV3::new_for_spice(
                prev_block_hash,
                Default::default(),
                Default::default(),
                height,
                shard_id,
                Default::default(),
                Default::default(),
                signer,
            ))
        } else {
            ShardChunkHeader::V3(ShardChunkHeaderV3::new(
                prev_block_hash,
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
                height,
                shard_id,
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
                CongestionInfo::default(),
                BandwidthRequests::empty(),
                None,
                signer,
                PROTOCOL_VERSION,
            ))
        }
    }

    let mut chunks = Vec::new();
    for chunk in block.chunks().iter_raw() {
        let shard_id = chunk.shard_id();
        let height = block.header().height() + 1;
        let chunk_producer = epoch_manager
            .get_chunk_producer_info(&ChunkProductionKey {
                shard_id,
                epoch_id: *block.header().epoch_id(),
                height_created: height,
            })
            .unwrap();
        let signer = create_test_signer(chunk_producer.account_id().as_str());
        let mut chunk_header =
            chunk_header(height, shard_id, *block.hash(), &signer, block.is_spice_block());
        *chunk_header.height_included_mut() = height;
        chunks.push(chunk_header);
    }
    chunks
}

#[cfg(test)]
mod test {
    use crate::Chain;
    use near_async::time::Clock;
    use near_primitives::hash::CryptoHash;
    use near_primitives::receipt::Receipt;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::sharding::ReceiptList;
    use near_primitives::types::{AccountId, Balance, NumShards};
    use rand::Rng;
    use std::convert::TryFrom;

    fn naive_build_receipt_hashes(
        receipts: &[Receipt],
        shard_layout: &ShardLayout,
    ) -> Vec<CryptoHash> {
        let mut receipts_hashes = vec![];
        for shard_id in shard_layout.shard_ids() {
            let shard_receipts: Vec<Receipt> = receipts
                .iter()
                .filter(|&receipt| receipt.receiver_shard_id(shard_layout).unwrap() == shard_id)
                .cloned()
                .collect();
            receipts_hashes.push(CryptoHash::hash_borsh(ReceiptList(shard_id, &shard_receipts)));
        }
        receipts_hashes
    }

    fn test_build_receipt_hashes_with_num_shard(num_shards: NumShards) {
        let shard_layout = ShardLayout::multi_shard(num_shards, 0);
        let create_receipt_from_receiver_id =
            |receiver_id| Receipt::new_balance_refund(&receiver_id, Balance::ZERO);
        let mut rng = rand::thread_rng();
        let receipts = (0..3000)
            .map(|_| {
                let random_number = rng.gen_range(0..1000);
                create_receipt_from_receiver_id(
                    AccountId::try_from(format!("test{}", random_number)).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let start = Clock::real().now();
        let naive_result = naive_build_receipt_hashes(&receipts, &shard_layout);
        let naive_duration = start.elapsed();
        let start = Clock::real().now();
        let prod_result = Chain::build_receipts_hashes(&receipts, &shard_layout).unwrap();
        let prod_duration = start.elapsed();
        assert_eq!(naive_result, prod_result);
        // production implementation is at least 50% faster
        assert!(
            2 * naive_duration > 3 * prod_duration,
            "naive duration vs production {:?} {:?}",
            naive_duration,
            prod_duration
        );
    }

    #[test]
    #[ignore]
    /// Disabled, see more details in #5836
    fn test_build_receipt_hashes() {
        for num_shards in 1..10 {
            test_build_receipt_hashes_with_num_shard(num_shards);
        }
    }
}
