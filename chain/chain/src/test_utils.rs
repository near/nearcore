mod kv_runtime;
mod validator_schedule;

use std::cmp::Ordering;
use std::sync::Arc;

use crate::block_processing_utils::BlockNotInPoolError;
use crate::chain::Chain;
use crate::rayon_spawner::RayonAsyncComputationSpawner;
use crate::runtime::NightshadeRuntime;
use crate::store::ChainStoreAccess;
use crate::types::{AcceptedBlock, ChainConfig, ChainGenesis};
use crate::DoomslugThresholdMode;
use crate::{BlockProcessingArtifact, Provenance};
use near_async::time::Clock;
use near_chain_configs::{Genesis, MutableConfigValue};
use near_chain_primitives::Error;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManager, EpochManagerHandle};
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, NumBlocks, NumShards};
use near_primitives::utils::MaybeValidated;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use near_store::DBCol;
use num_rational::Ratio;
use tracing::debug;

pub use self::kv_runtime::{account_id_to_shard_id, KeyValueRuntime, MockEpochManager};
pub use self::validator_schedule::ValidatorSchedule;

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
    let store = create_test_store();
    let mut genesis = Genesis::test_sharded(
        clock.clone(),
        vec!["test1".parse::<AccountId>().unwrap()],
        1,
        vec![1; num_shards as usize],
    );
    genesis.config.epoch_length = epoch_length;
    let tempdir = tempfile::tempdir().unwrap();
    initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
    let chain_genesis = ChainGenesis::new(&genesis.config);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
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
        Arc::new(RayonAsyncComputationSpawner),
        MutableConfigValue::new(None, "validator_signer"),
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
    chain.blocks_in_processing.contains(block_hash)
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
    me: &Option<AccountId>,
    block: MaybeValidated<Block>,
    provenance: Provenance,
    block_processing_artifacts: &mut BlockProcessingArtifact,
) -> Result<Vec<AcceptedBlock>, Error> {
    let block_hash = *block.hash();
    chain.start_process_block_async(me, block, provenance, block_processing_artifacts, None)?;
    wait_for_block_in_processing(chain, &block_hash).unwrap();
    let (accepted_blocks, errors) =
        chain.postprocess_ready_blocks(me, block_processing_artifacts, None);
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
    let store = create_test_store();
    let mut genesis = Genesis::test_sharded(
        clock.clone(),
        vec!["test".parse::<AccountId>().unwrap()],
        1,
        vec![1; 1],
    );
    genesis.config.epoch_length = epoch_length;
    genesis.config.transaction_validity_period = tx_validity_period;
    genesis.config.gas_limit = 1_000_000;
    genesis.config.min_gas_price = 100;
    genesis.config.max_gas_price = 1_000_000_000;
    genesis.config.total_supply = 1_000_000_000;
    genesis.config.gas_price_adjustment_rate = Ratio::from_integer(0);
    genesis.config.protocol_version = PROTOCOL_VERSION;
    let tempdir = tempfile::tempdir().unwrap();
    initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
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
        Arc::new(RayonAsyncComputationSpawner),
        MutableConfigValue::new(None, "validator_signer"),
        None,
    )
    .unwrap();

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
    debug!(
        "{:?} Chain head ({}): {} / {}",
        me,
        if tail { "tail" } else { "full" },
        head.height,
        head.last_block_hash
    );
    let mut headers = vec![];
    for (key, _) in chain_store.store().clone().iter(DBCol::BlockHeader).map(Result::unwrap) {
        let header = chain_store
            .get_block_header(&CryptoHash::try_from(key.as_ref()).unwrap())
            .unwrap()
            .clone();
        if !tail || header.height() + 10 > head.height {
            headers.push(header);
        }
    }
    headers.sort_by(|h_left, h_right| {
        if h_left.height() > h_right.height() {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    });
    for header in headers {
        if header.is_genesis() {
            // Genesis block.
            debug!("{: >3} {}", header.height(), format_hash(*header.hash()));
        } else {
            let parent_header = chain_store.get_block_header(header.prev_hash()).unwrap().clone();
            let maybe_block = chain_store.get_block(header.hash()).ok();
            let epoch_id = epoch_manager.get_epoch_id_from_prev_block(header.prev_hash()).unwrap();
            let block_producer =
                epoch_manager.get_block_producer(&epoch_id, header.height()).unwrap();
            debug!(
                "{: >3} {} | {: >10} | parent: {: >3} {} | {}",
                header.height(),
                format_hash(*header.hash()),
                block_producer,
                parent_header.height(),
                format_hash(*parent_header.hash()),
                if let Some(block) = &maybe_block {
                    format!("chunks: {}", block.chunks().len())
                } else {
                    "-".to_string()
                }
            );
            if let Some(block) = maybe_block {
                for chunk_header in block.chunks().iter() {
                    let chunk_producer = epoch_manager
                        .get_chunk_producer(
                            &epoch_id,
                            chunk_header.height_created(),
                            chunk_header.shard_id(),
                        )
                        .unwrap();
                    if let Ok(chunk) = chain_store.get_chunk(&chunk_header.chunk_hash()) {
                        debug!(
                            "    {: >3} {} | {} | {: >10} | tx = {: >2}, receipts = {: >2}",
                            chunk_header.height_created(),
                            format_hash(chunk_header.chunk_hash().0),
                            chunk_header.shard_id(),
                            chunk_producer,
                            chunk.transactions().len(),
                            chunk.prev_outgoing_receipts().len()
                        );
                    } else if let Ok(partial_chunk) =
                        chain_store.get_partial_chunk(&chunk_header.chunk_hash())
                    {
                        debug!(
                            "    {: >3} {} | {} | {: >10} | parts = {:?} receipts = {:?}",
                            chunk_header.height_created(),
                            format_hash(chunk_header.chunk_hash().0),
                            chunk_header.shard_id(),
                            chunk_producer,
                            partial_chunk.parts().iter().map(|x| x.part_ord).collect::<Vec<_>>(),
                            partial_chunk
                                .prev_outgoing_receipts()
                                .iter()
                                .map(|x| format!("{} => {}", x.0.len(), x.1.to_shard_id))
                                .collect::<Vec<_>>(),
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::convert::TryFrom;

    use near_async::time::Clock;
    use rand::Rng;

    use near_primitives::hash::CryptoHash;
    use near_primitives::receipt::{Receipt, ReceiptPriority};
    use near_primitives::sharding::ReceiptList;
    use near_primitives::types::{AccountId, NumShards};

    use crate::Chain;

    use near_primitives::shard_layout::{account_id_to_shard_id, ShardLayout};

    fn naive_build_receipt_hashes(
        receipts: &[Receipt],
        shard_layout: &ShardLayout,
    ) -> Vec<CryptoHash> {
        let mut receipts_hashes = vec![];
        for shard_id in shard_layout.shard_ids() {
            let shard_receipts: Vec<Receipt> = receipts
                .iter()
                .filter(|&receipt| {
                    account_id_to_shard_id(receipt.receiver_id(), shard_layout) == shard_id
                })
                .cloned()
                .collect();
            receipts_hashes.push(CryptoHash::hash_borsh(ReceiptList(shard_id, &shard_receipts)));
        }
        receipts_hashes
    }

    fn test_build_receipt_hashes_with_num_shard(num_shards: NumShards) {
        let shard_layout = ShardLayout::v0(num_shards, 0);
        let create_receipt_from_receiver_id =
            |receiver_id| Receipt::new_balance_refund(&receiver_id, 0, ReceiptPriority::NoPriority);
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
        let prod_result = Chain::build_receipts_hashes(&receipts, &shard_layout);
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
