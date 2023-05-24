mod kv_runtime;
mod validator_schedule;

use std::cmp::Ordering;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::test_utils::create_test_signer;
use num_rational::Ratio;
use tracing::debug;

use near_chain_primitives::Error;

use near_primitives::block::Block;

use near_primitives::hash::CryptoHash;

use near_primitives::types::{AccountId, NumBlocks};
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;

use near_store::test_utils::create_test_store;
use near_store::DBCol;

use crate::block_processing_utils::BlockNotInPoolError;
use crate::chain::Chain;
use crate::store::ChainStoreAccess;
use crate::types::{AcceptedBlock, ChainConfig, ChainGenesis};
use crate::DoomslugThresholdMode;
use crate::{BlockProcessingArtifact, Provenance};
use near_primitives::static_clock::StaticClock;
use near_primitives::utils::MaybeValidated;

pub use self::kv_runtime::account_id_to_shard_id;
pub use self::kv_runtime::KeyValueRuntime;
pub use self::kv_runtime::MockEpochManager;

pub use self::validator_schedule::ValidatorSchedule;

/// Wait for all blocks that started processing to be ready for postprocessing
/// Returns true if there are new blocks that are ready
pub fn wait_for_all_blocks_in_processing(chain: &mut Chain) -> bool {
    chain.blocks_in_processing.wait_for_all_blocks()
}

pub fn is_block_in_processing(chain: &Chain, block_hash: &CryptoHash) -> bool {
    chain.blocks_in_processing.contains(block_hash)
}

pub fn wait_for_block_in_processing(
    chain: &mut Chain,
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
    chain.start_process_block_async(
        me,
        block,
        provenance,
        block_processing_artifacts,
        Arc::new(|_| {}),
    )?;
    wait_for_block_in_processing(chain, &block_hash).unwrap();
    let (accepted_blocks, errors) =
        chain.postprocess_ready_blocks(me, block_processing_artifacts, Arc::new(|_| {}));
    // This is in test, we should never get errors when postprocessing blocks
    debug_assert!(errors.is_empty());
    Ok(accepted_blocks)
}

// TODO(#8190) Improve this testing API.
pub fn setup() -> (Chain, Arc<MockEpochManager>, Arc<KeyValueRuntime>, Arc<InMemoryValidatorSigner>)
{
    setup_with_tx_validity_period(100)
}

pub fn setup_with_tx_validity_period(
    tx_validity_period: NumBlocks,
) -> (Chain, Arc<MockEpochManager>, Arc<KeyValueRuntime>, Arc<InMemoryValidatorSigner>) {
    let store = create_test_store();
    let epoch_length = 1000;
    let epoch_manager = MockEpochManager::new(store.clone(), epoch_length);
    let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
    let runtime = KeyValueRuntime::new(store, epoch_manager.as_ref());
    let chain = Chain::new(
        epoch_manager.clone(),
        shard_tracker,
        runtime.clone(),
        &ChainGenesis {
            time: StaticClock::utc(),
            height: 0,
            gas_limit: 1_000_000,
            min_gas_price: 100,
            max_gas_price: 1_000_000_000,
            total_supply: 1_000_000_000,
            gas_price_adjustment_rate: Ratio::from_integer(0),
            transaction_validity_period: tx_validity_period,
            epoch_length,
            protocol_version: PROTOCOL_VERSION,
        },
        DoomslugThresholdMode::NoApprovals,
        ChainConfig::test(),
        None,
    )
    .unwrap();

    let signer = Arc::new(create_test_signer("test"));
    (chain, epoch_manager, runtime, signer)
}

pub fn setup_with_validators(
    vs: ValidatorSchedule,
    epoch_length: u64,
    tx_validity_period: NumBlocks,
) -> (Chain, Arc<MockEpochManager>, Arc<KeyValueRuntime>, Vec<Arc<InMemoryValidatorSigner>>) {
    let store = create_test_store();
    let signers =
        vs.all_block_producers().map(|x| Arc::new(create_test_signer(x.as_str()))).collect();
    let epoch_manager = MockEpochManager::new_with_validators(store.clone(), vs, epoch_length);
    let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
    let runtime = KeyValueRuntime::new(store, epoch_manager.as_ref());
    let chain = Chain::new(
        epoch_manager.clone(),
        shard_tracker,
        runtime.clone(),
        &ChainGenesis {
            time: StaticClock::utc(),
            height: 0,
            gas_limit: 1_000_000,
            min_gas_price: 100,
            max_gas_price: 1_000_000_000,
            total_supply: 1_000_000_000,
            gas_price_adjustment_rate: Ratio::from_integer(0),
            transaction_validity_period: tx_validity_period,
            epoch_length,
            protocol_version: PROTOCOL_VERSION,
        },
        DoomslugThresholdMode::NoApprovals,
        ChainConfig::test(),
        None,
    )
    .unwrap();
    (chain, epoch_manager, runtime, signers)
}

pub fn setup_with_validators_and_start_time(
    vs: ValidatorSchedule,
    epoch_length: u64,
    tx_validity_period: NumBlocks,
    start_time: DateTime<Utc>,
) -> (Chain, Arc<MockEpochManager>, Arc<KeyValueRuntime>, Vec<Arc<InMemoryValidatorSigner>>) {
    let store = create_test_store();
    let signers =
        vs.all_block_producers().map(|x| Arc::new(create_test_signer(x.as_str()))).collect();
    let epoch_manager = MockEpochManager::new_with_validators(store.clone(), vs, epoch_length);
    let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
    let runtime = KeyValueRuntime::new(store, epoch_manager.as_ref());
    let chain = Chain::new(
        epoch_manager.clone(),
        shard_tracker,
        runtime.clone(),
        &ChainGenesis {
            time: start_time,
            height: 0,
            gas_limit: 1_000_000,
            min_gas_price: 100,
            max_gas_price: 1_000_000_000,
            total_supply: 1_000_000_000,
            gas_price_adjustment_rate: Ratio::from_integer(0),
            transaction_validity_period: tx_validity_period,
            epoch_length,
            protocol_version: PROTOCOL_VERSION,
        },
        DoomslugThresholdMode::NoApprovals,
        ChainConfig::test(),
        None,
    )
    .unwrap();
    (chain, epoch_manager, runtime, signers)
}

pub fn format_hash(hash: CryptoHash) -> String {
    let mut hash = hash.to_string();
    hash.truncate(6);
    hash
}

/// Displays chain from given store.
pub fn display_chain(me: &Option<AccountId>, chain: &mut Chain, tail: bool) {
    let epoch_manager = chain.epoch_manager.clone();
    let chain_store = chain.mut_store();
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
        if header.prev_hash() == &CryptoHash::default() {
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
                            chunk.receipts().len()
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
                                .receipts()
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

impl ChainGenesis {
    pub fn test() -> Self {
        ChainGenesis {
            time: StaticClock::utc(),
            height: 0,
            gas_limit: 10u64.pow(15),
            min_gas_price: 0,
            max_gas_price: 1_000_000_000,
            total_supply: 1_000_000_000,
            gas_price_adjustment_rate: Ratio::from_integer(0),
            transaction_validity_period: 100,
            epoch_length: 5,
            protocol_version: PROTOCOL_VERSION,
        }
    }
}

#[cfg(test)]
mod test {
    use std::convert::TryFrom;

    use rand::Rng;

    use near_primitives::hash::CryptoHash;
    use near_primitives::receipt::Receipt;
    use near_primitives::sharding::ReceiptList;
    use near_primitives::static_clock::StaticClock;
    use near_primitives::types::{AccountId, NumShards};

    use crate::Chain;

    use near_primitives::shard_layout::{account_id_to_shard_id, ShardLayout};

    fn naive_build_receipt_hashes(
        receipts: &[Receipt],
        shard_layout: &ShardLayout,
    ) -> Vec<CryptoHash> {
        let mut receipts_hashes = vec![];
        for shard_id in 0..shard_layout.num_shards() {
            let shard_receipts: Vec<Receipt> = receipts
                .iter()
                .filter(|&receipt| {
                    account_id_to_shard_id(&receipt.receiver_id, shard_layout) == shard_id
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
            |receiver_id| Receipt::new_balance_refund(&receiver_id, 0);
        let mut rng = rand::thread_rng();
        let receipts = (0..3000)
            .map(|_| {
                let random_number = rng.gen_range(0..1000);
                create_receipt_from_receiver_id(
                    AccountId::try_from(format!("test{}", random_number)).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let start = StaticClock::instant();
        let naive_result = naive_build_receipt_hashes(&receipts, &shard_layout);
        let naive_duration = start.elapsed();
        let start = StaticClock::instant();
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
