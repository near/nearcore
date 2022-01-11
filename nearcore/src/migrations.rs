use crate::{NearConfig, NightshadeRuntime};
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain::chain::collect_receipts_from_response;
use near_chain::migrations::check_if_block_is_first_with_chunk_of_version;
use near_chain::types::{ApplyTransactionResult, BlockHeaderInfo};
use near_chain::{ChainStore, ChainStoreAccess, ChainStoreUpdate, RuntimeAdapter};
use near_epoch_manager::{EpochManager, RewardCalculator};
use near_primitives::epoch_manager::{AllEpochConfig, EpochConfig};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::MerklePath;
use near_primitives::receipt::ReceiptResult;
use near_primitives::runtime::migration_data::MigrationData;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader, ShardChunkV1};
use near_primitives::transaction::{
    ExecutionMetadata, ExecutionOutcome, ExecutionOutcomeWithId, ExecutionOutcomeWithIdAndProof,
    ExecutionStatus, LogEntry,
};
use near_primitives::types::{AccountId, Balance, Gas};
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::utils::index_to_bytes;
use near_store::db::DBCol::ColReceipts;
use near_store::migrations::{set_store_version, BatchedStoreUpdate};
use near_store::{create_store, DBCol, StoreUpdate};
use std::path::Path;

fn get_chunk(chain_store: &ChainStore, chunk_hash: ChunkHash) -> ShardChunkV1 {
    let store = chain_store.store();
    let maybe_chunk = store.get_ser(DBCol::ColChunks, chunk_hash.as_ref()).unwrap();

    match maybe_chunk {
        Some(chunk) => chunk,
        None => panic!("Could not find chunk {} in DB", chunk_hash.0),
    }
}

fn apply_block_at_height(
    store_update: &mut StoreUpdate,
    chain_store: &mut ChainStore,
    runtime_adapter: &dyn RuntimeAdapter,
    block_height: BlockHeight,
    shard_id: ShardId,
) -> Result<(), near_chain::Error> {
    let block_hash = chain_store.get_block_hash_by_height(block_height)?;
    let block = chain_store.get_block(&block_hash)?.clone();
    if block.chunks()[shard_id as usize].height_included() != block_height {
        return Ok(());
    }

    let prev_block = chain_store.get_block(block.header().prev_hash())?.clone();
    let mut chain_store_update = ChainStoreUpdate::new(chain_store);
    let receipt_proof_response = chain_store_update.get_incoming_receipts_for_shard(
        shard_id,
        block_hash,
        prev_block.chunks()[shard_id as usize].height_included(),
    )?;
    let is_first_block_with_chunk_of_version = check_if_block_is_first_with_chunk_of_version(
        &mut chain_store_update,
        runtime_adapter,
        prev_block.hash(),
        shard_id,
    )?;
    let receipts = collect_receipts_from_response(&receipt_proof_response);
    let chunk_hash = block.chunks()[shard_id as usize].chunk_hash();
    let chunk = get_chunk(chain_store, chunk_hash);
    let chunk_header = ShardChunkHeader::V1(chunk.header);
    let apply_result = runtime_adapter
        .apply_transactions(
            shard_id,
            &chunk_header.prev_state_root(),
            block_height,
            block.header().raw_timestamp(),
            block.header().prev_hash(),
            block.hash(),
            &receipts,
            &chunk.transactions,
            chunk_header.validator_proposals(),
            prev_block.header().gas_price(),
            chunk_header.gas_limit(),
            block.header().challenges_result(),
            *block.header().random_value(),
            true,
            is_first_block_with_chunk_of_version,
            None,
        )
        .unwrap();
    let (_, outcome_paths) = ApplyTransactionResult::compute_outcomes_proof(&apply_result.outcomes);

    for (outcome_with_id, proof) in apply_result.outcomes.into_iter().zip(outcome_paths.into_iter())
    {
        let id = outcome_with_id.id;
        let outcome = vec![ExecutionOutcomeWithIdAndProof { proof, block_hash, outcome_with_id }];
        store_update.set_ser(DBCol::ColTransactionResult, id.as_ref(), &outcome)?;
    }
    Ok(())
}

pub fn migrate_12_to_13(path: &Path, near_config: &NearConfig) {
    let store = create_store(path);
    if !near_config.client_config.archive {
        // Non archival node. Perform a simply migration without necessarily fixing the inconsistencies
        // since the old data will be garbage collected in five epochs
        let mut store_update = store.store_update();
        for (key, value) in store.iter_without_rc_logic(DBCol::ColTransactionResult) {
            let outcome = ExecutionOutcomeWithIdAndProof::try_from_slice(&value).unwrap();
            store_update.set_ser(DBCol::ColTransactionResult, &key, &vec![outcome]).unwrap();
        }
        store_update.commit().unwrap();
    } else {
        // archival node. Fix the inconsistencies by re-applying the entire history.
        let genesis_height = near_config.genesis.config.genesis_height;
        let mut chain_store = ChainStore::new(store.clone(), genesis_height);
        let head = chain_store.head().expect("head must exist");
        let runtime = NightshadeRuntime::with_config(path, store.clone(), near_config, None, None);
        let mut store_update = store.store_update();
        store_update.delete_all(DBCol::ColTransactionResult);
        store_update.commit().unwrap();

        let mut cur_height = genesis_height;
        while cur_height <= head.height {
            let mut store_update = store.store_update();
            for height in cur_height..std::cmp::min(cur_height + 10000, head.height + 1) {
                if let Err(e) =
                    apply_block_at_height(&mut store_update, &mut chain_store, &runtime, height, 0)
                {
                    match e.kind() {
                        near_chain::ErrorKind::DBNotFoundErr(_) => continue,
                        _ => panic!("unexpected error during migration, {}", e),
                    }
                }
            }
            cur_height += 10000;
            store_update.commit().unwrap();
        }
    }
    set_store_version(&store, 13);
}

pub fn migrate_18_to_19(path: &Path, near_config: &NearConfig) {
    use near_primitives::types::EpochId;
    let store = create_store(path);
    if near_config.client_config.archive {
        let genesis_height = near_config.genesis.config.genesis_height;
        let mut chain_store = ChainStore::new(store.clone(), genesis_height);
        let epoch_config = EpochConfig::from(&near_config.genesis.config);
        let mut epoch_manager = EpochManager::new(
            store.clone(),
            AllEpochConfig::new(epoch_config, None),
            near_config.genesis.config.protocol_version,
            RewardCalculator::new(&near_config.genesis.config),
            near_config.genesis.config.validators(),
        )
        .unwrap();
        for (key, value) in store.iter(DBCol::ColEpochStart) {
            let epoch_id = EpochId::try_from_slice(&key).unwrap();
            let epoch_start_height = u64::try_from_slice(&value).unwrap();
            // This is a temporary workaround due to https://github.com/near/nearcore/issues/4243
            let mut counter = 0;
            let mut check_height = |height: u64| -> bool {
                if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
                    let block_header = chain_store.get_block_header(&block_hash).unwrap().clone();
                    let prev_block_epoch_id = {
                        if let Ok(block_header) = chain_store.get_previous_header(&block_header) {
                            block_header.epoch_id().clone()
                        } else {
                            EpochId::default()
                        }
                    };
                    if block_header.epoch_id() == &epoch_id
                        && (prev_block_epoch_id != epoch_id || epoch_id == EpochId::default())
                    {
                        return true;
                    }
                }
                false
            };
            let real_epoch_start_height = loop {
                let height1 = epoch_start_height + counter;
                let height2 = epoch_start_height - counter;
                if check_height(height1) {
                    break height1;
                }
                if check_height(height2) {
                    break height2;
                }
                counter += 1;
            };
            let block_hash = chain_store.get_block_hash_by_height(real_epoch_start_height).unwrap();
            let block_header = chain_store.get_block_header(&block_hash).unwrap().clone();
            if let Ok(prev_header) =
                chain_store.get_previous_header(&block_header).map(Clone::clone)
            {
                let last_finalized_height = chain_store
                    .get_block_header(prev_header.last_final_block())
                    .map(|h| h.height())
                    .unwrap_or(genesis_height);
                let mut store_update = store.store_update();
                epoch_manager
                    .migrate_18_to_19(
                        &BlockHeaderInfo::new(&prev_header, last_finalized_height),
                        &mut store_update,
                    )
                    .unwrap();
                store_update.commit().unwrap();
            }
        }
    }
    set_store_version(&store, 19);
}

pub fn migrate_19_to_20(path: &Path, near_config: &NearConfig) {
    let store = create_store(path);
    if near_config.client_config.archive && &near_config.genesis.config.chain_id == "mainnet" {
        let genesis_height = near_config.genesis.config.genesis_height;
        let mut chain_store = ChainStore::new(store.clone(), genesis_height);
        let head = chain_store.head().unwrap();
        let runtime = NightshadeRuntime::with_config(path, store.clone(), near_config, None, None);
        let shard_id = 0;
        let shard_uid = ShardUId::single_shard();
        // This is hardcoded for mainnet specifically. Blocks with lower heights have been checked.
        let start_height = 34691244;
        for block_height in start_height..=head.height {
            if let Ok(block_hash) = chain_store.get_block_hash_by_height(block_height) {
                let block = chain_store.get_block(&block_hash).unwrap().clone();
                if block.chunks()[shard_id as usize].height_included() != block.header().height() {
                    let mut chain_store_update = ChainStoreUpdate::new(&mut chain_store);
                    let new_extra = chain_store_update
                        .get_chunk_extra(block.header().prev_hash(), &shard_uid)
                        .unwrap()
                        .clone();

                    let apply_result = runtime
                        .apply_transactions(
                            shard_id,
                            new_extra.state_root(),
                            block.header().height(),
                            block.header().raw_timestamp(),
                            block.header().prev_hash(),
                            block.hash(),
                            &[],
                            &[],
                            new_extra.validator_proposals(),
                            block.header().gas_price(),
                            new_extra.gas_limit(),
                            block.header().challenges_result(),
                            *block.header().random_value(),
                            // doesn't really matter here since the old blocks are on the old version
                            false,
                            false,
                            None,
                        )
                        .unwrap();
                    if !apply_result.outcomes.is_empty() {
                        let (_, outcome_paths) =
                            ApplyTransactionResult::compute_outcomes_proof(&apply_result.outcomes);
                        chain_store_update.save_outcomes_with_proofs(
                            block.hash(),
                            shard_id,
                            apply_result.outcomes,
                            outcome_paths,
                        );
                        chain_store_update.commit().unwrap();
                    }
                }
            }
        }
    }
    set_store_version(&store, 20);
}

/// This is a one time patch to fix an existing issue in mainnet database
/// (<https://github.com/near/near-indexer-for-explorer/issues/110>)
pub fn migrate_22_to_23(path: &Path, near_config: &NearConfig) {
    let store = create_store(path);
    if near_config.client_config.archive && &near_config.genesis.config.chain_id == "mainnet" {
        let genesis_height = near_config.genesis.config.genesis_height;
        let mut chain_store = ChainStore::new(store.clone(), genesis_height);
        let runtime = NightshadeRuntime::with_config(path, store.clone(), near_config, None, None);
        let shard_id = 0;
        // This is hardcoded for mainnet specifically. Blocks with lower heights have been checked.
        let block_heights = vec![22633807];
        for height in block_heights {
            let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
            let block = chain_store.get_block(&block_hash).unwrap().clone();
            if block.chunks()[shard_id as usize].height_included() == block.header().height() {
                let chunk_hash = block.chunks()[shard_id as usize].chunk_hash();
                let chunk = chain_store.get_chunk(&chunk_hash).unwrap().clone();
                let chunk_header = chunk.cloned_header();
                let mut chain_store_update = ChainStoreUpdate::new(&mut chain_store);
                let prev_block =
                    chain_store_update.get_block(block.header().prev_hash()).unwrap().clone();
                let receipt_proof_response = chain_store_update
                    .get_incoming_receipts_for_shard(
                        shard_id,
                        block_hash,
                        prev_block.chunks()[shard_id as usize].height_included(),
                    )
                    .unwrap();
                let receipts = collect_receipts_from_response(&receipt_proof_response);

                let apply_result = runtime
                    .apply_transactions(
                        shard_id,
                        &chunk_header.prev_state_root(),
                        block.header().height(),
                        block.header().raw_timestamp(),
                        block.header().prev_hash(),
                        block.hash(),
                        &receipts,
                        chunk.transactions(),
                        chunk_header.validator_proposals(),
                        block.header().gas_price(),
                        chunk_header.gas_limit(),
                        block.header().challenges_result(),
                        *block.header().random_value(),
                        true,
                        false,
                        None,
                    )
                    .unwrap();
                if !apply_result.outcomes.is_empty() {
                    let (_, outcome_paths) =
                        ApplyTransactionResult::compute_outcomes_proof(&apply_result.outcomes);
                    chain_store_update.save_outcomes_with_proofs(
                        block.hash(),
                        shard_id,
                        apply_result.outcomes,
                        outcome_paths,
                    );
                    chain_store_update.commit().unwrap();
                }
            }
        }
    }
    set_store_version(&store, 23);
}

lazy_static_include::lazy_static_include_bytes! {
    /// File with receipts which were lost because of a bug in apply_chunks to the runtime config.
    /// Follows the ReceiptResult format which is HashMap<ShardId, Vec<Receipt>>.
    /// See https://github.com/near/nearcore/pull/4248/ for more details.
    MAINNET_RESTORED_RECEIPTS => "res/mainnet_restored_receipts.json",
}

/// Put receipts restored in scope of issue
/// <https://github.com/near/nearcore/pull/4248> to storage.
pub fn migrate_23_to_24(path: &Path, near_config: &NearConfig) {
    let store = create_store(path);
    if &near_config.genesis.config.chain_id == "mainnet" {
        let mut store_update = store.store_update();
        let restored_receipts: ReceiptResult = serde_json::from_slice(&MAINNET_RESTORED_RECEIPTS)
            .expect("File with receipts restored after apply_chunks fix have to be correct");
        for receipt in restored_receipts.get(&0u64).unwrap().iter() {
            let bytes = receipt.try_to_vec().expect("Borsh cannot fail");
            store_update.update_refcount(ColReceipts, receipt.get_hash().as_ref(), &bytes, 1);
        }
        store_update.commit().unwrap();
    }
    set_store_version(&store, 24);
}

pub fn migrate_24_to_25(path: &Path) {
    use smart_default::SmartDefault;

    let store = create_store(path);

    #[derive(BorshSerialize, BorshDeserialize, PartialEq, Clone, SmartDefault, Eq, Debug)]
    pub struct OldExecutionOutcome {
        pub logs: Vec<LogEntry>,
        pub receipt_ids: Vec<CryptoHash>,
        pub gas_burnt: Gas,
        pub tokens_burnt: Balance,
        #[default("test".parse().unwrap())]
        pub executor_id: AccountId,
        pub status: ExecutionStatus,
    }

    #[derive(PartialEq, Clone, Default, Debug, BorshSerialize, BorshDeserialize, Eq)]
    pub struct OldExecutionOutcomeWithId {
        pub id: CryptoHash,
        pub outcome: OldExecutionOutcome,
    }

    #[derive(PartialEq, Clone, Default, Debug, BorshSerialize, BorshDeserialize, Eq)]
    pub struct OldExecutionOutcomeWithIdAndProof {
        pub proof: MerklePath,
        pub block_hash: CryptoHash,
        pub outcome_with_id: OldExecutionOutcomeWithId,
    }

    #[derive(PartialEq, Clone, Default, Debug, BorshSerialize, BorshDeserialize, Eq)]
    // In the case of old execution outcome hadn't migrated and failed to deserialize (inside
    // error enums in ExecutionOutcome, so we can still get the first block_hash)
    pub struct PartialExecutionOutcomeWithIdAndProof {
        pub proof: MerklePath,
        pub block_hash: CryptoHash,
    }

    impl Into<ExecutionOutcomeWithIdAndProof> for OldExecutionOutcomeWithIdAndProof {
        fn into(self) -> ExecutionOutcomeWithIdAndProof {
            ExecutionOutcomeWithIdAndProof {
                proof: self.proof,
                block_hash: self.block_hash,
                outcome_with_id: ExecutionOutcomeWithId {
                    id: self.outcome_with_id.id,
                    outcome: ExecutionOutcome {
                        logs: self.outcome_with_id.outcome.logs,
                        receipt_ids: self.outcome_with_id.outcome.receipt_ids,
                        gas_burnt: self.outcome_with_id.outcome.gas_burnt,
                        tokens_burnt: self.outcome_with_id.outcome.tokens_burnt,
                        executor_id: self.outcome_with_id.outcome.executor_id,
                        status: self.outcome_with_id.outcome.status,
                        metadata: ExecutionMetadata::V1,
                    },
                },
            }
        }
    }

    let mut store_update = BatchedStoreUpdate::new(&store, 10_000_000);
    for (key, value) in store.iter(DBCol::ColTransactionResult) {
        if Vec::<ExecutionOutcomeWithIdAndProof>::try_from_slice(&value).is_ok() {
            // has success in previous attempt of this migration
            continue;
        }

        let old_outcomes = Vec::<OldExecutionOutcomeWithIdAndProof>::try_from_slice(&value);
        let old_outcomes = match old_outcomes {
            Ok(old_outcomes) => old_outcomes,
            _ => {
                // try_from_slice will not success if there's remaining bytes, so it must be exactly one OldExecutionOutcomeWithIdAndProof
                if let Ok(old_outcome) = OldExecutionOutcomeWithIdAndProof::try_from_slice(&value) {
                    println!(
                        "! Format Changed. Check outcome id, block hash for indexer: {},{}",
                        old_outcome.outcome_with_id.id, old_outcome.block_hash
                    );
                    vec![old_outcome]
                } else {
                    let mut v2: Vec<u8> = value.clone().into();
                    // We investigate how deserialization went wrong, by manually borsh deserialize struct field-by-field and found
                    // the problem comes from https://github.com/near/nearcore/commit/6c3c2f7475a5e8c258a39ed94f11f6a8a7b2108e, where
                    // where MethodUTF8Error was dropped without a migration, which lead to deser index 3 of MethodResolveError
                    // error. We compare bytes here to try recovery this case and still panic in case we hit other cases. This only
                    // happened at testnet archival node.
                    if &v2[0..4] == [1, 0, 0, 0] {
                        // ensure there's one execution outcome
                        if &v2[v2.len() - 3..] == [12, 2, 3] {
                            // FunctionCallError (12), MethodResolveError (2), MethodInvalidSignature (used to be 3)
                            let last = v2.len() - 1;
                            v2[last] = 2; // MethodInvalidSignature error is now at index 2.
                            let old_outcomes =
                                Vec::<OldExecutionOutcomeWithIdAndProof>::try_from_slice(&v2)
                                    .unwrap();
                            println!(
                                "! Byte Changed. Check outcome id, block hash for indexer: {},{}",
                                old_outcomes[0].outcome_with_id.id, old_outcomes[0].block_hash
                            );
                            old_outcomes
                        } else {
                            unimplemented!(
                                "Unknown corruption for outcome: {}",
                                CryptoHash::try_from(key.as_ref()).unwrap()
                            );
                        }
                    } else {
                        unimplemented!("More than one execution outcome not supported",);
                    }
                }
            }
        };
        let outcomes: Vec<ExecutionOutcomeWithIdAndProof> =
            old_outcomes.into_iter().map(|outcome| outcome.into()).collect();
        store_update
            .set_ser(DBCol::ColTransactionResult, key.as_ref(), &outcomes)
            .expect("BorshSerialize should not fail");
    }
    store_update.finish().expect("Failed to migrate");

    set_store_version(&store, 25);
}

/// Fix an issue with block ordinal (#5761)
// This migration takes at least 3 hours to complete on mainnet
pub fn migrate_30_to_31(path: &Path, near_config: &NearConfig) {
    let store = create_store(path);
    if near_config.client_config.archive && &near_config.genesis.config.chain_id == "mainnet" {
        let genesis_height = near_config.genesis.config.genesis_height;
        let mut chain_store = ChainStore::new(store.clone(), genesis_height);
        let head = chain_store.head().unwrap();
        let mut store_update = BatchedStoreUpdate::new(&store, 10_000_000);
        let mut count = 0;
        // we manually checked mainnet archival data and the first block where the discrepancy happened is `47443088`.
        for height in 47443088..=head.height {
            if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
                let block_ordinal = chain_store.get_block_merkle_tree(&block_hash).unwrap().size();
                let block_hash_from_block_ordinal =
                    chain_store.get_block_hash_from_ordinal(block_ordinal).unwrap();
                if *block_hash_from_block_ordinal != block_hash {
                    println!("Inconsistency in block ordinal to block hash mapping found at block height {}", height);
                    count += 1;
                    store_update
                        .set_ser(
                            DBCol::ColBlockOrdinal,
                            &index_to_bytes(block_ordinal),
                            &block_hash,
                        )
                        .expect("BorshSerialize should not fail");
                }
            }
        }
        println!("total inconsistency count: {}", count);
        store_update.finish().expect("Failed to migrate");
    }
    set_store_version(&store, 31);
}

lazy_static_include::lazy_static_include_bytes! {
    /// File with account ids and deltas that need to be applied in order to fix storage usage
    /// difference between actual and stored usage, introduced due to bug in access key deletion,
    /// see https://github.com/near/nearcore/issues/3824
    /// This file was generated using tools/storage-usage-delta-calculator
    MAINNET_STORAGE_USAGE_DELTA => "res/storage_usage_delta.json",
}

/// In test runs reads and writes here used 442 TGas, but in test on live net migration take
/// between 4 and 4.5s. We do not want to process any receipts in this block
const GAS_USED_FOR_STORAGE_USAGE_DELTA_MIGRATION: Gas = 1_000_000_000_000_000;

pub fn load_migration_data(chain_id: &str) -> MigrationData {
    let is_mainnet = chain_id == "mainnet";
    MigrationData {
        storage_usage_delta: if is_mainnet {
            serde_json::from_slice(&MAINNET_STORAGE_USAGE_DELTA).unwrap()
        } else {
            Vec::new()
        },
        storage_usage_fix_gas: if is_mainnet {
            GAS_USED_FOR_STORAGE_USAGE_DELTA_MIGRATION
        } else {
            0
        },
        restored_receipts: if is_mainnet {
            serde_json::from_slice(&MAINNET_RESTORED_RECEIPTS)
                .expect("File with receipts restored after apply_chunks fix have to be correct")
        } else {
            ReceiptResult::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::hash::hash;
    use near_primitives::serialize::to_base;

    #[test]
    fn test_migration_data() {
        assert_eq!(
            to_base(&hash(&MAINNET_STORAGE_USAGE_DELTA)),
            "6CFkdSZZVj4v83cMPD3z6Y8XSQhDh3EQjFh3PRAqFEAx"
        );
        let mainnet_migration_data = load_migration_data("mainnet");
        assert_eq!(mainnet_migration_data.storage_usage_delta.len(), 3112);
        let testnet_migration_data = load_migration_data("testnet");
        assert_eq!(testnet_migration_data.storage_usage_delta.len(), 0);
    }

    #[test]
    fn test_restored_receipts_data() {
        assert_eq!(
            to_base(&hash(&MAINNET_RESTORED_RECEIPTS)),
            "3ZHK51a2zVnLnG8Pq1y7fLaEhP9SGU1CGCmspcBUi5vT"
        );
        let mainnet_migration_data = load_migration_data("mainnet");
        assert_eq!(mainnet_migration_data.restored_receipts.get(&0u64).unwrap().len(), 383);
        let testnet_migration_data = load_migration_data("testnet");
        assert!(testnet_migration_data.restored_receipts.is_empty());
    }
}
