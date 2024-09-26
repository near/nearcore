use crate::entity_debug_serializer::serialize_entity;
use anyhow::{anyhow, Context};

use borsh::BorshDeserialize;
use near_chain::types::{LatestKnown, RuntimeAdapter};
use near_chain::{Block, BlockHeader};
use near_epoch_manager::types::EpochInfoAggregator;
use near_epoch_manager::EpochManagerAdapter;
use near_jsonrpc_primitives::errors::RpcError;
use near_jsonrpc_primitives::types::entity_debug::{
    EntityDataStruct, EntityDataValue, EntityDebugHandler, EntityQuery, EntityQueryWithParams,
};
use near_primitives::block::Tip;
use near_primitives::challenge::{PartialState, TrieValue};
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::get_block_shard_uid;
use near_primitives::sharding::ShardChunk;
use near_primitives::state::FlatStateValue;
use near_primitives::state_sync::StateSyncDumpProgress;
use near_primitives::stateless_validation::stored_chunk_state_transition_data::StoredChunkStateTransitionData;
use near_primitives::transaction::{ExecutionOutcomeWithProof, SignedTransaction};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, Balance, BlockHeight, StateRoot};
use near_primitives::utils::{get_block_shard_id, get_outcome_id_block_hash};
use near_primitives::views::{
    BlockHeaderView, BlockView, ChunkView, ExecutionOutcomeView, ReceiptView, SignedTransactionView,
};
use near_store::adapter::flat_store::encode_flat_state_db_key;
use near_store::adapter::trie_store::get_key_from_shard_uid_and_hash;
use near_store::db::GENESIS_CONGESTION_INFO_KEY;
use near_store::flat::delta::KeyForFlatStateDelta;
use near_store::flat::{FlatStateChanges, FlatStateDeltaMetadata, FlatStorageStatus};
use near_store::{
    DBCol, NibbleSlice, RawTrieNode, RawTrieNodeWithSize, ShardUId, Store, CHUNK_TAIL_KEY,
    COLD_HEAD_KEY, FINAL_HEAD_KEY, FORK_TAIL_KEY, GENESIS_JSON_HASH_KEY, GENESIS_STATE_ROOTS_KEY,
    HEADER_HEAD_KEY, HEAD_KEY, LARGEST_TARGET_HEIGHT_KEY, LATEST_KNOWN_KEY, STATE_SNAPSHOT_KEY,
    STATE_SYNC_DUMP_KEY, TAIL_KEY,
};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct EntityDebugHandlerImpl {
    pub epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub runtime: Arc<dyn RuntimeAdapter>,
    pub hot_store: Store,
    pub cold_store: Option<Store>,
}

impl EntityDebugHandlerImpl {
    fn query_impl(&self, store: Store, query: EntityQuery) -> anyhow::Result<EntityDataValue> {
        match query {
            EntityQuery::AllShardsByEpochId { epoch_id } => {
                let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
                Ok(serialize_entity(&shard_layout.shard_uids().collect::<Vec<_>>()))
            }
            EntityQuery::BlockByHash { block_hash } => {
                let block = store
                    .get_ser::<Block>(DBCol::Block, &borsh::to_vec(&block_hash).unwrap())?
                    .ok_or_else(|| anyhow!("Block not found"))?;
                let author = self
                    .epoch_manager
                    .get_block_producer(block.header().epoch_id(), block.header().height())?;
                let mut ret =
                    serialize_entity(&BlockView::from_author_block(author, block.clone()));
                if let EntityDataValue::Struct(inner) = &mut ret {
                    inner.add("chunk_endorsements", serialize_entity(block.chunk_endorsements()));
                }
                Ok(ret)
            }
            EntityQuery::BlockHashByHeight { block_height } => {
                let block_hash = store
                    .get_ser::<CryptoHash>(
                        DBCol::BlockHeight,
                        &borsh::to_vec(&block_height).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Block height not found"))?;
                Ok(serialize_entity(&block_hash))
            }
            EntityQuery::BlockHeaderByHash { block_hash } => {
                let block_header = store
                    .get_ser::<BlockHeader>(
                        DBCol::BlockHeader,
                        &borsh::to_vec(&block_hash).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Block header not found"))?;
                Ok(serialize_entity(&BlockHeaderView::from(block_header)))
            }
            EntityQuery::BlockInfoByHash { block_hash } => {
                let block_info = store
                    .get_ser::<BlockInfo>(DBCol::BlockInfo, &borsh::to_vec(&block_hash).unwrap())?
                    .ok_or_else(|| anyhow!("BlockInfo not found"))?;
                Ok(serialize_entity(&block_info))
            }
            EntityQuery::BlockMerkleTreeByHash { block_hash } => {
                let block_merkle_tree = store
                    .get_ser::<PartialMerkleTree>(
                        DBCol::BlockMerkleTree,
                        &borsh::to_vec(&block_hash).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Block merkle tree not found"))?;
                Ok(serialize_entity(&block_merkle_tree))
            }
            EntityQuery::BlockMisc(()) => {
                let block_misc = BlockMiscData::from_store(&store)?;
                Ok(serialize_entity(&block_misc))
            }
            EntityQuery::ChunkByHash { chunk_hash } => {
                let chunk = store
                    .get_ser::<ShardChunk>(DBCol::Chunks, &borsh::to_vec(&chunk_hash).unwrap())?
                    .ok_or_else(|| anyhow!("Chunk not found"))?;
                let epoch_id =
                    self.epoch_manager.get_epoch_id_from_prev_block(chunk.prev_block())?;
                let author = self.epoch_manager.get_chunk_producer(
                    &epoch_id,
                    chunk.height_created(),
                    chunk.shard_id(),
                )?;
                Ok(serialize_entity(&ChunkView::from_author_chunk(author, chunk)))
            }
            EntityQuery::ChunkExtraByBlockHashShardUId { block_hash, shard_uid } => {
                let chunk_extra = store
                    .get_ser::<ChunkExtra>(
                        DBCol::ChunkExtra,
                        &get_block_shard_uid(&block_hash, &shard_uid),
                    )?
                    .ok_or_else(|| anyhow!("Chunk extra not found"))?;
                Ok(serialize_entity(&chunk_extra))
            }
            EntityQuery::ChunkExtraByChunkHash { chunk_hash } => {
                let chunk = store
                    .get_ser::<ShardChunk>(DBCol::Chunks, &borsh::to_vec(&chunk_hash).unwrap())?
                    .ok_or_else(|| anyhow!("Chunk not found"))?;
                let block_hash = store
                    .get_ser::<CryptoHash>(
                        DBCol::BlockHeight,
                        &borsh::to_vec(&chunk.height_included()).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Cannot find block at chunk's height"))?;
                let epoch_id =
                    self.epoch_manager.get_epoch_id_from_prev_block(chunk.prev_block())?;
                let shard_id = chunk.shard_id();
                let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, &epoch_id)?;
                let chunk_extra = store
                    .get_ser::<ChunkExtra>(
                        DBCol::ChunkExtra,
                        &get_block_shard_uid(&block_hash, &shard_uid),
                    )?
                    .ok_or_else(|| anyhow!("Chunk extra not found"))?;
                Ok(serialize_entity(&chunk_extra))
            }
            EntityQuery::EpochInfoAggregator(()) => {
                let aggregator = store
                    .get_ser::<EpochInfoAggregator>(DBCol::EpochInfo, AGGREGATOR_KEY)?
                    .ok_or_else(|| anyhow!("Aggregator not found"))?;
                Ok(serialize_entity(&aggregator))
            }
            EntityQuery::EpochInfoByEpochId { epoch_id } => {
                let epoch_info = self.epoch_manager.get_epoch_info(&epoch_id)?;
                Ok(serialize_entity(&*epoch_info))
            }
            EntityQuery::FlatStateByTrieKey { trie_key, shard_uid } => {
                let state = store
                    .get_ser::<FlatStateValue>(
                        DBCol::FlatState,
                        &encode_flat_state_db_key(shard_uid, &hex::decode(&trie_key)?),
                    )?
                    .ok_or_else(|| anyhow!("Flat state not found"))?;
                let data = self.deref_flat_state_value(&store, state, shard_uid)?;
                Ok(serialize_entity(&hex::encode(&data)))
            }
            EntityQuery::FlatStateChangesByBlockHash { block_hash, shard_uid } => {
                let changes = store
                    .get_ser::<FlatStateChanges>(
                        DBCol::FlatStateChanges,
                        &borsh::to_vec(&KeyForFlatStateDelta { block_hash, shard_uid }).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Flat state changes not found"))?;
                let mut changes_view = Vec::new();
                for (key, value) in changes.0.into_iter() {
                    let key = hex::encode(&key);
                    let value = match value {
                        Some(v) => {
                            Some(hex::encode(&self.deref_flat_state_value(&store, v, shard_uid)?))
                        }
                        None => None,
                    };
                    changes_view.push(FlatStateChangeView { key, value });
                }
                Ok(serialize_entity(&changes_view))
            }
            EntityQuery::FlatStateDeltaMetadataByBlockHash { block_hash, shard_uid } => {
                let metadata = store
                    .get_ser::<FlatStateDeltaMetadata>(
                        DBCol::FlatStateDeltaMetadata,
                        &borsh::to_vec(&KeyForFlatStateDelta { block_hash, shard_uid }).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Flat state delta metadata not found"))?;
                Ok(serialize_entity(&metadata))
            }
            EntityQuery::FlatStorageStatusByShardUId { shard_uid } => {
                let status = store
                    .get_ser::<FlatStorageStatus>(
                        DBCol::FlatStorageStatus,
                        &borsh::to_vec(&shard_uid).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Flat storage status not found"))?;
                Ok(serialize_entity(&status))
            }
            EntityQuery::NextBlockHashByHash { block_hash } => {
                let next_block_hash = store
                    .get_ser::<CryptoHash>(
                        DBCol::NextBlockHashes,
                        &borsh::to_vec(&block_hash).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Next block hash not found"))?;
                Ok(serialize_entity(&next_block_hash))
            }
            EntityQuery::OutcomeByTransactionHash { transaction_hash: outcome_id }
            | EntityQuery::OutcomeByReceiptId { receipt_id: outcome_id } => {
                let (_, outcome) = store
                    .iter_prefix_ser::<ExecutionOutcomeWithProof>(
                        DBCol::TransactionResultForBlock,
                        &borsh::to_vec(&outcome_id).unwrap(),
                    )
                    .next()
                    .ok_or_else(|| anyhow!("Outcome not found"))??;
                Ok(serialize_entity(&ExecutionOutcomeView::from(outcome.outcome)))
            }
            EntityQuery::OutcomeByTransactionHashAndBlockHash {
                transaction_hash: outcome_id,
                block_hash,
            }
            | EntityQuery::OutcomeByReceiptIdAndBlockHash { receipt_id: outcome_id, block_hash } => {
                let outcome = store
                    .get_ser::<ExecutionOutcomeWithProof>(
                        DBCol::TransactionResultForBlock,
                        &get_outcome_id_block_hash(&outcome_id, &block_hash),
                    )?
                    .ok_or_else(|| anyhow!("Outcome not found"))?;
                Ok(serialize_entity(&ExecutionOutcomeView::from(outcome.outcome)))
            }
            EntityQuery::RawTrieNodeByHash { trie_node_hash, shard_uid } => {
                let node = store
                    .get_ser::<RawTrieNodeWithSize>(
                        DBCol::State,
                        &get_key_from_shard_uid_and_hash(shard_uid, &trie_node_hash),
                    )?
                    .ok_or_else(|| anyhow!("Trie node not found"))?;
                Ok(serialize_raw_trie_node(node))
            }
            EntityQuery::RawTrieRootByChunkHash { chunk_hash } => {
                let chunk = store
                    .get_ser::<ShardChunk>(DBCol::Chunks, &borsh::to_vec(&chunk_hash).unwrap())?
                    .ok_or_else(|| anyhow!("Chunk not found"))?;
                let shard_layout = self
                    .epoch_manager
                    .get_shard_layout_from_prev_block(&chunk.cloned_header().prev_block_hash())?;
                let shard_uid = shard_layout
                    .shard_uids()
                    .nth(chunk.shard_id() as usize)
                    .ok_or_else(|| anyhow!("Shard {} not found", chunk.shard_id()))?;
                let node = store
                    .get_ser::<RawTrieNodeWithSize>(
                        DBCol::State,
                        &get_key_from_shard_uid_and_hash(shard_uid, &chunk.prev_state_root()),
                    )?
                    .ok_or_else(|| anyhow!("State root not found"))?;
                Ok(serialize_raw_trie_node(node))
            }
            EntityQuery::RawTrieValueByHash { trie_value_hash, shard_uid } => {
                let value = store
                    .get(
                        DBCol::State,
                        &get_key_from_shard_uid_and_hash(shard_uid, &trie_value_hash),
                    )?
                    .ok_or_else(|| anyhow!("Trie value not found"))?;
                Ok(serialize_entity(&hex::encode(value.as_slice())))
            }
            EntityQuery::ReceiptById { receipt_id } => {
                let receipt = store
                    .get_ser::<Receipt>(DBCol::Receipts, &borsh::to_vec(&receipt_id).unwrap())?
                    .ok_or_else(|| anyhow!("Receipt not found"))?;
                Ok(serialize_entity(&ReceiptView::from(receipt)))
            }
            EntityQuery::ShardIdByAccountId { account_id, epoch_id } => {
                let shard_id =
                    self.epoch_manager.account_id_to_shard_id(&account_id.parse()?, &epoch_id)?;
                Ok(serialize_entity(&shard_id))
            }
            EntityQuery::ShardLayoutByEpochId { epoch_id } => {
                let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
                Ok(serialize_entity(&shard_layout))
            }
            EntityQuery::ShardUIdByShardId { shard_id, epoch_id } => {
                let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
                let shard_uid = shard_layout
                    .shard_uids()
                    .nth(shard_id as usize)
                    .ok_or_else(|| anyhow!("Shard {} not found", shard_id))?;
                Ok(serialize_entity(&shard_uid))
            }
            EntityQuery::StateTransitionData { block_hash } => {
                let block = store
                    .get_ser::<Block>(DBCol::Block, &borsh::to_vec(&block_hash).unwrap())?
                    .ok_or_else(|| anyhow!("Block not found"))?;
                let epoch_id = block.header().epoch_id();
                let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
                let shard_ids = shard_layout.shard_ids().collect::<Vec<_>>();
                let mut state_transitions = EntityDataStruct::new();
                for shard_id in shard_ids {
                    let state_transition = store
                        .get_ser::<StoredChunkStateTransitionData>(
                            DBCol::StateTransitionData,
                            &get_block_shard_id(&block_hash, shard_id),
                        )?
                        .ok_or_else(|| anyhow!("State transition not found"))?;
                    let mut serialized = EntityDataStruct::new();
                    serialized.add(
                        "base_state",
                        PartialStateParser::parse_and_serialize_partial_state(
                            state_transition.base_state,
                        ),
                    );
                    serialized
                        .add("receipts_hash", serialize_entity(&state_transition.receipts_hash));
                    state_transitions
                        .add(&shard_id.to_string(), EntityDataValue::Struct(serialized.into()));
                }
                Ok(EntityDataValue::Struct(state_transitions.into()))
            }
            EntityQuery::TipAtFinalHead(_) => {
                let tip = store
                    .get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?
                    .ok_or_else(|| anyhow!("Tip not found"))?;
                Ok(serialize_entity(&tip))
            }
            EntityQuery::TipAtHead(_) => {
                let tip = store
                    .get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?
                    .ok_or_else(|| anyhow!("Tip not found"))?;
                Ok(serialize_entity(&tip))
            }
            EntityQuery::TipAtHeaderHead(_) => {
                let tip = store
                    .get_ser::<Tip>(DBCol::BlockMisc, HEADER_HEAD_KEY)?
                    .ok_or_else(|| anyhow!("Tip not found"))?;
                Ok(serialize_entity(&tip))
            }
            EntityQuery::TransactionByHash { transaction_hash } => {
                let transaction = store
                    .get_ser::<SignedTransaction>(
                        DBCol::Transactions,
                        &borsh::to_vec(&transaction_hash).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Transaction not found"))?;
                Ok(serialize_entity(&SignedTransactionView::from(transaction)))
            }
            EntityQuery::TrieNode { trie_path } => {
                let trie_path =
                    TriePath::parse(trie_path).ok_or_else(|| anyhow!("Invalid path"))?;
                let trie = self
                    .runtime
                    .get_tries()
                    .get_trie_for_shard(trie_path.shard_uid, trie_path.state_root);
                let node = trie
                    .debug_get_node(&trie_path.path)?
                    .ok_or_else(|| anyhow!("Node not found"))?;
                serialize_trie_node(trie_path, node, trie)
            }
            EntityQuery::TrieRootByChunkHash { chunk_hash } => {
                let chunk = store
                    .get_ser::<ShardChunk>(DBCol::Chunks, &borsh::to_vec(&chunk_hash).unwrap())?
                    .ok_or_else(|| anyhow!("Chunk not found"))?;
                let shard_layout = self
                    .epoch_manager
                    .get_shard_layout_from_prev_block(&chunk.cloned_header().prev_block_hash())?;
                let shard_uid = shard_layout
                    .shard_uids()
                    .nth(chunk.shard_id() as usize)
                    .ok_or_else(|| anyhow!("Shard {} not found", chunk.shard_id()))?;
                let path =
                    TriePath { path: vec![], shard_uid, state_root: chunk.prev_state_root() };
                Ok(serialize_entity(&path.to_string()))
            }
            EntityQuery::TrieRootByStateRoot { state_root, shard_uid } => {
                let path = TriePath { path: vec![], shard_uid, state_root };
                Ok(serialize_entity(&path.to_string()))
            }
            EntityQuery::ValidatorAssignmentsAtHeight { block_height, epoch_id } => {
                let block_producer = self
                    .epoch_manager
                    .get_block_producer(&epoch_id, block_height)
                    .context("Getting block producer")?;
                let shard_layout = self
                    .epoch_manager
                    .get_shard_layout(&epoch_id)
                    .context("Getting shard layout")?;
                let chunk_producers = shard_layout
                    .shard_ids()
                    .map(|shard_id| {
                        self.epoch_manager
                            .get_chunk_producer(&epoch_id, block_height, shard_id)
                            .context("Getting chunk producer")
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let chunk_validator_assignments = shard_layout
                    .shard_ids()
                    .map(|shard_id| {
                        self.epoch_manager
                            .get_chunk_validator_assignments(&epoch_id, shard_id, block_height)
                            .context("Getting chunk validator assignments")
                            .map(|assignments| {
                                assignments
                                    .assignments()
                                    .iter()
                                    .cloned()
                                    .map(|(account_id, stake)| OneValidatorAssignment {
                                        account_id,
                                        stake,
                                    })
                                    .collect::<Vec<_>>()
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let ret = ValidatorAssignmentsAtHeight {
                    block_producer,
                    chunk_producers,
                    chunk_validator_assignments,
                };
                Ok(serialize_entity(&ret))
            }
        }
    }

    fn deref_flat_state_value(
        &self,
        store: &Store,
        state: FlatStateValue,
        shard_uid: ShardUId,
    ) -> anyhow::Result<Vec<u8>> {
        Ok(match state {
            FlatStateValue::Ref(value) => store
                .get(DBCol::State, &get_key_from_shard_uid_and_hash(shard_uid, &value.hash))?
                .ok_or_else(|| anyhow!("ValueRef could not be dereferenced"))?
                .to_vec(),
            FlatStateValue::Inlined(data) => data,
        })
    }
}

fn serialize_trie_node(
    trie_path: TriePath,
    node: RawTrieNode,
    trie: near_store::Trie,
) -> Result<EntityDataValue, anyhow::Error> {
    let mut entity_data = EntityDataStruct::new();
    entity_data.add_string("path", &TriePath::nibbles_to_hex(&trie_path.path));
    match node {
        near_store::RawTrieNode::Leaf(extension, value) => {
            let extension_nibbles = NibbleSlice::from_encoded(&extension);
            let leaf_nibbles = trie_path
                .path
                .iter()
                .copied()
                .chain(extension_nibbles.0.iter())
                .collect::<Vec<_>>();
            let data = trie.retrieve_value(&value.hash)?;
            entity_data.add_string("leaf_path", &TriePath::nibbles_to_hex(&leaf_nibbles));
            entity_data.add_string("value", &hex::encode(&data))
        }
        near_store::RawTrieNode::BranchNoValue(children) => {
            for index in 0..16 {
                if let Some(_) = children[index] {
                    let path = TriePath {
                        shard_uid: trie_path.shard_uid,
                        state_root: trie_path.state_root,
                        path: [&trie_path.path[..], &[index]].concat(),
                    };
                    entity_data.add_string(&format!("{:x}", index), &path.to_string());
                }
            }
        }
        near_store::RawTrieNode::BranchWithValue(value, children) => {
            let data = trie.retrieve_value(&value.hash)?;
            entity_data.add_string("leaf_path", &TriePath::nibbles_to_hex(&trie_path.path));
            entity_data.add_string("value", &hex::encode(&data));
            for index in 0..16 {
                if let Some(_) = children[index] {
                    let path = TriePath {
                        shard_uid: trie_path.shard_uid,
                        state_root: trie_path.state_root,
                        path: [&trie_path.path[..], &[index]].concat(),
                    };
                    entity_data.add_string(&format!("{:x}", index), &path.to_string());
                }
            }
        }
        near_store::RawTrieNode::Extension(extension, _) => {
            let extension_nibbles = NibbleSlice::from_encoded(&extension);
            let child_nibbles = trie_path
                .path
                .iter()
                .copied()
                .chain(extension_nibbles.0.iter())
                .collect::<Vec<_>>();
            let child_path = TriePath {
                shard_uid: trie_path.shard_uid,
                state_root: trie_path.state_root,
                path: child_nibbles,
            };
            entity_data.add_string("extension", &child_path.to_string());
        }
    }
    Ok(EntityDataValue::Struct(Box::new(entity_data)))
}

fn serialize_raw_trie_node(node: RawTrieNodeWithSize) -> EntityDataValue {
    let mut entity_data = EntityDataStruct::new();
    entity_data.add_string("memory_usage", &node.memory_usage.to_string());

    match node.node {
        RawTrieNode::Leaf(extension, value_ref) => {
            let extension = NibbleSlice::from_encoded(&extension);
            entity_data.add_string(
                "extension",
                &TriePath::nibbles_to_hex(&extension.0.iter().collect::<Vec<_>>()),
            );
            entity_data.add("value_hash", serialize_entity(&value_ref.hash));
            entity_data.add_string("value_len", &value_ref.length.to_string());
        }
        RawTrieNode::BranchNoValue(children) => {
            entity_data.add("children", serialize_entity(&children.0));
        }
        RawTrieNode::BranchWithValue(value_ref, children) => {
            entity_data.add("children", serialize_entity(&children.0));
            entity_data.add("value_hash", serialize_entity(&value_ref.hash));
            entity_data.add_string("value_len", &value_ref.length.to_string());
        }
        RawTrieNode::Extension(extension, child) => {
            let extension = NibbleSlice::from_encoded(&extension);
            entity_data.add_string(
                "extension",
                &TriePath::nibbles_to_hex(&extension.0.iter().collect::<Vec<_>>()),
            );
            entity_data.add("child", serialize_entity(&child));
        }
    }
    EntityDataValue::Struct(entity_data.into())
}

impl EntityDebugHandler for EntityDebugHandlerImpl {
    fn query(&self, query: EntityQueryWithParams) -> Result<EntityDataValue, RpcError> {
        let store = if query.use_cold_storage {
            self.cold_store.clone().ok_or_else(|| {
                RpcError::new_internal_error(None, "Cold storage is not available".to_string())
            })?
        } else {
            self.hot_store.clone()
        };
        self.query_impl(store, query.query)
            .map_err(|err| RpcError::new_internal_error(None, format!("{:?}", err)))
    }
}

/// A helper to represent the complete location of a trie node with a string.
/// The format is "shard_uid/state_root/path", where path is a hex-encoded
/// string of nibbles (which may be of odd length, because each nibble is 4 bits
/// which is a single hex character).
pub struct TriePath {
    pub shard_uid: ShardUId,
    pub state_root: CryptoHash,
    pub path: Vec<u8>,
}

impl TriePath {
    pub fn to_string(&self) -> String {
        format!("{}/{}/{}", self.shard_uid, self.state_root, Self::nibbles_to_hex(&self.path))
    }

    pub fn parse(encoded: String) -> Option<TriePath> {
        let mut parts = encoded.split("/");
        let shard_uid = parts.next()?.parse().ok()?;
        let state_root = parts.next()?.parse().ok()?;
        let nibbles = parts.next()?;
        let path = Self::nibbles_from_hex(nibbles)?;
        Some(TriePath { shard_uid, state_root, path })
    }

    /// Format of nibbles is an array of 4-bit integers.
    pub fn nibbles_to_hex(nibbles: &[u8]) -> String {
        nibbles.iter().map(|x| format!("{:x}", x)).collect::<Vec<_>>().join("")
    }

    /// Format of returned value is an array of 4-bit integers, or None if parsing failed.
    pub fn nibbles_from_hex(hex: &str) -> Option<Vec<u8>> {
        let mut result = Vec::new();
        for nibble in hex.chars() {
            result.push(u8::from_str_radix(&nibble.to_string(), 16).ok()?);
        }
        Some(result)
    }
}

#[derive(Serialize)]
struct FlatStateChangeView {
    pub key: String,
    pub value: Option<String>,
}

struct PartialStateParser {
    nodes: HashMap<CryptoHash, TrieValue>,
}

impl PartialStateParser {
    /// Takes the flattened partial trie nodes and turn them into a hierarchical view,
    /// automatically finding the root. Only used for debugging.
    pub fn parse_and_serialize_partial_state(partial_state: PartialState) -> EntityDataValue {
        let PartialState::TrieValues(nodes) = partial_state;
        let parser = Self::new(&nodes);
        let root = parser.find_root();
        match root {
            Some(root) => {
                let mut ret = EntityDataStruct::new();
                ret.add("root", parser.serialize_node(root));
                EntityDataValue::Struct(ret.into())
            }
            None => {
                // If finding root failed, just dump the raw nodes as hex.
                let mut ret = EntityDataStruct::new();
                ret.add("error", EntityDataValue::String("No root found".to_string()));
                let mut inner = EntityDataStruct::new();
                for (hash, data) in &parser.nodes {
                    inner.add(&format!("{}", hash), EntityDataValue::String(hex::encode(data)));
                }
                ret.add("raw_nodes", EntityDataValue::Struct(inner.into()));
                EntityDataValue::Struct(ret.into())
            }
        }
    }

    fn new(nodes: &[TrieValue]) -> Self {
        Self {
            nodes: nodes
                .iter()
                .map(|node| {
                    let hash = hash(&node);
                    (hash, node.clone())
                })
                .collect(),
        }
    }

    /// Finds what's most likely the root node, which is the node that isn't listed
    /// as a child of any other node.
    fn find_root(&self) -> Option<CryptoHash> {
        let mut nodes_not_yet_seen_as_children: HashSet<CryptoHash> = HashSet::new();
        for hash in self.nodes.keys() {
            nodes_not_yet_seen_as_children.insert(*hash);
        }
        for data in self.nodes.values() {
            // Note that here it's possible that we're parsing a value that is not a trie
            // node, so we may get some false positive. But that is very rare and only
            // a problem if a child parsed from such a ill-constructed value happens to
            // be the root hash. In that case, we would fail to find the root and will just
            // fall back to showing the raw values.
            let children = self.detect_possible_children_of(&data);
            for child in children {
                nodes_not_yet_seen_as_children.remove(&child);
            }
        }
        if nodes_not_yet_seen_as_children.len() == 1 {
            nodes_not_yet_seen_as_children.iter().next().copied()
        } else {
            None
        }
    }

    /// Parses the given data that is possibly a trie node (and possibly a value),
    /// and if it looks like a trie node, return all its children hashes (nodes and
    /// values).
    fn detect_possible_children_of(&self, data: &[u8]) -> Vec<CryptoHash> {
        let Ok(node) = RawTrieNodeWithSize::try_from_slice(data) else {
            return vec![];
        };
        match &node.node {
            RawTrieNode::Leaf(_, value) => {
                vec![value.hash]
            }
            RawTrieNode::BranchNoValue(children) => {
                children.iter().map(|(_, child)| *child).collect()
            }
            RawTrieNode::BranchWithValue(value, children) => children
                .iter()
                .map(|(_, child)| *child)
                .chain(std::iter::once(value.hash))
                .collect(),
            RawTrieNode::Extension(_, child) => vec![*child],
        }
    }

    /// Visits node, serializing it as entity debug output.
    fn serialize_node(&self, hash: CryptoHash) -> EntityDataValue {
        let Some(data) = self.nodes.get(&hash) else {
            // This is a partial trie, so missing is very normal.
            return EntityDataValue::String("(missing)".to_string());
        };
        let mut ret = EntityDataStruct::new();
        let node = RawTrieNodeWithSize::try_from_slice(data.as_ref()).unwrap();
        match &node.node {
            RawTrieNode::Leaf(extension, value_ref) => {
                let (nibbles, _) = NibbleSlice::from_encoded(&extension);
                ret.add(
                    "extension",
                    EntityDataValue::String(TriePath::nibbles_to_hex(
                        &nibbles.iter().collect::<Vec<_>>(),
                    )),
                );
                ret.add("value", self.serialize_value(value_ref.hash));
            }
            RawTrieNode::BranchNoValue(children) => {
                for (index, child) in children.iter() {
                    ret.add(&format!("{:x}", index), self.serialize_node(*child));
                }
            }
            RawTrieNode::BranchWithValue(value_ref, children) => {
                ret.add("value", self.serialize_value(value_ref.hash));
                for (index, child) in children.iter() {
                    ret.add(&format!("{:x}", index), self.serialize_node(*child));
                }
            }
            RawTrieNode::Extension(extension, child) => {
                let (nibbles, _) = NibbleSlice::from_encoded(&extension);
                ret.add(
                    "extension",
                    EntityDataValue::String(TriePath::nibbles_to_hex(
                        &nibbles.iter().collect::<Vec<_>>(),
                    )),
                );
                ret.add("child", self.serialize_node(*child));
            }
        }
        EntityDataValue::Struct(ret.into())
    }

    /// Visits value, serializing it as entity debug output.
    fn serialize_value(&self, hash: CryptoHash) -> EntityDataValue {
        let value = match self.nodes.get(&hash) {
            Some(data) => hex::encode(data),
            // This is a partial trie, so missing is very normal.
            None => "(missing)".to_string(),
        };
        EntityDataValue::String(value)
    }
}

#[derive(serde::Serialize)]
struct ValidatorAssignmentsAtHeight {
    block_producer: AccountId,
    chunk_producers: Vec<AccountId>,
    chunk_validator_assignments: Vec<Vec<OneValidatorAssignment>>,
}

#[derive(serde::Serialize)]
struct OneValidatorAssignment {
    account_id: AccountId,
    stake: Balance,
}

#[derive(serde::Serialize)]
struct BlockMiscData {
    head: Option<Tip>,
    tail: Option<BlockHeight>,
    chunk_tail: Option<BlockHeight>,
    fork_tail: Option<BlockHeight>,
    header_head: Option<Tip>,
    final_head: Option<Tip>,
    latest_known: Option<LatestKnown>,
    largest_target_height: Option<BlockHeight>,
    genesis_json_hash: Option<CryptoHash>,
    genesis_state_roots: Option<Vec<StateRoot>>,
    genesis_congestion_info: Option<Vec<CongestionInfo>>,
    cold_head: Option<Tip>,
    state_sync_dump: Option<StateSyncDumpProgress>,
    state_snapshot: Option<CryptoHash>,
}

impl BlockMiscData {
    pub fn from_store(store: &Store) -> anyhow::Result<BlockMiscData> {
        Ok(BlockMiscData {
            head: store.get_ser(DBCol::BlockMisc, HEAD_KEY)?,
            tail: store.get_ser(DBCol::BlockMisc, TAIL_KEY)?,
            chunk_tail: store.get_ser(DBCol::BlockMisc, CHUNK_TAIL_KEY)?,
            fork_tail: store.get_ser(DBCol::BlockMisc, FORK_TAIL_KEY)?,
            header_head: store.get_ser(DBCol::BlockMisc, HEADER_HEAD_KEY)?,
            final_head: store.get_ser(DBCol::BlockMisc, FINAL_HEAD_KEY)?,
            latest_known: store.get_ser(DBCol::BlockMisc, LATEST_KNOWN_KEY)?,
            largest_target_height: store.get_ser(DBCol::BlockMisc, LARGEST_TARGET_HEIGHT_KEY)?,
            genesis_json_hash: store.get_ser(DBCol::BlockMisc, GENESIS_JSON_HASH_KEY)?,
            genesis_state_roots: store.get_ser(DBCol::BlockMisc, GENESIS_STATE_ROOTS_KEY)?,
            genesis_congestion_info: store
                .get_ser(DBCol::BlockMisc, GENESIS_CONGESTION_INFO_KEY)?,
            cold_head: store.get_ser(DBCol::BlockMisc, COLD_HEAD_KEY)?,
            state_sync_dump: store.get_ser(DBCol::BlockMisc, STATE_SYNC_DUMP_KEY)?,
            state_snapshot: store.get_ser(DBCol::BlockMisc, STATE_SNAPSHOT_KEY)?,
        })
    }
}
