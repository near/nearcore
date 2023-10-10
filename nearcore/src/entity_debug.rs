use crate::entity_debug_serializer::serialize_entity;
use anyhow::anyhow;

use near_chain::types::RuntimeAdapter;
use near_chain::{Block, BlockHeader};
use near_epoch_manager::EpochManagerAdapter;
use near_jsonrpc_primitives::errors::RpcError;
use near_jsonrpc_primitives::types::entity_debug::{
    EntityDataEntry, EntityDataStruct, EntityDataValue, EntityDebugHandler, EntityQuery,
};
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::ShardChunk;
use near_primitives::state::FlatStateValue;
use near_primitives::transaction::{ExecutionOutcomeWithProof, SignedTransaction};
use near_primitives::utils::get_outcome_id_block_hash;
use near_primitives::views::{
    BlockHeaderView, BlockView, ChunkView, ExecutionOutcomeView, ReceiptView, SignedTransactionView,
};
use near_store::flat::delta::KeyForFlatStateDelta;
use near_store::flat::store_helper::encode_flat_state_db_key;
use near_store::flat::{FlatStateChanges, FlatStateDeltaMetadata, FlatStorageStatus};
use near_store::{
    DBCol, NibbleSlice, ShardUId, Store, TrieCachingStorage, FINAL_HEAD_KEY, HEADER_HEAD_KEY,
    HEAD_KEY,
};
use serde::Serialize;
use std::sync::Arc;

pub struct EntityDebugHandlerImpl {
    pub epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub runtime: Arc<dyn RuntimeAdapter>,
    pub store: Store,
}

impl EntityDebugHandlerImpl {
    fn query_impl(&self, query: EntityQuery) -> anyhow::Result<EntityDataValue> {
        match query {
            EntityQuery::AllShardsByEpochId { epoch_id } => {
                let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
                Ok(serialize_entity(&shard_layout.get_shard_uids()))
            }
            EntityQuery::BlockByHash { block_hash } => {
                let block = self
                    .store
                    .get_ser::<Block>(DBCol::Block, &borsh::to_vec(&block_hash).unwrap())?
                    .ok_or_else(|| anyhow!("Block not found"))?;
                let author = self
                    .epoch_manager
                    .get_block_producer(block.header().epoch_id(), block.header().height())?;
                Ok(serialize_entity(&BlockView::from_author_block(author, block)))
            }
            EntityQuery::BlockHashByHeight { block_height } => {
                let block_hash = self
                    .store
                    .get_ser::<CryptoHash>(
                        DBCol::BlockHeight,
                        &borsh::to_vec(&block_height).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Block height not found"))?;
                Ok(serialize_entity(&block_hash))
            }
            EntityQuery::BlockHeaderByHash { block_hash } => {
                let block_header = self
                    .store
                    .get_ser::<BlockHeader>(
                        DBCol::BlockHeader,
                        &borsh::to_vec(&block_hash).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Block header not found"))?;
                Ok(serialize_entity(&BlockHeaderView::from(block_header)))
            }
            EntityQuery::ChunkByHash { chunk_hash } => {
                let chunk = self
                    .store
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
            EntityQuery::EpochInfoByEpochId { epoch_id } => {
                let epoch_info = self.epoch_manager.get_epoch_info(&epoch_id)?;
                Ok(serialize_entity(&*epoch_info))
            }
            EntityQuery::FlatStateByTrieKey { trie_key, shard_uid } => {
                let state = self
                    .store
                    .get_ser::<FlatStateValue>(
                        DBCol::FlatState,
                        &encode_flat_state_db_key(shard_uid, &hex::decode(&trie_key)?),
                    )?
                    .ok_or_else(|| anyhow!("Flat state not found"))?;
                let data = self.deref_flat_state_value(state, shard_uid)?;
                Ok(serialize_entity(&hex::encode(&data)))
            }
            EntityQuery::FlatStateChangesByBlockHash { block_hash, shard_uid } => {
                let changes = self
                    .store
                    .get_ser::<FlatStateChanges>(
                        DBCol::FlatStateChanges,
                        &borsh::to_vec(&KeyForFlatStateDelta { block_hash, shard_uid }).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Flat state changes not found"))?;
                let mut changes_view = Vec::new();
                for (key, value) in changes.0.into_iter() {
                    let key = hex::encode(&key);
                    let value = match value {
                        Some(v) => Some(hex::encode(&self.deref_flat_state_value(v, shard_uid)?)),
                        None => None,
                    };
                    changes_view.push(FlatStateChangeView { key, value });
                }
                Ok(serialize_entity(&changes_view))
            }
            EntityQuery::FlatStateDeltaMetadataByBlockHash { block_hash, shard_uid } => {
                let metadata = self
                    .store
                    .get_ser::<FlatStateDeltaMetadata>(
                        DBCol::FlatStateDeltaMetadata,
                        &borsh::to_vec(&KeyForFlatStateDelta { block_hash, shard_uid }).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Flat state delta metadata not found"))?;
                Ok(serialize_entity(&metadata))
            }
            EntityQuery::FlatStorageStatusByShardUId { shard_uid } => {
                let status = self
                    .store
                    .get_ser::<FlatStorageStatus>(
                        DBCol::FlatStorageStatus,
                        &borsh::to_vec(&shard_uid).unwrap(),
                    )?
                    .ok_or_else(|| anyhow!("Flat storage status not found"))?;
                Ok(serialize_entity(&status))
            }
            EntityQuery::OutcomeByTransactionHash { transaction_hash: outcome_id }
            | EntityQuery::OutcomeByReceiptId { receipt_id: outcome_id } => {
                let (_, outcome) = self
                    .store
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
                let outcome = self
                    .store
                    .get_ser::<ExecutionOutcomeWithProof>(
                        DBCol::TransactionResultForBlock,
                        &get_outcome_id_block_hash(&outcome_id, &block_hash),
                    )?
                    .ok_or_else(|| anyhow!("Outcome not found"))?;
                Ok(serialize_entity(&ExecutionOutcomeView::from(outcome.outcome)))
            }
            EntityQuery::ReceiptById { receipt_id } => {
                let receipt = self
                    .store
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
                let shard_uid = *shard_layout
                    .get_shard_uids()
                    .get(shard_id as usize)
                    .ok_or_else(|| anyhow!("Shard {} not found", shard_id))?;
                Ok(serialize_entity(&shard_uid))
            }
            EntityQuery::TipAtFinalHead(_) => {
                let tip = self
                    .store
                    .get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?
                    .ok_or_else(|| anyhow!("Tip not found"))?;
                Ok(serialize_entity(&tip))
            }
            EntityQuery::TipAtHead(_) => {
                let tip = self
                    .store
                    .get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?
                    .ok_or_else(|| anyhow!("Tip not found"))?;
                Ok(serialize_entity(&tip))
            }
            EntityQuery::TipAtHeaderHead(_) => {
                let tip = self
                    .store
                    .get_ser::<Tip>(DBCol::BlockMisc, HEADER_HEAD_KEY)?
                    .ok_or_else(|| anyhow!("Tip not found"))?;
                Ok(serialize_entity(&tip))
            }
            EntityQuery::TransactionByHash { transaction_hash } => {
                let transaction = self
                    .store
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
                let mut entity_data = EntityDataStruct::new();
                entity_data.entries.push(EntityDataEntry {
                    name: "path".to_owned(),
                    value: EntityDataValue::String(TriePath::nibbles_to_hex(&trie_path.path)),
                });
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
                        entity_data.entries.push(EntityDataEntry {
                            name: "leaf_path".to_owned(),
                            value: EntityDataValue::String(TriePath::nibbles_to_hex(&leaf_nibbles)),
                        });
                        entity_data.entries.push(EntityDataEntry {
                            name: "value".to_owned(),
                            value: EntityDataValue::String(hex::encode(&data)),
                        });
                    }
                    near_store::RawTrieNode::BranchNoValue(children) => {
                        for index in 0..16 {
                            if let Some(_) = children[index] {
                                let path = TriePath {
                                    shard_uid: trie_path.shard_uid,
                                    state_root: trie_path.state_root,
                                    path: [&trie_path.path[..], &[index]].concat(),
                                };
                                entity_data.entries.push(EntityDataEntry {
                                    name: format!("{:x}", index),
                                    value: EntityDataValue::String(path.to_string()),
                                });
                            }
                        }
                    }
                    near_store::RawTrieNode::BranchWithValue(value, children) => {
                        let data = trie.retrieve_value(&value.hash)?;
                        entity_data.entries.push(EntityDataEntry {
                            name: "leaf_path".to_owned(),
                            value: EntityDataValue::String(TriePath::nibbles_to_hex(
                                &trie_path.path,
                            )),
                        });
                        entity_data.entries.push(EntityDataEntry {
                            name: "value".to_owned(),
                            value: EntityDataValue::String(hex::encode(&data)),
                        });
                        for index in 0..16 {
                            if let Some(_) = children[index] {
                                let path = TriePath {
                                    shard_uid: trie_path.shard_uid,
                                    state_root: trie_path.state_root,
                                    path: [&trie_path.path[..], &[index]].concat(),
                                };
                                entity_data.entries.push(EntityDataEntry {
                                    name: format!("{:x}", index),
                                    value: EntityDataValue::String(path.to_string()),
                                });
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
                        entity_data.entries.push(EntityDataEntry {
                            name: "extension".to_owned(),
                            value: EntityDataValue::String(child_path.to_string()),
                        });
                    }
                }
                Ok(EntityDataValue::Struct(Box::new(entity_data)))
            }
            EntityQuery::TrieRootByChunkHash { chunk_hash } => {
                let chunk = self
                    .store
                    .get_ser::<ShardChunk>(DBCol::Chunks, &borsh::to_vec(&chunk_hash).unwrap())?
                    .ok_or_else(|| anyhow!("Chunk not found"))?;
                let shard_layout = self
                    .epoch_manager
                    .get_shard_layout_from_prev_block(&chunk.cloned_header().prev_block_hash())?;
                let shard_uid = *shard_layout
                    .get_shard_uids()
                    .get(chunk.shard_id() as usize)
                    .ok_or_else(|| anyhow!("Shard {} not found", chunk.shard_id()))?;
                let path =
                    TriePath { path: vec![], shard_uid, state_root: chunk.prev_state_root() };
                Ok(serialize_entity(&path.to_string()))
            }
            EntityQuery::TrieRootByStateRoot { state_root, shard_uid } => {
                let path = TriePath { path: vec![], shard_uid, state_root };
                Ok(serialize_entity(&path.to_string()))
            }
        }
    }

    fn deref_flat_state_value(
        &self,
        state: FlatStateValue,
        shard_uid: ShardUId,
    ) -> anyhow::Result<Vec<u8>> {
        Ok(match state {
            FlatStateValue::Ref(value) => self
                .store
                .get(
                    DBCol::State,
                    &TrieCachingStorage::get_key_from_shard_uid_and_hash(shard_uid, &value.hash),
                )?
                .ok_or_else(|| anyhow!("ValueRef could not be dereferenced"))?
                .to_vec(),
            FlatStateValue::Inlined(data) => data,
        })
    }
}

impl EntityDebugHandler for EntityDebugHandlerImpl {
    fn query(&self, query: EntityQuery) -> Result<EntityDataValue, RpcError> {
        self.query_impl(query)
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
