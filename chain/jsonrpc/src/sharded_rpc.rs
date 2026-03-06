use std::sync::Arc;

use near_epoch_manager::shard_tracker::ShardTracker;
use near_jsonrpc_client_internal::JsonRpcClient;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, BlockId, BlockReference, ShardId};
use near_store::adapter::chain_store::ChainStoreAdapter;

use crate::ShardedRpcConfig;

/// Hint about which block a jsonrpc query targets.
/// Used to determine which nodes can serve the query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockHint {
    /// No block information available.
    None,
    /// A recent block (final, near-final, or optimistic).
    Recent,
    /// Exact hash
    Hash(CryptoHash),
    /// Exact height
    Height(BlockHeight),
    /// Not supported in sharded rpc routing for now, used only in block header queries, which can be served locally.
    SyncCheckpoint,
}

impl From<BlockReference> for BlockHint {
    fn from(r: BlockReference) -> BlockHint {
        match r {
            BlockReference::BlockId(BlockId::Height(h)) => BlockHint::Height(h),
            BlockReference::BlockId(BlockId::Hash(h)) => BlockHint::Hash(h),
            BlockReference::Finality(_) => BlockHint::Recent,
            BlockReference::SyncCheckpoint(_) => BlockHint::SyncCheckpoint,
        }
    }
}

/// Hint about which shard a jsonrpc query targets.
/// Used to determine which nodes can serve the query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardHint {
    /// No shard information available.
    None,
    /// A specific shard_id
    Id(ShardId),
    /// The shard that contains the given account.
    Account(AccountId),
}

/// A remote RPC node in the pool, along with the shards it tracks.
#[derive(Clone)]
pub struct ShardedRpcNode {
    pub client: Arc<JsonRpcClient>,
    pub tracked_shards: Vec<ShardId>,
}

/// Pool of jsonrpc nodes for serving queries across shards.
pub struct ShardedRpcPool {
    /// All nodes in the pool
    pub nodes: Vec<ShardedRpcNode>,
    /// Provides shard tracking information for the local node.
    pub shard_tracker: ShardTracker,
    pub chain_store: ChainStoreAdapter,
}

impl ShardedRpcPool {
    /// Create a new sharded rpc pool.
    /// When config is provided, HTTP clients are created for each configured node.
    /// When config is None, the pool starts with no remote nodes.
    pub fn new(
        config: Option<ShardedRpcConfig>,
        shard_tracker: ShardTracker,
        chain_store: ChainStoreAdapter,
    ) -> Self {
        let nodes = match config {
            Some(config) => {
                let nodes: Vec<_> = config
                    .nodes
                    .iter()
                    .map(|node_config| ShardedRpcNode {
                        client: Arc::new(near_jsonrpc_client_internal::new_client(
                            &node_config.address,
                        )),
                        tracked_shards: node_config.tracked_shards.clone(),
                    })
                    .collect();
                nodes
            }
            None => vec![],
        };
        Self { nodes, shard_tracker, chain_store }
    }

    /// Creates a pool with pre-built nodes. Used in TestLoop where nodes
    /// are wired with in-process transports rather than HTTP clients.
    pub fn new_with_nodes(
        nodes: Vec<ShardedRpcNode>,
        shard_tracker: ShardTracker,
        chain_store: ChainStoreAdapter,
    ) -> Self {
        Self { nodes, shard_tracker, chain_store }
    }
}
