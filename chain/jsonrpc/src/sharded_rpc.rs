use crate::ShardedRpcConfig;
use near_chain_primitives::Error;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_jsonrpc_client_internal::JsonRpcClient;
use near_jsonrpc_primitives::errors::RpcError;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, BlockId, BlockReference, EpochId, ShardId};
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::sync::Arc;

/// Indicates the origin of a jsonrpc request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestSource {
    /// The request came directly from a user.
    User,
    /// The request was internally forwarded by a pool coordinator.
    /// Indicated by the `X-Near-Pool-Coordinator-Query` HTTP header.
    Coordinator,
}

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

/// Handle to a node that can serve a query.
#[derive(Clone)]
pub enum RpcNodeHandle {
    /// Forward the request to a remote node via its JSON-RPC client.
    RemoteNode(Arc<JsonRpcClient>),
    /// Handle the request locally.
    LocalNode,
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

    /// Returns all nodes that might be able to serve a query with the given routing hints.
    pub fn nodes_for_query(
        &self,
        block_hint: BlockHint,
        shard_hint: ShardHint,
    ) -> Result<Vec<RpcNodeHandle>, RpcError> {
        // TODO(sharded-rpc): Handle all (shard_hint, block_hint) combinations.
        // TODO(sharded-rpc): what should happen when the block is not known?

        let nodes = match (&block_hint, &shard_hint) {
            (BlockHint::None, ShardHint::None) => self.all_nodes(),
            (BlockHint::Hash(block_hash), ShardHint::Account(account_id)) => {
                let epoch_id = match self.chain_store.get_block_header(block_hash) {
                    Ok(header) => *header.epoch_id(),
                    Err(Error::DBNotFoundErr(_)) => return Ok(self.all_nodes()), // Unknown block, try all nodes
                    Err(e) => return Err(make_rpc_error(e)),
                };
                self.nodes_for_account_in_epochs(vec![epoch_id], account_id)?
            }
            (BlockHint::Height(height), ShardHint::Account(account_id)) => {
                let epoch_ids: Vec<_> = self
                    .chain_store
                    .get_all_block_hashes_by_height(*height)
                    .keys()
                    .cloned()
                    .collect();
                if epoch_ids.is_empty() {
                    return Ok(self.all_nodes()); // Unknown block, try all nodes
                }
                self.nodes_for_account_in_epochs(epoch_ids, account_id)?
            }
            (BlockHint::Recent, ShardHint::Account(account_id)) => {
                let head = match self.chain_store.head() {
                    Ok(tip) => tip,
                    Err(Error::DBNotFoundErr(_)) => return Ok(self.all_nodes()), // Unknown block, try all nodes
                    Err(e) => return Err(make_rpc_error(e)),
                };

                // TODO(sharded-rpc): only check adjacent epochs if we're close to the epoch boundary.
                let mut possible_epochs = vec![head.epoch_id, head.next_epoch_id];
                if let Ok(prev_epoch) = self
                    .shard_tracker
                    .epoch_manager()
                    .get_prev_epoch_id_from_prev_block(&head.prev_block_hash)
                {
                    possible_epochs.push(prev_epoch);
                }

                self.nodes_for_account_in_epochs(possible_epochs, account_id)?
            }
            _ => self.all_nodes(),
        };

        if nodes.is_empty() {
            // Emergency fallback - if there are no nodes that can handle the query, try the local
            // one, although it'll probably fail.
            // TODO(sharded-rpc): maybe remove when we're confident in the logic.
            return Ok(vec![RpcNodeHandle::LocalNode]);
        }

        Ok(nodes)
    }

    /// Returns handles to all nodes in the pool (local first, then all remotes).
    pub fn all_nodes(&self) -> Vec<RpcNodeHandle> {
        let mut result = vec![RpcNodeHandle::LocalNode];
        for node in &self.nodes {
            result.push(RpcNodeHandle::RemoteNode(node.client.clone()));
        }
        result
    }

    /// Returns nodes that might have data for the given account in any of the given epochs.
    fn nodes_for_account_in_epochs(
        &self,
        epoch_ids: Vec<EpochId>,
        account_id: &AccountId,
    ) -> Result<Vec<RpcNodeHandle>, RpcError> {
        let mut result = Vec::new();

        // Create a list of (epoch_id, shard_id)
        let mut epoch_shard_ids: Vec<(EpochId, ShardId)> = Vec::new();
        for epoch_id in epoch_ids {
            let shard_layout = match self.shard_tracker.epoch_manager().get_shard_layout(&epoch_id)
            {
                Ok(layout) => layout,
                Err(EpochError::EpochOutOfBounds(_)) => return Ok(self.all_nodes()), // Unknown epoch, try all nodes
                Err(e) => return Err(make_rpc_error(e)),
            };
            let shard_id = shard_layout.account_id_to_shard_id(account_id);

            epoch_shard_ids.push((epoch_id, shard_id));
        }

        // Check if the local node matches.
        for (epoch_id, shard_id) in &epoch_shard_ids {
            match self.shard_tracker.rpc_tracks_shard_at_epoch(*shard_id, epoch_id) {
                Ok(tracks) => {
                    if tracks {
                        result.push(RpcNodeHandle::LocalNode);
                        break;
                    }
                }
                Err(EpochError::EpochOutOfBounds(_)) => return Ok(self.all_nodes()), // Unknown epoch, try all nodes
                Err(e) => return Err(make_rpc_error(e)),
            };
        }

        // Check which remote nodes match.
        for node in &self.nodes {
            if epoch_shard_ids
                .iter()
                .any(|(_epoch_id, shard_id)| node.tracked_shards.contains(shard_id))
            {
                result.push(RpcNodeHandle::RemoteNode(node.client.clone()));
            }
        }

        Ok(result)
    }
}

fn make_rpc_error(err: impl std::fmt::Display) -> RpcError {
    RpcError::new_internal_error(None, format!("get_nodes_for_query internal error: {}", err))
}
