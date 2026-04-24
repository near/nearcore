use crate::ShardedRpcConfig;
use near_chain_primitives::Error;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_jsonrpc_client_internal::JsonRpcClient;
use near_jsonrpc_primitives::errors::RpcError;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{AccountId, BlockHeight, BlockId, BlockReference, EpochId, ShardId};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::Arc;
use url::Url;

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

/// Strategy for how the coordinator routes a request across candidate nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorRequestStrategy {
    /// Try nodes one by one in order. Return the first success.
    Sequential,
    /// Fan out to all candidate nodes concurrently.
    /// Return the first success; cancel remaining requests.
    ParallelTakeFirst,
}

/// Handle to a node that can serve a query.
#[derive(Clone)]
pub enum RpcNodeHandle {
    /// Forward the request to a remote node via its JSON-RPC client.
    RemoteNode(Arc<JsonRpcClient>),
    /// Handle the request locally.
    LocalNode,
}

/// A node assignment for scatter-gather: a node handle, its index in the pool,
/// and the shards it has been assigned to serve.
pub struct NodeRequestAssignment {
    pub handle: RpcNodeHandle,
    /// 0 for the local node, `i + 1` for `pool.nodes[i]`.
    pub node_idx: usize,
    pub assigned_shards: Vec<ShardId>,
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
    /// Any configured node whose address resolves to a local IP is detected as
    /// a self-connection and excluded from the remote pool.
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
                    .filter(|node_config| {
                        if is_local_address(&node_config.address) {
                            tracing::info!(
                                target: "jsonrpc",
                                address = %node_config.address,
                                tracked_shards = ?node_config.tracked_shards,
                                "sharded rpc pool: detected self-connection, \
                                 excluding from remote pool"
                            );
                            return false;
                        }
                        true
                    })
                    .map(|node_config| ShardedRpcNode {
                        client: Arc::new(near_jsonrpc_client_internal::new_client(
                            &node_config.address,
                        )),
                        tracked_shards: node_config.tracked_shards.clone(),
                    })
                    .collect();
                tracing::info!(
                    target: "jsonrpc",
                    total_configured = config.nodes.len(),
                    remote_nodes = nodes.len(),
                    "sharded rpc pool initialized"
                );
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
            (BlockHint::Hash(block_hash), ShardHint::Id(shard_id)) => {
                let epoch_id = match self.chain_store.get_block_header(block_hash) {
                    Ok(header) => *header.epoch_id(),
                    Err(Error::DBNotFoundErr(_)) => return Ok(self.all_nodes()), // Unknown block, try all nodes
                    Err(e) => return Err(make_rpc_error(e)),
                };
                self.nodes_for_shard_in_epochs(vec![epoch_id], *shard_id)?
            }
            (BlockHint::Height(height), ShardHint::Id(shard_id)) => {
                let epoch_ids: Vec<_> = self
                    .chain_store
                    .get_all_block_hashes_by_height(*height)
                    .keys()
                    .cloned()
                    .collect();
                if epoch_ids.is_empty() {
                    return Ok(self.all_nodes()); // Unknown block, try all nodes
                }
                self.nodes_for_shard_in_epochs(epoch_ids, *shard_id)?
            }
            (BlockHint::Recent, ShardHint::Id(shard_id))
            | (BlockHint::None, ShardHint::Id(shard_id)) => {
                let head = match self.chain_store.head() {
                    Ok(tip) => tip,
                    Err(Error::DBNotFoundErr(_)) => return Ok(self.all_nodes()),
                    Err(e) => return Err(make_rpc_error(e)),
                };
                self.nodes_for_shard_in_epochs(vec![head.epoch_id], *shard_id)?
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

    /// Returns nodes that track the given shard in any of the given epochs.
    fn nodes_for_shard_in_epochs(
        &self,
        epoch_ids: Vec<EpochId>,
        shard_id: ShardId,
    ) -> Result<Vec<RpcNodeHandle>, RpcError> {
        let mut result = Vec::new();

        // Check if the local node tracks this shard in any of the given epochs.
        for epoch_id in &epoch_ids {
            match self.shard_tracker.rpc_tracks_shard_at_epoch(shard_id, epoch_id) {
                Ok(true) => {
                    result.push(RpcNodeHandle::LocalNode);
                    break;
                }
                Ok(false) => {}
                Err(EpochError::EpochOutOfBounds(_)) => return Ok(self.all_nodes()), // Unknown epoch, try all nodes
                Err(e) => return Err(make_rpc_error(e)),
            }
        }

        // Check remote nodes. Their `tracked_shards` is a static config and
        // not epoch-dependent, so a single membership check is enough.
        for node in &self.nodes {
            if node.tracked_shards.contains(&shard_id) {
                result.push(RpcNodeHandle::RemoteNode(node.client.clone()));
            }
        }

        Ok(result)
    }

    /// Try to resolve a chunk hash to its (block_height, shard_id) by looking
    /// up the partial chunk in the store. All nodes persist partial chunks for
    /// all shards (header-only for untracked shards), so this works regardless
    /// of which shards the local node tracks.
    pub fn try_resolve_chunk_block_and_shard(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Option<(BlockHeight, ShardId)> {
        match self.chain_store.chunk_store().get_partial_chunk(chunk_hash) {
            Ok(partial_chunk) => Some((partial_chunk.height_included(), partial_chunk.shard_id())),
            Err(err) => {
                tracing::debug!(target: "jsonrpc", ?chunk_hash, ?err, "failed to resolve chunk from partial chunk store");
                None
            }
        }
    }

    /// Assigns groups of target shards to nodes, returning disjoint shard groups.
    ///
    /// Each shard in `target_shards` is assigned to exactly one node. Shards
    /// assigned to the same node are grouped together so only one request is
    /// needed per node.
    ///
    /// Prefers the local node (index 0) when it tracks a shard. Falls back to
    /// the first non-excluded remote node. Returns Error if target_shards cannot be
    /// covered by non-excluded nodes.
    pub fn one_node_per_group_of_shard(
        &self,
        epoch_id: &EpochId,
        target_shards: &HashSet<ShardId>,
        exclude: &HashSet<usize>,
    ) -> Result<Vec<NodeRequestAssignment>, RpcError> {
        let mut node_groups: HashMap<usize, (RpcNodeHandle, Vec<ShardId>)> = HashMap::new();

        for &shard_id in target_shards {
            let (node_idx, handle) = self.pick_node_for_shard(shard_id, epoch_id, exclude)?;
            node_groups.entry(node_idx).or_insert_with(|| (handle, Vec::new())).1.push(shard_id);
        }

        Ok(node_groups
            .into_iter()
            .map(|(node_idx, (handle, shards))| NodeRequestAssignment {
                handle,
                node_idx,
                assigned_shards: shards,
            })
            .collect())
    }

    /// Pick the best node for a single shard: local if it tracks it, then
    /// first non-excluded remote. Returns `Error` if all candidates are excluded.
    fn pick_node_for_shard(
        &self,
        shard_id: ShardId,
        epoch_id: &EpochId,
        exclude: &HashSet<usize>,
    ) -> Result<(usize, RpcNodeHandle), RpcError> {
        // Prefer local node (index 0).
        if !exclude.contains(&0) {
            match self.shard_tracker.rpc_tracks_shard_at_epoch(shard_id, epoch_id) {
                Ok(true) => return Ok((0, RpcNodeHandle::LocalNode)),
                Ok(false) => {}
                Err(EpochError::EpochOutOfBounds(_)) => return Ok((0, RpcNodeHandle::LocalNode)),
                Err(e) => return Err(make_rpc_error(e)),
            }
        }

        // Try remote nodes.
        for (i, node) in self.nodes.iter().enumerate() {
            let node_index = i + 1;
            if exclude.contains(&node_index) {
                continue;
            }
            if node.tracked_shards.contains(&shard_id) {
                return Ok((node_index, RpcNodeHandle::RemoteNode(node.client.clone())));
            }
        }

        // No non-excluded node available for this shard.
        tracing::debug!(target: "jsonrpc", ?shard_id, ?epoch_id, "no available node found for shard");
        Err(make_rpc_error("No available host for given shard."))
    }
}

fn make_rpc_error(err: impl std::fmt::Display) -> RpcError {
    RpcError::new_internal_error(None, format!("get_nodes_for_query internal error: {}", err))
}

/// Checks whether the given IP address belongs to a local network interface
/// by attempting to bind a UDP socket to it. The OS only allows binding to
/// IPs assigned to local interfaces, making this a reliable detection method.
///
/// Note: the bind may fail in hardened environments even
/// for local IPs, causing a false negative. This is not a bit problem: the
/// node would forward requests to itself over the network.
fn is_local_ip(ip: IpAddr) -> bool {
    if ip.is_loopback() || ip.is_unspecified() {
        return true;
    }
    UdpSocket::bind(SocketAddr::new(ip, 0)).is_ok()
}

/// Checks if a node URL (e.g. "http://10.128.0.5:3030") points to the local machine.
/// Resolves the host to IP addresses and returns true if any of them are local.
fn is_local_address(node_url: &str) -> bool {
    match try_resolve_is_local(node_url) {
        Ok(is_local) => is_local,
        Err(err) => {
            tracing::warn!(
                target: "jsonrpc",
                url = %node_url,
                %err,
                "sharded rpc pool: could not determine if address is local, treating as remote"
            );
            false
        }
    }
}

/// Called once at pool init time, so synchronous DNS resolution is acceptable.
fn try_resolve_is_local(node_url: &str) -> Result<bool, String> {
    let parsed = Url::parse(node_url).map_err(|e| format!("failed to parse URL: {e}"))?;
    let port = parsed.port_or_known_default().unwrap_or(80);
    let resolved: Vec<SocketAddr> = parsed
        .socket_addrs(|| Some(port))
        .map_err(|e| format!("failed to resolve {node_url}: {e}"))?;
    // If any resolved address is local, treat the entire entry as self and
    // drop it from the pool. This avoids the node forwarding requests to
    // itself over the network, at the cost of also discarding any non-local
    // addresses that the same hostname may resolve to.
    Ok(resolved.iter().any(|addr| is_local_ip(addr.ip())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_local_ip() {
        assert!(is_local_ip("127.0.0.1".parse::<IpAddr>().unwrap()));
        assert!(is_local_ip("0.0.0.0".parse::<IpAddr>().unwrap()));
        assert!(!is_local_ip("10.99.99.99".parse::<IpAddr>().unwrap()));
        assert!(is_local_ip("::1".parse::<IpAddr>().unwrap()));
        assert!(is_local_ip("::".parse::<IpAddr>().unwrap()));
    }

    #[test]
    fn test_is_local_address_loopback() {
        assert!(is_local_address("http://127.0.0.1:3030"));
        assert!(is_local_address("http://127.0.0.1:4040"));
        assert!(is_local_address("http://[::1]:3030"));
    }

    #[test]
    fn test_is_local_address_external() {
        assert!(!is_local_address("http://10.99.99.99:3030"));
    }

    #[test]
    fn test_is_local_address_invalid_url() {
        assert!(!is_local_address("not-a-url"));
        assert!(!is_local_address(""));
    }
}
