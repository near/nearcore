export interface StatusResponse {
    chain_id: string;
    latest_protocol_version: number;
    node_key: string;
    node_public_key: string;
    protocol_version: number;
    rpc_addr?: string;
    sync_info: StatusSyncInfo;
    uptime_sec: number;
    validator_account_id: string | null;
    validator_public_key: string | null;
    validators: ValidatorInfo[];
    version: BuildInfo;
    detailed_debug_status: DetailedDebugStatus | null;
}

export interface StatusSyncInfo {
    earliest_block_hash: string;
    earliest_block_height: number;
    earliest_block_time: string;
    epoch_id: string;
    epoch_start_height: number;
    latest_block_hash: string;
    latest_block_height: number;
    latest_block_time: string;
    latest_state_root: string;
    syncing: boolean;
}

export interface ValidatorInfo {
    account_id: string;
    is_slashed: boolean;
}

export interface BuildInfo {
    build: string;
    rustc_version: string;
    version: string;
}

export interface DetailedDebugStatus {
    network_info: NetworkInfoView;
    sync_status: string;
    catchup_status: CatchupStatusView[];
    current_head_status: BlockStatusView;
    current_header_head_status: BlockStatusView;
    block_production_delay_millis: number;
}

export interface NetworkInfoView {
    peer_max_count: number;
    num_connected_peers: number;
    connected_peers: PeerInfoView[];
    known_producers: KnownProducerView[];
    tier1_accounts_keys?: string[];
    tier1_accounts_data?: AccountData[];
    tier1_connections?: PeerInfoView[];
}

export interface CatchupStatusView {
    sync_block_hash: string;
    sync_block_height: number;
    shard_sync_status: { [shard_id: number]: string };
    blocks_to_catchup: BlockStatusView[];
}

export interface BlockStatusView {
    height: number;
    hash: string;
}

export interface PeerInfoView {
    addr: string;
    account_id: string | null;
    height: number | null;
    block_hash: string | null;
    is_highest_block_invalid: boolean;
    tracked_shards: number[];
    archival: boolean;
    peer_id: string;
    received_bytes_per_sec: number;
    sent_bytes_per_sec: number;
    last_time_peer_requested_millis: number;
    last_time_received_message_millis: number;
    connection_established_time_millis: number;
    is_outbound_peer: boolean;
    nonce: number;
}

export interface KnownProducerView {
    account_id: string;
    peer_id: string;
    next_hops: string[] | null;
}

export interface PeerAddr {
    addr: string;
    peer_id: string;
}

export interface AccountData {
    peer_id: string;
    proxies: PeerAddr[];
    account_key: string;
    version: number;
    timestamp: string;
}

export type SyncStatusView =
    | 'AwaitingPeers'
    | 'NoSync'
    | {
          EpochSync: { epoch_ord: number };
      }
    | {
          HeaderSync: {
              start_height: number;
              current_height: number;
              highest_height: number;
          };
      }
    | {
          StateSync: [string, { [shard_id: number]: ShardSyncDownloadView }];
      }
    | 'StateSyncDone'
    | {
          BodySync: {
              start_height: number;
              current_height: number;
              highest_height: number;
          };
      };

export interface ShardSyncDownloadView {
    downloads: { error: boolean; done: boolean }[];
    status: string;
}

export interface DebugBlockStatusData {
    blocks: DebugBlockStatus[];
    missed_heights: MissedHeightInfo[];
    head: string;
    header_head: string;
}

export interface DebugBlockStatus {
    block_hash: string;
    prev_block_hash: string;
    block_height: number;
    block_timestamp: number;
    block_producer: string | null;
    full_block_missing: boolean;
    is_on_canonical_chain: boolean;
    chunks: DebugChunkStatus[];
    processing_time_ms?: number;
    gas_price_ratio: number;
}

export interface MissedHeightInfo {
    block_height: number;
    block_producer: string | null;
}

export interface DebugChunkStatus {
    shard_id: number;
    chunk_hash: string;
    chunk_producer: string | null;
    gas_used: number;
    processing_time_ms?: number;
}

export interface EpochInfoView {
    epoch_id: string;
    height: number;
    first_block: null | [string, string];
    block_producers: ValidatorInfo[];
    chunk_only_producers: string[];
    validator_info: EpochValidatorInfo;
    protocol_version: number;
    shards_size_and_parts: [number, number, boolean][];
}

export interface EpochValidatorInfo {
    current_validators: CurrentEpochValidatorInfo[];
    next_validators: NextEpochValidatorInfo[];
    current_fishermen: ValidatorStakeView[];
    next_fishermen: ValidatorStakeView[];
    current_proposals: ValidatorStakeView[];
    prev_epoch_kickout: ValidatorKickoutView[];
    epoch_start_height: number;
    epoch_height: number;
}

export interface CurrentEpochValidatorInfo {
    account_id: string;
    public_key: string;
    is_slashed: boolean;
    stake: string;
    shards: number[];
    num_produced_blocks: number;
    num_expected_blocks: number;
    num_produced_chunks: number;
    num_expected_chunks: number;
}

export interface NextEpochValidatorInfo {
    account_id: string;
    public_key: string;
    stake: string;
    shards: number[];
}

export interface ValidatorStakeView {
    account_id: string;
    public_key: string;
    stake: string;
    validator_stake_struct_version: 'V1';
}

export interface ValidatorKickoutView {
    account_id: string;
    reason: ValidatorKickoutReason;
}

export type ValidatorKickoutReason =
    | 'Slashed'
    | { NotEnoughBlocks: { produced: number; expected: number } }
    | { NotEnoughChunks: { produced: number; expected: number } }
    | 'Unstaked'
    | { NotEnoughStake: { stake: string; threshold: string } }
    | 'DidNotGetASeat';

export interface PeerStoreView {
    peer_states: KnownPeerStateView[];
}

export interface KnownPeerStateView {
    peer_id: string;
    status: string;
    addr: string;
    first_seen: number;
    last_seen: number;
    last_attempt: [number, string] | null;
}

export interface SyncStatusResponse {
    status_response: {
        SyncStatus: SyncStatusView;
    };
}

export interface TrackedShardsResponse {
    status_response: {
        TrackedShards: {
            shards_tracked_this_epoch: boolean[];
            shards_tracked_next_epoch: boolean[];
        };
    };
}

export interface BlockStatusResponse {
    status_response: {
        BlockStatus: DebugBlockStatusData;
    };
}

export interface EpochInfoResponse {
    status_response: {
        EpochInfo: EpochInfoView[];
    };
}

export interface PeerStoreResponse {
    status_response: {
        PeerStore: PeerStoreView;
    };
}

export interface ConnectionInfoView {
    peer_id: string;
    addr: string;
    time_established: number;
    time_connected_until: number;
}

export interface RecentOutboundConnectionsView {
    recent_outbound_connections: ConnectionInfoView[];
}

export interface RecentOutboundConnectionsResponse {
    status_response: {
        RecentOutboundConnections: RecentOutboundConnectionsView;
    };
}

export interface EdgeView {
    peer0: string;
    peer1: string;
    nonce: number;
};

export interface LabeledEdgeView {
    peer0: number;
    peer1: number;
    nonce: number;
};

export interface EdgeCacheView {
    peer_labels: { [peer_id: string]: number };
    spanning_trees: { [peer_label: number]: LabeledEdgeView[] };
}

export interface PeerRoutesView {
    distance: number[];
    min_nonce: number;
}

export interface RoutingTableView {
    edge_cache: EdgeCacheView;
    local_edges: { [peer_id: string]: EdgeView };
    peer_distances: { [peer_id: string]: PeerRoutesView };
    my_distances: { [peer_id: string]:  number };
}

export interface RoutingTableResponse {
    status_response: {
        Routes: RoutingTableView;
    };
}

export type DroppedReason = 'HeightProcessed' | 'TooManyProcessingBlocks';

export type BlockProcessingStatus =
    | 'Orphan'
    | 'WaitingForChunks'
    | 'InProcessing'
    | 'Accepted'
    | { Error: string }
    | { Dropped: DroppedReason }
    | 'Unknown';

export interface BlockProcessingInfo {
    height: number;
    hash: string;
    received_timestamp: string;
    in_progress_ms: number;
    orphaned_ms: number | null;
    missing_chunks_ms: number | null;
    block_status: BlockProcessingStatus;
    chunks_info: ChunkProcessingInfo[] | null;
}

export interface PartCollectionInfo {
    part_owner: string;
    received_time: string | null;
    forwarded_received_time: string | null;
    chunk_received_time: string | null;
}

export type ChunkProcessingStatus = 'NeedToRequest' | 'Requested' | 'Completed';

export interface ChunkProcessingInfo {
    height_created: number;
    shard_id: number;
    chunk_hash: string;
    prev_block_hash: string;
    created_by: string | null;
    status: ChunkProcessingStatus;
    requested_timestamp: string | null;
    completed_timestamp: string | null;
    request_duration: number | null;
    chunk_parts_collection: PartCollectionInfo[];
}

export interface ChainProcessingInfo {
    num_blocks_in_processing: number;
    num_orphans: number;
    num_blocks_missing_chunks: number;
    blocks_info: BlockProcessingInfo[];
    floating_chunks_info: ChunkProcessingInfo[];
}

export interface ChainProcessingStatusResponse {
    status_response: {
        ChainProcessingStatus: ChainProcessingInfo;
    };
}

export async function fetchBasicStatus(addr: string): Promise<StatusResponse> {
    const response = await fetch(`http://${addr}/status`);
    return await response.json();
}

export async function fetchFullStatus(addr: string): Promise<StatusResponse> {
    const response = await fetch(`http://${addr}/debug/api/status`);
    return await response.json();
}

export async function fetchSyncStatus(addr: string): Promise<SyncStatusResponse> {
    const response = await fetch(`http://${addr}/debug/api/sync_status`);
    return await response.json();
}

export async function fetchTrackedShards(addr: string): Promise<TrackedShardsResponse> {
    const response = await fetch(`http://${addr}/debug/api/tracked_shards`);
    return await response.json();
}

export async function fetchBlockStatus(
    addr: string,
    height: number | null
): Promise<BlockStatusResponse> {
    const trailing = height ? `/${height}` : '';
    const response = await fetch(`http://${addr}/debug/api/block_status${trailing}`);
    return await response.json();
}

export async function fetchEpochInfo(addr: string): Promise<EpochInfoResponse> {
    const response = await fetch(`http://${addr}/debug/api/epoch_info`);
    return await response.json();
}

export async function fetchPeerStore(addr: string): Promise<PeerStoreResponse> {
    const response = await fetch(`http://${addr}/debug/api/peer_store`);
    return await response.json();
}

export async function fetchRecentOutboundConnections(
    addr: string
): Promise<RecentOutboundConnectionsResponse> {
    const response = await fetch(`http://${addr}/debug/api/recent_outbound_connections`);
    return await response.json();
}

export async function fetchRoutingTable(
    addr: string
): Promise<RoutingTableResponse> {
    const response = await fetch(`http://${addr}/debug/api/network_routes`);
    return await response.json();
}

export async function fetchChainProcessingStatus(
    addr: string
): Promise<ChainProcessingStatusResponse> {
    const response = await fetch(`http://${addr}/debug/api/chain_processing_status`);
    return await response.json();
}
