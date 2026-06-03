import { EntityQueryWithParams } from './entity_debug/types';

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
        EpochSync:
        | 'NotStarted'
        | {
            InProgress: {
                source_peer_height: number;
                source_peer_id: string;
                attempt_time: string;
            };
        }
        | 'Done';
    }
    | {
        HeaderSync: {
            start_height: number;
            current_height: number;
            highest_height: number;
        };
    }
    | {
        StateSync: {
            sync_hash: string;
            sync_status: { [shard_id: number]: string };
            download_tasks: string[];
            computation_tasks: string[];
        };
    }
    | {
        BlockSync: {
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
    endorsement_ratio?: number;
}

export interface EpochInfoView {
    epoch_height: number;
    epoch_id: string;
    height: number;
    first_block: null | [string, string];
    block_producers: ValidatorInfo[];
    chunk_producers: string[];
    chunk_validators: string[];
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
    num_produced_endorsements: number;
    num_expected_endorsements: number;
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
    | { NotEnoughChunkEndorsements: { produced: number; expected: number } }
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
}

export interface SnapshotHostInfoView {
    peer_id: string;
    sync_hash: string;
    epoch_height: number;
    shards: number[];
}

export interface SnapshotHostsView {
    hosts: SnapshotHostInfoView[];
}

export interface SnapshotHostsResponse {
    status_response: {
        SnapshotHosts: SnapshotHostsView;
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

// Helper to format the URL through the proxy
const getProxyUrl = (addr: string, endpoint: string) => {
    // This requests /api-proxy/address/endpoint relative to current domain
    return `/api-proxy/${addr}/${endpoint}`;
};

// Helper to format the target URL based on current protocol
function getTargetUrl(addr: string, endpoint: string): string {
    const protocol = window.location.protocol;

    if (protocol === 'https:') {
        return getProxyUrl(addr, endpoint);
    }

    if (protocol === 'http:') {
        return `http://${addr}/${endpoint}`;
    }

    throw new Error(`Unsupported protocol: ${protocol}`);
}

export interface ChainProcessingStatusResponse {
    status_response: {
        ChainProcessingStatus: ChainProcessingInfo;
    };
}

export class HttpError extends Error {
    status: number;
    constructor(status: number, message: string) {
        super(message);
        this.status = status;
    }
}

export function isClientError(error: unknown): boolean {
    return error instanceof HttpError && error.status >= 400 && error.status < 500;
}

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
    const response = await fetch(url, init);
    if (!response.ok) {
        const body = await response.text().catch(() => '');
        throw new HttpError(response.status, body || response.statusText);
    }
    return response.json();
}

export function fetchBasicStatus(addr: string): Promise<StatusResponse> {
    return fetchJson(getTargetUrl(addr, 'status'));
}

export function fetchFullStatus(addr: string): Promise<StatusResponse> {
    return fetchJson(getTargetUrl(addr, 'debug/api/status'));
}

export function fetchSyncStatus(addr: string): Promise<SyncStatusResponse> {
    return fetchJson(getTargetUrl(addr, 'debug/api/sync_status'));
}

export function fetchTrackedShards(addr: string): Promise<TrackedShardsResponse> {
    return fetchJson(getTargetUrl(addr, 'debug/api/tracked_shards'));
}

export function fetchBlockStatus(
    addr: string,
    height: number | null,
    mode: string | null,
    numBlocks: number | null
): Promise<BlockStatusResponse> {
    const params = new URLSearchParams();
    if (height !== null) {
        params.append('starting_height', height.toString());
    }
    if (mode !== null) {
        params.append('mode', mode);
    }
    if (numBlocks !== null) {
        params.append('num_blocks', numBlocks.toString());
    }
    const query = params.toString() ? '?' + params : '';
    return fetchJson(getTargetUrl(addr, `debug/api/block_status${query}`));
}

export function fetchEpochInfo(
    addr: string,
    epochId: string | null
): Promise<EpochInfoResponse> {
    const trailing = epochId ? `/${epochId}` : '';
    return fetchJson(getTargetUrl(addr, `debug/api/epoch_info${trailing}`));
}

// Lightweight variant of the recent-epochs list that omits the heavy per-validator
// `validator_info`. Use this for views that only need epoch metadata and
// producer/validator counts (recent epochs, epoch shards, current peers).
//
// The debug-ui is released ahead of the node side, so this gracefully falls back to
// the full `epoch_info` endpoint when talking to a node that predates
// `epoch_info_light`. Such a node has no route for either form, so both fall through
// to the `/debug/api/{*path}` catch-all and return 405 (we also treat 404 as missing
// for safety, e.g. behind a proxy).
// TODO: remove the fallback once all nodes expose `epoch_info_light`.
export async function fetchEpochInfoLight(
    addr: string,
    epochId: string | null
): Promise<EpochInfoResponse> {
    const trailing = epochId ? `/${epochId}` : '';
    try {
        return await fetchJson(getTargetUrl(addr, `debug/api/epoch_info_light${trailing}`));
    } catch (error) {
        if (error instanceof HttpError && (error.status === 404 || error.status === 405)) {
            return fetchEpochInfo(addr, epochId);
        }
        throw error;
    }
}

export function fetchPeerStore(addr: string): Promise<PeerStoreResponse> {
    return fetchJson(getTargetUrl(addr, 'debug/api/peer_store'));
}

export function fetchRecentOutboundConnections(
    addr: string
): Promise<RecentOutboundConnectionsResponse> {
    return fetchJson(getTargetUrl(addr, 'debug/api/recent_outbound_connections'));
}

export function fetchSnapshotHosts(addr: string): Promise<SnapshotHostsResponse> {
    return fetchJson(getTargetUrl(addr, 'debug/api/snapshot_hosts'));
}

export function fetchChainProcessingStatus(
    addr: string
): Promise<ChainProcessingStatusResponse> {
    return fetchJson(getTargetUrl(addr, 'debug/api/chain_processing_status'));
}

export type ApiEntityDataEntryValue = string | ApiEntityData;
export type ApiEntityData = { entries: ApiEntityDataEntry[] };
export type ApiEntityDataEntry = { name: string; value: ApiEntityDataEntryValue };

export function fetchEntity(
    addr: string,
    request: EntityQueryWithParams
): Promise<ApiEntityDataEntryValue> {
    return fetchJson(getTargetUrl(addr, 'debug/api/entity'), {
        body: JSON.stringify(request),
        headers: {
            'Content-Type': 'application/json',
        },
        method: 'POST',
    });
}

export const INSTRUMENTED_WINDOW_LEN_MS = 500;

export function fetchInstrumentedThreadsView(
    addr: string
): Promise<InstrumentedThreadsViewResponse> {
    return fetchJson(getTargetUrl(addr, 'debug/api/instrumented_threads'));
}

export interface InstrumentedThreadsViewResponse {
    status_response: {
        InstrumentedThreads: InstrumentedThreads;
    }
}

export interface InstrumentedThreads {
    threads: InstrumentedThread[];
    current_time_unix_ms: number;
    current_time_relative_ms: number;
}

export interface InstrumentedThread {
    thread_name: string;
    active_time_ns: number;
    message_types: string[];
    windows: InstrumentedWindow[];
    active_event: InstrumentedActiveEvent | null;
    queue: { [message_type: string]: number };
}

export interface InstrumentedActiveEvent {
    message_type: number;
    active_for_ns: number;
}

export interface InstrumentedWindow {
    start_time_ms: number;
    end_time_ms: number;
    events: InstrumentedEvent[];
    events_overfilled: boolean;
    summary: InstrumentedWindowSummary;
    dequeue_summary: InstrumentedWindowSummary;
}

export interface InstrumentedEvent {
    m: number;
    s: boolean;
    t: number;
}

export interface InstrumentedWindowSummary {
    message_stats_by_type: MessageStatsForType[];
}

export interface MessageStatsForType {
    m: number;
    c: number;
    t: number;
}

