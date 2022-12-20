import { ReactElement, useMemo } from "react";
import { useQuery } from "react-query";
import { fetchEpochInfo, fetchFullStatus } from "./api";
import './NetworkInfoView.scss';

function formatDurationInMillis(millis: number): string {
    if (millis == null) {
        return '(null)';
    }
    let total_seconds = Math.floor(millis / 1000);
    let hours = Math.floor(total_seconds / 3600)
    let minutes = Math.floor((total_seconds - (hours * 3600)) / 60)
    let seconds = total_seconds - (hours * 3600) - (minutes * 60)
    if (hours > 0) {
        if (minutes > 0) {
            return `${hours}h ${minutes}m ${seconds}s`
        } else {
            return `${hours}h ${seconds}s`
        }
    }
    if (minutes > 0) {
        return `${minutes}m ${seconds}s`
    }
    return `${seconds}s`
}

function formatBytesPerSecond(bytes_per_second: number): string {
    if (bytes_per_second == null) {
        return '-';
    }
    if (bytes_per_second < 3000) {
        return `${bytes_per_second} bps`;
    }
    let kilobytes_per_second = bytes_per_second / 1024;
    if (kilobytes_per_second < 3000) {
        return `${kilobytes_per_second.toFixed(1)} Kbps`;
    }
    let megabytes_per_second = kilobytes_per_second / 1024;
    return `${megabytes_per_second.toFixed(1)} Mbps`;
}

function formatTraffic(bytes_received: number, bytes_sent: number): ReactElement {
    return <div>
        <div>{"⬇ " + formatBytesPerSecond(bytes_received)}</div>
        <div>{"⬆ " + formatBytesPerSecond(bytes_sent)}</div>
    </div>;
}

function addDebugPortLink(peer_addr: string): ReactElement {
    // TODO: use new UI
    return <a
        href={"http://" + peer_addr.replace(/:.*/, ":3030/debug")}>
        {peer_addr}
    </a>;
}

function peerClass(current_height: number, peer_height: number): string {
    if (peer_height > current_height + 5) {
        return 'peer_ahead_alot';
    }
    if (peer_height > current_height + 2) {
        return 'peer_ahead';
    }

    if (peer_height < current_height - 100) {
        return 'peer_far_behind';
    }
    if (peer_height < current_height - 10) {
        return 'peer_behind';
    }
    if (peer_height < current_height - 3) {
        return 'peer_behind_a_little';
    }
    return 'peer_in_sync';
}

function getIntersection<T>(setA: Set<T>, setB: Set<T>): Set<T> {
    const intersection = new Set<T>(
        [...setA].filter(element => setB.has(element))
    );

    return intersection;
}

function getDifference<T>(setA: Set<T>, setB: Set<T>): Set<T> {
    return new Set(
        [...setA].filter(element => !setB.has(element))
    );
}

type NetworkInfoViewProps = {
    addr: string,
};

export const NetworkInfoView = ({ addr }: NetworkInfoViewProps) => {
    const { data: fullStatus, error: fullStatusError, isLoading: fullStatusLoading } =
        useQuery(['full_status', addr], () => fetchFullStatus(addr));
    const { data: epochInfo, error: epochInfoError, isLoading: epochInfoLoading } =
        useQuery(['epoch_info', addr], () => fetchEpochInfo(addr));

    const epochId = fullStatus?.sync_info.epoch_id;
    const { blockProducers, chunkProducers, knownSet, reachableSet } = useMemo(() => {
        if (fullStatus && epochInfo) {
            const knownSet = new Set(
                fullStatus.detailed_debug_status!.network_info.known_producers
                    .map((p) => p.account_id));
            const reachableSet = new Set(
                fullStatus.detailed_debug_status!.network_info.known_producers
                    .filter((p) => (p.next_hops?.length ?? 0) > 0)
                    .map((p) => p.account_id));
            for (const oneEpoch of epochInfo.status_response.EpochInfo) {
                if (oneEpoch.epoch_id === epochId) {
                    return {
                        blockProducers: new Set(
                            oneEpoch.block_producers.map(bp => bp.account_id)),
                        chunkProducers: new Set(oneEpoch.chunk_only_producers),
                        knownSet,
                        reachableSet,
                    }
                }
            }
        }
        return {
            blockProducers: new Set(),
            chunkProducers: new Set(),
            knownSet: new Set(),
            reachableSet: new Set(),
        };
    }, [epochId, fullStatus, epochInfo]);

    if (fullStatusLoading || epochInfoLoading) {
        return <div>Loading...</div>;
    }
    if (fullStatusError || epochInfoError) {
        return <div className="network-info-view">
            <div className="error">
                {((fullStatusError || epochInfoError) as Error).stack}
            </div>
        </div>;
    }
    if (!fullStatus || !epochInfo) {
        return <div className="network-info-view">
            <div className="error">No Data</div>
        </div>;
    }

    const detailedDebugStatus = fullStatus.detailed_debug_status!;

    return <div className="network-info-view">
        <p>Current Sync Status: {detailedDebugStatus.sync_status}</p>
        <p>Number of peers: {detailedDebugStatus.network_info.num_connected_peers} / {detailedDebugStatus.network_info.peer_max_count}</p>

        <p>TODO: migrate the rest</p>
    </div>;
};