import { useEffect } from 'react';
import { useQuery } from 'react-query';
import {
    SyncStatusResponse,
    TrackedShardsResponse,
    fetchBasicStatus,
    fetchFullStatus,
    fetchSyncStatus,
    fetchTrackedShards,
} from './api';
import './ClusterNodeView.scss';

interface Props {
    addr: string;
    highestHeight: number;
    basicStatusChanged: (addr: string, name: string | null, height: number) => void;
    newNodesDiscovered: (nodes: string[]) => void;
}

export const ClusterNodeView = ({
    addr,
    highestHeight,
    basicStatusChanged,
    newNodesDiscovered,
}: Props) => {
    const status = useQuery(['status', addr], () => fetchBasicStatus(addr));
    const fullStatus = useQuery(['fullStatus', addr], () => fetchFullStatus(addr));
    const syncStatus = useQuery(['syncStatus', addr], () => fetchSyncStatus(addr));
    const trackedShards = useQuery(['trackedShards', addr], () => fetchTrackedShards(addr));

    useEffect(() => {
        if (status.data) {
            basicStatusChanged(
                addr,
                status.data.validator_account_id,
                status.data.sync_info.latest_block_height
            );
        }
    }, [addr, status.data, basicStatusChanged]);

    useEffect(() => {
        const networkInfo = fullStatus.data?.detailed_debug_status?.network_info;
        if (networkInfo) {
            const newAddrs = [];
            for (const peer of networkInfo.connected_peers) {
                newAddrs.push(peer.addr);
            }
            if (networkInfo.tier1_connections) {
                for (const peer of networkInfo.tier1_connections) {
                    newAddrs.push(peer.addr);
                }
            }
            const newAddrsWithCorrectedPort = [];
            for (const addr of newAddrs) {
                newAddrsWithCorrectedPort.push(addr.split(':')[0] + ':3030');
            }

            newNodesDiscovered(newAddrsWithCorrectedPort);
        }
    }, [addr, fullStatus.data, newNodesDiscovered]);

    const anyError = status.error || fullStatus.error || syncStatus.error || trackedShards.error;
    const anyLoading =
        status.isLoading || fullStatus.isLoading || syncStatus.isLoading || trackedShards.isLoading;
    return (
        <div className="cluster-node-view">
            <div className="addr">
                <a target="_blank" rel="noreferrer" href={'http://' + addr + '/debug'}>
                    {addr}
                </a>
            </div>
            {status.data && (
                <div
                    className={
                        'height' +
                        (status.data.sync_info.latest_block_height < highestHeight - 3
                            ? ' old-height'
                            : '')
                    }>
                    {status.data.sync_info.latest_block_height}
                </div>
            )}
            {status.data && status.data.validator_account_id && (
                <div className="account">{status.data.validator_account_id}</div>
            )}
            {syncStatus.data && (
                <div className="sync-status">{syncStatusToText(syncStatus.data)}</div>
            )}
            {trackedShards.data && <div>{trackedShardsToText(trackedShards.data)}</div>}
            {!!anyError && <div className="error">{'' + anyError}</div>}
            {anyLoading && !anyError && <div className="loading">Loading...</div>}
        </div>
    );
};

function syncStatusToText(syncStatus: SyncStatusResponse): string {
    const status = syncStatus.status_response.SyncStatus;
    if (status == null) {
        return 'No sync status??';
    }
    if (status === 'AwaitingPeers') {
        return 'Awaiting peers';
    }
    if (status === 'NoSync') {
        return '';
    }
    if (status === 'StateSyncDone') {
        return 'State sync done';
    }
    if ('EpochSync' in status) {
        return 'Epoch sync';
    }
    if ('HeaderSync' in status) {
        return 'Header sync';
    }
    if ('StateSync' in status) {
        return 'State sync';
    }
    return `Body sync ${status.BodySync.start_height} -> ${status.BodySync.highest_height}`;
}

function booleanArrayToIndexList(array: boolean[]): number[] {
    const result = [];
    for (let i = 0; i < array.length; i++) {
        if (array[i]) {
            result.push(i);
        }
    }
    return result;
}

function trackedShardsToText(trackedShards: TrackedShardsResponse): string {
    const { shards_tracked_this_epoch, shards_tracked_next_epoch } =
        trackedShards.status_response.TrackedShards;
    const thisShards = booleanArrayToIndexList(shards_tracked_this_epoch).join(', ');
    const nextShards = booleanArrayToIndexList(shards_tracked_next_epoch).join(', ');
    return `[${thisShards}] next: [${nextShards}]`;
}
