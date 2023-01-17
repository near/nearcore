import React, { MouseEvent, ReactElement, useCallback, useMemo, useState } from "react";
import { useQuery } from "react-query";
import { fetchEpochInfo, fetchFullStatus, fetchPeerStore, PeerInfoView } from "./api";
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

type PeerInfo = {
    peer: PeerInfoView,
    validator: string[],
    routedValidator: string[],
    statusClassName: string,
}

type NetworkInfoViewProps = {
    addr: string,
};

export const NetworkInfoView = ({ addr }: NetworkInfoViewProps) => {
    const { data: fullStatus, error: fullStatusError, isLoading: fullStatusLoading } =
        useQuery(['full_status', addr], () => fetchFullStatus(addr));
    const { data: epochInfo, error: epochInfoError, isLoading: epochInfoLoading } =
        useQuery(['epoch_info', addr], () => fetchEpochInfo(addr));

    const { blockProducers, chunkProducers, knownSet, reachableSet, numPeersByStatus, peers } = useMemo(() => {
        if (fullStatus && epochInfo) {
            const epochId = fullStatus?.sync_info.epoch_id;
            const networkInfo = fullStatus.detailed_debug_status!.network_info;
            const knownSet = new Set(
                networkInfo.known_producers
                    .map((p) => p.account_id));
            const reachableSet = new Set(
                networkInfo.known_producers
                    .filter((p) => (p.next_hops?.length ?? 0) > 0)
                    .map((p) => p.account_id));
            let blockProducers = new Set<string>();
            let chunkProducers = new Set<string>();
            for (const oneEpoch of epochInfo.status_response.EpochInfo) {
                if (oneEpoch.epoch_id === epochId) {
                    blockProducers = new Set(
                        oneEpoch.block_producers.map(bp => bp.account_id));
                    chunkProducers = new Set(oneEpoch.chunk_only_producers);
                    break;
                }
            }

            const currentHeight = fullStatus.sync_info.latest_block_height;
            const numPeersByStatus = new Map<string, number>();
            const peers = [] as PeerInfo[];
            for (const peer of networkInfo.connected_peers) {
                const validator = [];
                const routedValidator = [];
                for (const element of networkInfo.known_producers) {
                    if (blockProducers.has(element.account_id) || chunkProducers.has(element.account_id)) {
                        if (element.peer_id === peer.peer_id) {
                            // This means that the peer that we're connected to is a validator.
                            validator.push(element.account_id);
                        } else if (element.next_hops?.includes(peer.peer_id)) {
                            // This means that the peer that we're connected to is on the shortest path
                            // to this validator.
                            routedValidator.push(element.account_id);
                        }
                    }
                }
                const statusClassName = peerClass(currentHeight, peer.height || 0);
                numPeersByStatus.set(statusClassName, (numPeersByStatus.get(statusClassName) || 0) + 1);
                peers.push({
                    peer,
                    validator,
                    routedValidator,
                    statusClassName,
                });
            }
            return {
                blockProducers,
                chunkProducers,
                knownSet,
                reachableSet,
                numPeersByStatus,
                peers,
            }
        }
        return {
            blockProducers: new Set(),
            chunkProducers: new Set(),
            knownSet: new Set(),
            reachableSet: new Set(),
            numPeersByStatus: new Map(),
            peers: [],
        };
    }, [fullStatus, epochInfo]);

    const peerStorageView = PeerStorageView(addr);

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
        <p>
            Number of peers: {detailedDebugStatus.network_info.num_connected_peers}{' '}
            / {detailedDebugStatus.network_info.peer_max_count}
        </p>

        <p>
            Block producers: {blockProducers.size}{' '}
            <ul>
                <li>Unknown: {[...getDifference(blockProducers, knownSet)].join(', ') || '(none)'}</li>
                <li>Known but not reachable: {
                    [...getDifference(getIntersection(blockProducers, knownSet), reachableSet)].join(', ') || '(none)'}</li>
            </ul>
        </p>

        <p>
            Chunk producers: {chunkProducers.size}{' '}
            <ul>
                <li>Unknown: {[...getDifference(chunkProducers, knownSet)].join(', ') || '(none)'}</li>
                <li>Known but not reachable: {
                    [...getDifference(getIntersection(chunkProducers, knownSet), reachableSet)].join(', ') || '(none)'}</li>
            </ul>
        </p>

        <p>
            <strong>Unknown</strong> means that we didn't receive 'announce' information about this
            validator (so we don't know on which peer it is). This usually means that the validator
            didn't connect to the network during current epoch. <br />
            <strong>Unreachable</strong> means, that we know the peer_id of this validator, but we
            cannot find it in our routing table. This usually means that validator did connect to
            the network in the past, but now it is gone for at least 1 hour.
        </p>

        <table className="legend">
            <tr>
                {[
                    ['peer_ahead_alot', 'Peer ahead a lot'],
                    ['peer_ahead', 'Peer ahead'],
                    ['peer_in_sync', 'Peer in sync'],
                    ['peer_behind_a_little', 'Peer behind a little'],
                    ['peer_behind', 'Peer behind'],
                    ['peer_far_behind', 'Peer far behind']
                ].map(([className, description]) =>
                    <td className={className}>{description} {numPeersByStatus.get(className) || 0}</td>)}
            </tr>
        </table>

        <table className="peers">
            <thead>
                <tr>
                    <th>Address</th>
                    <th>Validator?</th>
                    <th>Account ID</th>
                    <th>Last ping</th>
                    <th>Height</th>
                    <th>Last Block Hash</th>
                    <th>Tracked Shards</th>
                    <th>Archival</th>
                    <th>Connection type</th>
                    <th>First connection</th>
                    <th>Traffic (last minute)</th>
                    <th>Route to validators</th>
                </tr>
            </thead>
            <tbody>
                {peers.map(({ peer, validator, routedValidator, statusClassName }) => {
                    return <tr key={peer.peer_id}>
                        <td>{addDebugPortLink(peer.addr)}</td>
                        <td>{validator.join(', ')}</td>
                        <td>{peer.peer_id.substring(8, 14)}...</td> {/* strips prefix 'ed25519:' */}
                        <td className={
                            peer.last_time_received_message_millis > 60 * 1000 ? 'peer_far_behind' : ''
                        }>{formatDurationInMillis(peer.last_time_received_message_millis)}</td>
                        <td className={statusClassName}>{peer.height}</td>
                        <td>{peer.block_hash}</td>
                        <td>{JSON.stringify(peer.tracked_shards)}</td>
                        <td>{JSON.stringify(peer.archival)}</td>
                        <td>{peer.is_outbound_peer ? 'OUT' : 'IN'}</td>
                        <td>{formatDurationInMillis(peer.connection_established_time_millis)}</td>
                        <td>{formatTraffic(peer.received_bytes_per_sec, peer.sent_bytes_per_sec)}</td>
                        <td><CollapsableValidatorList validators={routedValidator} /></td>
                    </tr>
                })}
            </tbody>
        </table>
        {peerStorageView}
    </div>;
};

function toHumanTime(seconds: number): string {
    let result = "";
    if (seconds >= 60) {
        let minutes = Math.floor(seconds / 60);
        seconds = seconds % 60;
        if (minutes > 60) {
            let hours = Math.floor(minutes / 60);
            minutes = minutes % 60;
            if (hours > 24) {
                let days = Math.floor(hours / 24);
                hours = hours % 24;
                result += days + " days ";
            }
            result += hours + " h ";
        }
        result += minutes + " m ";
    }
    result += seconds + " s"
    return result;
}

function PeerStorageView(addr: string): ReactElement {
    const [showPeerStorage, setShowPeerStorage] = useState(false);

    const { data: peerStore, error, isLoading } =
        useQuery(['peer_store', addr], () => fetchPeerStore(addr));

    const button = <button onClick={() => setShowPeerStorage((show) => !show)}>
        {showPeerStorage ? 'Hide' : 'Show'} detailed peer storage
    </button>
    let view = <></>;
    if (showPeerStorage) {
        if (isLoading) {
            view = <div>Loading...</div>;
        } else if (error) {
            view = <div className="error">
                {(error as Error).stack}
            </div>;
        } else {
            view = <>
                <h3>Peers in storage</h3>
                <table className="detailed-peer-storage">
                    <thead>
                        <th>Peer ID</th>
                        <th>Peer address</th>
                        <th>First seen</th>
                        <th>Last seen</th>
                        <th>Last connection attempt</th>
                        <th>Status</th>
                    </thead>
                    <tbody>
                        {peerStore!.status_response.PeerStore.peer_states.map((peer) => {
                            return <tr key={peer.peer_id}>
                                <td>{peer.peer_id}</td>
                                <td>{peer.addr}</td>
                                <td>{toHumanTime(Math.floor(Date.now() / 1000) - peer.first_seen)}</td>
                                <td>{toHumanTime(Math.floor(Date.now() / 1000) - peer.last_seen)}</td>
                                {peer.last_attempt ? <>
                                    <td>{toHumanTime(Math.floor(Date.now() / 1000) - peer.last_attempt[0])}</td>
                                    <td>{peer.status}<br />Last attempt: {peer.last_attempt[1]}</td>
                                </> : <>
                                    <td></td>
                                    <td>{peer.status}</td>
                                </>}
                            </tr>
                        })}
                    </tbody>
                </table>
            </>;
        }
    }
    return <>{button}{view}</>
};

const CollapsableValidatorList = ({ validators }: { validators: string[] }) => {
    const [showAll, setShowAll] = useState(false);
    const callback = useCallback((e: MouseEvent) => {
        e.preventDefault();
        setShowAll((show) => !show);
    }, []);
    const MAX_TO_SHOW = 2;
    if (validators.length <= MAX_TO_SHOW) {
        return <>{validators.join(', ')}</>;
    }
    if (showAll) {
        return <>{validators.join(', ')} <a href="#" onClick={callback}>Show fewer</a></>;
    }
    return <>
        {validators.slice(0, MAX_TO_SHOW).join(', ')}, ...<br />
        <a href="#" onClick={callback}>Show all {validators.length}</a>
    </>;
}