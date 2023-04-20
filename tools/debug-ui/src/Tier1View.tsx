import { MouseEvent, useCallback, useState } from 'react';
import { useQuery } from 'react-query';
import { PeerAddr, fetchFullStatus } from './api';
import { addDebugPortLink, formatDurationInMillis, formatTraffic } from './utils';
import './Tier1View.scss';

type Tier1ViewProps = {
    addr: string;
};

export const Tier1View = ({ addr }: Tier1ViewProps) => {
    const {
        data: fullStatus,
        error: fullStatusError,
        isLoading: fullStatusLoading,
    } = useQuery(['fullStatus', addr], () => fetchFullStatus(addr));

    if (fullStatusLoading) {
        return <div>Loading...</div>;
    }
    if (fullStatusError) {
        return (
            <div className="network-info-view">
                <div className="error">{(fullStatusError as Error).stack}</div>
            </div>
        );
    }
    if (!fullStatus) {
        return (
            <div className="network-info-view">
                <div className="error">No Data</div>
            </div>
        );
    }

    const networkInfo = fullStatus.detailed_debug_status!.network_info;

    const rendered = new Set();
    const rows = [];

    // tier1_connections contains TIER1 nodes we are currently connected to
    for (const peer of networkInfo.tier1_connections!) {
        let accountKey = '';
        let proxies: PeerAddr[] = [];

        networkInfo.tier1_accounts_data!.forEach((account) => {
            if (account.peer_id == peer.peer_id) {
                accountKey = account.account_key;
                proxies = account.proxies;
            }
        });

        rendered.add(accountKey);

        let accountId = '';
        networkInfo.known_producers.forEach((producer) => {
            if (producer.peer_id == peer.peer_id) {
                accountId = producer.account_id;
            }
        });

        let lastPing = formatDurationInMillis(peer.last_time_received_message_millis);
        let lastPingClass = '';
        if (fullStatus.node_public_key == accountKey) {
            lastPing = 'n/a';
        } else if (peer.last_time_received_message_millis > 60 * 1000) {
            lastPingClass = 'peer_far_behind';
        }

        rows.push(
            <tr key={accountKey}>
                <td>{addDebugPortLink(peer.addr)}</td>
                <td>{accountKey.substring(8, 14)}...</td>
                <td>{accountId}</td>
                <td>
                    <CollapsibleProxyList proxies={proxies} />
                </td>
                <td>{peer.peer_id.substring(8, 14)}...</td>
                <td className={lastPingClass}>{lastPing}</td>
                <td>{JSON.stringify(peer.tracked_shards)}</td>
                <td>{JSON.stringify(peer.archival)}</td>
                <td>{peer.is_outbound_peer ? 'OUT' : 'IN'}</td>
                <td>{formatDurationInMillis(peer.connection_established_time_millis)}</td>
                <td>{formatTraffic(peer.received_bytes_per_sec, peer.sent_bytes_per_sec)}</td>
            </tr>
        );
    }

    // tier1_accounts_data contains data about TIER1 nodes we would like to connect to
    for (const account of networkInfo.tier1_accounts_data!) {
        const accountKey = account.account_key;

        if (rendered.has(accountKey)) {
            continue;
        }
        rendered.add(accountKey);

        let accountId = '';
        networkInfo.known_producers.forEach((producer) => {
            if (producer.peer_id == account.peer_id) {
                accountId = producer.account_id;
            }
        });

        rows.push(
            <tr key={accountKey}>
                <td></td>
                <td>{accountKey.substring(8, 14)}...</td>
                <td>{accountId}</td>
                <td>
                    <CollapsibleProxyList proxies={account.proxies} />
                </td>
                <td>{account.peer_id.substring(8, 14)}...</td>
                <td></td>
                <td></td>
                <td></td>
                <td></td>
                <td></td>
                <td></td>
            </tr>
        );
    }

    // tier1_accounts_keys contains accounts whose data we would like to collect
    for (const accountKey of networkInfo.tier1_accounts_keys!) {
        if (rendered.has(accountKey)) {
            continue;
        }
        rendered.add(accountKey);

        rows.push(
            <tr key={accountKey}>
                <td></td>
                <td>{accountKey.substring(8, 14)}...</td>
                <td></td>
                <td></td>
                <td></td>
                <td></td>
                <td></td>
                <td></td>
                <td></td>
                <td></td>
                <td></td>
            </tr>
        );
    }

    return (
        <div className="tier1-peers-view">
            <table className="peers">
                <thead>
                    <tr>
                        <th>Address</th>
                        <th>AccountKey</th>
                        <th>Account ID</th>
                        <th>Proxies</th>
                        <th>PeerId</th>
                        <th>Last ping</th>
                        <th>Tracked Shards</th>
                        <th>Archival</th>
                        <th>Connection type</th>
                        <th>First connection</th>
                        <th>Traffic (last minute)</th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
        </div>
    );
};

const CollapsibleProxyList = ({ proxies }: { proxies: PeerAddr[] }) => {
    const [showAll, setShowAll] = useState(false);
    const callback = useCallback((e: MouseEvent) => {
        e.preventDefault();
        setShowAll((show) => !show);
    }, []);
    const MAX_TO_SHOW = 2;

    const formatted = proxies.map((proxy) => {
        return proxy.peer_id.substring(8, 14) + '...@' + proxy.addr;
    });

    if (formatted.length <= MAX_TO_SHOW) {
        return <>{formatted.join(', ')}</>;
    }
    if (showAll) {
        return (
            <>
                {formatted.join(', ')}{' '}
                <a href="#" onClick={callback}>
                    Show fewer
                </a>
            </>
        );
    }
    return (
        <>
            {formatted.slice(0, MAX_TO_SHOW).join(', ')}, ...
            <br />
            <a href="#" onClick={callback}>
                Show all {formatted.length}
            </a>
        </>
    );
};
