import { MouseEvent, ReactElement, useCallback, useMemo, useState } from "react";
import { useQuery } from "react-query";
import { fetchEpochInfo, fetchFullStatus, PeerInfoView } from "./api";
import { formatDurationInMillis, formatTraffic, addDebugPortLink } from "./utils";
import "./Tier1View.scss";

type Tier1ViewProps = {
    addr: string,
};

type PeerInfo = {
    peer: PeerInfoView,
    validator: string[],
    routedValidator: string[],
    statusClassName: string,
}

export const Tier1View = ({ addr }: Tier1ViewProps) => {
    const { data: fullStatus, error: fullStatusError, isLoading: fullStatusLoading } =
        useQuery(['fullStatus', addr], () => fetchFullStatus(addr));

    if (fullStatusLoading) {
        return <div>Loading...</div>;
    }
    if (fullStatusError) {
        return <div className="network-info-view">
            <div className="error">
                {(fullStatusError as Error).stack}
            </div>
        </div>;
    }
    if (!fullStatus) {
        return <div className="network-info-view">
            <div className="error">No Data</div>
        </div>;
    }

    let rendered = new Set();
    const network_info = fullStatus.detailed_debug_status!.network_info;

    let rows = new Array();

    // tier1_connections contains TIER1 nodes we are currently connected to
    network_info.tier1_connections!.forEach(function (peer) {
        let account_key = "";
        let proxies = new Array();

        network_info.tier1_accounts_data!.forEach(account => {
            if (account.peer_id == peer.peer_id) {
                account_key = account.account_key;
                account.proxies.forEach(proxy => {
                    proxies.push(proxy.peer_id.substr(8, 5) + "...@" + proxy.addr);
                });
            }
        });

        rendered.add(account_key);

        let account_id = "";
        network_info.known_producers.forEach(producer => {
            if (producer.peer_id == peer.peer_id) {
                account_id = producer.account_id;
            }
        });

        let last_ping = formatDurationInMillis(peer.last_time_received_message_millis)
        let last_ping_class = ""
        if (fullStatus.node_public_key == account_key) {
            last_ping = "n/a"
        } else if (peer.last_time_received_message_millis > 60 * 1000) {
            last_ping_class = "peer_far_behind";
        }

        rows.push(<tr key={account_key}>
            <td>{addDebugPortLink(peer.addr)}</td>
            <td>{account_key.substring(8, 14)}...</td>
            <td>{account_id}</td>
            <td><CollapsableProxyList proxies={proxies} /></td>
            <td>{peer.peer_id.substring(8, 14)}...</td>
            <td className={last_ping_class}>{last_ping}</td>
            <td>{JSON.stringify(peer.tracked_shards)}</td>
            <td>{JSON.stringify(peer.archival)}</td>
            <td>{peer.is_outbound_peer ? 'OUT' : 'IN'}</td>
            <td>{formatDurationInMillis(peer.connection_established_time_millis)}</td>
            <td>{formatTraffic(peer.received_bytes_per_sec, peer.sent_bytes_per_sec)}</td>
        </tr>);
    });

    // tier1_accounts_data contains data about TIER1 nodes we would like to connect to
    network_info.tier1_accounts_data!.forEach(account => {
        let account_key = account.account_key;

        if (rendered.has(account_key)) {
            return;
        }
        rendered.add(account_key);

        let account_id = "";
        network_info.known_producers.forEach(producer => {
            if (producer.peer_id == account.peer_id) {
                account_id = producer.account_id;
            }
        });

        let proxies = new Array();
        account.proxies.forEach(proxy => {
            proxies.push(proxy.peer_id.substr(8, 5) + "...@" + proxy.addr);
        });

        rows.push(<tr key={account_key}>
            <td></td>
            <td>{account_key.substring(8, 14)}...</td>
            <td>{account_id}</td>
            <td><CollapsableProxyList proxies={proxies} /></td>
            <td>{account.peer_id.substring(8, 14)}...</td>
            <td></td>
            <td></td>
            <td></td>
            <td></td>
            <td></td>
            <td></td>
        </tr>);
    });

    // tier1_accounts_keys contains accounts whose data we would like to collect
    network_info.tier1_accounts_keys!.forEach(account_key => {
        if (rendered.has(account_key)) {
            return;
        }
        rendered.add(account_key);

        rows.push(<tr key={account_key}>
            <td></td>
            <td>{account_key.substring(8, 14)}...</td>
            <td></td>
            <td></td>
            <td></td>
            <td></td>
            <td></td>
            <td></td>
            <td></td>
            <td></td>
            <td></td>
        </tr>);
    });

    return <div className="tier1-peers-view">
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
            <tbody>
                {rows}
            </tbody>
        </table>
    </div>;
};

const CollapsableProxyList = ({ proxies }: { proxies: string[] }) => {
    const [showAll, setShowAll] = useState(false);
    const callback = useCallback((e: MouseEvent) => {
        e.preventDefault();
        setShowAll((show) => !show);
    }, []);
    const MAX_TO_SHOW = 2;
    if (proxies.length <= MAX_TO_SHOW) {
        return <>{proxies.join(', ')}</>;
    }
    if (showAll) {
        return <>{proxies.join(', ')} <a href="#" onClick={callback}>Show fewer</a></>;
    }
    return <>
        {proxies.slice(0, MAX_TO_SHOW).join(', ')}, ...<br />
        <a href="#" onClick={callback}>Show all {proxies.length}</a>
    </>;
}
