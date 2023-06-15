import { useQuery } from 'react-query';
import { toHumanTime } from './utils';
import { fetchRoutingTable } from './api';
import './RoutingTableView.scss';

type RoutingTableViewProps = {
    addr: string;
};

export const RoutingTableView = ({ addr }: RoutingTableViewProps) => {
    const {
        data: routingTable,
        error,
        isLoading,
    } = useQuery(['routingTable', addr], () => fetchRoutingTable(addr));

    if (isLoading) {
        return <div>Loading...</div>;
    } else if (error) {
        return <div className="error">{(error as Error).stack}</div>;
    }

    const routingInfo = routingTable!.status_response.Routes;

    const peerLabels = routingInfo.edge_cache.peer_labels;

    const routable_peers = Object.keys(routingInfo.my_distances);
    routable_peers.sort((a, b) => peerLabels[a] > peerLabels[b] ? 1 : -1);

    const direct_peers = Object.keys(routingInfo.local_edges);
    direct_peers.sort((a, b) => peerLabels[a] > peerLabels[b] ? 1 : -1);

    return (
        <div className="routing-table-view">
            <p><b>Routable Peers</b></p>
            <table>
                <thead>
                    <th>Peer ID</th>
                    <th>Peer Label</th>
                    <th>Shortest Path Length (Hops)</th>
                </thead>
                <tbody>
                    {routable_peers.map((peer_id) => {
                        const peer_label = peerLabels[peer_id];
                        return (
                            <tr key={peer_label}>
                                <td>{peer_id.substring(8, 14)}...</td>
                                <td>{peer_label}</td>
                                <td>{routingInfo.my_distances[peer_id]}</td>
                            </tr>
                        );
                    })}
                </tbody>
            </table>
            <br/>
            <p><b>Direct Peers</b></p>
            <table>
                <thead>
                    <th>Peer ID</th>
                    <th>Peer Label</th>
                    <th>Advertised Distances</th>
                </thead>
                <tbody>
                    {direct_peers.map((peer_id) => {
                        const peer_label = peerLabels[peer_id];
                        return (
                            <tr key={peer_label}>
                                <td>{peer_id.substring(8, 14)}...</td>
                                <td>{peer_label}</td>
                                <td>{routingInfo.peer_distances[peer_id].distance.join(', ')}</td>
                            </tr>
                        );
                    })}
                </tbody>
            </table>
        </div>
    );
};
