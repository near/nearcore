import { useQuery } from 'react-query';
import { toHumanTime } from './utils';
import { fetchRecentOutboundConnections } from './api';
import './ConnectionStorageView.scss';

type ConnectionStorageViewProps = {
    addr: string;
};

export const ConnectionStorageView = ({ addr }: ConnectionStorageViewProps) => {
    const {
        data: connectionStore,
        error,
        isLoading,
    } = useQuery(['connectionStore', addr], () => fetchRecentOutboundConnections(addr));

    if (isLoading) {
        return <div>Loading...</div>;
    } else if (error) {
        return <div className="error">{(error as Error).stack}</div>;
    }
    return (
        <table className="connection-storage-table">
            <thead>
                <th>Peer ID</th>
                <th>Peer address</th>
                <th>Time established</th>
                <th>Time connected until</th>
            </thead>
            <tbody>
                {connectionStore!.status_response.RecentOutboundConnections.recent_outbound_connections.map(
                    (conn) => {
                        return (
                            <tr key={conn.peer_id}>
                                <td>{conn.peer_id}</td>
                                <td>{conn.addr}</td>
                                <td>
                                    {toHumanTime(
                                        Math.floor(Date.now() / 1000) - conn.time_established
                                    )}
                                </td>
                                <td>
                                    {toHumanTime(
                                        Math.floor(Date.now() / 1000) - conn.time_connected_until
                                    )}
                                </td>
                            </tr>
                        );
                    }
                )}
            </tbody>
        </table>
    );
};
