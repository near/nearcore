import { useQuery } from '@tanstack/react-query';
import { fetchSnapshotHosts } from './api';
import './SnapshotHostsView.scss';

type SnapshotHostsViewProps = {
    addr: string;
};

export const SnapshotHostsView = ({ addr }: SnapshotHostsViewProps) => {
    const {
        data: snapshotHosts,
        error,
        isLoading,
    } = useQuery(['snapshotHosts', addr], () => fetchSnapshotHosts(addr));

    if (isLoading) {
        return <div>Loading...</div>;
    } else if (error) {
        return <div className="error">{(error as Error).stack}</div>;
    }

    let snapshot_hosts = snapshotHosts!.status_response.SnapshotHosts.hosts;
    snapshot_hosts.sort((a, b) => b.epoch_height - a.epoch_height);

    return (
        <div className="snapshot-hosts-view">
            <table>
                <thead>
                    <th>Peer ID</th>
                    <th>Shards</th>
                    <th>Epoch Height</th>
                    <th>Sync Hash</th>
                </thead>
                <tbody>
                    {snapshot_hosts.map((host) => {
                        return (
                            <tr key={host.peer_id}>
                                <td>{host.peer_id}</td>
                                <td>{JSON.stringify(host.shards)}</td>
                                <td>{host.epoch_height}</td>
                                <td>{host.sync_hash}</td>
                            </tr>
                        );
                    })}
                </tbody>
            </table>
        </div>
    );
};
