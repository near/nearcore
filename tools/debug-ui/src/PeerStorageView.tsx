import { useQuery } from 'react-query';
import { toHumanTime } from './utils';
import { fetchPeerStore } from './api';
import './PeerStorageView.scss';

type PeerStorageViewProps = {
    addr: string;
};

export const PeerStorageView = ({ addr }: PeerStorageViewProps) => {
    const {
        data: peerStore,
        error,
        isLoading,
    } = useQuery(['peerStore', addr], () => fetchPeerStore(addr));

    if (isLoading) {
        return <div>Loading...</div>;
    } else if (error) {
        return <div className="error">{(error as Error).stack}</div>;
    }
    return (
        <table className="peer-storage-table">
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
                    return (
                        <tr key={peer.peer_id}>
                            <td>{peer.peer_id}</td>
                            <td>{peer.addr}</td>
                            <td>{toHumanTime(Math.floor(Date.now() / 1000) - peer.first_seen)}</td>
                            <td>{toHumanTime(Math.floor(Date.now() / 1000) - peer.last_seen)}</td>
                            {peer.last_attempt ? (
                                <>
                                    <td>
                                        {toHumanTime(
                                            Math.floor(Date.now() / 1000) - peer.last_attempt[0]
                                        )}
                                    </td>
                                    <td>
                                        {peer.status}
                                        <br />
                                        Last attempt: {peer.last_attempt[1]}
                                    </td>
                                </>
                            ) : (
                                <>
                                    <td></td>
                                    <td>{peer.status}</td>
                                </>
                            )}
                        </tr>
                    );
                })}
            </tbody>
        </table>
    );
};
