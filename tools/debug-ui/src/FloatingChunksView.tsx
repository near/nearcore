import './FloatingChunksView.scss';
import { useQuery } from 'react-query';
import { fetchChainProcessingStatus } from './api';

type FloatingChunksViewProps = {
    addr: string;
};

export const FloatingChunksView = ({ addr }: FloatingChunksViewProps) => {
    const {
        data: chainProcessingInfo,
        error: chainProcessingInfoError,
        isLoading: chainProcessingInfoLoading,
    } = useQuery(['chainProcessingInfo', addr], () => fetchChainProcessingStatus(addr));
    if (chainProcessingInfoLoading) {
        return <div>Loading...</div>;
    } else if (chainProcessingInfoError) {
        return (
            <div className="floating-chunks-view">
                <div className="error">{(chainProcessingInfoError as Error).stack}</div>
            </div>
        );
    } else if (!chainProcessingInfo) {
        return (
            <div className="floating-chunks-view">
                <div className="error">No Data</div>
            </div>
        );
    }
    return (
        <div className="floating-chunks-view">
            <h1>
                {' '}
                Floating chunks are chunks that we are not yet sure which blocks they belong to.{' '}
            </h1>
            <table className="floating-chunks-table">
                <thead>
                    <tr>
                        <th>Height</th>
                        <th>ShardId</th>
                        <th>Hash</th>
                        <th>Created by</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {chainProcessingInfo!.status_response.ChainProcessingStatus.floating_chunks_info.map(
                        (chunk) => {
                            return (
                                <tr key={chunk.height_created}>
                                    <td>{chunk.height_created}</td>
                                    <td>{chunk.shard_id}</td>
                                    <td>{chunk.chunk_hash}</td>
                                    <td>{chunk.created_by}</td>
                                    <td>{chunk.status}</td>
                                </tr>
                            );
                        }
                    )}
                </tbody>
            </table>
        </div>
    );
};
