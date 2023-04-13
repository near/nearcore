import './BlocksView.scss';
import { useQuery } from 'react-query';
import { fetchChainProcessingStatus, BlockProcessingStatus, ChunkProcessingStatus } from './api';

type BlocksViewProps = {
    addr: string;
};

function prettyTime(datetime: string): string {
    const time = new Date(Date.parse(datetime));
    return String(time.getUTCHours()).concat(
        ':',
        String(time.getUTCMinutes()).padStart(2, '0'),
        ':',
        String(time.getUTCSeconds()).padStart(2, '0'),
        '.',
        String(time.getUTCMilliseconds()).padStart(3, '0')
    );
}

function printTimeInMs(time: number | null): string {
    if (time == null) {
        return 'N/A';
    } else {
        return time + 'ms';
    }
}

function printDuration(start: string, end: string): string {
    const duration = Date.parse(end) - Date.parse(start);
    if (duration > 0) {
        return `+${duration}ms`;
    } else {
        return `${duration}ms`;
    }
}

function getChunkStatusSymbol(chunkStatus: ChunkProcessingStatus): string {
    switch (chunkStatus) {
        case 'Completed':
            return '✔';
        case 'Requested':
            return '⬇';
        case 'NeedToRequest':
            return '.';
        default:
            return '';
    }
}

function printBlockStatus(blockStatus: BlockProcessingStatus): string {
    if (typeof blockStatus === 'string') {
        return blockStatus;
    }
    if ('Error' in blockStatus) {
        return `Error: ${blockStatus.Error}`;
    }
    return `Dropped: ${blockStatus.Dropped}`;
}

export const BlocksView = ({ addr }: BlocksViewProps) => {
    const {
        data: chainProcessingInfo,
        error: chainProcessingInfoError,
        isLoading: chainProcessingInfoLoading,
    } = useQuery(['chainProcessingInfo', addr], () => fetchChainProcessingStatus(addr));
    if (chainProcessingInfoLoading) {
        return <div>Loading...</div>;
    } else if (chainProcessingInfoError) {
        return (
            <div className="chain-and-chunk-blocks-view">
                <div className="error">{(chainProcessingInfoError as Error).stack}</div>
            </div>
        );
    } else if (!chainProcessingInfo) {
        return (
            <div className="chain-and-chunk-blocks-view">
                <div className="error">No Data</div>
            </div>
        );
    }
    const numShards =
        chainProcessingInfo!.status_response.ChainProcessingStatus.blocks_info[0].chunks_info!
            .length;
    const shardIndices = [...Array(numShards).keys()];
    return (
        <div className="chain-and-chunk-blocks-view">
            <table className="chain-and-chunk-blocks-table">
                <thead>
                    <tr>
                        <th>Height</th>
                        <th>Hash</th>
                        <th>Received</th>
                        <th>Status</th>
                        <th>In Progress for</th>
                        <th>In Orphan for</th>
                        <th>Missing Chunks for</th>
                        {shardIndices.map((shardIndex) => {
                            return <th key={shardIndex}> Shard {shardIndex} </th>;
                        })}
                    </tr>
                </thead>
                <tbody>
                    {chainProcessingInfo!.status_response.ChainProcessingStatus.blocks_info.map(
                        (block) => {
                            return (
                                <tr key={block.height}>
                                    <td>{block.height}</td>
                                    <td>{block.hash}</td>
                                    <td>{prettyTime(block.received_timestamp)}</td>
                                    <td>{printBlockStatus(block.block_status)}</td>
                                    <td>{printTimeInMs(block.in_progress_ms)}</td>
                                    <td>{printTimeInMs(block.orphaned_ms)}</td>
                                    <td>{printTimeInMs(block.missing_chunks_ms)}</td>
                                    {block.chunks_info!.map((chunk) => {
                                        if (chunk) {
                                            return (
                                                <td key={chunk.shard_id}>
                                                    <strong>{`${
                                                        chunk.status
                                                    } ${getChunkStatusSymbol(
                                                        chunk.status
                                                    )}`}</strong>
                                                    {chunk.completed_timestamp && (
                                                        <>
                                                            <br /> Completed @ BR
                                                            {`${printDuration(
                                                                block.received_timestamp,
                                                                chunk.completed_timestamp
                                                            )}`}
                                                        </>
                                                    )}
                                                    {chunk.requested_timestamp && (
                                                        <>
                                                            <br /> Requested @ BR
                                                            {`${printDuration(
                                                                block.received_timestamp,
                                                                chunk.requested_timestamp
                                                            )}`}
                                                        </>
                                                    )}
                                                    {chunk.request_duration && (
                                                        <>
                                                            <br /> Duration
                                                            {`${printTimeInMs(
                                                                chunk.request_duration
                                                            )}`}
                                                        </>
                                                    )}
                                                </td>
                                            );
                                        } else {
                                            return <td key={block.height}> No Chunk </td>;
                                        }
                                    })}
                                </tr>
                            );
                        }
                    )}
                </tbody>
            </table>
        </div>
    );
};
