import { useQuery } from 'react-query';
import { fetchEpochInfo, fetchFullStatus } from './api';
import { formatDurationInMillis } from './utils';
import './RecentEpochsView.scss';

type RecentEpochsViewProps = {
    addr: string;
};

export const RecentEpochsView = ({ addr }: RecentEpochsViewProps) => {
    const {
        data: epochData,
        error: epochError,
        isLoading: epochIsLoading,
    } = useQuery(['epochInfo', addr], () => fetchEpochInfo(addr));
    const {
        data: statusData,
        error: statusError,
        isLoading: statusIsLoading,
    } = useQuery(['fullStatus', addr], () => fetchFullStatus(addr));

    if (epochIsLoading || statusIsLoading) {
        return <div>Loading...</div>;
    }
    if (epochError || statusError) {
        return <div className="error">{((epochError || statusError) as Error).stack}</div>;
    }
    const epochInfos = epochData!.status_response.EpochInfo;
    const status = statusData!;

    return (
        <table className="recent-epochs-table">
            <thead>
                <tr>
                    <th></th>
                    <th>Epoch ID</th>
                    <th>Start Height</th>
                    <th>Protocol Version</th>
                    <th>First Block</th>
                    <th>Epoch Start</th>
                    <th>Block Producers</th>
                    <th>Chunk-only Producers</th>
                </tr>
            </thead>
            <tbody>
                {epochInfos.map((epochInfo, index) => {
                    let firstBlockColumn = '';
                    let epochStartColumn = '';
                    if (epochInfo.first_block === null) {
                        if (index == 0) {
                            const blocksRemaining =
                                epochInfo.height - status.sync_info.latest_block_height;
                            const millisecondsRemaining =
                                blocksRemaining *
                                status.detailed_debug_status!.block_production_delay_millis;
                            firstBlockColumn = `Next epoch - in ${blocksRemaining} blocks`;
                            epochStartColumn = `in ${formatDurationInMillis(
                                millisecondsRemaining
                            )}`;
                        }
                    } else {
                        firstBlockColumn = epochInfo.first_block[0];
                        epochStartColumn = `${formatDurationInMillis(
                            Date.now() - Date.parse(epochInfo.first_block[1])
                        )} ago`;
                    }
                    let rowClassName = '';
                    let firstColumnText = '';
                    if (index == 0) {
                        rowClassName = 'next-epoch';
                        firstColumnText = 'Next ⮕';
                    } else if (index == 1) {
                        rowClassName = 'current-epoch';
                        firstColumnText = 'Current ⮕';
                    }

                    return (
                        <tr className={rowClassName} key={epochInfo.epoch_id}>
                            <td>{firstColumnText}</td>
                            <td>{epochInfo.epoch_id}</td>
                            <td>{epochInfo.height}</td>
                            <td>{epochInfo.protocol_version}</td>
                            <td>{firstBlockColumn}</td>
                            <td>{epochStartColumn}</td>
                            <td>{epochInfo.block_producers.length}</td>
                            <td>{epochInfo.chunk_only_producers.length}</td>
                        </tr>
                    );
                })}
            </tbody>
        </table>
    );
};
