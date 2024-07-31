import { useQuery } from '@tanstack/react-query';
import { parse } from 'date-fns';
import { EpochInfoView, fetchEpochInfo, fetchFullStatus } from './api';
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
                    <th>Epoch Height</th>
                    <th>Epoch ID</th>
                    <th>Start Height</th>
                    <th>Protocol Version</th>
                    <th>First Block</th>
                    <th>Epoch Start</th>
                    <th>Block Producers</th>
                    <th>Chunk Producers</th>
                    <th>Chunk Validators</th>
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
                        // The date object inside epochInfo.first_block is very particular.
                        // It looks like this:
                        //      2024,180,0,15,28,88423066,0,0,0
                        //      year,days,hours,minutes,seconds,nanoseconds,timezone offsets
                        // The solution below parses the first part of the date object, up the seconds, in UTC.
                        epochStartColumn = `${formatDurationInMillis(
                            Date.now() -
                              parse(
                                epochInfo.first_block[1]
                                  .toString()
                                  .split(",")
                                  .slice(0, 5)
                                  .concat(["+00"])
                                  .join(","),
                                "yyyy,D,H,m,s,x",
                                new Date(),
                                { useAdditionalDayOfYearTokens: true }
                              ).getTime()
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
                            <td>{epochInfo.epoch_height}</td>
                            <td>{epochInfo.epoch_id}</td>
                            <td>{epochInfo.height}</td>
                            <td>{epochInfo.protocol_version}</td>
                            <td>{firstBlockColumn}</td>
                            <td>{epochStartColumn}</td>
                            <td>{epochInfo.block_producers.length}</td>
                            <td>{getChunkProducersTotal(epochInfo)}</td>
                            <td>{getChunkValidatorsTotal(epochInfo)}</td>
                            <td>{epochInfo.chunk_only_producers.length}</td>
                        </tr>
                    );
                })}
            </tbody>
        </table>
    );
};

function getChunkProducersTotal(epochInfo: EpochInfoView)  {
    return epochInfo.validator_info?.current_validators.reduce((acc, it) => {
        if (it.num_expected_chunks > 0) {
            acc = acc + 1;
        }
        return acc;
      }, 0) ?? "N/A"
}

function getChunkValidatorsTotal(epochInfo: EpochInfoView)  {
    return epochInfo.validator_info?.current_validators.reduce((acc, it) => {
        if (it.num_expected_endorsements > 0) {
            acc = acc + 1;
        }
        return acc;
      }, 0) ?? "N/A";
}