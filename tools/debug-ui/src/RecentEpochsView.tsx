import { useQuery } from '@tanstack/react-query';
import { parse } from 'date-fns';
import { fetchEpochInfo, fetchEpochInfoLight, fetchFullStatus } from './api';
import './RecentEpochsView.scss';
import { formatDurationInMillis } from './utils';

type RecentEpochsViewProps = {
    addr: string;
};

export const RecentEpochsView = ({ addr }: RecentEpochsViewProps) => {
    // The lightweight variant omits the heavy per-validator `validator_info`, so the
    // page renders fast. This drives the initial load.
    const {
        data: epochData,
        error: epochError,
        isLoading: epochIsLoading,
    } = useQuery(['epochInfoLight', addr], () => fetchEpochInfoLight(addr, null));
    const {
        data: statusData,
        error: statusError,
        isLoading: statusIsLoading,
    } = useQuery(['fullStatus', addr], () => fetchFullStatus(addr));
    // The full variant carries per-validator stake. We fetch it separately and let it
    // populate the Total Stake column once it arrives, so it never blocks the page.
    // `stakeIsLoading` is true only while the full data is loading for the first time;
    // after it has loaded once, the data stays available and this flag stays false, so
    // the "Loading…" placeholder can never replace a value that was already shown.
    const { data: fullEpochData, isLoading: stakeIsLoading } = useQuery(['epochInfo', addr], () =>
        fetchEpochInfo(addr, null)
    );

    if (epochIsLoading || statusIsLoading) {
        return <div>Loading...</div>;
    }
    if (epochError || statusError) {
        return <div className="error">{((epochError || statusError) as Error).stack}</div>;
    }
    const epochInfos = epochData!.status_response.EpochInfo;
    const status = statusData!;

    // Map each epoch id to its total stake, derived from the full epoch info. The next
    // epoch's row has no `validator_info` of its own, but its already-finalized stake is
    // exposed as `next_validators` on the current epoch (the entry right after it).
    // Proposals are deliberately excluded: they only affect the next-next epoch and can
    // still change.
    const fullEpochInfos = fullEpochData?.status_response.EpochInfo;
    const totalStakeByEpochId = new Map<string, number>();
    if (fullEpochInfos !== undefined) {
        const sumStake = (validators: { stake: string }[]) =>
            validators.reduce((sum, validator) => sum + parseFloat(validator.stake), 0);
        for (const epochInfo of fullEpochInfos) {
            if (epochInfo.validator_info) {
                totalStakeByEpochId.set(
                    epochInfo.epoch_id,
                    sumStake(epochInfo.validator_info.current_validators)
                );
            }
        }
        const [nextEpoch, currentEpoch] = fullEpochInfos;
        if (nextEpoch && currentEpoch?.validator_info) {
            totalStakeByEpochId.set(
                nextEpoch.epoch_id,
                sumStake(currentEpoch.validator_info.next_validators)
            );
        }
    }

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
                    <th>Total Stake</th>
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
                                        .split(',')
                                        .slice(0, 5)
                                        .concat(['+00'])
                                        .join(','),
                                    'yyyy,D,H,m,s,x',
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
                            <td>{epochInfo.chunk_producers.length}</td>
                            <td>{epochInfo.chunk_validators.length}</td>
                            <td>
                                {totalStakeByEpochId.has(epochInfo.epoch_id)
                                    ? Math.floor(
                                          totalStakeByEpochId.get(epochInfo.epoch_id)! / 1e24
                                      ).toLocaleString('en-US')
                                    : stakeIsLoading
                                    ? 'Loading…'
                                    : ''}
                            </td>
                        </tr>
                    );
                })}
            </tbody>
        </table>
    );
};
