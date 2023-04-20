import { useQuery } from 'react-query';
import { fetchEpochInfo } from './api';
import './EpochShardsView.scss';

function humanFileSize(bytes: number, si = false, dp = 1): string {
    const thresh = si ? 1000 : 1024;

    if (Math.abs(bytes) < thresh) {
        return bytes + ' B';
    }

    const units = si
        ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
        : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
    let u = -1;
    const r = 10 ** dp;

    do {
        bytes /= thresh;
        ++u;
    } while (Math.round(Math.abs(bytes) * r) / r >= thresh && u < units.length - 1);

    return bytes.toFixed(dp) + ' ' + units[u];
}

type EpochShardsViewProps = {
    addr: string;
};

export const EpochShardsView = ({ addr }: EpochShardsViewProps) => {
    const {
        data: epochData,
        error: epochError,
        isLoading: epochIsLoading,
    } = useQuery(['epochInfo', addr], () => fetchEpochInfo(addr));

    if (epochIsLoading) {
        return <div>Loading...</div>;
    }
    if (epochError) {
        return <div className="error">{(epochError as Error).stack}</div>;
    }
    const epochs = epochData!.status_response.EpochInfo;

    let numShards = 0;
    let maxShardSize = 0;
    for (const epoch of epochs.slice(1)) {
        numShards = Math.max(epoch.shards_size_and_parts.length, numShards);
        for (const [size] of epoch.shards_size_and_parts) {
            maxShardSize = Math.max(maxShardSize, size);
        }
    }
    return (
        <table className="epoch-shards-table">
            <thead>
                <tr>
                    <th></th>
                    <th>Current Epoch</th>
                    <th colSpan={epochs.length - 2}>Past Epochs</th>
                </tr>
                <tr>
                    <th></th>
                    {epochs.slice(1).map((epoch) => {
                        return <th key={epoch.epoch_id}>{epoch.epoch_id.substring(0, 6)}...</th>;
                    })}
                </tr>
            </thead>
            <tbody>
                {[...Array(numShards).keys()].map((i) => {
                    return (
                        <tr key={i}>
                            <td>Shard {i}</td>
                            {epochs.slice(1).map((epoch) => {
                                if (epoch.shards_size_and_parts.length <= i) {
                                    return <></>;
                                }
                                const [size, parts, requested] = epoch.shards_size_and_parts[i];
                                return (
                                    <td key={epoch.epoch_id}>
                                        <div
                                            className={`shard-cell ${
                                                requested ? 'requested' : ''
                                            }`}>
                                            {drawShardSizeBar(size, maxShardSize)}
                                            <div className="shard-parts">{parts} parts</div>
                                        </div>
                                    </td>
                                );
                            })}
                        </tr>
                    );
                })}
            </tbody>
        </table>
    );
};

function drawShardSizeBar(size: number, maxSize: number): JSX.Element {
    const width = (size / maxSize) * 100 + 5;
    const text = humanFileSize(size);
    return (
        <div className="shard-size-bar">
            <div className="bar" style={{ width }}></div>
            <div className="text">{text}</div>
        </div>
    );
}
