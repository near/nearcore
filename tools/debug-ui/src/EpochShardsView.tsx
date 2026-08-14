import { useQuery } from '@tanstack/react-query';
import { fetchEpochInfoLight, normalizeShardsSizeAndParts } from './api';
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
    } = useQuery(['epochInfoLight', addr], () => fetchEpochInfoLight(addr, null));

    if (epochIsLoading) {
        return <div>Loading...</div>;
    }
    if (epochError) {
        return <div className="error">{(epochError as Error).stack}</div>;
    }
    const epochs = epochData!.status_response.EpochInfo;
    const displayedEpochs = epochs.slice(1);

    const shardSizesByEpoch = displayedEpochs.map((epoch) =>
        normalizeShardsSizeAndParts(epoch.shards_size_and_parts)
    );
    // A single response comes from a single node, so the shape is uniform across epochs.
    const keyedByShardId = shardSizesByEpoch.some((sizes) => sizes.keyedByShardId);

    // One row per shard id (or per shard index on older nodes). Taking the union across
    // epochs rather than the largest count means a shard split or merged partway through
    // the window still gets its own row, populated only for the epochs it existed in.
    const shardKeys = [
        ...new Set(shardSizesByEpoch.flatMap((sizes) => [...sizes.entries.keys()])),
    ].sort((a, b) => a - b);

    let maxShardSize = 0;
    for (const sizes of shardSizesByEpoch) {
        for (const entry of sizes.entries.values()) {
            maxShardSize = Math.max(maxShardSize, entry.shard_size);
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
                    {displayedEpochs.map((epoch) => {
                        return <th key={epoch.epoch_id}>{epoch.epoch_id.substring(0, 6)}...</th>;
                    })}
                </tr>
            </thead>
            <tbody>
                {shardKeys.map((shardKey) => {
                    return (
                        <tr key={shardKey}>
                            <td>{keyedByShardId ? 'Shard Id' : 'Shard Index'} {shardKey}</td>
                            {displayedEpochs.map((epoch, epochIndex) => {
                                const entry = shardSizesByEpoch[epochIndex].entries.get(
                                    shardKey
                                );
                                if (entry === undefined) {
                                    return <td key={epoch.epoch_id} />;
                                }
                                return (
                                    <td key={epoch.epoch_id}>
                                        <div
                                            className={`shard-cell ${
                                                entry.state_header_exists ? 'requested' : ''
                                            }`}>
                                            {drawShardSizeBar(entry.shard_size, maxShardSize)}
                                            <div className="shard-parts">
                                                {entry.state_parts_count} parts
                                            </div>
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
