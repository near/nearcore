import { useQuery } from 'react-query';
import { fetchBasicStatus } from './api';
import './HeaderBar.scss';

type Props = {
    addr: string;
};

export const HeaderBar = ({ addr }: Props) => {
    const {
        data: nodeStatus,
        error,
        isLoading,
    } = useQuery(['status', addr], () => fetchBasicStatus(addr));
    if (isLoading) {
        return (
            <div className="header-bar">
                <div className="loading">Loading...</div>
            </div>
        );
    }
    if (error) {
        return (
            <div className="header-bar">
                <div className="error">{'' + error}</div>
            </div>
        );
    }
    if (!nodeStatus) {
        return (
            <div className="header-bar">
                <div className="error">No node status</div>
            </div>
        );
    }
    const version = nodeStatus.version;
    return (
        <div className="header-bar">
            <div className="label">Chain</div>
            <div className="value">{nodeStatus.chain_id}</div>
            {nodeStatus.validator_account_id && <div className="label">Validator</div>}
            {nodeStatus.validator_account_id && (
                <div className="value">{nodeStatus.validator_account_id}</div>
            )}
            <div className="label">Protocol</div>
            <div className="value">{nodeStatus.protocol_version}</div>
            <div className="label">Build</div>
            <div className="value">
                {version.version === 'trunk' && 'Nightly build '}
                {version.version === version.build && 'Release '}
                {version.version === 'trunk' || version.version === version.build ? (
                    <a
                        href={`https://github.com/near/nearcore/tree/${version.build}`}
                        target="_blank"
                        rel="noreferrer">
                        {version.build}
                    </a>
                ) : (
                    `Release ${version.version} (build ${version.build})`
                )}
            </div>
            <div className="label">rustc</div>
            <div className="value">{version.rustc_version}</div>
            <div className="label">Uptime</div>
            <div className="value">{formatDurationInSec(nodeStatus.uptime_sec)}</div>
            <div className="label">Address</div>
            <div className="value">{addr}</div>
        </div>
    );
};

function formatDurationInSec(totalSeconds: number): string {
    const seconds = totalSeconds % 60;
    const total_minutes = (totalSeconds - seconds) / 60;
    const minutes = total_minutes % 60;
    const total_hours = (total_minutes - minutes) / 60;
    const hours = total_hours % 24;
    const days = (total_hours - hours) / 24;
    return `${days}d ${addZeros(hours)}:${addZeros(minutes)}:${addZeros(seconds)}`;
}

function addZeros(x: number): string {
    if (x >= 10) {
        return '' + x;
    } else {
        return '0' + x;
    }
}
