import { useQuery } from '@tanstack/react-query';
import { fetchSyncStatus } from './api';

type SyncInfoViewProps = {
    addr: string;
};

export const SyncInfoView = ({ addr }: SyncInfoViewProps) => {
    const {
        data: syncInfo,
        error,
        isLoading,
    } = useQuery(['syncInfo', addr], () => fetchSyncStatus(addr));

    if (isLoading) {
        return <div>Loading...</div>;
    } else if (error) {
        return <div className="error">{(error as Error).stack}</div>;
    }

    return (
        <div className="sync-info-view">
            <p>
                <b>Sync Info</b>
            </p>
            <pre>{JSON.stringify(syncInfo, null, 2)}</pre>
        </div>
    );
};
