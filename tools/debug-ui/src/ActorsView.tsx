import './ActorsView.scss';
import { useQuery } from '@tanstack/react-query';
import { fetchInstrumentedThreadsView } from './api';

type ActorsViewProps = {
    addr: string;
};

export const ActorsView = ({ addr }: ActorsViewProps) => {
    const {
        data: instrumentedThreads,
        error: instrumentedThreadsError,
        isLoading: instrumentedThreadsIsLoading,
    } = useQuery(['instrumentedThreads', addr], () => fetchInstrumentedThreadsView(addr));
    if (instrumentedThreadsIsLoading) {
        return <div>Loading...</div>;
    } else if (instrumentedThreadsError) {
        return (
            <div className="actors-view">
                <div className="error">{(instrumentedThreadsError as Error).stack}</div>
            </div>
        );
    }
    let allThreads = instrumentedThreads?.status_response.InstrumentedThreads.threads;
    let sortedThreads = allThreads?.slice().sort((a, b) => a.thread_name.localeCompare(b.thread_name));

    return (
        <div className="actors-view">
            <h2>Instrumented Threads</h2>
            <table className="actors-table">
                <thead>
                    <tr>
                        <th>Thread Name</th>
                        <th>Message types</th>
                    </tr>
                </thead>
                <tbody>
                    {sortedThreads?.map((thread, idx) => (
                        <tr key={idx}>
                            <td>{thread.thread_name}</td>
                            <td>{thread.message_types.join(', ')}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
            <pre>{JSON.stringify(instrumentedThreads, null, 2)}</pre>
        </div>
    );
}