import { useState, useRef, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import './ActorsView.scss';
import { fetchInstrumentedThreadsView, InstrumentedThread } from './api';
import { ThreadTimeline } from './actors/ThreadTimeline';

type ActorsViewProps = {
    addr: string;
};

// Helper function to format thread names for better word wrapping at `::`
const formatThreadName = (threadName: string): string => {
    // Insert zero-width space (\u200B) after :: to allow line breaks
    return threadName.replace(/::/g, '::​')
};

export const ActorsView = ({ addr }: ActorsViewProps) => {
    const [loadedData, setLoadedData] = useState<any>(null);
    const [hasInitiallyFetched, setHasInitiallyFetched] = useState<boolean>(false);
    const [yAxisMode, setYAxisMode] = useState<'auto' | 'fixed'>('auto');
    const [timelineChartMode, setTimelineChartMode] = useState<'cpu' | 'dequeue'>('cpu');
    const fileInputRef = useRef<HTMLInputElement>(null);

    const {
        data: instrumentedThreads,
        error: instrumentedThreadsError,
        isLoading: instrumentedThreadsIsLoading,
        refetch: refetchInstrumentedThreads,
    } = useQuery(['instrumentedThreads', addr], () => fetchInstrumentedThreadsView(addr), {
        enabled: false, // Avoid auto-fetch
    });

    // Fetch data only once on initial mount
    useEffect(() => {
        if (!hasInitiallyFetched && !loadedData) {
            refetchInstrumentedThreads();
            setHasInitiallyFetched(true);
        }
    }, [refetchInstrumentedThreads, hasInitiallyFetched, loadedData]);

    // Use loaded data if available, otherwise use fetched data
    const currentData = loadedData || instrumentedThreads;

    const handleRefreshData = async () => {
        await refetchInstrumentedThreads();
    };

    const handleSaveData = () => {
        if (!currentData) return;

        const dataStr = JSON.stringify(currentData, null, 2);
        const dataBlob = new Blob([dataStr], { type: 'application/json' });
        const url = URL.createObjectURL(dataBlob);

        const link = document.createElement('a');
        link.href = url;
        link.download = `actors-view-${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.json`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);

        URL.revokeObjectURL(url);
    };

    const handleLoadData = (event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        if (!file) return;

        const reader = new FileReader();
        reader.onload = (e) => {
            try {
                const data = JSON.parse(e.target?.result as string);
                setLoadedData(data);
            } catch (error) {
                alert('Error parsing JSON file: ' + error);
            }
        };
        reader.readAsText(file);
    };

    const handleClearLoadedData = () => {
        setLoadedData(null);
        if (fileInputRef.current) {
            fileInputRef.current.value = '';
        }
    };

    if ((instrumentedThreadsIsLoading && !hasInitiallyFetched) && !loadedData) {
        return <div>Loading...</div>;
    } else if (instrumentedThreadsError && !loadedData) {
        return (
            <div className="actors-view">
                <div className="error">{(instrumentedThreadsError as Error).stack}</div>
            </div>
        );
    }
    const allThreads = currentData?.status_response.InstrumentedThreads.threads || [];
    const sortedThreads = allThreads.slice().sort((a: InstrumentedThread, b: InstrumentedThread) => a.thread_name.localeCompare(b.thread_name));
    const [minStartTime, maxStartTime] = [getMinStartTime(allThreads), getMaxStartTime(allThreads)];
    const currentTimeUnixMs = currentData?.status_response.InstrumentedThreads.current_time_unix_ms || 0;
    const currentTimeMs = currentData?.status_response.InstrumentedThreads.current_time_relative_ms || 0;
    const startTimeUnixEstimatedMs = currentTimeUnixMs - (maxStartTime - minStartTime);

    return (
        <div className="actors-view">
            <div className="actors-controls">
                <h2>Instrumented Threads</h2>
                <div className="time-info">
                    {`Showing data from ~${new Date(startTimeUnixEstimatedMs).toISOString()} to ${new Date(currentTimeUnixMs).toISOString()} (${((currentTimeUnixMs - startTimeUnixEstimatedMs) / 1000).toFixed(1)}s) `}
                </div>
                <div className="y-axis-controls">
                    <span className="control-label">Timeline Chart:</span>
                    <label className="radio-option">
                        <input
                            type="radio"
                            value="cpu"
                            checked={timelineChartMode === 'cpu'}
                            onChange={(e) => setTimelineChartMode(e.target.value as 'cpu' | 'dequeue')}
                        />
                        CPU
                    </label>
                    <label className="radio-option">
                        <input
                            type="radio"
                            value="dequeue"
                            checked={timelineChartMode === 'dequeue'}
                            onChange={(e) => setTimelineChartMode(e.target.value as 'cpu' | 'dequeue')}
                        />
                        Dequeue Delay
                    </label>
                </div>
                <div className="y-axis-controls">
                    <span className="control-label">Y-Axis Scale:</span>
                    <label className="radio-option">
                        <input
                            type="radio"
                            value="auto"
                            checked={yAxisMode === 'auto'}
                            onChange={(e) => setYAxisMode(e.target.value as 'auto' | 'fixed')}
                        />
                        Auto
                    </label>
                    <label className="radio-option">
                        <input
                            type="radio"
                            value="fixed"
                            checked={yAxisMode === 'fixed'}
                            onChange={(e) => setYAxisMode(e.target.value as 'auto' | 'fixed')}
                        />
                        100%
                    </label>
                </div>
                <div className="control-buttons">
                    <button
                        onClick={handleRefreshData}
                        disabled={instrumentedThreadsIsLoading || !!loadedData}
                        className="refresh-button"
                    >
                        {instrumentedThreadsIsLoading ? 'Refreshing...' : 'Refresh Data'}
                    </button>
                    <button onClick={handleSaveData} disabled={!currentData} className="save-button">
                        Save View
                    </button>
                    <input
                        type="file"
                        accept=".json"
                        onChange={handleLoadData}
                        ref={fileInputRef}
                        style={{ display: 'none' }}
                    />
                    <button onClick={() => fileInputRef.current?.click()} className="load-button">
                        Load View
                    </button>
                    {loadedData && (
                        <button onClick={handleClearLoadedData} className="clear-button">
                            Clear Loaded Data
                        </button>
                    )}
                    {loadedData && (
                        <span className="loaded-indicator">Loaded from file</span>
                    )}
                </div>
            </div>
            <table className="actors-table">
                <thead>
                    <tr>
                        <th>Thread Name</th>
                        <th>Timeline</th>
                    </tr>
                </thead>
                <tbody>
                    {sortedThreads?.map((thread: InstrumentedThread, idx: number) => (
                        <tr key={idx}>
                            <td>{formatThreadName(thread.thread_name)}</td>
                            <td><ThreadTimeline thread={thread} minTimeMs={minStartTime} messageTypes={thread.message_types} currentTimeMs={currentTimeMs} chartMode={timelineChartMode} yAxisMode={yAxisMode} /></td>
                        </tr>
                    ))}
                </tbody>
            </table>
            {/*
            <pre>{JSON.stringify(instrumentedThreads, null, 2)}</pre>
            */}
        </div>
    );
}

const getFirstStartTime = (thread: InstrumentedThread): number => {
    if (thread.windows.length === 0) {
        return 0;
    }
    return thread.windows[0].start_time_ms;
}

const getLastStartTime = (thread: InstrumentedThread): number => {
    if (thread.windows.length === 0) {
        return 0;
    }
    return thread.windows[thread.windows.length - 1].start_time_ms;
}

const getMinStartTime = (threads: InstrumentedThread[]): number => {
    if (threads.length === 0) {
        return 0;
    }
    return Math.min(...threads.map(t => getLastStartTime(t)));
};

const getMaxStartTime = (threads: InstrumentedThread[]): number => {
    if (threads.length === 0) {
        return 0;
    }
    return Math.max(...threads.map(t => getFirstStartTime(t)));
}
