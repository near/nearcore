import { useState, useRef, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import './ActorsView.scss';
import { fetchInstrumentedThreadsView, InstrumentedThread, InstrumentedThreadsViewResponse } from './api';
import { ThreadTimeline } from './actors/ThreadTimeline';

type ActorsViewProps = {
    addr: string;
};

// Helper function to format thread names for better word wrapping at `::`
const formatThreadName = (threadName: string): string => {
    // Insert zero-width space (\u200B) after :: to allow line breaks
    return threadName.replace(/::/g, '::​')
};

const ThreadTimelineRow = ({ thread, minTimeMs, currentTimeMs, chartMode, yAxisMode }: {
    thread: InstrumentedThread;
    minTimeMs: number;
    currentTimeMs: number;
    chartMode: 'cpu' | 'dequeue';
    yAxisMode: 'auto' | 'fixed';
}) => {
    const [isExpanded, setIsExpanded] = useState(false);
    const [resetKey, setResetKey] = useState(0);

    const handleToggle = () => {
        if (isExpanded) {
            // When collapsing, trigger a reset
            setResetKey(prev => prev + 1);
        }
        setIsExpanded(!isExpanded);
    };

    return (
        <div className="one-thread-row">
            <div className="thread-name" onClick={handleToggle} style={{ cursor: 'pointer', display: 'flex', alignItems: 'center' }}>
                <svg width="16" height="16" viewBox="0 0 16 16" style={{ marginRight: '8px', flexShrink: 0 }}>
                    {isExpanded ? (
                        <path d="M3 5 L8 10 L13 5" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                    ) : (
                        <path d="M5 3 L10 8 L5 13" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                    )}
                </svg>
                <span>{formatThreadName(thread.thread_name)}</span>
            </div>
            <div className="thread-timeline">
                <ThreadTimeline
                    key={resetKey}
                    thread={thread}
                    minTimeMs={minTimeMs}
                    messageTypes={thread.message_types}
                    currentTimeMs={currentTimeMs}
                    chartMode={chartMode}
                    yAxisMode={yAxisMode}
                    isExpanded={isExpanded}
                />
            </div>
        </div>
    );
};

// Helper function to normalize data format for backward compatibility
const normalizeData = (data: any): InstrumentedThreadsViewResponse | null => {
    if (!data) return null;

    // If data has the old format (just instrumented threads response)
    if (data.status_response?.InstrumentedThreads) {
        return data as InstrumentedThreadsViewResponse;
    }

    // If data might be the individual response objects
    return data as InstrumentedThreadsViewResponse;
};

// Helper function to render queue table rows with sorted data from instrumented threads
const renderQueueTableRows = (threads: InstrumentedThread[]) => {
    const rows: JSX.Element[] = [];

    // Get unique actors (take first occurrence of each actor_name)
    const seenActors = new Set<string>();
    const uniqueActors: { actorName: string; pendingCounts: { [key: string]: number } }[] = [];

    threads.forEach(thread => {
        const actorName = thread.actor_name;
        if (!seenActors.has(actorName)) {
            seenActors.add(actorName);
            uniqueActors.push({
                actorName,
                pendingCounts: thread.queue.pending_counts as any
            });
        }
    });

    // Sort actors by name
    const sortedActors = uniqueActors.sort((a, b) => a.actorName.localeCompare(b.actorName));

    let totalPendingMessages = 0;

    sortedActors.forEach(({ actorName, pendingCounts }) => {
        // Convert object to array and sort by message type name
        console.log('Pending counts for actor', actorName, pendingCounts);
        const sortedMessages = Object.entries(pendingCounts).sort((a, b) => a[0].localeCompare(b[0]));

        if (sortedMessages.length === 0) {
            // Show actor with no known message types
            rows.push(
                <tr key={`${actorName}-empty`}>
                    <td>{actorName}</td>
                    <td><em>No known message types</em></td>
                    <td>0</td>
                </tr>
            );
        } else {
            // Show each message type for this actor
            sortedMessages.forEach(([messageType, count], index) => {
                const countNum = Number(count);
                totalPendingMessages += countNum;
                rows.push(
                    <tr key={`${actorName}-${messageType}`}>
                        <td>{index === 0 ? actorName : ''}</td>
                        <td>{messageType}</td>
                        <td>{countNum}</td>
                    </tr>
                );
            });
        }
    });

    // Add summary row
    if (rows.length > 0 && totalPendingMessages > 0) {
        rows.push(
            <tr key="summary" className="summary-row">
                <td><strong>Total</strong></td>
                <td><strong>{sortedActors.length} queue(s)</strong></td>
                <td><strong>{totalPendingMessages}</strong></td>
            </tr>
        );
    }

    return rows;
};

export const ActorsView = ({ addr }: ActorsViewProps) => {
    const [loadedData, setLoadedData] = useState<InstrumentedThreadsViewResponse | null>(null);
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
    const currentData = normalizeData(loadedData || instrumentedThreads);

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
                setLoadedData(normalizeData(data));
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
        const error = instrumentedThreadsError;
        return (
            <div className="actors-view">
                <div className="error">{(error as Error).stack}</div>
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
            <div className="threads-container">
                <div className="scroll-space" />
                <div className="threads">
                    {sortedThreads?.map((thread: InstrumentedThread, idx: number) => (
                        <ThreadTimelineRow
                            key={idx}
                            thread={thread}
                            minTimeMs={minStartTime}
                            currentTimeMs={currentTimeMs}
                            chartMode={timelineChartMode}
                            yAxisMode={yAxisMode}
                        />
                    ))}
                </div>
                <div className="scroll-space" />
            </div>

            {/* Queue Data Table */}
            {currentData && allThreads.length > 0 && (
                <div className="queues-container">
                    <h3>Queue Status ({new Set(allThreads.map(t => t.actor_name)).size} actors)</h3>
                    <div className="queues-table-container">
                        <table className="queues-table">
                            <thead>
                                <tr>
                                    <th>Actor/Queue</th>
                                    <th>Message/Future Description</th>
                                    <th>Pending Count</th>
                                </tr>
                            </thead>
                            <tbody>
                                {(() => {
                                    const rows = renderQueueTableRows(allThreads);
                                    return rows.length > 0 ? rows : (
                                        <tr>
                                            <td colSpan={3} className="empty-table-message">
                                                No queues found
                                            </td>
                                        </tr>
                                    );
                                })()}
                            </tbody>
                        </table>
                    </div>
                </div>
            )}

            {/*
            <pre>{JSON.stringify(currentData?.instrumentedThreads, null, 2)}</pre>
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
