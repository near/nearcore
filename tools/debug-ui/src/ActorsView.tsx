import './ActorsView.scss';
import { useQuery } from '@tanstack/react-query';
import { fetchInstrumentedThreadsView, InstrumentedThread, InstrumentedWindow } from './api';
import { BarChart, Bar, XAxis, YAxis, Tooltip, Legend } from "recharts";
import { useState, useRef, useEffect } from 'react';

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
    let allThreads = currentData?.status_response.InstrumentedThreads.threads || [];
    let sortedThreads = allThreads.slice().sort((a: InstrumentedThread, b: InstrumentedThread) => a.thread_name.localeCompare(b.thread_name));
    let maxStartTime = getMaxStartTime(allThreads);

    return (
        <div className="actors-view">
            <div className="actors-controls">
                <h2>Instrumented Threads</h2>
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
                        Fixed (1000ms)
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
                        <th>Time buckets</th>
                    </tr>
                </thead>
                <tbody>
                    {sortedThreads?.map((thread: InstrumentedThread, idx: number) => (
                        <tr key={idx}>
                            <td>{formatThreadName(thread.thread_name)}</td>
                            <td><BucketChart windows={thread.windows} max_start_time={maxStartTime} message_types={thread.message_types} yAxisMode={yAxisMode} /></td>
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

const getMaxStartTime = (threads: InstrumentedThread[]): number => {
    if (threads.length === 0) {
        return 0;
    }
    return Math.max(...threads.map(t => getFirstStartTime(t)));
};

const WINDOW_LEN_MS = 500;
const MAX_BUCKETS_TO_DISPLAY = 15;

type BucketChartProps = {
    windows: InstrumentedWindow[];
    max_start_time: number;
    message_types: string[];
    yAxisMode: 'auto' | 'fixed';
};

function BucketChart({ windows, max_start_time, message_types, yAxisMode }: BucketChartProps) {
    // First, we're going to go through the windows. We will add empty ones at the
    // beginning to make sure we start at max_start_time.
    // Assuming windows are given in reverse chronological order.
    while (windows.length == 0 || windows[0].start_time_ms < max_start_time) {
        windows.unshift({
            start_time_ms: windows.length == 0 ? max_start_time : windows[0].start_time_ms + WINDOW_LEN_MS,
            events: [],
            summary: {
                message_stats_by_type: [],
            }
        });
    };
    // Now, we will truncate the list to MAX_BUCKETS_TO_DISPLAY
    windows = windows.slice(0, MAX_BUCKETS_TO_DISPLAY);

    // Now, we will convert windows to a format suitable for recharts.
    // For each message type, we will create a field in the data.
    // The value will be the total_time_ns for that message type in that window.
    const data = windows.map(window => {
        let entry: { [key: string]: number } = { bucket: window.start_time_ms };
        window.summary.message_stats_by_type.forEach((stat) => {
            entry[message_types[stat.message_type]] = stat.total_time_ns / 1_000_000; // convert to ms
        });
        return entry;
    });

    const COLORS = [
        "#1f77b4", // blue
        "#ff7f0e", // orange
        "#2ca02c", // green
        "#d62728", // red
        "#9467bd", // purple
        "#8c564b", // brown
        "#e377c2", // pink
        "#7f7f7f", // gray
        "#bcbd22", // olive
        "#17becf", // teal
        "#393b79", // dark blue
        "#637939", // dark green
        "#8c6d31", // dark brown
        "#843c39", // dark red
        "#7b4173", // dark purple
        "#cedb9c", // light green
        "#9c9ede", // light blue
        "#f7b6d2", // light pink
        "#c7c7c7", // light gray
        "#dbdb8d"  // light olive
    ];


    return (
        <BarChart width={600} height={150} data={data}>
            <XAxis dataKey="bucket" />
            <YAxis domain={[0, yAxisMode === 'auto' ? 'auto' : 1000]} hide={true} />
            <Tooltip />
            <Legend align={"right"} verticalAlign={"middle"} layout="vertical" iconSize={8} width={250} wrapperStyle={
                { fontSize: "12px", paddingLeft: "10px" }
            } />
            {message_types.map((type, index) => (
                <Bar dataKey={type} stackId="a" fill={COLORS[index % COLORS.length]} />
            ))}
        </BarChart>
    );
}
