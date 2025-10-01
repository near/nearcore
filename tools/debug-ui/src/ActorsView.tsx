import './ActorsView.scss';
import { useQuery } from '@tanstack/react-query';
import { fetchInstrumentedThreadsView, InstrumentedThread, InstrumentedWindow } from './api';
import { BarChart, Bar, XAxis, YAxis, Tooltip, Legend } from "recharts";

type ActorsViewProps = {
    addr: string;
};

// Helper function to format thread names for better word wrapping at `::`
const formatThreadName = (threadName: string): string => {
    // Insert zero-width space (\u200B) after :: to allow line breaks
    return threadName.replace(/::/g, '::​')
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
    let allThreads = instrumentedThreads?.status_response.InstrumentedThreads.threads || [];
    let sortedThreads = allThreads.slice().sort((a, b) => a.thread_name.localeCompare(b.thread_name));
    let maxStartTime = getMaxStartTime(allThreads);

    return (
        <div className="actors-view">
            <h2>Instrumented Threads</h2>
            <table className="actors-table">
                <thead>
                    <tr>
                        <th>Thread Name</th>
                        <th>Time buckets</th>
                    </tr>
                </thead>
                <tbody>
                    {sortedThreads?.map((thread, idx) => (
                        <tr key={idx}>
                            <td>{formatThreadName(thread.thread_name)}</td>
                            <td><BucketChart windows={thread.windows} max_start_time={maxStartTime} message_types={thread.message_types} /></td>
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
};

function BucketChart({ windows, max_start_time, message_types }: BucketChartProps) {
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
            <YAxis domain={[0, 'auto']} hide={true} />
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
