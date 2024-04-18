import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';

export const TraceVisualizer = () => {
    const [startTimestampInput, setStartTimestampInput] = useState('' + Math.floor(Date.now()));
    const [endTimestampInput, setEndTimestampInput] = useState('' + Math.floor(Date.now() + 1000));
    const [query, setQuery] = useState<TraceQuery | null>(null);

    return (
        <div>
            <div>
                Start timestamp:{' '}
                <input
                    value={startTimestampInput}
                    onChange={(e) => setStartTimestampInput(e.target.value)}></input>
            </div>
            <div>
                End timestamp:{' '}
                <input
                    value={endTimestampInput}
                    onChange={(e) => setEndTimestampInput(e.target.value)}></input>
            </div>
            <button
                onClick={() => {
                    const startTimestamp = parseInt(startTimestampInput);
                    const endTimestamp = parseInt(endTimestampInput);
                    if (isNaN(startTimestamp) || isNaN(endTimestamp)) {
                        return;
                    }
                    setQuery({
                        start_timestamp_unix_ms: startTimestamp,
                        end_timestamp_unix_ms: endTimestamp,
                        time_resolution_ms: 1,
                        filter: {
                            nodes: [],
                            threads: [],
                        },
                        focus: [],
                    });
                }}>
                Load
            </button>

            {query && <TracePlot traceQuery={query} />}
        </div>
    );
};

export interface TracePlotProps {
    traceQuery: TraceQuery;
}

export interface TraceQuery {
    start_timestamp_unix_ms: number;
    end_timestamp_unix_ms: number;
    time_resolution_ms: number;
    filter: TraceFilter;
    focus: TraceFocus[];
}

export interface TraceFilter {
    nodes: string[];
    threads: number[];
}

export interface TraceFocus {
    attributes: Record<string, string>;
    names: string[];
}

export const TracePlot = ({ traceQuery }: TracePlotProps) => {
    const { data, isLoading, error } = useQuery(['trace', traceQuery], () => {
        return fetch('http://localhost:8080/query', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(traceQuery),
        }).then((res) => res.json());
    });
    if (isLoading) {
        return <div>Loading...</div>;
    }
    if (error) {
        return <div>Error: {'' + error}</div>;
    }
    if (data) {
        return (
            <div style={{ display: 'flex' }}>
                {Object.keys(data.nodes).map((node) => (
                    <TraceNodePlot
                        name={node}
                        data={data.nodes[node]}
                        minTimestamp={traceQuery.start_timestamp_unix_ms}></TraceNodePlot>
                ))}
            </div>
        );
    }
    return <div>Unknown error</div>;
};

export interface TraceNodePlotProps {
    name: string;
    data: any;
    minTimestamp: number;
}

export const TraceNodePlot = ({ name, data, minTimestamp }: TraceNodePlotProps) => {
    return (
        <div style={{ border: '1px solid black', margin: '5px', padding: '5px' }}>
            <div>{name}</div>
            <div style={{ display: 'flex' }}>
                {Object.keys(data.threads).map((thread) => (
                    <TraceThreadPlot
                        name={thread}
                        data={data.threads[thread]}
                        minTimestamp={minTimestamp}
                        key={thread}></TraceThreadPlot>
                ))}
            </div>
        </div>
    );
};

export interface TraceThreadPlotProps {
    name: string;
    data: any;
    minTimestamp: number;
}

export const TraceThreadPlot = ({ name, data, minTimestamp }: TraceThreadPlotProps) => {
    return (
        <div style={{ border: '1px solid black', margin: '5px', padding: '5px' }}>
            <div>{name}</div>
            <div
                style={{
                    position: 'relative',
                    height: '1000px',
                    width: '50px',
                    overflowY: 'hidden',
                }}>
                {data.spans.map((span: any) => {
                    return (
                        <div
                            key={span.id}
                            style={{
                                position: 'absolute',
                                top: span.startTimeUnixNano / 1000000 - minTimestamp,
                                height:
                                    (span.endTimeUnixNano - span.startTimeUnixNano) / 1000000 -
                                    minTimestamp,
                                width: '10px',
                                border: '1px solid blue',
                                opacity: 0.5,
                            }}></div>
                    );
                })}
            </div>
        </div>
    );
};
