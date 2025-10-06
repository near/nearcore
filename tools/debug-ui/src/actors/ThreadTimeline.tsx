import { useMemo, useState, useRef, useEffect } from "react";
import { INSTRUMENTED_WINDOW_LEN_MS, InstrumentedEvent, InstrumentedThread } from "../api";

type ActorsViewProps = {
    thread: InstrumentedThread;
    messageTypes: string[];
    minTimeMs: number;
    currentTimeMs: number;
};

type EventToDisplay = {
    startSMT: number; // milliseconds since minimum time
    endSMT: number; // milliseconds since minimum time
    leftUncertain: boolean; // If we're not sure about when the event's left edge is
    rightUncertain: boolean; // If we're not sure about when the event's right edge is
    type: string;
    row: number; // vertical row for stacking
};

function getEventsToDisplay(
    thread: InstrumentedThread,
    messageTypes: string[],
    minTimeMs: number,
    currentTimeUnixMs: number
): EventToDisplay[] {
    const events = [];
    let currentEvent: null | { messageType: number, startSMT: number } = null;
    let currentOffsetSMT = 0; // offset in ms since minimum time
    const windows = [...thread.windows];
    windows.reverse();
    let lastLostHorizonSMT = windows[0].start_time_ms - minTimeMs; // if we encounter an end event without start, this is the last known time the event was running at
    for (let i = 0; i < windows.length; i++) {
        const window = windows[i];
        currentOffsetSMT = window.start_time_ms - minTimeMs;

        if (!window.events_overfilled) {
            for (let event of window.events) {
                if (!event.is_start) {
                    if (currentEvent == null) {
                        events.push({
                            startSMT: lastLostHorizonSMT,
                            endSMT: event.relative_timestamp_ns / 1_000_000 + currentOffsetSMT,
                            leftUncertain: true,
                            rightUncertain: false,
                            type: messageTypes[event.message_type],
                            row: 0
                        });
                    } else {
                        if (currentEvent.messageType !== event.message_type) {
                            console.warn(`Mismatched event types: expected ${currentEvent.messageType}, got ${event.message_type}`);
                        }
                        events.push({
                            startSMT: currentEvent.startSMT,
                            endSMT: event.relative_timestamp_ns / 1_000_000 + currentOffsetSMT,
                            leftUncertain: false,
                            rightUncertain: false,
                            type: messageTypes[event.message_type],
                            row: 0
                        });
                        currentEvent = null;
                    }
                } else {
                    if (currentEvent != null) {
                        console.warn(`Unexpected start event while current event is active: ${currentEvent.messageType}`);
                    }
                    currentEvent = {
                        messageType: event.message_type,
                        startSMT: event.relative_timestamp_ns / 1_000_000 + currentOffsetSMT
                    };
                }
            }
        } else {
            // If the window is overfilled, we rely on the summary only. If there's an ongoing event,
            // we don't know when exactly it ended.
            if (currentEvent != null) {
                events.push({
                    startSMT: currentEvent.startSMT,
                    endSMT: currentOffsetSMT,
                    leftUncertain: false,
                    rightUncertain: true,
                    type: messageTypes[currentEvent.messageType],
                    row: 0
                });
                currentEvent = null;
            }
            // If there are more windows, set the last lost horizon to the start of the next window,
            // because if the first event is the end of a message, we don't know when it started either,
            // so we assume it started at the beginning of the next window.
            if (i < windows.length - 1) {
                lastLostHorizonSMT = windows[i + 1].start_time_ms - minTimeMs;
            }
        }
    }
    // If there's an ongoing event at the end of the thread, we display it with an uncertain end time.
    if (currentEvent != null) {
        events.push({
            startSMT: currentEvent.startSMT,
            endSMT: currentTimeUnixMs - minTimeMs,
            leftUncertain: false,
            rightUncertain: true,
            type: messageTypes[currentEvent.messageType],
            row: 0
        });
    }
    // There's another possibility that there is an ongoing event, but it has been ongoing for so long
    // that no window captured anything at all. In this case, we still need to display it as spanning
    // across all the windows.
    if (events.length === 0 && thread.active_event != null) {
        events.push({
            startSMT: currentTimeUnixMs - thread.active_event.active_for_ns / 1_000_000 - minTimeMs,
            endSMT: currentTimeUnixMs - minTimeMs,
            leftUncertain: true,
            rightUncertain: true,
            type: messageTypes[thread.active_event.message_type],
            row: 0
        });
    }
    return events;
}

/// Assigns rows to events so that overlapping events are displayed in different rows
function assignRows(events: EventToDisplay[], viewport: Viewport, spacingPx: number, lineStrokeWidth: number): { events: EventToDisplay[], maxRow: number } {
    // Create a copy of events with row assignments
    const positionedEvents = events.map(e => ({ ...e }));

    // Sort events by start time
    positionedEvents.sort((a, b) => a.startSMT - b.startSMT);

    // Track the end position (in pixels) of the last event in each row
    const rowEndPositions: number[] = [];

    let maxRow = 0;

    for (const event of positionedEvents) {
        const startPx = viewport.transform(event.startSMT);
        const endPx = viewport.transform(event.endSMT);
        const widthPx = endPx - startPx;
        // Account for the line stroke width with round caps (adds strokeWidth/2 on each side)
        const actualEndPx = startPx + widthPx + lineStrokeWidth / 2;

        // Find the first row where this event fits (no overlap with spacing)
        let assignedRow = 0;
        for (let row = 0; row < rowEndPositions.length; row++) {
            if (startPx - lineStrokeWidth / 2 >= rowEndPositions[row] + spacingPx) {
                assignedRow = row;
                break;
            }
            assignedRow = row + 1;
        }

        event.row = assignedRow;
        rowEndPositions[assignedRow] = actualEndPx;
        maxRow = Math.max(maxRow, assignedRow);
    }

    return { events: positionedEvents, maxRow };
}

/// Creates a color map for event types with well-distributed colors
function createColorMap(events: EventToDisplay[], thread: InstrumentedThread): Map<string, string> {
    // Get unique event types from events
    const uniqueTypes = new Set(events.map(e => e.type));

    // Also check if any windows have type ID -1 (unknown) in their summaries
    for (const window of thread.windows) {
        for (const stat of window.summary.message_stats_by_type) {
            if (stat.message_type === -1) {
                uniqueTypes.add("Unknown");
            }
        }
    }

    // Sort types for stable colors
    const sortedTypes = Array.from(uniqueTypes).sort();

    const colorMap = new Map<string, string>();
    const numTypes = sortedTypes.length;

    // Distribute hues evenly across the color wheel
    sortedTypes.forEach((type, index) => {
        const hue = (index * 360) / numTypes;
        colorMap.set(type, `hsl(${hue}, 70%, 60%)`);
    });

    return colorMap;
}

/// Calculates appropriate time tick interval based on viewport span
function getTimeTickInterval(viewportSpanMs: number): number {
    // Target around 8-12 ticks across the viewport
    const targetTicks = 10;
    const roughInterval = viewportSpanMs / targetTicks;

    // Round to nice intervals: 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, etc.
    const magnitude = Math.pow(10, Math.floor(Math.log10(roughInterval)));
    const normalizedInterval = roughInterval / magnitude;

    let niceInterval;
    if (normalizedInterval < 1.5) {
        niceInterval = 1;
    } else if (normalizedInterval < 3.5) {
        niceInterval = 2;
    } else if (normalizedInterval < 7.5) {
        niceInterval = 5;
    } else {
        niceInterval = 10;
    }

    return niceInterval * magnitude;
}

/// Formats time in milliseconds to seconds, removing trailing zeros
function formatTime(ms: number): string {
    const seconds = ms / 1000;
    // Remove trailing zeros and unnecessary decimal point
    return seconds.toFixed(3).replace(/\.?0+$/, '');
}

/// Displays the data between start and end in a viewport of a given width.
class Viewport {
    constructor(
        private start: number,
        private end: number,
        private width: number,
        private minBound: number = 0,
        private maxBound: number = Infinity,
        private minZoom: number = 1
    ) { }

    /// Transforms the given value to a position in the viewport.
    transform(value: number): number {
        return (value - this.start) / (this.end - this.start) * this.width;
    }

    invTransform(position: number): number {
        return position / this.width * (this.end - this.start) + this.start;
    }

    transformLength(length: number): number {
        return length / (this.end - this.start) * this.width;
    }

    invTransformLength(length: number): number {
        return length / this.width * (this.end - this.start);
    }

    getStart(): number {
        return this.start;
    }

    getEnd(): number {
        return this.end;
    }

    zoom(pivot: number, factor: number): Viewport {
        const pivotPos = this.invTransform(pivot);
        let newStart = pivotPos - (pivotPos - this.start) * factor;
        let newEnd = pivotPos + (this.end - pivotPos) * factor;

        // Enforce minimum zoom (maximum span)
        if (newEnd - newStart < this.minZoom) {
            const center = (newStart + newEnd) / 2;
            newStart = center - this.minZoom / 2;
            newEnd = center + this.minZoom / 2;
        }

        // Constrain to bounds
        if (newStart < this.minBound) {
            newEnd += this.minBound - newStart;
            newStart = this.minBound;
        }
        if (newEnd > this.maxBound) {
            newStart -= newEnd - this.maxBound;
            newEnd = this.maxBound;
        }
        // Ensure we don't exceed bounds after adjustment
        newStart = Math.max(this.minBound, newStart);
        newEnd = Math.min(this.maxBound, newEnd);

        return new Viewport(newStart, newEnd, this.width, this.minBound, this.maxBound, this.minZoom);
    }

    pan(delta: number): Viewport {
        let offset = this.invTransformLength(delta);
        let newStart = this.start + offset;
        let newEnd = this.end + offset;

        // Constrain to bounds
        if (newStart < this.minBound) {
            newEnd += this.minBound - newStart;
            newStart = this.minBound;
        }
        if (newEnd > this.maxBound) {
            newStart -= newEnd - this.maxBound;
            newEnd = this.maxBound;
        }

        return new Viewport(newStart, newEnd, this.width, this.minBound, this.maxBound, this.minZoom);
    }
}

export const ThreadTimeline = ({ thread, messageTypes, minTimeMs, currentTimeMs }: ActorsViewProps) => {
    const events = useMemo(
        () => getEventsToDisplay(thread, messageTypes, minTimeMs, currentTimeMs),
        [thread, messageTypes, minTimeMs, currentTimeMs]
    );

    const ROW_HEIGHT = 10;
    const ROW_PADDING = 2;
    const EVENT_SPACING_PX = 1;
    const LINE_STROKE_WIDTH = 10;
    const LEGEND_VERTICAL_MARGIN = 5;
    const LEGEND_HEIGHT_PER_ROW = 25;
    const CPU_CHART_HEIGHT = 50;
    const GRID_LABEL_TOP_MARGIN = 20;
    const VIEWPORT_WIDTH = 1200;

    const maxTimeMs = currentTimeMs - minTimeMs;
    const [viewport, setViewport] = useState(
        new Viewport(-1000, maxTimeMs + 1000, VIEWPORT_WIDTH, -1000, maxTimeMs + 1000, 1)
    );
    const svgRef = useRef<SVGSVGElement>(null);


    const colorMap = useMemo(() => createColorMap(events, thread), [events, thread]);

    const { events: positionedEvents, maxRow } = useMemo(
        () => assignRows(events, viewport, EVENT_SPACING_PX, LINE_STROKE_WIDTH),
        [events, viewport]
    );

    const cpuChartTop = 0;
    const cpuChartBottom = cpuChartTop + CPU_CHART_HEIGHT;
    const gridTop = cpuChartBottom + GRID_LABEL_TOP_MARGIN;
    const eventsTop = gridTop + 10;
    const chartHeight = eventsTop + (maxRow + 1) * (ROW_HEIGHT + ROW_PADDING) + 10;
    const legendHeight = colorMap.size > 0 ? LEGEND_VERTICAL_MARGIN * 2 + LEGEND_HEIGHT_PER_ROW * Math.ceil(colorMap.size / 5) : 0;
    const svgHeight = chartHeight + legendHeight;

    const [hoveredEvent, setHoveredEvent] = useState<{ event: EventToDisplay, x: number, y: number } | null>(null);
    const [hoveredLegendType, setHoveredLegendType] = useState<string | null>(null);

    useEffect(() => {
        const svg = svgRef.current;
        if (!svg) return;

        const handleWheel = (e: WheelEvent) => {
            e.preventDefault();
            e.stopPropagation();
            const zoomFactor = e.deltaY < 0 ? 0.9 : 1.1;
            const rect = svg.getBoundingClientRect();
            const mouseX = e.clientX - rect.left;
            setViewport(viewport.zoom(mouseX, zoomFactor));
        };

        svg.addEventListener('wheel', handleWheel, { passive: false });
        return () => svg.removeEventListener('wheel', handleWheel);
    }, [viewport]);

    return (<>
        <svg
            ref={svgRef}
            width={VIEWPORT_WIDTH}
            height={svgHeight}
            style={{ border: "1px solid black", backgroundColor: "white" }}
            onMouseMove={(e) => {
                if (e.buttons === 1) {
                    setViewport(viewport.pan(-e.movementX));
                }
            }}
        >
            {/* CPU Load Chart */}
            <g>
                {/* White background for entire CPU chart area */}
                <rect
                    x={0}
                    y={cpuChartTop}
                    width={VIEWPORT_WIDTH}
                    height={CPU_CHART_HEIGHT}
                    fill="white"
                />
                {(() => {
                    const windows = [...thread.windows];
                    windows.reverse(); // Match the order from getEventsToDisplay
                    const paths: JSX.Element[] = [];

                    // Build stacked areas for each message type
                    const typeOrder = Array.from(colorMap.keys());

                    windows.forEach((window, windowIndex) => {
                        const windowStartMs = window.start_time_ms - minTimeMs;
                        const windowEndMs = window.end_time_ms - minTimeMs;

                        const x1 = viewport.transform(windowStartMs);
                        const x2 = viewport.transform(windowEndMs);

                        // Skip windows outside viewport
                        if (x2 < 0 || x1 > VIEWPORT_WIDTH) return;

                        // Window duration is INSTRUMENTED_WINDOW_LEN_MS in nanoseconds
                        const windowDurationNs = INSTRUMENTED_WINDOW_LEN_MS * 1_000_000;

                        // Create stacked bars
                        let cumulativeTimeNs = 0;

                        typeOrder.forEach((type) => {
                            const stat = window.summary.message_stats_by_type.find(
                                s => {
                                    if (s.message_type === -1) return type === "Unknown";
                                    return messageTypes[s.message_type] === type;
                                }
                            );

                            if (!stat || stat.total_time_ns === 0) return;

                            const y1 = cpuChartTop + CPU_CHART_HEIGHT - (cumulativeTimeNs / windowDurationNs * CPU_CHART_HEIGHT);
                            const y2 = cpuChartTop + CPU_CHART_HEIGHT - ((cumulativeTimeNs + stat.total_time_ns) / windowDurationNs * CPU_CHART_HEIGHT);

                            const color = colorMap.get(type) || "#888";
                            const isDimmed = hoveredLegendType !== null && hoveredLegendType !== type;

                            paths.push(
                                <rect
                                    key={`${windowIndex}-${type}`}
                                    x={Math.max(0, x1)}
                                    y={y2}
                                    width={Math.max(1, x2 - x1)}
                                    height={y1 - y2}
                                    fill={color}
                                    opacity={isDimmed ? 0.1 : 0.8}
                                />
                            );

                            cumulativeTimeNs += stat.total_time_ns;
                        });
                    });

                    return paths;
                })()}
            </g>

            {/* Background shading for areas outside window range */}
            <g>
                {(() => {
                    const windows = [...thread.windows];
                    windows.reverse();
                    if (windows.length === 0) return null;

                    const firstWindowStartMs = windows[0].start_time_ms - minTimeMs;
                    const lastWindowEndMs = currentTimeMs - minTimeMs;

                    const beforeX1 = viewport.transform(viewport.getStart());
                    const beforeX2 = viewport.transform(firstWindowStartMs);
                    const afterX1 = viewport.transform(lastWindowEndMs);
                    const afterX2 = viewport.transform(viewport.getEnd());

                    return (
                        <>
                            {/* Area before first window */}
                            {beforeX2 > 0 && (
                                <rect
                                    x={Math.max(0, beforeX1)}
                                    y={0}
                                    width={Math.max(0, beforeX2 - Math.max(0, beforeX1))}
                                    height={chartHeight}
                                    fill="#444"
                                    opacity={0.3}
                                />
                            )}
                            {/* Area after last window */}
                            {afterX1 < VIEWPORT_WIDTH && (
                                <rect
                                    x={Math.max(0, afterX1)}
                                    y={0}
                                    width={Math.min(VIEWPORT_WIDTH, afterX2) - Math.max(0, afterX1)}
                                    height={chartHeight}
                                    fill="#444"
                                    opacity={0.3}
                                />
                            )}
                        </>
                    );
                })()}
            </g>

            {/* Overfilled window indicators in event timeline area */}
            <g>
                {(() => {
                    const windows = [...thread.windows];
                    windows.reverse();
                    const indicators: JSX.Element[] = [];

                    windows.forEach((window, index) => {
                        if (!window.events_overfilled) return;

                        const windowStartMs = window.start_time_ms - minTimeMs;
                        const windowEndMs = window.end_time_ms - minTimeMs;

                        const x1 = viewport.transform(windowStartMs);
                        const x2 = viewport.transform(windowEndMs);

                        // Skip windows outside viewport
                        if (x2 < 0 || x1 > VIEWPORT_WIDTH) return;

                        indicators.push(
                            <rect
                                key={`overfilled-${index}`}
                                x={Math.max(0, x1)}
                                y={eventsTop}
                                width={Math.max(1, x2 - x1)}
                                height={chartHeight - eventsTop}
                                fill="#f88"
                                opacity={0.3}
                            />
                        );
                    });

                    return indicators;
                })()}
            </g>

            {/* Grid lines */}
            <g>
                {(() => {
                    const viewportSpan = viewport.getEnd() - viewport.getStart();
                    const tickInterval = getTimeTickInterval(viewportSpan);
                    const startTick = Math.ceil(viewport.getStart() / tickInterval) * tickInterval;
                    const ticks = [];

                    for (let tick = startTick; tick <= viewport.getEnd(); tick += tickInterval) {
                        const x = viewport.transform(tick);
                        ticks.push(
                            <g key={tick}>
                                <line
                                    x1={x}
                                    y1={gridTop}
                                    x2={x}
                                    y2={chartHeight}
                                    stroke="#ccc"
                                    strokeWidth={1}
                                />
                                {(
                                    <text
                                        x={x}
                                        y={gridTop - 8}
                                        fontSize={10}
                                        fontFamily="sans-serif"
                                        fill="#666"
                                        textAnchor="middle"
                                    >
                                        {formatTime(tick)}
                                    </text>
                                )}
                            </g>
                        );
                    }
                    return ticks;
                })()}
            </g>

            {positionedEvents.map((event, index) => {
                const xStart = viewport.transform(event.startSMT);
                const xEnd = viewport.transform(event.endSMT);
                const width = xEnd - xStart;
                if (xEnd < 0 || xStart > VIEWPORT_WIDTH) {
                    return null; // Skip rendering events outside the viewport
                }

                const y = eventsTop + event.row * (ROW_HEIGHT + ROW_PADDING) + ROW_HEIGHT / 2;
                const baseColor = colorMap.get(event.type) || "#888";
                const x1 = Math.max(0, xStart);
                const x2 = Math.max(0, xStart) + width;

                // Determine opacity based on legend hover state
                let opacity = event.leftUncertain || event.rightUncertain ? 0.6 : 0.9;
                if (hoveredLegendType !== null) {
                    opacity = event.type === hoveredLegendType ? opacity : 0.1;
                }

                return (
                    <line
                        key={index}
                        x1={x1}
                        y1={y}
                        x2={x2}
                        y2={y}
                        stroke={baseColor}
                        strokeWidth={LINE_STROKE_WIDTH}
                        strokeLinecap="round"
                        opacity={opacity}
                        onMouseEnter={(e) => {
                            const rect = e.currentTarget.getBoundingClientRect();
                            setHoveredEvent({
                                event,
                                x: rect.right + 5,
                                y: rect.top
                            });
                        }}
                        onMouseLeave={() => setHoveredEvent(null)}
                        style={{ cursor: 'pointer' }}
                    />
                );
            })}

            {/* Border lines */}
            <g>
                {/* Border between CPU chart and grid markings */}
                <line
                    x1={0}
                    y1={cpuChartBottom + 0.5}
                    x2={VIEWPORT_WIDTH}
                    y2={cpuChartBottom + 0.5}
                    stroke="#666"
                    strokeWidth={1}
                />
                {/* Border between event timeline and legend */}
                {legendHeight > 0 && (
                    <line
                        x1={0}
                        y1={chartHeight + 0.5}
                        x2={VIEWPORT_WIDTH}
                        y2={chartHeight + 0.5}
                        stroke="#666"
                        strokeWidth={1}
                    />
                )}
            </g>

            {/* Legend */}
            {legendHeight > 0 && (
                <g transform={`translate(0, ${chartHeight + LEGEND_VERTICAL_MARGIN})`}>
                    {Array.from(colorMap.entries()).map(([type, color], index) => {
                        const x = (index % 5) * 200;
                        const y = Math.floor(index / 5) * 25;
                        const isHovered = hoveredLegendType === type;
                        const isDimmed = hoveredLegendType !== null && !isHovered;

                        return (
                            <g
                                key={type}
                                transform={`translate(${x}, ${y})`}
                                onMouseEnter={() => setHoveredLegendType(type)}
                                onMouseLeave={() => setHoveredLegendType(null)}
                                style={{ cursor: 'pointer' }}
                                opacity={isDimmed ? 0.3 : 1}
                            >
                                {/* Backdrop for larger hover area */}
                                <rect x={12} y={0} width={180} height={20} fill="transparent" />
                                <line x1={16} y1={9} x2={24} y2={9} stroke={color} strokeWidth={LINE_STROKE_WIDTH} strokeLinecap="round" />
                                <text x={36} y={13} fontSize={10} fontFamily="sans-serif">{type}</text>
                            </g>
                        );
                    })}
                </g>
            )}
        </svg>

        {/* Tooltip */}
        {hoveredEvent && (
            <div
                style={{
                    position: 'fixed',
                    left: hoveredEvent.x,
                    top: hoveredEvent.y,
                    backgroundColor: 'rgba(0, 0, 0, 0.9)',
                    color: 'white',
                    padding: '8px 12px',
                    borderRadius: '4px',
                    fontSize: '12px',
                    pointerEvents: 'none',
                    zIndex: 1000,
                    whiteSpace: 'nowrap',
                    boxShadow: '0 2px 8px rgba(0,0,0,0.3)'
                }}
            >
                <div><strong>{hoveredEvent.event.type}</strong></div>
                <div>Duration: {(hoveredEvent.event.endSMT - hoveredEvent.event.startSMT).toFixed(3)} ms</div>
                <div>Start: {hoveredEvent.event.startSMT.toFixed(2)} ms</div>
                <div>End: {hoveredEvent.event.endSMT.toFixed(2)} ms</div>
            </div>
        )}
    </>);
};