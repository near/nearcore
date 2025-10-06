import { useMemo, useState, useRef, useEffect } from "react";
import { INSTRUMENTED_WINDOW_LEN_MS, InstrumentedEvent, InstrumentedThread } from "../api";

type ActorsViewProps = {
    thread: InstrumentedThread;
    messageTypes: string[];
    minTimeMs: number;
    currentTimeUnixMs: number;
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
    let lastLostHorizonSMT = 0; // if we encounter an end event without start, this is the last known time the event was running at
    const windows = [...thread.windows];
    windows.reverse();
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
function assignRows(events: EventToDisplay[], viewport: Viewport, minWidthPx: number, spacingPx: number): { events: EventToDisplay[], maxRow: number } {
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
        const widthPx = Math.max(minWidthPx, endPx - startPx);
        const actualEndPx = startPx + widthPx;

        // Find the first row where this event fits (no overlap with spacing)
        let assignedRow = 0;
        for (let row = 0; row < rowEndPositions.length; row++) {
            if (startPx >= rowEndPositions[row] + spacingPx) {
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
function createColorMap(events: EventToDisplay[]): Map<string, string> {
    // Get unique event types and sort them
    const uniqueTypes = Array.from(new Set(events.map(e => e.type))).sort();

    const colorMap = new Map<string, string>();
    const numTypes = uniqueTypes.length;

    // Distribute hues evenly across the color wheel
    uniqueTypes.forEach((type, index) => {
        const hue = (index * 360) / numTypes;
        colorMap.set(type, `hsl(${hue}, 70%, 60%)`);
    });

    return colorMap;
}

/// Displays the data between start and end in a viewport of a given width.
class Viewport {
    constructor(private start: number, private end: number, private width: number) { }

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

    zoom(pivot: number, factor: number): Viewport {
        const pivotPos = this.invTransform(pivot);
        const newStart = pivotPos - (pivotPos - this.start) * factor;
        const newEnd = pivotPos + (this.end - pivotPos) * factor;
        return new Viewport(newStart, newEnd, this.width);
    }

    pan(delta: number): Viewport {
        let offset = this.invTransformLength(delta);
        return new Viewport(this.start + offset, this.end + offset, this.width);
    }
}

export const ThreadTimeline = ({ thread, messageTypes, minTimeMs, currentTimeUnixMs }: ActorsViewProps) => {
    const events = useMemo(
        () => getEventsToDisplay(thread, messageTypes, minTimeMs, currentTimeUnixMs),
        [thread, messageTypes, minTimeMs, currentTimeUnixMs]
    );
    console.log("Events to display:", events);
    const [viewport, setViewport] = useState(
        new Viewport(0, INSTRUMENTED_WINDOW_LEN_MS * thread.windows.length, 800)
    );
    const svgRef = useRef<SVGSVGElement>(null);

    const MIN_WIDTH_PX = 4;
    const ROW_HEIGHT = 20;
    const ROW_PADDING = 2;
    const EVENT_SPACING_PX = 1;
    const LEGEND_HEIGHT = 30;

    const colorMap = useMemo(() => createColorMap(events), [events]);

    const { events: positionedEvents, maxRow } = useMemo(
        () => assignRows(events, viewport, MIN_WIDTH_PX, EVENT_SPACING_PX),
        [events, viewport]
    );

    const chartHeight = (maxRow + 1) * (ROW_HEIGHT + ROW_PADDING) + 20;
    const svgHeight = chartHeight + LEGEND_HEIGHT;

    useEffect(() => {
        const svg = svgRef.current;
        if (!svg) return;

        const handleWheel = (e: WheelEvent) => {
            e.preventDefault();
            e.stopPropagation();
            const zoomFactor = e.deltaY < 0 ? 0.9 : 1.1;
            setViewport(viewport.zoom(e.clientX, zoomFactor));
        };

        svg.addEventListener('wheel', handleWheel, { passive: false });
        return () => svg.removeEventListener('wheel', handleWheel);
    }, [viewport]);

    return (
        <svg
            ref={svgRef}
            width={800}
            height={svgHeight}
            style={{ border: "1px solid black", backgroundColor: "#f0f0f0" }}
            onMouseMove={(e) => {
                if (e.buttons === 1) {
                    setViewport(viewport.pan(-e.movementX));
                }
            }}
        >
            {positionedEvents.map((event, index) => {
                const xStart = viewport.transform(event.startSMT);
                const xEnd = viewport.transform(event.endSMT);
                const width = Math.max(MIN_WIDTH_PX, xEnd - xStart);
                if (xEnd < 0 || xStart > 800) {
                    return null; // Skip rendering events outside the viewport
                }

                const y = 10 + event.row * (ROW_HEIGHT + ROW_PADDING);
                const baseColor = colorMap.get(event.type) || "#888";

                return (
                    <rect
                        key={index}
                        x={Math.max(0, xStart)}
                        y={y}
                        width={width}
                        height={ROW_HEIGHT}
                        fill={baseColor}
                        opacity={event.leftUncertain || event.rightUncertain ? 0.6 : 0.9}
                        stroke={event.leftUncertain || event.rightUncertain ? "orange" : "#333"}
                        strokeWidth={1}
                    >
                        <title>
                            {event.type} ({event.leftUncertain ? "?" : ""}{(event.startSMT).toFixed(2)} ms - {(
                                event.endSMT
                            ).toFixed(2)} ms{event.rightUncertain ? "?" : ""})
                        </title>
                    </rect>
                );
            })}

            {/* Legend */}
            <g transform={`translate(0, ${chartHeight})`}>
                {Array.from(colorMap.entries()).map(([type, color], index) => {
                    const x = 10 + (index % 4) * 200;
                    const y = Math.floor(index / 4) * 15;
                    return (
                        <g key={type} transform={`translate(${x}, ${y})`}>
                            <rect x={0} y={0} width={12} height={12} fill={color} stroke="#333" strokeWidth={1} />
                            <text x={16} y={10} fontSize={10} fontFamily="sans-serif">{type}</text>
                        </g>
                    );
                })}
            </g>
        </svg>
    )
};