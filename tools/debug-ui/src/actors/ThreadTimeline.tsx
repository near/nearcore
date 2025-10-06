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
                            type: messageTypes[event.message_type]
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
                            type: messageTypes[event.message_type]
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
                    type: messageTypes[currentEvent.messageType]
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
            type: messageTypes[currentEvent.messageType]
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
            type: messageTypes[thread.active_event.message_type]
        });
    }
    return events;
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
            height={100}
            style={{ border: "1px solid black", backgroundColor: "#f0f0f0" }}
            onMouseMove={(e) => {
                if (e.buttons === 1) {
                    setViewport(viewport.pan(-e.movementX));
                }
            }}
        >
            {events.map((event, index) => {
                const xStart = viewport.transform(event.startSMT);
                const xEnd = viewport.transform(event.endSMT);
                const width = xEnd - xStart;
                if (xEnd < 0 || xStart > 800) {
                    return null; // Skip rendering events outside the viewport
                }
                return (
                    <rect
                        key={index}
                        x={Math.max(0, xStart)}
                        y={20}
                        width={Math.max(1, width)}
                        height={60}
                        fill={event.leftUncertain || event.rightUncertain ? "orange" : "blue"}
                        opacity={event.leftUncertain || event.rightUncertain ? 0.5 : 1.0}
                    >
                        <title>
                            {event.type} ({event.leftUncertain ? "?" : ""}{(event.startSMT).toFixed(2)} ms - {(
                                event.endSMT
                            ).toFixed(2)} ms{event.rightUncertain ? "?" : ""})
                        </title>
                    </rect>
                );
            })}
        </svg>
    )
};