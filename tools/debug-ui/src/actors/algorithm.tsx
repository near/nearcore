import { InstrumentedThread, InstrumentedWindowSummary } from "../api";
import { EVENT_SPACING_PX, LINE_STROKE_WIDTH } from "./constants";

export type EventToDisplay = {
    startSMT: number; // milliseconds since minimum time
    endSMT: number; // milliseconds since minimum time
    leftUncertain: boolean; // If we're not sure about when the event's left edge is
    rightUncertain: boolean; // If we're not sure about when the event's right edge is
    typeId: number;
    row: number; // vertical row for stacking
};

export type WindowToDisplay = {
    startSMT: number;
    endSMT: number;
    eventsOverfilled: boolean;
    summary: InstrumentedWindowSummary;
    dequeueSummary: InstrumentedWindowSummary;
};


/// Displays the data between start and end in a viewport of a given width.
export class Viewport {
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

    isRangeOutsideViewport(start: number, end: number): boolean {
        return end < this.start || start > this.end;
    }

    zoom(pivot: number, factor: number): Viewport {
        const pivotPos = this.invTransform(pivot);
        let newStart = pivotPos - (pivotPos - this.start) * factor;
        let newEnd = pivotPos + (this.end - pivotPos) * factor;

        // Enforce minimum zoom (maximum span)
        if (newEnd - newStart < this.minZoom) {
            const pivotFraction = (pivotPos - newStart) / (newEnd - newStart);
            newStart = pivotPos - this.minZoom * pivotFraction;
            newEnd = pivotPos + this.minZoom * (1 - pivotFraction);
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

    resize(newWidth: number): Viewport {
        return new Viewport(this.start, this.end, newWidth, this.minBound, this.maxBound, this.minZoom);
    }

    reset(): Viewport {
        return new Viewport(this.minBound, this.maxBound, this.width, this.minBound, this.maxBound, this.minZoom);
    }

    pan(delta: number): Viewport {
        const offset = this.invTransformLength(delta);
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

export function getEventsToDisplay(
    thread: InstrumentedThread,
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
            for (const event of window.events) {
                if (!event.is_start) {
                    if (currentEvent == null) {
                        events.push({
                            startSMT: lastLostHorizonSMT,
                            endSMT: event.relative_timestamp_ns / 1_000_000 + currentOffsetSMT,
                            leftUncertain: true,
                            rightUncertain: false,
                            typeId: event.message_type,
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
                            typeId: event.message_type,
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
                    typeId: currentEvent.messageType,
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
            typeId: currentEvent.messageType,
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
            typeId: thread.active_event.message_type,
            row: 0
        });
    }
    return events;
}

/// Assigns rows to events so that overlapping events are displayed in different rows
export function assignRows(events: EventToDisplay[], viewport: Viewport): { events: EventToDisplay[], maxRow: number } {
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
        const actualEndPx = startPx + widthPx + LINE_STROKE_WIDTH / 2;

        // Find the first row where this event fits (no overlap with spacing)
        let assignedRow = 0;
        for (let row = 0; row < rowEndPositions.length; row++) {
            if (startPx - LINE_STROKE_WIDTH / 2 >= rowEndPositions[row] + EVENT_SPACING_PX) {
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

export function makeWindowsToDisplay(
    thread: InstrumentedThread,
    minTimeMs: number
): WindowToDisplay[] {
    const windows: WindowToDisplay[] = [];
    const threadWindows = [...thread.windows];
    threadWindows.reverse();
    for (const window of threadWindows) {
        windows.push({
            startSMT: window.start_time_ms - minTimeMs,
            endSMT: window.end_time_ms - minTimeMs,
            eventsOverfilled: window.events_overfilled,
            summary: window.summary,
            dequeueSummary: window.dequeue_summary,
        });
    }
    return windows;
}

export class MessageTypeAndColorMap {
    private _map: Map<number, { name: string, color: string }>;
    private _order: number[];
    constructor(events: EventToDisplay[], windows: WindowToDisplay[], messageTypes: string[]) {
        this._map = new Map();
        // Get unique event types from events
        const uniqueTypes = new Set(events.map(e => e.typeId));
        for (const window of windows) {
            for (const stat of window.summary.message_stats_by_type) {
                uniqueTypes.add(stat.message_type);
            }
            for (const stat of window.dequeueSummary.message_stats_by_type) {
                uniqueTypes.add(stat.message_type);
            }
        }

        // Sort by type ID.
        this._order = Array.from(uniqueTypes).sort();

        const numTypes = this._order.length;

        // Distribute hues evenly across the color wheel
        this._order.forEach((type, index) => {
            const hue = (index * 360) / numTypes;
            const name = type === -1 ? "Unknown" : messageTypes[type];
            this.set(type, name, `hsl(${hue}, 70%, 60%)`);
        });
    }

    set(typeId: number, typeName: string, color: string) {
        this._map.set(typeId, { name: typeName, color: color });
    }

    get(typeId: number): { name: string, color: string } {
        return this._map.get(typeId) || { name: "Unknown", color: "#888" };
    }

    get size(): number {
        return this._map.size;
    }

    forEach(callback: (typeId: number, name: string, color: string) => void) {
        this._order.forEach((typeId) => {
            const value = this._map.get(typeId)!;
            callback(typeId, value.name, value.color);
        });
    }

    map<T>(callback: (typeId: number, name: string, color: string, index: number) => T): T[] {
        const result: T[] = [];
        this._order.forEach((typeId, index) => {
            const value = this._map.get(typeId)!;
            result.push(callback(typeId, value.name, value.color, index));
        });
        return result;
    }

    keys(): IterableIterator<number> {
        return this._map.keys();
    }
}
