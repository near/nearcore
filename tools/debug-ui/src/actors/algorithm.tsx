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

export type MergedEventToDisplay = {
    startSMT: number; // milliseconds since minimum time (start of earliest event)
    endSMT: number; // milliseconds since minimum time (end of latest event)
    leftUncertain: boolean;
    rightUncertain: boolean;
    typeId: number;
    row: number;
    count: number; // number of events merged (1 for single event)
    totalDurationMs: number; // sum of all individual event durations
};

export type WindowToDisplay = {
    startSMT: number;
    endSMT: number;
    eventsOverfilled: boolean;
    lastCertainTimeBeforeOverfilling: number;
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

    isAllowedRangeTheSameAs(other: Viewport): boolean {
        return this.minBound === other.minBound && this.maxBound === other.maxBound;
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

        for (const event of window.events) {
            if (!event.s) {
                if (currentEvent == null) {
                    events.push({
                        startSMT: lastLostHorizonSMT,
                        endSMT: event.t / 1_000_000 + currentOffsetSMT,
                        leftUncertain: true,
                        rightUncertain: false,
                        typeId: event.m,
                        row: 0
                    });
                } else {
                    if (currentEvent.messageType !== event.m) {
                        console.warn(`Mismatched event types: expected ${currentEvent.messageType}, got ${event.m}`);
                    }
                    events.push({
                        startSMT: currentEvent.startSMT,
                        endSMT: event.t / 1_000_000 + currentOffsetSMT,
                        leftUncertain: false,
                        rightUncertain: false,
                        typeId: event.m,
                        row: 0
                    });
                    currentEvent = null;
                }
            } else {
                if (currentEvent != null) {
                    console.warn(`Unexpected start event while current event is active: ${currentEvent.messageType}`);
                }
                currentEvent = {
                    messageType: event.m,
                    startSMT: event.t / 1_000_000 + currentOffsetSMT
                };
            }
        }
        if (window.events_overfilled) {
            // If the window is overfilled, if there is any last event still active at the end of the
            // buffer (which isn't the complete data), we don't know when it ended. So we just
            // discard that event.
            currentEvent = null;
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

    // Pre-sort events by start time for faster processing
    events.sort((a, b) => a.startSMT - b.startSMT);

    return events;
}

/// Assigns rows to events based on overlaps (no stacking/merging)
export function assignRowsNoStacking(events: EventToDisplay[], viewport: Viewport): { events: MergedEventToDisplay[], maxRow: number } {
    const mergedEvents: MergedEventToDisplay[] = [];
    const rowEndTimes: number[] = []; // Track the end time (in pixels) of the last event in each row

    for (const event of events) {
        const startPx = viewport.transform(event.startSMT);
        const endPx = viewport.transform(event.endSMT);
        const visualStart = startPx - LINE_STROKE_WIDTH / 2;
        const visualEnd = endPx + LINE_STROKE_WIDTH / 2;
        const eventDuration = event.endSMT - event.startSMT;

        // Find the first available row where this event doesn't overlap
        let assignedRow = 0;
        for (let row = 0; row < rowEndTimes.length; row++) {
            if (visualStart >= rowEndTimes[row] + EVENT_SPACING_PX) {
                assignedRow = row;
                break;
            }
            assignedRow = row + 1;
        }

        // Update or create row end time
        if (assignedRow >= rowEndTimes.length) {
            rowEndTimes.push(visualEnd);
        } else {
            rowEndTimes[assignedRow] = visualEnd;
        }

        // Create event (no merging, count is always 1)
        mergedEvents.push({
            startSMT: event.startSMT,
            endSMT: event.endSMT,
            leftUncertain: event.leftUncertain,
            rightUncertain: event.rightUncertain,
            typeId: event.typeId,
            row: assignedRow,
            count: 1,
            totalDurationMs: eventDuration
        });
    }

    const maxRow = Math.max(0, rowEndTimes.length - 1);
    return { events: mergedEvents, maxRow };
}

/// Assigns rows to events so that overlapping events are displayed in different rows
/// Also collapses visually overlapping events of the same type into single elements with a count
export function assignRows(events: EventToDisplay[], viewport: Viewport): { events: MergedEventToDisplay[], maxRow: number } {
    // Events are already sorted by startSMT in getEventsToDisplay

    const mergedEvents: MergedEventToDisplay[] = [];
    // Track the last merged event for each type
    const lastEventByType = new Map<number, number>(); // typeId -> index in mergedEvents

    for (const event of events) {
        const startPx = viewport.transform(event.startSMT);
        const visualStart = startPx - LINE_STROKE_WIDTH / 2;
        const eventDuration = event.endSMT - event.startSMT;

        const lastIndex = lastEventByType.get(event.typeId);

        if (lastIndex !== undefined) {
            const lastEvent = mergedEvents[lastIndex];
            const lastEndPx = viewport.transform(lastEvent.endSMT);
            const lastVisualEnd = lastEndPx + LINE_STROKE_WIDTH / 2;

            // Check if visually overlaps with last event of same type
            if (visualStart < lastVisualEnd + EVENT_SPACING_PX) {
                // Merge into last event
                lastEvent.endSMT = Math.max(lastEvent.endSMT, event.endSMT);
                lastEvent.leftUncertain = lastEvent.leftUncertain || event.leftUncertain;
                lastEvent.rightUncertain = lastEvent.rightUncertain || event.rightUncertain;
                lastEvent.count += 1;
                lastEvent.totalDurationMs += eventDuration;
            } else {
                // Create new merged event
                const newIndex = mergedEvents.length;
                mergedEvents.push({
                    startSMT: event.startSMT,
                    endSMT: event.endSMT,
                    leftUncertain: event.leftUncertain,
                    rightUncertain: event.rightUncertain,
                    typeId: event.typeId,
                    row: 0, // Will be assigned based on type
                    count: 1,
                    totalDurationMs: eventDuration
                });
                lastEventByType.set(event.typeId, newIndex);
            }
        } else {
            // First event of this type
            const newIndex = mergedEvents.length;
            mergedEvents.push({
                startSMT: event.startSMT,
                endSMT: event.endSMT,
                leftUncertain: event.leftUncertain,
                rightUncertain: event.rightUncertain,
                typeId: event.typeId,
                row: 0, // Will be assigned based on type
                count: 1,
                totalDurationMs: eventDuration
            });
            lastEventByType.set(event.typeId, newIndex);
        }
    }

    // Phase 2: Assign rows based on message type (each type gets its own row)
    const uniqueTypes = Array.from(new Set(mergedEvents.map(e => e.typeId))).sort((a, b) => a - b);
    const typeToRow = new Map<number, number>();
    uniqueTypes.forEach((typeId, index) => {
        typeToRow.set(typeId, index);
    });

    for (const event of mergedEvents) {
        event.row = typeToRow.get(event.typeId)!;
    }

    const maxRow = uniqueTypes.length - 1;

    return { events: mergedEvents, maxRow };
}

export function makeWindowsToDisplay(
    thread: InstrumentedThread,
    minTimeMs: number
): WindowToDisplay[] {
    const windows: WindowToDisplay[] = [];
    const threadWindows = [...thread.windows];
    threadWindows.reverse();
    for (const window of threadWindows) {
        let lastCertainTimeBeforeOverfilling = window.end_time_ms;
        if (window.events_overfilled) {
            if (window.events[window.events.length - 1].s) {
                lastCertainTimeBeforeOverfilling = window.events[window.events.length - 2].t / 1e6 + window.start_time_ms;
            } else {
                lastCertainTimeBeforeOverfilling = window.events[window.events.length - 1].t / 1e6 + window.start_time_ms;
            }
        }
        windows.push({
            startSMT: window.start_time_ms - minTimeMs,
            endSMT: window.end_time_ms - minTimeMs,
            eventsOverfilled: window.events_overfilled,
            lastCertainTimeBeforeOverfilling: lastCertainTimeBeforeOverfilling - minTimeMs,
            summary: window.summary,
            dequeueSummary: window.dequeue_summary,
        });
    }
    return windows;
}

export class MessageTypeAndColorMap {
    private _map: Map<number, { name: string, color: string }>;
    private _nameToTypeId: Map<string, number>;
    private _order: number[];
    constructor(events: EventToDisplay[], windows: WindowToDisplay[], messageTypes: string[]) {
        this._map = new Map();
        this._nameToTypeId = new Map();
        // Get unique event types from events
        const uniqueTypes = new Set(events.map(e => e.typeId));
        for (const window of windows) {
            for (const stat of window.summary.message_stats_by_type) {
                uniqueTypes.add(stat.m);
            }
            for (const stat of window.dequeueSummary.message_stats_by_type) {
                uniqueTypes.add(stat.m);
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
        this._nameToTypeId.set(typeName, typeId);
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

    colorByName(name: string): string {
        const typeId = this._nameToTypeId.get(name);
        if (typeId !== undefined) {
            return this._map.get(typeId)?.color || "#888";
        }
        return "#888";
    }
}
