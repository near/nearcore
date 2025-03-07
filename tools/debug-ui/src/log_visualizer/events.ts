// A single event executed by the TestLoop framework on the Rust side.
// The events are parsed from log messages emitted by the framework.
// See core/async/src/test_loop.rs for the Rust side code.
export class EventItem {
    // The ID of the event, a number that increments for each event created,
    // emitted from the Rust side.
    public readonly id: number;
    // The virtual timestamp this event was executed at. Emitted from the Rust
    // side.
    public readonly time: number;
    // Which instance this event occurred on; in a single instance test, this
    // is zero; in a multi-instance test, this is the instance number.
    // Emitted from the Rust side as part of the event dump (like "(0, Event)")
    // and parsed here.
    public readonly identifier: string;
    // The title we display for the event. Parsed from the event dump after
    // extracting column number; this is usually the enum variant name.
    public readonly title: string;
    // The subtitle we display after the title. Parsed from the event dump,
    // after the column number and after the enum variant name.
    public readonly subtitle: string | null;
    // The full event dump; emitted from the Rust side.
    public readonly data: string;
    // The parent event whose handler spawned this event. See the parsing logic
    // for how this is derived.
    public readonly parentId: number | null = null;
    // Whether this event was executed or ignored. Emitted from the Rust side.
    public readonly ignored: boolean;

    // The row and column of the event in the log visualizer. This is set during layout.
    public rowNumber = 0;
    public columnNumber = 0;
    // The children IDs of this event. Derived from parentId.
    public readonly childIds: number[] = [];
    // The log lines emitted by the Rust program while handling this event.
    public readonly logRows: string[] = [];

    constructor(id: number, identifier: string, parentId: number | null, time: number, eventDump: string, eventIgnored: boolean) {
        this.id = id;
        this.time = time;
        this.parentId = parentId;
        this.identifier = identifier;
        this.data = eventDump;
        this.ignored = eventIgnored;

        // Parse the title and subtitle; for example, if the event dump is
        // "OutboundNetwork(NetworkRequests(...))""
        // then the title is "OutboundNetwork" and the subtitle is
        // "NetworkRequests".
        const firstParens = this.data.indexOf('(');
        if (firstParens !== -1) {
            this.title = this.data.substring(0, firstParens);
            const afterFirstParens = this.data.substring(firstParens + 1, this.data.length - 1);
            const secondBracket = /[({]/.exec(afterFirstParens);
            if (secondBracket !== null) {
                this.subtitle = afterFirstParens.substring(0, secondBracket.index);
                if (
                    this.subtitle === 'DelayedAction' ||
                    this.subtitle === 'Task' ||
                    this.subtitle === 'AsyncComputation'
                ) {
                    // Special logic for DelayedAction and Task, where we're more interested
                    // in the name, which is printed in the parens.
                    this.subtitle = afterFirstParens.substring(
                        secondBracket.index + 1,
                        afterFirstParens.length - 1
                    );
                }

                // Show type of the message for network messages
                if (this.subtitle === 'NetworkRequests') {
                    const afterSecondBracket = afterFirstParens.substring(secondBracket.index + 1);
                    const thirdBracket = /[({]/.exec(afterSecondBracket);
                    if (thirdBracket !== null) {
                        this.subtitle =
                            'Network::' + afterSecondBracket.substring(0, thirdBracket.index);
                    }
                }
            } else {
                this.subtitle = afterFirstParens;
            }
        } else {
            this.title = this.data;
            this.subtitle = null;
        }
    }

    // Attachment means that instead of rendering this event as a separate
    // item on the visualizer UI, we render it as an attached item to its
    // parent. This is useful for outgoing network messages that are simply
    // forwarded to another node, so we don't have to draw an extra edge just
    // for that.
    //
    // Attachment looks like this:
    //
    //  +------------------+
    //  |   Parent Event   |
    //  +------------------+
    //       \/  \/  \/       <-- these are 3 attachments
    //       |   |   |
    //       |   |   |
    //      (arrows leading to the children of the attachments)
    //
    // This method just computes whether an event is desirable to be rendered
    // as an attachment, but EventItemCollection.isAttachedToParent is the
    // source of truth.
    get isEligibleForAttachment(): boolean {
        // Modify this as needed.
        return this.subtitle === 'NetworkRequests';
    }
}

// The collection of all events.
export class EventItemCollection {
    private readonly items: EventItem[] = [];
    private readonly idToItem: Map<number, EventItem> = new Map();

    public get(id: number): EventItem | null {
        return this.idToItem.get(id)!;
    }

    // Returns whether the given event is attached to its parent.
    public isAttachedToParent(id: number): boolean {
        const item = this.get(id)!;
        if (item.parentId === null || !item.isEligibleForAttachment) {
            return false;
        }
        if (this.isAttachedToParent(item.parentId)) {
            return false;
        }
        const parent = this.get(item.parentId)!;
        if (parent.time != item.time) {
            return false;
        }
        return true;
    }

    // Returns all events, whether or not they are attached.
    public getRawItemsIncludingAttachments(): EventItem[] {
        return this.items;
    }

    // Returns all events that are not attached.
    public getAllNonAttachedItems(): EventItem[] {
        return this.items.filter((item) => !this.isAttachedToParent(item.id));
    }

    // Returns all events to which we should draw an outgoing arrow from this
    // event. This includes all children, as well as all children of any
    // attachments.
    public getAllOutgoingEdges(id: number): OutgoingEdge[] {
        const children: OutgoingEdge[] = [];
        for (const childId of this.get(id)!.childIds) {
            if (this.isAttachedToParent(childId)) {
                for (const grandchildId of this.get(childId)!.childIds) {
                    children.push({
                        throughAttachedId: childId,
                        toId: grandchildId,
                    });
                }
            } else {
                children.push({
                    throughAttachedId: null,
                    toId: childId,
                });
            }
        }
        return children;
    }

    private add(item: EventItem) {
        this.items.push(item);
        this.idToItem.set(item.id, item);
        if (item.parentId !== null) {
            this.idToItem.get(item.parentId)!.childIds.push(item.id);
        }
    }

    private lastEvent(): EventItem | null {
        return this.items.length == 0 ? null : this.items[this.items.length - 1];
    }

    // Parses the events from a log file emitted from Rust.
    public static parseFromLogLines(lines: string[]): EventItemCollection {
        const items = new EventItemCollection();
        const parentIds = [] as (number | null)[];
        let totalEventCount = 0;
        for (const line of lines) {
            // The start and end markers are emitted for each event handled by
            // the TestLoop. Lines between these markers are log messages
            // emitted while handling this event.
            const startMarker = 'TEST_LOOP_EVENT_START ';
            const endMarker = 'TEST_LOOP_EVENT_END ';
            const startIndex = line.indexOf(startMarker);
            const endIndex = line.indexOf(endMarker);
            if (startIndex != -1) {
                type EventStartLogLineData = {
                    current_index: number;
                    total_events: number;
                    identifier: string;
                    current_event: string;
                    current_time_ms: number;
                    event_ignored: boolean;
                };
                const startData = JSON.parse(
                    line.substring(startIndex + startMarker.length)
                ) as EventStartLogLineData;
                totalEventCount = startData.total_events;
                const event = new EventItem(
                    startData.current_index,
                    startData.identifier,
                    parentIds[startData.current_index] ?? null,
                    startData.current_time_ms,
                    startData.current_event,
                    startData.event_ignored
                );
                items.add(event);
            } else if (endIndex != -1) {
                type EventEndLogLineData = {
                    total_events: number;
                };
                const endData = JSON.parse(
                    line.substring(endIndex + endMarker.length)
                ) as EventEndLogLineData;
                for (let i = totalEventCount; i < endData.total_events; i++) {
                    parentIds[i] = items.lastEvent()!.id;
                }
            } else {
                items.lastEvent()?.logRows?.push(line);
            }
        }
        return items;
    }
}

// An edge from one event (implicit) to another, that we should render on the
// UI.
export type OutgoingEdge = {
    // The attachment event that actually is the parent of the toId event,
    // if present.
    throughAttachedId: number | null;
    // The child event.
    toId: number;
};
