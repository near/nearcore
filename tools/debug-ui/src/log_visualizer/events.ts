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
    public readonly column: number;
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

    // The row of the event in the log visualizer. This is set during layout.
    public row = 0;
    // The children IDs of this event. Derived from parentId.
    public readonly childIds: number[] = [];
    // The log lines emitted by the Rust program while handling this event.
    public readonly logRows: string[] = [];

    constructor(id: number, parentId: number | null, time: number, eventDump: string) {
        this.id = id;
        this.time = time;
        this.parentId = parentId;

        // If the event dump is a tuple, the first element is the instance ID.
        if (eventDump.startsWith('(')) {
            const split = eventDump.indexOf(',');
            this.column = parseInt(eventDump.substring(1, split));
            this.data = eventDump.substring(split + 1, eventDump.length - 1).trim();
        } else {
            this.column = 0;
            this.data = eventDump;
        }
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
            // This marker marks the beginning of handling a TestLoop event.
            // Search for this string on the Rust side to see the logic that
            // emitted these markers. Lines after this marker and before
            // the next marker are log messages that are emitted while handling
            // this event.
            const marker = 'TEST_LOOP_EVENT_START ';
            const index = line.indexOf(marker);
            if (index == -1) {
                items.lastEvent()?.logRows?.push(line);
                continue;
            }
            type LogLineData = {
                current_index: number;
                total_events: number;
                current_event: string;
                current_time_ms: number;
            };
            const parsed = JSON.parse(line.substring(index + marker.length)) as LogLineData;
            // Any event IDs between the previously seen total_events count and
            // the currently reported total_events count are events that were
            // emitted by the handling of the previous event. Therefore, the
            // previous event is the parent of all these new IDs.
            for (let i = totalEventCount; i < parsed.total_events; i++) {
                parentIds[i] = items.lastEvent()?.id ?? null;
            }
            totalEventCount = parsed.total_events;
            const event = new EventItem(
                parsed.current_index,
                parentIds[parsed.current_index],
                parsed.current_time_ms,
                parsed.current_event
            );
            items.add(event);
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
