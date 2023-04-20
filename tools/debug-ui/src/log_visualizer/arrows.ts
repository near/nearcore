import { EventItem, EventItemCollection } from './events';

// Arrows in the log visualizer are axes-parallel. They are drawn in one of two
// fashions:
//
//       +--------------+                  +--------------+               +--------------+
//       |    Source    |                  |    Source    |               |              |
//   +---|    Event     |                  |    Event     |               |              |
//   |   +--------------+                  +--------------+               +--------------+
//   |                                             |
//   |   +--------------+                          +--------------+
//   |   |              |        OR                               |
//   |   |              |                  +--------------+       |       +--------------+
//   |   +--------------+                  |              |       |       |              |
//   |                                     |              |       |       |              |
//   |   +--------------+                  +--------------+       |       +--------------+
//   +-->|    Target    |                                         |
//       |    Event     |                  +--------------+       |       +--------------+
//       +--------------+                  |              |       +------>|    Target    |
//                                         |              |               |    Event     |
//                                         +--------------+               +--------------+
//
// The one on the left is used for events that spawn children events only in
// the same instance, whereas the one on the right is used for events that
// spawn events into different instances (such as a network message).
//
// Additionally, an arrow of the second fashion may be drawn out of an
// attachment, like this:
//
//  +------------------+
//  |   Parent Event   |
//  +------------------+
//           \/           <-- attachment
//           |
//           |
//           +--------------...>
//
// Since the arrows we draw may overlap, for each horizontal and vertical
// segment that may overlap, we compute a different offset. For example,
// if two arrows would both draw a horizontal segment at the same vertical
// position, then we would use the interval assignment algorithm to assign
// each of these two horizontal segments a different layer, so that they are
// actually drawn with different vertical positions. This layer assignment is
// carried out independently for each unique (originally desired) vertical
// position and horizontal position. The result is that the arrows are
// guaranteed to not overlap, even though they may cross each other. For
// example,
//
//       -----------------+            \
//                        |            |  (ArrowRow 1)
//       -----------------|---+        /
//                        |   |
//                        |   |
//                        |   |
//                        |   |
//                        +---|-------------->  (arrow A)     \
//                            |                               |  (ArrowRow 2)
//                            +-----------------> (arrow B)   /
//
//                        \___/
//                    (ArrowColumn 1)
//
// Suppose two arrows both want to draw the following:
//    - horizontal line on (ArrowRow 1) from (ArrowColumn 0) to (ArrowColumn 1)
//    - vertical line on (ArrowColumn 1) from (ArrowRow 1) to (ArrowRow 2)
//    - horizontal line on (ArrowRow 2) frmo (ArrowColumn 1) to (ArrowColumn 2)
//
// then since both arrows draw horizontal lines on ArrowRow1 with overlapping
// intervals, we assign them a different offset (layer) so that they are in
// fact drawn at different vertical positions. Same goes with the other two
// segments.
//
// To do this, each arrow we draw is represented by a set of ArrowParts, called
// an ArrowGroup. For each unique vertical position, we collect the horizontal
// ArrowParts (assume at most one per ArrowGroup), and use the interval
// scheduling algorithm to assign layers to them (in fact, to each ArrowGroup).
// We do the same for vertical ArrowParts for each unique horizontal position.
// Then, when rendering, we would look up the layer for each ArrowPart and
// draw them with the appropriate offsets.
//
// A unique vertical position where a horizontal arrow may be drawn is called
// an ArrowRow. A unique horizontal position where a vertical arrow may be
// drawn is called an ArrowColumn.

// All possible ArrowRow positions for each item (event) row.
//   - 'above' means above the boxes rendered for the items in that row.
//   - 'inbound' means 1/3 of the way down from the top of the item box.
//   - 'outbound' means 2/3 of the way down from the top of the item box.
export type ArrowRowPosition = 'above' | 'inbound' | 'outbound';
export const ARROW_ROW_POSITIONS: ArrowRowPosition[] = ['above', 'inbound', 'outbound'];

export class ArrowRow {
    constructor(public readonly gridRow: number, public readonly positioning: ArrowRowPosition) {}

    // Gets a unique key for this ArrowRow that is larger if it's more
    // vertically down.
    get key(): number {
        return (
            this.gridRow * 3 +
            (this.positioning == 'above' ? 0 : this.positioning == 'inbound' ? 1 : 2)
        );
    }

    max(other: ArrowRow): ArrowRow {
        return this.key > other.key ? this : other;
    }

    min(other: ArrowRow): ArrowRow {
        return this.key < other.key ? this : other;
    }
}

// All possible ArrowColumn positions for each item (event) column.
//  - 'left' means left of the boxes rendered for the items in that column.
//  - 'middle' means the vertical middle of the item box.
export type ArrowColumnPosition = 'left' | 'middle';
export const ARROW_COLUMN_POSITIONS: ArrowColumnPosition[] = ['left', 'middle'];

export class ArrowColumn {
    constructor(
        public readonly gridColumn: number,
        public readonly positioning: ArrowColumnPosition
    ) {}

    get key(): number {
        return this.gridColumn * 2 + (this.positioning == 'left' ? 0 : 1);
    }

    max(other: ArrowColumn): ArrowColumn {
        return this.key > other.key ? this : other;
    }

    min(other: ArrowColumn): ArrowColumn {
        return this.key < other.key ? this : other;
    }
}

// A segment of an arrow that is horizontally drawn.
export class ArrowPartHorizontal {
    constructor(
        // The column where the arrow starts.
        public fromColumn: ArrowColumn,
        // The column where the arrow ends.
        public toColumn: ArrowColumn,
        // The row where the arrow is drawn.
        public row: ArrowRow,
        // If true, draw an arrow head at the right end of the segment.
        public isArrow: boolean
    ) {
        if (fromColumn.key >= toColumn.key) {
            throw new Error('fromColumn must be before toColumn');
        }
    }

    // A horizontal arrow going into an item from the left.
    public static intoItem(gridRow: number, gridColumn: number): ArrowPartHorizontal {
        return new ArrowPartHorizontal(
            new ArrowColumn(gridColumn, 'left'),
            new ArrowColumn(gridColumn, 'middle'),
            new ArrowRow(gridRow, 'inbound'),
            true
        );
    }

    // A horizontal segment going out of an item on the left.
    public static outOfItem(gridRow: number, gridColumn: number): ArrowPartHorizontal {
        return new ArrowPartHorizontal(
            new ArrowColumn(gridColumn, 'left'),
            new ArrowColumn(gridColumn, 'middle'),
            new ArrowRow(gridRow, 'outbound'),
            false
        );
    }

    // A horizontal segment going across columns along the horizontal margin
    // between items.
    public static acrossColumns(
        gridRow: number,
        fromColumn: ArrowColumn,
        toColumn: ArrowColumn
    ): ArrowPartHorizontal {
        return new ArrowPartHorizontal(fromColumn, toColumn, new ArrowRow(gridRow, 'above'), false);
    }
}

// A segment of an arrow that is vertically drawn.
export class ArrowPartVertical {
    constructor(
        // The column where the arrow is drawn.
        public column: ArrowColumn,
        // The row where the arrow starts.
        public fromRow: ArrowRow,
        // The row where the arrow ends.
        public toRow: ArrowRow
    ) {
        if (fromRow.key >= toRow.key) {
            throw new Error('fromRow must be before toRow');
        }
    }

    // A vertical arrow going out of an item towards the bottom.
    public static outOfItem(gridRow: number, gridColumn: number): ArrowPartVertical {
        return new ArrowPartVertical(
            new ArrowColumn(gridColumn, 'middle'),
            new ArrowRow(gridRow, 'outbound'),
            new ArrowRow(gridRow + 1, 'above')
        );
    }

    // A vertical arrow going across rows along the vertical margin between
    // items.
    public static acrossRows(fromRow: ArrowRow, toRow: ArrowRow, gridColumn: number) {
        return new ArrowPartVertical(new ArrowColumn(gridColumn, 'left'), fromRow, toRow);
    }
}

// A group of ArrowParts, representing a single set of arrows.
// These arrows may point to multiple targets, but we draw them together so
// that their layer assignments are consistent, e.g. the vertical line in
// the below example should be a single line, not 3 separate lines.
//
//  source
//     |
//     +------> target1
//     |
//     +------> target2
//     |
//     +------> target3
export type ArrowGroup = {
    // The source event that the arrows originate from.
    sourceEventId: number;
    // If present, the attachment of the event the arrows come from.
    throughAttachedEventId: number | null;
    // The target events that the arrows point to.
    targetEventIds: Set<number>;

    horizontalParts: ArrowPartHorizontal[];
    verticalParts: ArrowPartVertical[];
};

// Makes the outgoing arrows for an event.
export function makeOutgoingArrowsForItem(
    item: EventItem,
    items: EventItemCollection
): ArrowGroup[] {
    const edges = items.getAllOutgoingEdges(item.id);

    // We'll draw one separate ArrowSet per attachment (or lack thereof).
    // This is because the attachments are separately drawn, and we want the
    // arrows going out of each attachment to visually come from that
    // attachment.
    const edgesByAttachment = new Map<number | null, number[]>();
    for (const edge of edges) {
        const list = edgesByAttachment.get(edge.throughAttachedId) ?? [];
        list.push(edge.toId);
        edgesByAttachment.set(edge.throughAttachedId, list);
    }

    const arrowGroups = [] as ArrowGroup[];
    for (const [attachmentId, targetIds] of edgesByAttachment) {
        // For each column we'll find the lowest point we need to draw towards.
        // That way we can draw a single vertical line and multiple horizontal
        // arrows.
        const columnToMaxRow = new Map<number, number>();
        for (const targetId of targetIds) {
            const child = items.get(targetId)!;
            columnToMaxRow.set(
                child.column,
                Math.max(columnToMaxRow.get(child.column) ?? 0, child.row)
            );
        }
        let minColumn = 100000,
            maxColumn = 0;
        for (const column of columnToMaxRow.keys()) {
            minColumn = Math.min(minColumn, column);
            maxColumn = Math.max(maxColumn, column);
        }
        const horizontalParts = [] as ArrowPartHorizontal[];
        const verticalParts = [] as ArrowPartVertical[];
        // If we only have children to the same instance, then draw them in the
        // first fashion (see comment at beginning of file).
        if (minColumn == maxColumn && minColumn == item.column && attachmentId === null) {
            horizontalParts.push(ArrowPartHorizontal.outOfItem(item.row, item.column));
            verticalParts.push(
                ArrowPartVertical.acrossRows(
                    new ArrowRow(item.row, 'outbound'),
                    new ArrowRow(columnToMaxRow.get(item.column)!, 'inbound'),
                    item.column
                )
            );
        } else {
            // Otherwise we draw them in the second fashion.
            const verticalOut = ArrowPartVertical.outOfItem(item.row, item.column);
            verticalParts.push(verticalOut);
            horizontalParts.push(
                ArrowPartHorizontal.acrossColumns(
                    item.row + 1,
                    verticalOut.column.min(new ArrowColumn(minColumn, 'left')),
                    verticalOut.column.max(new ArrowColumn(maxColumn, 'left'))
                )
            );
            for (const column of columnToMaxRow.keys()) {
                verticalParts.push(
                    ArrowPartVertical.acrossRows(
                        new ArrowRow(item.row + 1, 'above'),
                        new ArrowRow(columnToMaxRow.get(column)!, 'inbound'),
                        column
                    )
                );
            }
        }
        for (const targetId of targetIds) {
            const child = items.get(targetId)!;
            horizontalParts.push(ArrowPartHorizontal.intoItem(child.row, child.column));
        }
        arrowGroups.push({
            horizontalParts,
            verticalParts,
            sourceEventId: item.id,
            throughAttachedEventId: attachmentId,
            targetEventIds: new Set(targetIds),
        });
    }
    return arrowGroups;
}
