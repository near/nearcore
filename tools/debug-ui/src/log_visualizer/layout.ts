import {
    ArrowColumn,
    ArrowColumnPosition,
    ArrowGroup,
    ArrowRow,
    ArrowRowPosition,
    ARROW_COLUMN_POSITIONS,
    ARROW_ROW_POSITIONS,
} from './arrows';
import { EventItemCollection } from './events';
import { assignLayerToOverlappingIntervals, Interval } from './interval';

// Layout logic for events and arrows.
//
// See arrows.ts file-level comment for how the arrows are laid out.
//
// For events, we display them in a grid:
// - Horizontally, each column represents an instance in a multi-instance test.
// - Vertically, the events are first ordered by virtual timestamp, and then
//   by the order of execution. A new timestamp always starts a fresh new row
//   across all columns, and we display a horizontal line before that, along
//   with the timestamp.

// The layout parameters for a single row of items.
//
//              | |
//              | |              <-- startOffset
//              | |
//  ------------|-|------------  <-- arrowLayout['above'].baseOffset
//  ------------|-+                  arrowLayout['above'].numLayers == 3
//              +--------------
//
//     +--------------------+    <-- itemOffset
//     |                    |
//  -->|                    |    <-- arrowLayout['inbound'].baseOffset
//     |        Event       |
//  ---|                    |    <-- arrowLayout['outbound'].baseOffset
//     |                    |
//     +--------------------+
class OneRowLayout {
    public startOffset = 0;
    public itemOffset = 0;
    public isNewTimestamp = false;

    public arrowLayout = new Map<ArrowRowPosition, ArrowLayoutStrip>();

    constructor(public readonly time: number) {}
}

// The layout parameters for a single column of items.
//
//    startOffset
//    |   arrowLayout['left'].baseOffset (.numLayers == 2)
//    |   |       itemOffset
//    |   |       |      arrowLayout['middle'].baseOffset (.numLayers == 2)
//    |   |       |      |          itemEndOffset
//    |   |       |      |          |
//    V   V       V      V          V
//        |  |
//        |  |
//        |  |
//        |  |    +-----------------+
//        |  |    |                 |
//        +--|--->|                 |
//           |    |      Event      |
//           |    |                 |
//           |    |                 |
//           |    +-----------------+
//           |          \ /  \ /
//           |           V    V
//           |           |    |
//           |           |    |
class OneColumnLayout {
    public startOffset = 0;
    public itemOffset = 0;
    public itemEndOffset = 0;

    public arrowLayout = new Map<ArrowColumnPosition, ArrowLayoutStrip>();
}

// The computed layout parameters for a unique vertical position of horizontal
// segments, or a unique horizontal position of vertical segments.
class ArrowLayoutStrip {
    baseOffset = 0; // offset of layer 0 segments
    arrowSpacing = 0; // spacing between successive layers
    numLayers = 0; // number of total layers in this strip
    layers = new Map<number, number>(); // Map from ArrowGroup ID to layer

    constructor(numLayers: number) {
        this.numLayers = numLayers;
    }

    // Computes the offset of the ArrowPart in the given ArrowGroup that
    // is drawn within this strip.
    offsetFor(arrowGroupId: number): number {
        const layer = this.layers.get(arrowGroupId) ?? 0;
        return this.baseOffset + (this.numLayers - layer - 0.5) * this.arrowSpacing;
    }
}

// Various widths and margins used for the layout.
export type SizesConfig = {
    itemWidth: number;
    itemHeight: number;
    horizontalMargin: number; // margin between columns
    verticalMargin: number; // margin between rows
    arrowSpacing: number;
    newTimestampMargin: number; // extra margin for rows with new timestamps
};

// All layout parameters.
export class Layouts {
    rows: OneRowLayout[] = [];
    columns: OneColumnLayout[] = [];
    totalHeight = 0;
    totalWidth = 0;

    constructor(public sizes: SizesConfig, items: EventItemCollection) {
        this.layoutItemsInGrid(items);
    }

    // Sorts the items into a grid. See the file-level comment for the layout.
    // this also prepares this.rows and this.columns with placeholder entries.
    private layoutItemsInGrid(items: EventItemCollection) {
        const sortedItems = [...items.getAllNonAttachedItems()];
        const nextRowForColumn: number[] = [];
        let currentTime = 0;
        for (const item of sortedItems) {
            while (this.columns.length <= item.column) {
                this.columns.push(new OneColumnLayout());
                nextRowForColumn.push(0);
            }
        }
        sortedItems.sort((a, b) => a.time - b.time);
        for (const item of sortedItems) {
            if (item.time > currentTime) {
                currentTime = item.time;
                for (let i = 0; i < this.columns.length; i++) {
                    nextRowForColumn[i] = this.rows.length;
                }
            }
            item.row = nextRowForColumn[item.column]++;
            if (item.row >= this.rows.length) {
                this.rows.push(new OneRowLayout(item.time));
            }
        }
    }

    // Given the arrows, computes all the layout parameters for the entire UI.
    public layoutWithArrows(arrowGroups: ArrowGroup[]) {
        // First, sort the arrows into intervals, by each unique horizontal and
        // vertical position (ArrowRow and ArrowColumn keys).
        const horizontalArrowIntervals = new Map<number, Interval[]>();
        const verticalArrowIntervals = new Map<number, Interval[]>();
        for (let i = 0; i < this.rows.length; i++) {
            for (const positioning of ARROW_ROW_POSITIONS) {
                horizontalArrowIntervals.set(new ArrowRow(i, positioning).key, []);
            }
        }
        for (let i = 0; i < this.columns.length; i++) {
            for (const positioning of ARROW_COLUMN_POSITIONS) {
                verticalArrowIntervals.set(new ArrowColumn(i, positioning).key, []);
            }
        }
        for (let i = 0; i < arrowGroups.length; i++) {
            const arrowGroup = arrowGroups[i];
            for (const part of arrowGroup.horizontalParts) {
                horizontalArrowIntervals.get(part.row.key)!.push({
                    arrowGroupId: i,
                    start: part.fromColumn.key,
                    end: part.toColumn.key,
                    assignedLayer: 0,
                });
            }
            for (const part of arrowGroup.verticalParts) {
                verticalArrowIntervals.get(part.column.key)!.push({
                    arrowGroupId: i,
                    start: part.fromRow.key,
                    end: part.toRow.key,
                    assignedLayer: 0,
                });
            }
        }

        // Now, visit each row and column in order, run interval scheduling
        // for the arrow segments, and compute the cumulative offsets.
        let rowOffset = 32; // start with some margin for column headings
        let prevRowTime = -1;
        for (let i = 0; i < this.rows.length; i++) {
            const rowData = this.rows[i];
            rowData.startOffset = rowOffset;
            rowData.isNewTimestamp = rowData.time !== prevRowTime;
            if (rowData.isNewTimestamp) {
                rowOffset += this.sizes.newTimestampMargin;
            }
            prevRowTime = rowData.time;

            for (const positioning of ARROW_ROW_POSITIONS) {
                const row = new ArrowRow(i, positioning);
                const intervals = horizontalArrowIntervals.get(row.key)!;
                const numLayers = assignLayerToOverlappingIntervals(intervals);
                const arrowLayout = new ArrowLayoutStrip(numLayers);
                rowData.arrowLayout.set(positioning, arrowLayout);
                for (const interval of intervals) {
                    arrowLayout.layers.set(interval.arrowGroupId, interval.assignedLayer);
                }
                arrowLayout.baseOffset = rowOffset;
                switch (positioning) {
                    case 'above':
                        arrowLayout.arrowSpacing = this.sizes.arrowSpacing;
                        rowOffset += numLayers * this.sizes.arrowSpacing;
                        rowOffset += this.sizes.verticalMargin / 2;
                        rowData.itemOffset = rowOffset;
                        rowOffset += this.sizes.itemHeight / 3;
                        break;
                    case 'inbound':
                        arrowLayout.arrowSpacing = 0;
                        rowOffset += this.sizes.itemHeight / 3;
                        break;
                    case 'outbound':
                        arrowLayout.arrowSpacing = 0;
                        rowOffset += this.sizes.itemHeight / 3;
                        rowOffset += this.sizes.verticalMargin / 2;
                        break;
                }
            }
        }
        this.totalHeight = rowOffset;

        let columnOffset = 100; // leave space for row timestamp headings
        for (let i = 0; i < this.columns.length; i++) {
            const columnData = this.columns[i];
            columnData.startOffset = columnOffset;
            for (const positioning of ARROW_COLUMN_POSITIONS) {
                const column = new ArrowColumn(i, positioning);
                const intervals = verticalArrowIntervals.get(column.key)!;
                const numLayers = assignLayerToOverlappingIntervals(intervals);
                const arrowLayout = new ArrowLayoutStrip(numLayers);
                columnData.arrowLayout.set(positioning, arrowLayout);
                for (const interval of intervals) {
                    arrowLayout.layers.set(interval.arrowGroupId, interval.assignedLayer);
                }
                switch (positioning) {
                    case 'left':
                        columnOffset += this.sizes.horizontalMargin / 3;
                        arrowLayout.baseOffset = columnOffset;
                        arrowLayout.arrowSpacing = this.sizes.arrowSpacing;
                        columnOffset += numLayers * arrowLayout.arrowSpacing;
                        columnOffset += this.sizes.horizontalMargin / 3;
                        columnData.itemOffset = columnOffset;
                        break;
                    case 'middle': {
                        arrowLayout.arrowSpacing = this.sizes.arrowSpacing * 2;
                        const totalArrowWidth = numLayers * arrowLayout.arrowSpacing;
                        const itemWidth = Math.max(this.sizes.itemWidth, totalArrowWidth);
                        arrowLayout.baseOffset = columnOffset + (itemWidth - totalArrowWidth) / 2;
                        columnData.itemEndOffset = columnOffset + itemWidth;
                        columnOffset += itemWidth;
                        columnOffset += this.sizes.horizontalMargin / 3;
                        break;
                    }
                }
            }
        }

        this.totalWidth = columnOffset;
    }

    getArrowRowYOffset(row: ArrowRow, arrowGroupId: number): number {
        const rowData = this.rows[row.gridRow];
        return rowData.arrowLayout.get(row.positioning)!.offsetFor(arrowGroupId);
    }

    getArrowColumnXOffset(column: ArrowColumn, arrowGroupId: number): number {
        const columnData = this.columns[column.gridColumn];
        return columnData.arrowLayout.get(column.positioning)!.offsetFor(arrowGroupId);
    }

    getItemXOffset(column: number): number {
        return this.columns[column].itemOffset;
    }

    getItemYOffset(row: number): number {
        return this.rows[row].itemOffset;
    }

    getItemWidth(column: number): number {
        return this.columns[column].itemEndOffset - this.columns[column].itemOffset;
    }

    getGridRowYOffset(row: number): number {
        return this.rows[row].startOffset;
    }

    getGridColumnXOffset(column: number): number {
        return this.columns[column].startOffset;
    }

    getGridColumnWidth(column: number): number {
        if (column == this.columns.length - 1) {
            return this.totalWidth - this.columns[column].startOffset;
        }
        return this.columns[column + 1].startOffset - this.columns[column].startOffset;
    }
}
