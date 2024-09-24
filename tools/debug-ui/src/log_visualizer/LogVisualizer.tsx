import React, { useMemo, useState } from 'react';
import { LogViewer } from '@patternfly/react-log-viewer';
import { LogFileDrop } from '../LogFileDrop';
import './LogVisualizer.scss';
import { prettyPrint } from '../pretty-print';
import { EventItemCollection } from './events';
import { Layouts, SizesConfig } from './layout';
import { ArrowColumn, ArrowGroup, makeOutgoingArrowsForItem } from './arrows';

const LAYOUT_SIZES: SizesConfig = {
    arrowSpacing: 8,
    horizontalMargin: 36,
    verticalMargin: 24,
    itemHeight: 40,
    itemWidth: 200,
    newTimestampMargin: 8,
};

// Visualizes the log output of a Rust test written in the TestLoop framework.
// See core/async/src/test_loop.rs about the framework itself.
export const LogVisualizer = () => {
    const [logLines, setLogLines] = useState<string[]>([]);
    const [selectedEventId, setSelectedEventId] = useState<number | null>(null);

    const { events, arrowGroups, layouts } = useMemo(() => {
        const events = EventItemCollection.parseFromLogLines(logLines);
        const layouts = new Layouts(LAYOUT_SIZES, events);
        const arrowGroups = [] as ArrowGroup[];
        for (const event of events.getAllNonAttachedItems()) {
            for (const arrowGroup of makeOutgoingArrowsForItem(event, events)) {
                arrowGroups.push(arrowGroup);
            }
        }
        layouts.layoutWithArrows(arrowGroups);
        return { events, arrowGroups, layouts };
    }, [logLines]);

    return (
        <div className="log-visualizer">
            {logLines.length === 0 && <LogFileDrop onFileDrop={setLogLines} />}
            <div
                className="visualizer-content"
                onClick={() => setSelectedEventId(null)}
                style={{
                    height: layouts.totalHeight,
                    // Leave an extra 800px blank space for the log viewer.
                    width: layouts.totalWidth + 800,
                }}>
                {/* Render a background div for each column. */}
                <div className="column-backgrounds">
                    {layouts.columns.map((_, i) => {
                        return (
                            <div
                                key={i}
                                className="column-background"
                                style={{
                                    left: layouts.getGridColumnXOffset(i),
                                    width: layouts.getGridColumnWidth(i),
                                    height: layouts.totalHeight,
                                }}></div>
                        );
                    })}
                </div>
                {/* Render the column headers. */}
                {layouts.columns.map((_, i) => {
                    return (
                        <div
                            key={i}
                            className="column-header"
                            style={{
                                left: layouts.getGridColumnXOffset(i),
                                width: layouts.getGridColumnWidth(i),
                            }}>
                            Node {i}
                        </div>
                    );
                })}
                {/* Render the row timestamp headers. */}
                {layouts.rows.map((row, i) => {
                    if (row.isNewTimestamp) {
                        return (
                            <div
                                key={i}
                                className="timestamp-header"
                                style={{
                                    top: layouts.getGridRowYOffset(i),
                                    width: layouts.totalWidth,
                                }}>
                                {row.time} ms
                            </div>
                        );
                    }
                })}

                {/* Render all the arrows. */}
                {arrowGroups.map((arrowGroup, arrowGroupId) => {
                    // Highlight outgoing arrows of the selected event.
                    const selected =
                        selectedEventId === arrowGroup.sourceEventId ||
                        (arrowGroup.throughAttachedEventId !== null &&
                            selectedEventId === arrowGroup.throughAttachedEventId);
                    // Also highlight inbound arrows of the selected event.
                    const childSelected =
                        selectedEventId !== null && arrowGroup.targetEventIds.has(selectedEventId);
                    return (
                        <React.Fragment key={arrowGroupId}>
                            {arrowGroup.horizontalParts.map((part, i) => {
                                const y = layouts.getArrowRowYOffset(part.row, arrowGroupId);
                                const x1 = layouts.getArrowColumnXOffset(
                                    part.fromColumn,
                                    arrowGroupId
                                );
                                const x2 = part.isArrow
                                    ? layouts.getItemXOffset(part.toColumn.gridColumn)
                                    : layouts.getArrowColumnXOffset(part.toColumn, arrowGroupId);
                                return drawLine(
                                    { x: x1, y },
                                    { x: x2, y },
                                    '' + i,
                                    selected,
                                    childSelected,
                                    part.isArrow
                                );
                            })}
                            {arrowGroup.verticalParts.map((part, i) => {
                                const x = layouts.getArrowColumnXOffset(part.column, arrowGroupId);
                                const y1 = layouts.getArrowRowYOffset(part.fromRow, arrowGroupId);
                                const y2 = layouts.getArrowRowYOffset(part.toRow, arrowGroupId);
                                return drawLine(
                                    { x, y: y1 },
                                    { x, y: y2 },
                                    '' + i,
                                    selected,
                                    childSelected,
                                    false
                                );
                            })}
                            {arrowGroup.throughAttachedEventId !== null &&
                                (() => {
                                    // Draw attachment here, since their positioning depends on
                                    // the arrow group.
                                    const event = events.get(arrowGroup.throughAttachedEventId)!;
                                    const parent = events.get(event.parentId!)!;
                                    return (
                                        <div
                                            className={
                                                'attached-event' +
                                                (event.id == selectedEventId ||
                                                parent.id == selectedEventId
                                                    ? ' selected'
                                                    : '')
                                            }
                                            style={{
                                                left: layouts.getArrowColumnXOffset(
                                                    new ArrowColumn(parent.column, 'middle'),
                                                    arrowGroupId
                                                ),
                                                top:
                                                    layouts.getItemYOffset(parent.row) +
                                                    layouts.sizes.itemHeight,
                                            }}
                                            onClick={(e) => {
                                                setSelectedEventId(event.id);
                                                e.stopPropagation();
                                            }}></div>
                                    );
                                })()}
                        </React.Fragment>
                    );
                })}

                {/* Render all the events (other than attachments). */}
                {events.getAllNonAttachedItems().map((event) => {
                    return (
                        <div
                            key={`event ${event.id}`}
                            className={'event' + (event.id == selectedEventId ? ' selected' : '')}
                            style={{
                                left: layouts.getItemXOffset(event.column),
                                top: layouts.getItemYOffset(event.row),
                                width: layouts.getItemWidth(event.column),
                                height: layouts.sizes.itemHeight,
                            }}
                            onClick={(e) => {
                                setSelectedEventId(event.id);
                                e.stopPropagation();
                            }}>
                            <div className="content">
                                <div className="title">{event.title}</div>
                                {event.subtitle && <div className="subtitle">{event.subtitle}</div>}
                            </div>
                        </div>
                    );
                })}
            </div>

            {/* Render the log viewer; this shows the log messages emitted during the handling
                of the event, as well as the detailed event message dump. */}
            {selectedEventId !== null && (
                <div className="log-view">
                    <LogViewer
                        data={events.get(selectedEventId)!.logRows.join('\n')}
                        theme="dark"
                        height="calc(40vh - 35px)"
                        header={<div className="log-view-header">Logs for this event</div>}
                    />
                    <LogViewer
                        data={prettyPrint(events.get(selectedEventId)!.data)}
                        theme="dark"
                        height="calc(60vh - 35px)"
                        header={<div className="log-view-header">Event details</div>}
                    />
                </div>
            )}
        </div>
    );
};

function drawLine(
    from: { x: number; y: number },
    to: { x: number; y: number },
    key: string,
    selected: boolean,
    childSelected: boolean,
    arrow: boolean
): JSX.Element {
    let className = 'line';
    if (selected) {
        className += ' selected';
    }
    if (childSelected) {
        className += ' child-selected';
    }
    if (arrow) {
        className += ' arrow';
    }
    return (
        <div
            key={key}
            className={className}
            style={{
                left: from.x,
                top: from.y,
                width: to.x - from.x,
                height: to.y - from.y,
            }}></div>
    );
}
