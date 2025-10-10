import { useMemo, useState, useRef, useEffect } from "react";
import { InstrumentedThread } from "../api";
import { CPU_CHART_HEIGHT, GRID_LABEL_TOP_MARGIN, LEGEND_HEIGHT_PER_ROW, LEGEND_VERTICAL_MARGIN, LINE_STROKE_WIDTH, ROW_HEIGHT, ROW_PADDING } from "./constants";
import { assignRows, getEventsToDisplay, makeWindowsToDisplay, MessageTypeAndColorMap, MergedEventToDisplay, Viewport } from "./algorithm";
import { renderCpuChart } from "./cpu_chart";
import { renderGridline } from "./gridline";
import "./ThreadTimeline.scss";

type ActorsViewProps = {
    thread: InstrumentedThread;
    messageTypes: string[];
    minTimeMs: number;
    currentTimeMs: number;
    chartMode: 'cpu' | 'dequeue';
    yAxisMode: 'auto' | 'fixed';
    isExpanded: boolean;
};

export const ThreadTimeline = ({ thread, messageTypes, minTimeMs, currentTimeMs, chartMode, yAxisMode, isExpanded }: ActorsViewProps) => {
    const events = useMemo(
        () => getEventsToDisplay(thread, minTimeMs, currentTimeMs),
        [thread, minTimeMs, currentTimeMs]
    );
    const windows = useMemo(
        () => makeWindowsToDisplay(thread, minTimeMs),
        [thread, minTimeMs]
    );

    const svgRef = useRef<SVGSVGElement>(null);
    const [svgWidth, setSvgWidth] = useState(800);

    const availableTimeInWindows = currentTimeMs - minTimeMs;
    const defaultViewport = new Viewport(availableTimeInWindows - 30000 - 500, availableTimeInWindows + 1000, svgWidth, availableTimeInWindows - 30000 - 500, availableTimeInWindows + 1000, 1);
    const [currentViewport, setViewport] = useState(defaultViewport);
    const viewport = currentViewport.isAllowedRangeTheSameAs(defaultViewport) ? currentViewport : defaultViewport;

    const colorMap = useMemo(() => new MessageTypeAndColorMap(events, windows, messageTypes), [events, windows, messageTypes]);

    const { events: positionedEvents, maxRow } = useMemo(
        () => assignRows(events, viewport),
        [events, viewport]
    );

    const gridTop = CPU_CHART_HEIGHT + GRID_LABEL_TOP_MARGIN;
    const eventsTop = gridTop + 10;
    const chartHeight = isExpanded ? eventsTop + (maxRow + 1) * (ROW_HEIGHT + ROW_PADDING) + 10 : gridTop;

    // Calculate legend layout based on text widths
    const legendItems = useMemo(() => {
        const items: { typeId: number, name: string, color: string, width: number }[] = [];
        colorMap.map((typeId, name, color) => {
            // Approximate width: 7px per character + 40px for color indicator and padding
            const width = Math.max(100, name.length * 7 + 40);
            items.push({ typeId, name, color, width });
        });
        return items;
    }, [colorMap]);

    const { legendHeight, legendLayout } = useMemo(() => {
        if (legendItems.length === 0 || !isExpanded) {
            return { legendHeight: 0, legendLayout: [] };
        }

        // Greedy bin packing: fill rows left to right
        const layout: { x: number, y: number, width: number }[] = [];
        let currentRow = 0;
        let currentX = 0;

        legendItems.forEach((item) => {
            if (currentX + item.width > svgWidth && currentX > 0) {
                // Move to next row
                currentRow++;
                currentX = 0;
            }
            layout.push({ x: currentX, y: currentRow * LEGEND_HEIGHT_PER_ROW, width: item.width });
            currentX += item.width;
        });

        const rows = currentRow + 1;
        const height = LEGEND_VERTICAL_MARGIN * 2 + LEGEND_HEIGHT_PER_ROW * rows;
        return { legendHeight: height, legendLayout: layout };
    }, [legendItems, svgWidth, isExpanded]);

    const svgHeight = chartHeight + legendHeight;

    const [hoveredEvent, setHoveredEvent] = useState<{ event: MergedEventToDisplay, x: number, y: number } | null>(null);
    const [hoveredLegendType, setHoveredLegendType] = useState<number | null>(null);
    const [hoveredCpuWindow, setHoveredCpuWindow] = useState<{ windowIndex: number, x: number, y: number } | null>(null);
    const [hoveredQueue, setHoveredQueue] = useState<{ x: number, y: number } | null>(null);

    useEffect(() => {
        const svg = svgRef.current;
        if (!svg) return;

        const handleWheel = (e: WheelEvent) => {
            if (!isExpanded) {
                // When collapsed, allow normal page scrolling
                return;
            }

            e.preventDefault();
            e.stopPropagation();
            const zoomFactor = e.deltaY < 0 ? 0.9 : 1.1;
            const rect = svg.getBoundingClientRect();
            const mouseX = e.clientX - rect.left;
            setViewport(viewport.zoom(mouseX, zoomFactor));
        };

        svg.addEventListener('wheel', handleWheel, { passive: false });
        return () => svg.removeEventListener('wheel', handleWheel);
    }, [viewport, isExpanded]);

    useEffect(() => {
        const svg = svgRef.current;
        if (!svg) return;

        const resizeObserver = new ResizeObserver((entries) => {
            for (const entry of entries) {
                const newWidth = entry.contentRect.width;
                if (newWidth !== svgWidth) {
                    setSvgWidth(newWidth);
                    setViewport(vp => vp.resize(newWidth));
                }
            }
        });

        resizeObserver.observe(svg);
        return () => resizeObserver.disconnect();
    }, [svgWidth]);

    return (<>
        <svg
            ref={svgRef}
            width="100%"
            height={svgHeight}
            style={{ backgroundColor: "white", borderTop: "1px solid #666", borderBottom: "1px solid #666", display: "block" }}
            onMouseMove={(e) => {
                if (e.buttons === 1) {
                    setViewport(viewport.pan(-e.movementX));
                }
            }}
            onDoubleClick={() => setViewport(viewport.reset())}
        >
            {/* CPU Load Chart */}
            <g>
                {renderCpuChart({
                    windows,
                    chartMode,
                    yAxisMode,
                    colorMap,
                    viewport,
                    hoveredLegendType,
                    hoveredWindowIndex: hoveredCpuWindow?.windowIndex ?? null,
                    onWindowHover: (windowIndex, x, y) => setHoveredCpuWindow({ windowIndex, x, y }),
                    onWindowLeave: () => setHoveredCpuWindow(null),
                    queueCounts: thread.queue.pending_counts,
                    hoveredQueue: hoveredQueue !== null,
                    onQueueHover: (x, y) => setHoveredQueue({ x, y }),
                    onQueueLeave: () => setHoveredQueue(null),
                })}
            </g>

            {/* Background shading for areas outside window range */}
            <g>
                {(() => {
                    if (windows.length === 0) return null;

                    const firstWindowStartMs = windows[0].startSMT;
                    const lastWindowEndMs = windows[windows.length - 1].endSMT;

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
                                    fill="#eee"
                                />
                            )}
                            {/* Area after last window */}
                            {afterX1 < svgWidth && (
                                <rect
                                    x={Math.max(0, afterX1)}
                                    y={CPU_CHART_HEIGHT}
                                    width={Math.min(svgWidth, afterX2) - Math.max(0, afterX1)}
                                    height={chartHeight - CPU_CHART_HEIGHT}
                                    fill="#eee"
                                />
                            )}
                            {/* Red line indicating current time */}
                            {afterX1 < svgWidth && (
                                <line
                                    x1={Math.floor(afterX1) + 0.5}
                                    y1={0}
                                    x2={Math.floor(afterX1) + 0.5}
                                    y2={chartHeight}
                                    stroke="#f00"
                                    strokeWidth={1}
                                />
                            )}
                        </>
                    );
                })()}
            </g>

            {/* Overfilled window indicators in event timeline area */}
            <g>
                {(() => {
                    const indicators: JSX.Element[] = [];
                    windows.forEach((window, index) => {
                        if (!window.eventsOverfilled) return;
                        const x1 = viewport.transform(window.lastCertainTimeBeforeOverfilling);
                        const x2 = viewport.transform(window.endSMT);

                        // Skip windows outside viewport
                        if (x2 < 0 || x1 > svgWidth) return;

                        indicators.push(
                            <rect
                                key={`overfilled-${index}`}
                                x={x1}
                                y={eventsTop}
                                width={x2 - x1}
                                height={chartHeight - eventsTop}
                                fill="#f88"
                                opacity={0.3}
                            />
                        );
                    });

                    return indicators;
                })()}
            </g>

            {renderGridline(viewport, gridTop, chartHeight)}

            {isExpanded && positionedEvents.map((event, index) => {
                const xStart = viewport.transform(event.startSMT);
                const xEnd = viewport.transform(event.endSMT);
                const width = xEnd - xStart;
                if (xEnd < 0 || xStart > svgWidth) {
                    return null; // Skip rendering events outside the viewport
                }

                const baseColor = colorMap.get(event.typeId).color;
                const x1 = Math.max(0, xStart);
                const x2 = Math.min(xStart + width, svgWidth);

                // Determine opacity based on legend hover state
                let opacity = event.leftUncertain || event.rightUncertain ? 0.6 : 0.9;
                if (hoveredLegendType !== null) {
                    opacity = event.typeId === hoveredLegendType ? opacity : 0.1;
                }

                const isCollapsed = event.count > 1;

                if (isCollapsed) {
                    // Render as a combined line with count
                    const y = eventsTop + event.row * (ROW_HEIGHT + ROW_PADDING) + ROW_HEIGHT / 2;
                    const rectHeight = LINE_STROKE_WIDTH;

                    return (
                        <g key={index}>
                            <rect
                                x={x1 - LINE_STROKE_WIDTH / 2}
                                y={y - rectHeight / 2}
                                width={x2 - x1 + LINE_STROKE_WIDTH}
                                height={rectHeight}
                                fill={baseColor}
                                opacity={opacity}
                                rx={2}
                                onMouseEnter={(e) => {
                                    const rect = e.currentTarget.getBoundingClientRect();
                                    setHoveredEvent({ event, x: rect.right + 5, y: rect.top });
                                }}
                                onMouseLeave={() => setHoveredEvent(null)}
                                style={{ cursor: 'pointer' }}
                            />
                            {/* Display count */}
                            <text
                                x={x1 + (x2 - x1) / 2}
                                y={y + 4}
                                fontSize={10}
                                fontFamily="sans-serif"
                                textAnchor="middle"
                                fill="white"
                                style={{ pointerEvents: 'none', fontWeight: 'bold' }}
                            >
                                {event.count}
                            </text>
                        </g>
                    );
                } else {
                    // Render as a single line
                    const y = eventsTop + event.row * (ROW_HEIGHT + ROW_PADDING) + ROW_HEIGHT / 2;

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
                }
            })}

            {/* Border lines */}
            <g>
                {/* Border between CPU chart and grid markings */}
                <line
                    x1={0}
                    y1={CPU_CHART_HEIGHT + 0.5}
                    x2={svgWidth}
                    y2={CPU_CHART_HEIGHT + 0.5}
                    stroke="#666"
                    strokeWidth={1}
                />
                {/* Border between event timeline and legend */}
                {legendHeight > 0 && (
                    <line
                        x1={0}
                        y1={chartHeight + 0.5}
                        x2={svgWidth}
                        y2={chartHeight + 0.5}
                        stroke="#666"
                        strokeWidth={1}
                    />
                )}
            </g>

            {/* Legend */}
            {legendHeight > 0 && (
                <g transform={`translate(0, ${chartHeight + LEGEND_VERTICAL_MARGIN})`}>
                    {legendItems.map((item, index) => {
                        const layout = legendLayout[index];
                        const isHovered = hoveredLegendType === item.typeId;
                        const isDimmed = hoveredLegendType !== null && !isHovered;

                        return (
                            <g
                                key={item.typeId}
                                transform={`translate(${layout.x}, ${layout.y})`}
                                onMouseEnter={() => setHoveredLegendType(item.typeId)}
                                onMouseLeave={() => setHoveredLegendType(null)}
                                style={{ cursor: 'pointer' }}
                                opacity={isDimmed ? 0.3 : 1}
                            >
                                {/* Backdrop for larger hover area */}
                                <rect x={0} y={0} width={layout.width} height={LEGEND_HEIGHT_PER_ROW} fill="transparent" />
                                <line x1={16} y1={LEGEND_HEIGHT_PER_ROW / 2} x2={24} y2={LEGEND_HEIGHT_PER_ROW / 2} stroke={item.color} strokeWidth={LINE_STROKE_WIDTH} strokeLinecap="round" />
                                <text x={36} y={LEGEND_HEIGHT_PER_ROW / 2 + 4} fontSize={10} fontFamily="sans-serif">{item.name}</text>
                            </g>
                        );
                    })}
                </g>
            )}
        </svg>

        {/* Event Tooltip */}
        {hoveredEvent && (() => {
            const tooltipWidth = 250; // Approximate tooltip width
            const shouldFlipLeft = hoveredEvent.x + tooltipWidth > globalThis.window.innerWidth;

            return (
                <div
                    className="tooltip event-tooltip"
                    style={{
                        left: shouldFlipLeft ? hoveredEvent.x - tooltipWidth - 10 : hoveredEvent.x,
                        top: hoveredEvent.y
                    }}
                >
                    <div><strong>{colorMap.get(hoveredEvent.event.typeId).name}</strong></div>
                    {hoveredEvent.event.count > 1 && (
                        <>
                            <div>Count: {hoveredEvent.event.count}</div>
                            <div>Total Time: {hoveredEvent.event.totalDurationMs.toFixed(3)} ms</div>
                            <div>Utilization: {((hoveredEvent.event.totalDurationMs / (hoveredEvent.event.endSMT - hoveredEvent.event.startSMT)) * 100).toFixed(1)}%</div>
                        </>
                    )}
                    {hoveredEvent.event.count === 1 && (
                        <div>Duration: {hoveredEvent.event.totalDurationMs.toFixed(3)} ms</div>
                    )}
                    <div>Interval: {hoveredEvent.event.startSMT.toFixed(2)} - {hoveredEvent.event.endSMT.toFixed(2)} ms</div>
                </div>
            );
        })()}

        {/* CPU Window Tooltip */}
        {hoveredCpuWindow && (() => {
            const window = windows[hoveredCpuWindow.windowIndex];
            const summary = chartMode === 'cpu' ? window.summary : window.dequeueSummary;
            const windowDurationMs = window.endSMT - window.startSMT;

            // Calculate total and build list of types
            const totalTimeNs = summary.message_stats_by_type.reduce((sum, stat) => sum + stat.total_time_ns, 0);
            const totalMs = totalTimeNs / 1e6;
            const totalPercent = (totalMs / windowDurationMs) * 100;
            const totalCount = summary.message_stats_by_type.reduce((sum, stat) => sum + stat.count, 0);
            const idleMs = windowDurationMs - totalMs;
            const idlePercent = (idleMs / windowDurationMs) * 100;

            const tooltipWidth = 300; // Approximate tooltip width
            const shouldFlipLeft = hoveredCpuWindow.x + tooltipWidth > globalThis.window.innerWidth;

            return (
                <div
                    className="tooltip"
                    style={{
                        left: shouldFlipLeft ? hoveredCpuWindow.x - tooltipWidth - 10 : hoveredCpuWindow.x,
                        top: hoveredCpuWindow.y
                    }}
                >
                    <div className="tooltip-title"><strong>Window {hoveredCpuWindow.windowIndex}</strong></div>
                    <table>
                        <thead>
                            <tr>
                                <th className="align-left">Type</th>
                                <th className="align-right">Count</th>
                                <th className="align-right">Time (ms)</th>
                                <th className="align-right">%</th>
                            </tr>
                        </thead>
                        <tbody>
                            {summary.message_stats_by_type.map(stat => {
                                const typeMs = stat.total_time_ns / 1e6;
                                const typePercent = (typeMs / windowDurationMs) * 100;
                                const typeColor = colorMap.get(stat.message_type).color;
                                const typeName = colorMap.get(stat.message_type).name;
                                return (
                                    <tr key={stat.message_type}>
                                        <td>
                                            <span className="color-indicator" style={{ backgroundColor: typeColor }}></span>
                                            {typeName}
                                        </td>
                                        <td className="align-right">{stat.count}</td>
                                        <td className="align-right">{typeMs.toFixed(2)}</td>
                                        <td className="align-right">{typePercent.toFixed(1)}%</td>
                                    </tr>
                                );
                            })}
                            <tr className="bordered">
                                <td><strong>Total</strong></td>
                                <td className="align-right"><strong>{totalCount}</strong></td>
                                <td className="align-right"><strong>{totalMs.toFixed(2)}</strong></td>
                                <td className="align-right"><strong>{totalPercent.toFixed(1)}%</strong></td>
                            </tr>
                            <tr>
                                <td className="top-padding">Idle</td>
                                <td className="align-right top-padding">-</td>
                                <td className="align-right top-padding">{idleMs.toFixed(2)}</td>
                                <td className="align-right top-padding">{idlePercent.toFixed(1)}%</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            );
        })()}

        {/* Queue Tooltip */}
        {hoveredQueue && (() => {
            const queueCounts = thread.queue.pending_counts ? new Map(Object.entries(thread.queue.pending_counts)) : new Map();
            const queueEntries: Array<{ typeName: string, count: number, typeId: number, color: string }> = [];

            queueCounts.forEach((count, typeName) => {
                const typeId = messageTypes.indexOf(typeName);
                if (typeId !== -1 && count > 0) {
                    const color = colorMap.get(typeId).color;
                    queueEntries.push({ typeName, count, typeId, color });
                }
            });

            // Sort by count descending
            queueEntries.sort((a, b) => b.count - a.count);

            const totalQueued = queueEntries.reduce((sum, entry) => sum + entry.count, 0);

            const tooltipWidth = 300;
            const shouldFlipLeft = hoveredQueue.x + tooltipWidth > globalThis.window.innerWidth;

            return (
                <div
                    className="tooltip"
                    style={{
                        left: shouldFlipLeft ? hoveredQueue.x - tooltipWidth - 10 : hoveredQueue.x,
                        top: hoveredQueue.y
                    }}
                >
                    <div className="tooltip-title"><strong>Queued Messages</strong></div>
                    <table>
                        <thead>
                            <tr>
                                <th className="align-left">Type</th>
                                <th className="align-right">Count</th>
                            </tr>
                        </thead>
                        <tbody>
                            {queueEntries.map(entry => (
                                <tr key={entry.typeId}>
                                    <td>
                                        <span className="color-indicator" style={{ backgroundColor: entry.color }}></span>
                                        {entry.typeName}
                                    </td>
                                    <td className="align-right">{entry.count}</td>
                                </tr>
                            ))}
                            <tr className="bordered">
                                <td><strong>Total</strong></td>
                                <td className="align-right"><strong>{totalQueued}</strong></td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            );
        })()}
    </>);
};