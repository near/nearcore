import { useMemo, useState, useRef, useEffect, useCallback } from "react";
import { InstrumentedThread } from "../api";
import { CPU_CHART_HEIGHT, GRID_LABEL_TOP_MARGIN, LEGEND_HEIGHT_PER_ROW, LEGEND_VERTICAL_MARGIN, LINE_STROKE_WIDTH, ROW_HEIGHT, ROW_PADDING } from "./constants";
import { assignRows, getEventsToDisplay, makeWindowsToDisplay, MessageTypeAndColorMap, MergedEventToDisplay, Viewport } from "./algorithm";
import { renderCpuChart } from "./cpu_chart";
import { renderGridline } from "./gridline";
import { HitDetector, clearCanvas, drawRect, drawLine, drawRoundedRect } from "./canvas_utils";
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

    const canvasRef = useRef<HTMLCanvasElement>(null);
    const containerRef = useRef<HTMLDivElement>(null);
    const [canvasWidth, setCanvasWidth] = useState(800);
    const hitDetectorRef = useRef<HitDetector>(new HitDetector());

    const availableTimeInWindows = currentTimeMs - minTimeMs;
    const defaultViewport = new Viewport(availableTimeInWindows - 30000 - 500, availableTimeInWindows + 1000, canvasWidth, availableTimeInWindows - 30000 - 500, availableTimeInWindows + 1000, 1);
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
            if (currentX + item.width > canvasWidth && currentX > 0) {
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
    }, [legendItems, canvasWidth, isExpanded]);

    const canvasHeight = chartHeight + legendHeight;

    const [hoveredEvent, setHoveredEvent] = useState<{ event: MergedEventToDisplay, x: number, y: number } | null>(null);
    const [hoveredLegendType, setHoveredLegendType] = useState<number | null>(null);
    const [hoveredCpuWindow, setHoveredCpuWindow] = useState<{ windowIndex: number, x: number, y: number } | null>(null);
    const [hoveredQueue, setHoveredQueue] = useState<{ x: number, y: number } | null>(null);

    // Main render function
    const renderCanvas = useCallback(() => {
        const canvas = canvasRef.current;
        if (!canvas) return;

        const ctx = canvas.getContext('2d');
        if (!ctx) return;

        const hitDetector = hitDetectorRef.current;
        hitDetector.clear();

        // Clear canvas
        clearCanvas(ctx, canvasWidth, canvasHeight);

        // Set white background
        ctx.fillStyle = 'white';
        ctx.fillRect(0, 0, canvasWidth, canvasHeight);

        // Render CPU chart
        renderCpuChart(ctx, hitDetector, {
            windows,
            chartMode,
            yAxisMode,
            colorMap,
            viewport,
            hoveredLegendType,
            hoveredWindowIndex: hoveredCpuWindow?.windowIndex ?? null,
            queueCounts: thread.queue.pending_counts,
            hoveredQueue: hoveredQueue !== null,
        });

        // Render background shading for areas outside window range
        if (windows.length > 0) {
            const firstWindowStartMs = windows[0].startSMT;
            const lastWindowEndMs = windows[windows.length - 1].endSMT;

            const beforeX1 = viewport.transform(viewport.getStart());
            const beforeX2 = viewport.transform(firstWindowStartMs);
            const afterX1 = viewport.transform(lastWindowEndMs);
            const afterX2 = viewport.transform(viewport.getEnd());

            // Area before first window
            if (beforeX2 > 0) {
                drawRect(ctx, Math.max(0, beforeX1), 0, Math.max(0, beforeX2 - Math.max(0, beforeX1)), chartHeight, '#eee');
            }

            // Area after last window
            if (afterX1 < canvasWidth) {
                drawRect(ctx, Math.max(0, afterX1), CPU_CHART_HEIGHT, Math.min(canvasWidth, afterX2) - Math.max(0, afterX1), chartHeight - CPU_CHART_HEIGHT, '#eee');
            }

            // Red line indicating current time
            if (afterX1 < canvasWidth) {
                drawLine(ctx, Math.floor(afterX1) + 0.5, 0, Math.floor(afterX1) + 0.5, chartHeight, '#f00', 1, 'butt');
            }
        }

        // Render overfilled window indicators
        windows.forEach((window) => {
            if (!window.eventsOverfilled) return;
            const x1 = viewport.transform(window.lastCertainTimeBeforeOverfilling);
            const x2 = viewport.transform(window.endSMT);

            // Skip windows outside viewport
            if (x2 < 0 || x1 > canvasWidth) return;

            drawRect(ctx, x1, eventsTop, x2 - x1, chartHeight - eventsTop, '#f88', undefined, 1, 0.3);
        });

        // Render gridlines
        renderGridline(ctx, viewport, gridTop, chartHeight);

        // Render events
        if (isExpanded) {
            positionedEvents.forEach((event, index) => {
                const xStart = viewport.transform(event.startSMT);
                const xEnd = viewport.transform(event.endSMT);
                const width = xEnd - xStart;
                if (xEnd < 0 || xStart > canvasWidth) {
                    return; // Skip rendering events outside the viewport
                }

                const baseColor = colorMap.get(event.typeId).color;
                const x1 = Math.max(0, xStart);
                const x2 = Math.min(xStart + width, canvasWidth);

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

                    drawRoundedRect(ctx, x1 - LINE_STROKE_WIDTH / 2, y - rectHeight / 2, x2 - x1 + LINE_STROKE_WIDTH, rectHeight, 2, baseColor, undefined, 1, opacity);

                    // Display count
                    ctx.font = 'bold 10px sans-serif';
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'middle';
                    ctx.fillStyle = 'white';
                    ctx.fillText(event.count.toString(), x1 + (x2 - x1) / 2, y + 1);
                } else {
                    // Render as a single line
                    const y = eventsTop + event.row * (ROW_HEIGHT + ROW_PADDING) + ROW_HEIGHT / 2;
                    drawRoundedRect(ctx, x1 - LINE_STROKE_WIDTH / 2, y - LINE_STROKE_WIDTH / 2, x2 - x1 + LINE_STROKE_WIDTH, LINE_STROKE_WIDTH, LINE_STROKE_WIDTH / 2, baseColor, undefined, 1, opacity);
                }

                // Add hit region for event
                hitDetector.addRegion({
                    x: x1 - LINE_STROKE_WIDTH / 2,
                    y: eventsTop + event.row * (ROW_HEIGHT + ROW_PADDING),
                    width: x2 - x1 + LINE_STROKE_WIDTH,
                    height: ROW_HEIGHT,
                    data: { event, index },
                    type: 'event'
                });
            });

            // Render highlight for hovered event
            if (hoveredEvent) {
                const hoveredEventData = hoveredEvent.event;
                const xStart = viewport.transform(hoveredEventData.startSMT);
                const xEnd = viewport.transform(hoveredEventData.endSMT);
                const x1 = Math.max(0, xStart);
                const x2 = Math.min(xStart + (xEnd - xStart), canvasWidth);
                const y = eventsTop + hoveredEventData.row * (ROW_HEIGHT + ROW_PADDING) + ROW_HEIGHT / 2;

                const highlightColor = colorMap.get(hoveredEventData.typeId).color;
                const isCollapsed = hoveredEventData.count > 1;

                if (isCollapsed) {
                    const rectHeight = LINE_STROKE_WIDTH;
                    drawRoundedRect(ctx, x1 - LINE_STROKE_WIDTH / 2 - 1, y - rectHeight / 2 - 1, x2 - x1 + LINE_STROKE_WIDTH + 2, rectHeight + 2, 3, undefined, highlightColor, 2);
                } else {
                    drawRoundedRect(ctx, x1 - LINE_STROKE_WIDTH / 2 - 1, y - LINE_STROKE_WIDTH / 2 - 1, x2 - x1 + LINE_STROKE_WIDTH + 2, LINE_STROKE_WIDTH + 2, LINE_STROKE_WIDTH / 2 + 1, undefined, highlightColor, 2);
                }
            }
        }

        // Render border lines
        drawLine(ctx, 0, CPU_CHART_HEIGHT + 0.5, canvasWidth, CPU_CHART_HEIGHT + 0.5, '#666', 1, 'butt');

        if (legendHeight > 0) {
            drawLine(ctx, 0, chartHeight + 0.5, canvasWidth, chartHeight + 0.5, '#666', 1, 'butt');
        }

        // Render legend
        if (legendHeight > 0) {
            const legendY = chartHeight + LEGEND_VERTICAL_MARGIN;

            legendItems.forEach((item, index) => {
                const layout = legendLayout[index];
                const isHovered = hoveredLegendType === item.typeId;
                const isDimmed = hoveredLegendType !== null && !isHovered;

                ctx.globalAlpha = isDimmed ? 0.3 : 1;

                const x = layout.x;
                const y = legendY + layout.y;

                // Line indicator
                drawLine(ctx, x + 16, y + LEGEND_HEIGHT_PER_ROW / 2, x + 24, y + LEGEND_HEIGHT_PER_ROW / 2, item.color, LINE_STROKE_WIDTH, 'round');

                // Text
                ctx.font = '10px sans-serif';
                ctx.textAlign = 'left';
                ctx.textBaseline = 'middle';
                ctx.fillStyle = 'black';
                ctx.fillText(item.name, x + 36, y + LEGEND_HEIGHT_PER_ROW / 2 + 1);

                ctx.globalAlpha = 1;

                // Add hit region for legend item
                hitDetector.addRegion({
                    x,
                    y,
                    width: layout.width,
                    height: LEGEND_HEIGHT_PER_ROW,
                    data: { typeId: item.typeId },
                    type: 'legend'
                });
            });
        }
    }, [canvasWidth, canvasHeight, windows, chartMode, yAxisMode, colorMap, viewport, hoveredLegendType, hoveredCpuWindow, hoveredQueue, hoveredEvent, thread.queue.pending_counts, gridTop, chartHeight, eventsTop, isExpanded, positionedEvents, legendHeight, legendItems, legendLayout]);

    // Render canvas whenever dependencies change
    useEffect(() => {
        renderCanvas();
    }, [renderCanvas]);

    // Wheel event handler
    useEffect(() => {
        const canvas = canvasRef.current;
        if (!canvas) return;

        const handleWheel = (e: WheelEvent) => {
            if (!isExpanded) {
                // When collapsed, allow normal page scrolling
                return;
            }

            e.preventDefault();
            e.stopPropagation();
            const zoomFactor = e.deltaY < 0 ? 0.9 : 1.1;
            const rect = canvas.getBoundingClientRect();
            const mouseX = e.clientX - rect.left;
            setViewport(viewport.zoom(mouseX, zoomFactor));
        };

        canvas.addEventListener('wheel', handleWheel, { passive: false });
        return () => canvas.removeEventListener('wheel', handleWheel);
    }, [viewport, isExpanded]);

    // Resize observer
    useEffect(() => {
        const container = containerRef.current;
        if (!container) return;

        const resizeObserver = new ResizeObserver((entries) => {
            for (const entry of entries) {
                const newWidth = entry.contentRect.width;
                if (newWidth !== canvasWidth) {
                    setCanvasWidth(newWidth);
                    setViewport(vp => vp.resize(newWidth));
                }
            }
        });

        resizeObserver.observe(container);
        return () => resizeObserver.disconnect();
    }, [canvasWidth]);

    // Mouse move handler for hit detection
    const handleMouseMove = useCallback((e: React.MouseEvent<HTMLCanvasElement>) => {
        const canvas = canvasRef.current;
        if (!canvas) return;

        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;

        // Handle panning
        if (e.buttons === 1) {
            setViewport(viewport.pan(-e.movementX));
            return;
        }

        // Hit detection
        const hitDetector = hitDetectorRef.current;
        const hitRegion = hitDetector.findRegion(x, y);

        if (hitRegion) {
            if (hitRegion.type === 'window') {
                const windowIndex = hitRegion.data.windowIndex;
                if (!hoveredCpuWindow || hoveredCpuWindow.windowIndex !== windowIndex) {
                    setHoveredCpuWindow({ windowIndex, x: rect.left + hitRegion.x, y: rect.top + CPU_CHART_HEIGHT + 5 });
                    setHoveredEvent(null);
                    setHoveredQueue(null);
                    setHoveredLegendType(null);
                }
            } else if (hitRegion.type === 'event') {
                const event = hitRegion.data.event;
                if (!hoveredEvent || hoveredEvent.event !== event) {
                    setHoveredEvent({ event, x: rect.left + hitRegion.x, y: rect.top + hitRegion.y + hitRegion.height + 10 });
                    setHoveredCpuWindow(null);
                    setHoveredQueue(null);
                    setHoveredLegendType(null);
                }
            } else if (hitRegion.type === 'queue') {
                if (!hoveredQueue) {
                    setHoveredQueue({ x: rect.left + hitRegion.x, y: rect.top + hitRegion.y });
                    setHoveredCpuWindow(null);
                    setHoveredEvent(null);
                    setHoveredLegendType(null);
                }
            } else if (hitRegion.type === 'legend') {
                const typeId = hitRegion.data.typeId;
                if (hoveredLegendType !== typeId) {
                    setHoveredLegendType(typeId);
                    setHoveredCpuWindow(null);
                    setHoveredEvent(null);
                    setHoveredQueue(null);
                }
            }
        } else {
            // No hit, clear all hovers
            if (hoveredCpuWindow) setHoveredCpuWindow(null);
            if (hoveredEvent) setHoveredEvent(null);
            if (hoveredQueue) setHoveredQueue(null);
            if (hoveredLegendType !== null) setHoveredLegendType(null);
        }
    }, [viewport, hoveredCpuWindow, hoveredEvent, hoveredQueue, hoveredLegendType]);

    const handleMouseLeave = useCallback(() => {
        setHoveredCpuWindow(null);
        setHoveredEvent(null);
        setHoveredQueue(null);
        setHoveredLegendType(null);
    }, []);

    const handleDoubleClick = useCallback(() => {
        setViewport(viewport.reset());
    }, [viewport]);

    return (<>
        <div ref={containerRef} style={{ width: '100%' }}>
            <canvas
                ref={canvasRef}
                width={canvasWidth}
                height={canvasHeight}
                style={{
                    backgroundColor: "white",
                    borderTop: "1px solid #666",
                    borderBottom: "1px solid #666",
                    display: "block",
                    width: '100%',
                    // cursor: 'pointer'
                }}
                onMouseMove={handleMouseMove}
                onMouseLeave={handleMouseLeave}
                onDoubleClick={handleDoubleClick}
            />
        </div>

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
            const totalTimeNs = summary.message_stats_by_type.reduce((sum, stat) => sum + stat.t, 0);
            const totalMs = totalTimeNs / 1e6;
            const totalPercent = (totalMs / windowDurationMs) * 100;
            const totalCount = summary.message_stats_by_type.reduce((sum, stat) => sum + stat.c, 0);
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
                                const typeMs = stat.t / 1e6;
                                const typePercent = (typeMs / windowDurationMs) * 100;
                                const typeColor = colorMap.get(stat.m).color;
                                const typeName = colorMap.get(stat.m).name;
                                return (
                                    <tr key={stat.m}>
                                        <td>
                                            <span className="color-indicator" style={{ backgroundColor: typeColor }}></span>
                                            {typeName}
                                        </td>
                                        <td className="align-right">{stat.c}</td>
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