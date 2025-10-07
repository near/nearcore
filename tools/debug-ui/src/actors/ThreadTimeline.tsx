import { useMemo, useState, useRef, useEffect } from "react";
import { InstrumentedThread, InstrumentedWindowSummary } from "../api";
import { CPU_CHART_HEIGHT, EVENT_SPACING_PX, GRID_LABEL_TOP_MARGIN, LEGEND_HEIGHT_PER_ROW, LEGEND_VERTICAL_MARGIN, LINE_STROKE_WIDTH, ROW_HEIGHT, ROW_PADDING, VIEWPORT_WIDTH } from "./constants";
import { assignRows, EventToDisplay, getEventsToDisplay, makeWindowsToDisplay, MessageTypeAndColorMap, Viewport } from "./algorithm";
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
};

export const ThreadTimeline = ({ thread, messageTypes, minTimeMs, currentTimeMs, chartMode, yAxisMode }: ActorsViewProps) => {
    const events = useMemo(
        () => getEventsToDisplay(thread, minTimeMs, currentTimeMs),
        [thread, minTimeMs, currentTimeMs]
    );
    const windows = useMemo(
        () => makeWindowsToDisplay(thread, minTimeMs),
        [thread, minTimeMs]
    );

    const maxTimeMs = currentTimeMs - minTimeMs;
    const svgRef = useRef<SVGSVGElement>(null);
    const [svgWidth, setSvgWidth] = useState(VIEWPORT_WIDTH);
    const [viewport, setViewport] = useState(
        new Viewport(-1000, maxTimeMs + 1000, svgWidth, -1000, maxTimeMs + 1000, 1)
    );

    const colorMap = useMemo(() => new MessageTypeAndColorMap(events, windows, messageTypes), [events, windows, messageTypes]);

    const { events: positionedEvents, maxRow } = useMemo(
        () => assignRows(events, viewport),
        [events, viewport]
    );

    const gridTop = CPU_CHART_HEIGHT + GRID_LABEL_TOP_MARGIN;
    const eventsTop = gridTop + 10;
    const chartHeight = eventsTop + (maxRow + 1) * (ROW_HEIGHT + ROW_PADDING) + 10;

    // Calculate legend columns based on available width
    const legendItemWidth = 200;
    const legendColumns = Math.max(1, Math.floor(svgWidth / legendItemWidth));
    const legendRows = colorMap.size > 0 ? Math.ceil(colorMap.size / legendColumns) : 0;
    const legendHeight = legendRows > 0 ? LEGEND_VERTICAL_MARGIN * 2 + LEGEND_HEIGHT_PER_ROW * legendRows : 0;

    const svgHeight = chartHeight + legendHeight;

    const [hoveredEvent, setHoveredEvent] = useState<{ event: EventToDisplay, x: number, y: number } | null>(null);
    const [hoveredLegendType, setHoveredLegendType] = useState<number | null>(null);
    const [hoveredCpuWindow, setHoveredCpuWindow] = useState<{ windowIndex: number, x: number, y: number } | null>(null);

    useEffect(() => {
        const svg = svgRef.current;
        if (!svg) return;

        const handleWheel = (e: WheelEvent) => {
            e.preventDefault();
            e.stopPropagation();
            const zoomFactor = e.deltaY < 0 ? 0.9 : 1.1;
            const rect = svg.getBoundingClientRect();
            const mouseX = e.clientX - rect.left;
            setViewport(viewport.zoom(mouseX, zoomFactor));
        };

        svg.addEventListener('wheel', handleWheel, { passive: false });
        return () => svg.removeEventListener('wheel', handleWheel);
    }, [viewport]);

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
                                    fill="#444"
                                    opacity={0.3}
                                />
                            )}
                            {/* Area after last window */}
                            {afterX1 < svgWidth && (
                                <rect
                                    x={Math.max(0, afterX1)}
                                    y={0}
                                    width={Math.min(svgWidth, afterX2) - Math.max(0, afterX1)}
                                    height={chartHeight}
                                    fill="#444"
                                    opacity={0.3}
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
                        const x1 = viewport.transform(window.startSMT);
                        const x2 = viewport.transform(window.endSMT);

                        // Skip windows outside viewport
                        if (x2 < 0 || x1 > svgWidth) return;

                        indicators.push(
                            <rect
                                key={`overfilled-${index}`}
                                x={Math.max(0, x1)}
                                y={eventsTop}
                                width={Math.max(1, x2 - x1)}
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

            {positionedEvents.map((event, index) => {
                const xStart = viewport.transform(event.startSMT);
                const xEnd = viewport.transform(event.endSMT);
                const width = xEnd - xStart;
                if (xEnd < 0 || xStart > svgWidth) {
                    return null; // Skip rendering events outside the viewport
                }

                const y = eventsTop + event.row * (ROW_HEIGHT + ROW_PADDING) + ROW_HEIGHT / 2;
                const baseColor = colorMap.get(event.typeId).color;
                const x1 = Math.max(0, xStart);
                const x2 = Math.max(0, xStart) + width;

                // Determine opacity based on legend hover state
                let opacity = event.leftUncertain || event.rightUncertain ? 0.6 : 0.9;
                if (hoveredLegendType !== null) {
                    opacity = event.typeId === hoveredLegendType ? opacity : 0.1;
                }

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
                    {colorMap.map((typeId, name, color, index) => {
                        const x = (index % legendColumns) * legendItemWidth;
                        const y = Math.floor(index / legendColumns) * LEGEND_HEIGHT_PER_ROW;
                        const isHovered = hoveredLegendType === typeId;
                        const isDimmed = hoveredLegendType !== null && !isHovered;

                        return (
                            <g
                                key={typeId}
                                transform={`translate(${x}, ${y})`}
                                onMouseEnter={() => setHoveredLegendType(typeId)}
                                onMouseLeave={() => setHoveredLegendType(null)}
                                style={{ cursor: 'pointer' }}
                                opacity={isDimmed ? 0.3 : 1}
                            >
                                {/* Backdrop for larger hover area */}
                                <rect x={0} y={0} width={legendItemWidth} height={LEGEND_HEIGHT_PER_ROW} fill="transparent" />
                                <line x1={16} y1={LEGEND_HEIGHT_PER_ROW / 2} x2={24} y2={LEGEND_HEIGHT_PER_ROW / 2} stroke={color} strokeWidth={LINE_STROKE_WIDTH} strokeLinecap="round" />
                                <text x={36} y={LEGEND_HEIGHT_PER_ROW / 2 + 4} fontSize={10} fontFamily="sans-serif">{name}</text>
                            </g>
                        );
                    })}
                </g>
            )}
        </svg>

        {/* Event Tooltip */}
        {hoveredEvent && (
            <div
                className="tooltip event-tooltip"
                style={{
                    left: hoveredEvent.x,
                    top: hoveredEvent.y
                }}
            >
                <div><strong>{colorMap.get(hoveredEvent.event.typeId).name}</strong></div>
                <div>Duration: {(hoveredEvent.event.endSMT - hoveredEvent.event.startSMT).toFixed(3)} ms</div>
                <div>Start: {hoveredEvent.event.startSMT.toFixed(2)} ms</div>
                <div>End: {hoveredEvent.event.endSMT.toFixed(2)} ms</div>
            </div>
        )}

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

            return (
                <div
                    className="tooltip"
                    style={{
                        left: hoveredCpuWindow.x,
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
                                const typeName = colorMap.get(stat.message_type).name;
                                return (
                                    <tr key={stat.message_type}>
                                        <td>{typeName}</td>
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
    </>);
};