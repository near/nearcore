import { MessageTypeAndColorMap, Viewport, WindowToDisplay } from "./algorithm";
import { CPU_CHART_HEIGHT } from "./constants";

/// Calculates y-max, as a value from 0 to 1.
function calculateYMaxForCpuChart(
    windows: WindowToDisplay[],
    chartMode: 'cpu' | 'dequeue',
    yAxisMode: 'auto' | 'fixed'
): number {
    if (yAxisMode === 'fixed') {
        return 1;
    }

    let maxFraction = 0;
    windows.forEach(window => {
        const summary = chartMode === 'cpu' ? window.summary : window.dequeueSummary;
        const totalTimeNs = summary.message_stats_by_type.reduce(
            (sum, stat) => sum + stat.total_time_ns, 0
        );
        const fractionInWindow = totalTimeNs / 1e6 / Math.max(window.endSMT - window.startSMT, 1);
        maxFraction = Math.max(maxFraction, fractionInWindow);
    });

    if (maxFraction >= 0.01) {
        return Math.ceil(maxFraction * 100) / 100;
    }
    // Return 0.001, 0.0001, 0.00001, ... whichever is smallest that is >= maxFraction
    return Math.pow(10, -Math.floor(-Math.log10(maxFraction)));
}

/// Renders the CPU/dequeue chart
export function renderCpuChart(params: {
    windows: WindowToDisplay[],
    chartMode: 'cpu' | 'dequeue',
    yAxisMode: 'auto' | 'fixed',
    colorMap: MessageTypeAndColorMap,
    viewport: Viewport,
    hoveredLegendType: number | null,
    hoveredWindowIndex: number | null,
    onWindowHover: (windowIndex: number, x: number, y: number) => void,
    onWindowLeave: () => void,
    queueCounts: { [message_type: string]: number },
    hoveredQueue: boolean,
    onQueueHover: (x: number, y: number) => void,
    onQueueLeave: () => void,
}): JSX.Element[] {
    const { windows, chartMode, yAxisMode, colorMap, viewport, hoveredLegendType, hoveredWindowIndex, onWindowHover, onWindowLeave, queueCounts, hoveredQueue, onQueueHover, onQueueLeave } = params;

    const elements: JSX.Element[] = [];

    const typeOrder = Array.from(colorMap.keys());
    const yMax = calculateYMaxForCpuChart(windows, chartMode, yAxisMode);

    windows.forEach((window, windowIndex) => {
        if (viewport.isRangeOutsideViewport(window.startSMT, window.endSMT)) return;
        const x1 = viewport.transform(window.startSMT);
        const x2 = viewport.transform(window.endSMT);

        // Create stacked bars - y is the cumulative fraction of time in window.
        let cumulativeY = 0;

        // Select the appropriate summary based on chart mode
        const summary = chartMode === 'cpu' ? window.summary : window.dequeueSummary;

        const windowDurationMs = Math.max(window.endSMT - window.startSMT, 1);  // avoid division by 0

        typeOrder.forEach((type) => {
            const stat = summary.message_stats_by_type.find(s => s.message_type === type);
            if (!stat || stat.total_time_ns === 0) return;

            const y1 = CPU_CHART_HEIGHT - (cumulativeY / yMax * CPU_CHART_HEIGHT);
            const thisBarHeight = stat.total_time_ns / 1e6 / windowDurationMs;
            const y2 = CPU_CHART_HEIGHT - ((cumulativeY + thisBarHeight) / yMax * CPU_CHART_HEIGHT);

            const color = colorMap.get(type).color;
            const isDimmed = hoveredLegendType !== null && hoveredLegendType !== type;

            elements.push(
                <rect
                    key={`${windowIndex}-${type}`}
                    x={x1}
                    y={y2}
                    width={x2 - x1}
                    height={y1 - y2}
                    fill={color}
                    opacity={isDimmed ? 0.1 : 0.8}
                />
            );

            cumulativeY += thisBarHeight;
        });

        // Add transparent overlay for hover detection
        elements.push(
            <rect
                key={`hover-${windowIndex}`}
                x={x1}
                y={0}
                width={x2 - x1}
                height={CPU_CHART_HEIGHT}
                fill="transparent"
                onMouseEnter={(e) => {
                    const rect = e.currentTarget.getBoundingClientRect();
                    onWindowHover(windowIndex, rect.right + 5, rect.top);
                }}
                onMouseLeave={onWindowLeave}
                style={{ cursor: 'pointer' }}
            />
        );
    });
    if (hoveredWindowIndex !== null) {
        const hoveredWindow = windows[hoveredWindowIndex];
        const x1 = viewport.transform(hoveredWindow.startSMT);
        const x2 = viewport.transform(hoveredWindow.endSMT);
        elements.push(
            <rect
                key={`outline-${hoveredWindowIndex}`}
                x={x1 + 0.5}
                y={0.5}
                width={x2 - x1 - 0.5}
                height={CPU_CHART_HEIGHT - 1}
                fill="none"
                stroke="#000"
                strokeWidth={1}
                style={{ pointerEvents: 'none' }}
            />
        );
    }

    // Render labels with backdrop
    const labelX = Math.max(0, viewport.transform(windows[0].startSMT)) + 5;
    const chartTypeName = chartMode === 'cpu' ? 'CPU' : 'Dequeue Delay';
    const maxPercent = yMax * 100;
    const maxLabelText = `${Math.round(maxPercent * 1e6) / 1e6}% (${chartTypeName})`;

    // Max label with backdrop
    elements.push(
        <g key="max-label" style={{ pointerEvents: 'none' }}>
            <rect
                x={labelX - 2}
                y={3}
                width={maxLabelText.length * 7}
                height={16}
                fill="white"
                opacity={0.7}
            />
            <text
                x={labelX}
                y={15}
                fontSize={12}
                fontFamily="sans-serif"
                fill="#888"
                fontWeight="bold"
            >
                {maxLabelText}
            </text>
        </g>
    );

    // Min label with backdrop
    elements.push(
        <g key="min-label" style={{ pointerEvents: 'none' }}>
            <rect
                x={labelX - 2}
                y={CPU_CHART_HEIGHT - 17}
                width={20}
                height={16}
                fill="white"
                opacity={0.7}
            />
            <text
                x={labelX}
                y={CPU_CHART_HEIGHT - 5}
                fontSize={12}
                fontFamily="sans-serif"
                fill="#888"
                fontWeight="bold"
            >
                0%
            </text>
        </g>
    );

    // Render queue indicator beyond last window
    {
        const totalQueued = Object.values(queueCounts).reduce((sum, count) => sum + count, 0);

        const lastWindow = windows[windows.length - 1];
        const queueX1 = viewport.transform(lastWindow.endSMT);
        const queueWidth = viewport.transformLength(1000);

        // Only render if at least partially visible
        if (queueX1 < viewport.transform(viewport.getEnd())) {
            const isHovered = hoveredQueue === true;

            // Background for queue indicator
            elements.push(
                <rect
                    key="queue-bg"
                    x={queueX1}
                    y={0}
                    width={queueWidth}
                    height={CPU_CHART_HEIGHT}
                    fill={isHovered ? "#ccc" : "#eee"}
                    onMouseEnter={(e) => {
                        const rect = e.currentTarget.getBoundingClientRect();
                        onQueueHover(rect.left, rect.top);
                    }}
                    onMouseLeave={onQueueLeave}
                    style={{ cursor: 'pointer' }}
                />
            );

            // Queue text
            const queueText = `Q: ${totalQueued}`;
            elements.push(
                <text
                    key="queue-text"
                    x={queueX1 + queueWidth / 2}
                    y={CPU_CHART_HEIGHT / 2 + 5}
                    fontSize={14}
                    fontFamily="sans-serif"
                    textAnchor="middle"
                    fill="#333"
                    style={{ pointerEvents: 'none' }}
                >
                    {queueText}
                </text>
            );

            // Outline when hovered
            if (isHovered) {
                elements.push(
                    <rect
                        key="queue-outline"
                        x={queueX1 + 0.5}
                        y={0.5}
                        width={queueWidth - 1}
                        height={CPU_CHART_HEIGHT - 1}
                        fill="none"
                        stroke="#000"
                        strokeWidth={1}
                        style={{ pointerEvents: 'none' }}
                    />
                );
            }
        }
    }

    return elements;
}
