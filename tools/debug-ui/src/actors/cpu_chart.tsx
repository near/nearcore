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
}): JSX.Element[] {
    const { windows, chartMode, yAxisMode, colorMap, viewport, hoveredLegendType } = params;

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
                    x={Math.max(0, x1)}
                    y={y2}
                    width={Math.max(1, x2 - x1)}
                    height={y1 - y2}
                    fill={color}
                    opacity={isDimmed ? 0.1 : 0.8}
                />
            );

            cumulativeY += thisBarHeight;
        });
    });

    // Render labels
    const labelX = Math.max(0, viewport.transform(windows[0].startSMT)) + 5;
    const chartTypeName = chartMode === 'cpu' ? 'CPU' : 'Dequeue Delay';
    const maxPercent = yMax * 100;

    // Max label
    elements.push(
        <text
            x={labelX}
            y={15}
            fontSize={12}
            fontFamily="sans-serif"
            fill="#888"
            fontWeight="bold"
        >
            {Math.round(maxPercent * 1e6) / 1e6}% ({chartTypeName})
        </text>);
    // Min label
    elements.push(
        <text
            x={labelX}
            y={CPU_CHART_HEIGHT - 5}
            fontSize={12}
            fontFamily="sans-serif"
            fill="#888"
            fontWeight="bold"
        >
            0%
        </text>);
    return elements;
}
