import { MessageTypeAndColorMap, Viewport, WindowToDisplay } from "./algorithm";
import { CPU_CHART_HEIGHT } from "./constants";
import { HitDetector, drawRect, drawTextWithBackground } from "./canvas_utils";

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
            (sum, stat) => sum + stat.t, 0
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
export function renderCpuChart(
    ctx: CanvasRenderingContext2D,
    hitDetector: HitDetector,
    params: {
        windows: WindowToDisplay[],
        chartMode: 'cpu' | 'dequeue',
        yAxisMode: 'auto' | 'fixed',
        colorMap: MessageTypeAndColorMap,
        viewport: Viewport,
        hoveredLegendType: number | null,
        hoveredWindowIndex: number | null,
        queueCounts: { [message_type: string]: number },
        hoveredQueue: boolean,
    }
): void {
    const { windows, chartMode, yAxisMode, colorMap, viewport, hoveredLegendType, hoveredWindowIndex, queueCounts, hoveredQueue } = params;

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
            const stat = summary.message_stats_by_type.find(s => s.m === type);
            if (!stat || stat.t === 0) return;

            const y1 = CPU_CHART_HEIGHT - (cumulativeY / yMax * CPU_CHART_HEIGHT);
            const thisBarHeight = stat.t / 1e6 / windowDurationMs;
            const y2 = CPU_CHART_HEIGHT - ((cumulativeY + thisBarHeight) / yMax * CPU_CHART_HEIGHT);

            const color = colorMap.get(type).color;
            const isDimmed = hoveredLegendType !== null && hoveredLegendType !== type;

            drawRect(ctx, x1, y2, x2 - x1, y1 - y2, color, undefined, 1, isDimmed ? 0.1 : 0.8);

            cumulativeY += thisBarHeight;
        });

        // Add hit region for hover detection
        hitDetector.addRegion({
            x: x1,
            y: 0,
            width: x2 - x1,
            height: CPU_CHART_HEIGHT,
            data: { windowIndex },
            type: 'window'
        });
    });

    // Draw outline for hovered window
    if (hoveredWindowIndex !== null) {
        const hoveredWindow = windows[hoveredWindowIndex];
        const x1 = viewport.transform(hoveredWindow.startSMT);
        const x2 = viewport.transform(hoveredWindow.endSMT);
        drawRect(ctx, x1 + 0.5, 0.5, x2 - x1 - 0.5, CPU_CHART_HEIGHT - 1, undefined, '#000', 1);
    }

    // Render labels with backdrop
    const labelX = Math.max(0, viewport.transform(windows[0].startSMT)) + 5;
    const chartTypeName = chartMode === 'cpu' ? 'Utilization' : 'Dequeue Delay';
    const maxPercent = yMax * 100;
    const maxLabelText = `${Math.round(maxPercent * 1e6) / 1e6}% (${chartTypeName})`;

    // Max label with backdrop
    drawTextWithBackground(ctx, maxLabelText, labelX, 15, 12, 'sans-serif', '#888', 'white', 0.7, 'bold');

    // Min label with backdrop
    drawTextWithBackground(ctx, '0%', labelX, CPU_CHART_HEIGHT - 5, 12, 'sans-serif', '#888', 'white', 0.7, 'bold');

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
            drawRect(ctx, queueX1, 0, queueWidth, CPU_CHART_HEIGHT, isHovered ? "#ccc" : "#eee");

            // Queue text
            const queueText = `Q: ${totalQueued}`;
            ctx.font = '14px sans-serif';
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillStyle = '#333';
            ctx.fillText(queueText, queueX1 + queueWidth / 2, CPU_CHART_HEIGHT / 2 + 1);

            // Outline when hovered
            if (isHovered) {
                drawRect(ctx, queueX1 + 0.5, 0.5, queueWidth - 1, CPU_CHART_HEIGHT - 1, undefined, '#000', 1);
            }

            // Add hit region for queue
            hitDetector.addRegion({
                x: queueX1,
                y: 0,
                width: queueWidth,
                height: CPU_CHART_HEIGHT,
                data: {},
                type: 'queue'
            });
        }
    }
}
