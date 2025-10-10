import { Viewport } from "./algorithm";

/// Calculates appropriate time tick interval based on viewport span
function getTimeTickInterval(viewportSpanMs: number): number {
    // Target around 8-12 ticks across the viewport
    const targetTicks = 10;
    const roughInterval = viewportSpanMs / targetTicks;

    // Round to nice intervals: 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, etc.
    const magnitude = Math.pow(10, Math.floor(Math.log10(roughInterval)));
    const normalizedInterval = roughInterval / magnitude;

    let niceInterval;
    if (normalizedInterval < 1.5) {
        niceInterval = 1;
    } else if (normalizedInterval < 3.5) {
        niceInterval = 2;
    } else if (normalizedInterval < 7.5) {
        niceInterval = 5;
    } else {
        niceInterval = 10;
    }

    return niceInterval * magnitude;
}

/// Formats time in milliseconds to seconds, removing trailing zeros
function formatTime(ms: number): string {
    const seconds = ms / 1000;
    // Remove trailing zeros and unnecessary decimal point
    return seconds.toFixed(3).replace(/\.?0+$/, '');
}

export function renderGridline(viewport: Viewport, gridTop: number, chartHeight: number): JSX.Element {
    return <g>
        {(() => {
            const viewportSpan = viewport.getEnd() - viewport.getStart();
            const tickInterval = getTimeTickInterval(viewportSpan);
            const startTick = Math.ceil(viewport.getStart() / tickInterval) * tickInterval;
            const ticks = [];

            for (let tick = startTick; tick <= viewport.getEnd(); tick += tickInterval) {
                const x = viewport.transform(tick);
                ticks.push(
                    <g key={tick}>
                        <line
                            x1={x}
                            y1={gridTop}
                            x2={x}
                            y2={chartHeight}
                            stroke="#ccc"
                            strokeWidth={1}
                        />
                        {(
                            <text
                                x={x}
                                y={gridTop - 8}
                                fontSize={10}
                                fontFamily="sans-serif"
                                fill="#666"
                                textAnchor="middle"
                            >
                                {formatTime(tick)}
                            </text>
                        )}
                    </g>
                );
            }
            return ticks;
        })()}
    </g>;
}