export type Interval = {
    // Start and end are arbitrary numbers, we only require start < end.
    start: number;
    end: number;

    // The layer assigned by assignLayerToOverlappingIntervals.
    assignedLayer: number;

    // The ArrowGroup that this interval belongs to (user data).
    arrowGroupId: number;
};

// Classic interval scheduling problem. Given a set of intervals,
// assign each interval to a layer such that no two intervals
// on the same layer overlap, and that the number of layers
// used is minimized.
//
// The layer is assigned to the assignedLayer field of each given
// interval. The number of total layers is returned. Two intervals are
// considered to overlap even if only their endpoints touch.
export function assignLayerToOverlappingIntervals(intervals: Interval[]): number {
    intervals.sort((a, b) => a.start - b.start);
    const activeIntervals = [] as (Interval | null)[];
    for (const interval of intervals) {
        for (let i = 0; i < activeIntervals.length + 1; i++) {
            if (i >= activeIntervals.length) {
                interval.assignedLayer = i;
                activeIntervals.push(interval);
                break;
            }
            if (activeIntervals[i] === null || activeIntervals[i]!.end < interval.start) {
                interval.assignedLayer = i;
                activeIntervals[i] = interval;
                break;
            }
        }
    }
    return activeIntervals.length;
}
