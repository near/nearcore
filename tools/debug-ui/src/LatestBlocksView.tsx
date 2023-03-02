import { Fragment, ReactElement, useCallback, useMemo, useState } from 'react';
import { useQuery } from 'react-query';
import Xarrow, { Xwrapper, useXarrow } from 'react-xarrows';
import { DebugBlockStatus, MissedHeightInfo, fetchBlockStatus, fetchFullStatus } from './api';
import './LatestBlocksView.scss';

function ellipsify(str: string, maxLen: number): string {
    if (str.length > maxLen) {
        return str.substring(0, maxLen - 3) + '...';
    }
    return str;
}

type HashElementProps = {
    hashValue: string;
    creator: string;
    expandAll: boolean;
    knownProducers: Set<string>;
};

// Makes an element that when clicked, expands or ellipsifies the hash and creator.
const HashElement = ({ hashValue, creator, expandAll, knownProducers }: HashElementProps) => {
    const [expanded, setExpanded] = useState(false);
    const updateXarrow = useXarrow();
    return (
        <span
            className={`hash-element ${knownProducers.has(creator) ? '' : 'validator-unavailable'}`}
            onClick={() => {
                setExpanded((value) => !value);
                // xarrows need to be updated whenever graph dot positions may change.
                updateXarrow();
            }}>
            {expanded || expandAll
                ? `${hashValue} ${creator}`
                : `${ellipsify(hashValue, 8)} ${ellipsify(creator, 13)}`}
        </span>
    );
};

type BlockTableRowBlock = {
    block: DebugBlockStatus;
    parentIndex: number | null; // the index of the parent block, or null if parent not included in the data
    graphColumn: number | null; // the column to display the graph node in
    blockDelay: number | null; // number of seconds since parent's block timestamp, or null if parent not included in the data
    chunkSkipped: boolean[]; // for each chunk, whether the chunk is the same as that chunk of parent block
    isHead: boolean;
    isHeaderHead: boolean;
};
type BlockTableRow = BlockTableRowBlock | { missedHeight: MissedHeightInfo };

// Sorts the API response into easily displayable rows, and computes the graph layout.
function sortBlocksAndDetermineBlockGraphLayout(
    blocks: DebugBlockStatus[],
    missedHeights: MissedHeightInfo[],
    head: string,
    headerHead: string
): BlockTableRow[] {
    const rows: BlockTableRow[] = [];
    for (const block of blocks) {
        rows.push({
            block,
            parentIndex: null,
            graphColumn: -1,
            blockDelay: null,
            chunkSkipped: block.chunks.map(() => false),
            isHead: head === block.block_hash,
            isHeaderHead: headerHead === block.block_hash,
        });
    }
    for (const missedHeight of missedHeights) {
        rows.push({ missedHeight });
    }

    function sortingKey(row: BlockTableRow) {
        if ('block' in row) {
            // some lousy tie-breaking for same-height rows.
            return row.block.block_height + ((row.block.block_timestamp / 1e12) % 1);
        } else {
            return row.missedHeight.block_height;
        }
    }

    rows.sort((a, b) => sortingKey(b) - sortingKey(a));

    const rowIndexByHash = new Map<string, number>();
    rows.forEach((row, rowIndex) => {
        if ('block' in row) {
            rowIndexByHash.set(row.block.block_hash, rowIndex);
        }
    });

    let highestNodeOnFirstColumn = rows.length;
    for (let i = rows.length - 1; i >= 0; i--) {
        const row = rows[i];
        if ('missedHeight' in row) {
            continue;
        }
        const block = row.block;

        // Look up parent index, and also compute things that depend on the parent block.
        if (rowIndexByHash.has(block.prev_block_hash)) {
            row.parentIndex = rowIndexByHash.get(block.prev_block_hash)!;
            const parentBlock = (rows[row.parentIndex] as BlockTableRowBlock).block;
            row.blockDelay = (block.block_timestamp - parentBlock.block_timestamp) / 1e9;
            for (let j = 0; j < Math.min(block.chunks.length, parentBlock.chunks.length); j++) {
                row.chunkSkipped[j] =
                    block.chunks[j].chunk_hash === parentBlock.chunks[j].chunk_hash;
            }
        }
        // We'll use a two-column layout for the block graph. We traverse from bottom
        // up (oldest block to latest), and for each row we pick the first column unless
        // that would make us draw a line (from the parent to this node) through another
        // node; in which case we would pick the second column. To do that we just need
        // to keep track of the highest node we've seen so far for the first column.
        //
        // Not the best layout for a graph, but it's sufficient since we rarely have forks.
        let column = 0;
        if (
            row.parentIndex !== null &&
            (rows[row.parentIndex] as BlockTableRowBlock).graphColumn === 0 &&
            row.parentIndex > highestNodeOnFirstColumn
        ) {
            column = 1;
        } else {
            highestNodeOnFirstColumn = i;
        }
        row.graphColumn = column;
    }
    return rows;
}

type BlocksTableProps = {
    rows: BlockTableRow[];
    knownProducers: Set<string>;
    expandAll: boolean;
    hideMissingHeights: boolean;
};

const BlocksTable = ({ rows, knownProducers, expandAll, hideMissingHeights }: BlocksTableProps) => {
    let numGraphColumns = 1; // either 1 or 2; determines the width of leftmost td
    let numShards = 0;
    for (const row of rows) {
        if ('block' in row) {
            numGraphColumns = Math.max(numGraphColumns, (row.graphColumn || 0) + 1);
            for (const chunk of row.block.chunks) {
                numShards = Math.max(numShards, chunk.shard_id + 1);
            }
        }
    }
    const header = (
        <tr>
            <th>Chain</th>
            <th>Height</th>
            <th>{'Hash & creator'}</th>
            <th>Processing Time (ms)</th>
            <th>Block Delay (s)</th>
            <th>Gas price ratio</th>
            {[...Array(numShards).keys()].map((i) => (
                <th key={i} colSpan={3}>
                    Shard {i} (hash/gas(Tgas)/time(ms))
                </th>
            ))}
        </tr>
    );

    // One xarrow element per arrow (from block to block).
    const graphArrows = [] as ReactElement[];

    // One 'tr' element per row.
    const tableRows = [] as ReactElement[];
    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        if ('missedHeight' in row) {
            if (!hideMissingHeights) {
                tableRows.push(
                    <tr key={row.missedHeight.block_height} className="missed-height">
                        <td className="graph-node-cell" />
                        <td>{row.missedHeight.block_height}</td>
                        <td colSpan={4 + numShards * 3}>
                            {row.missedHeight.block_producer} missed block
                        </td>
                    </tr>
                );
            }
            continue;
        }
        const block = row.block;

        const chunkCells = [] as ReactElement[];
        block.chunks.forEach((chunk, shardId) => {
            chunkCells.push(
                <Fragment key={shardId}>
                    <td className={row.chunkSkipped[shardId] ? 'skipped-chunk' : ''}>
                        <HashElement
                            hashValue={chunk.chunk_hash}
                            creator={chunk.chunk_producer || ''}
                            expandAll={expandAll}
                            knownProducers={knownProducers}
                        />
                    </td>
                    <td>{(chunk.gas_used / (1024 * 1024 * 1024 * 1024)).toFixed(1)}</td>
                    <td>{chunk.processing_time_ms}</td>
                </Fragment>
            );
        });

        tableRows.push(
            <tr
                key={block.block_hash}
                className={`block-row ${
                    row.block.is_on_canonical_chain ? '' : 'not-on-canonical-chain'
                }`}>
                <td className="graph-node-cell">
                    <div
                        id={`graph-node-${i}`}
                        className={`graph-dot graph-dot-col-${row.graphColumn} graph-dot-total-${numGraphColumns}`}></div>
                </td>
                <td>
                    <span>{block.block_height}</span>
                    {row.isHead && <div className="head-label">HEAD</div>}
                    {row.isHeaderHead && <div className="header-head-label">HEADER HEAD</div>}
                </td>
                <td>
                    <HashElement
                        hashValue={block.block_hash}
                        creator={block.block_producer || ''}
                        expandAll={expandAll}
                        knownProducers={knownProducers}
                    />
                </td>
                <td>{block.processing_time_ms}</td>
                <td>{row.blockDelay ?? ''}</td>
                <td>{block.gas_price_ratio}</td>
                {block.full_block_missing && <td colSpan={numShards * 3}>header only</td>}
                {chunkCells}
            </tr>
        );
        if (row.parentIndex != null) {
            graphArrows.push(
                <Xarrow
                    key={i}
                    start={`graph-node-${i}`}
                    end={`graph-node-${row.parentIndex}`}
                    color={row.block.is_on_canonical_chain ? 'black' : 'darkgray'}
                    strokeWidth={row.block.is_on_canonical_chain ? 3 : 1}
                    headSize={0}
                    path="straight"
                />
            );
        }
    }
    return (
        <div>
            {graphArrows}
            <table>
                <tbody>
                    {header}
                    {tableRows}
                </tbody>
            </table>
        </div>
    );
};

type LatestBlockViewProps = {
    addr: string;
};

export const LatestBlocksView = ({ addr }: LatestBlockViewProps) => {
    const [height, setHeight] = useState<number | null>(null);
    const [heightInInput, setHeightInInput] = useState<string>('');
    const [expandAll, setExpandAll] = useState(false);
    const [hideMissingHeights, setHideMissingHeights] = useState(false);
    const [showMissingChunksStats, setShowMissingChunksStats] = useState(false);
    const updateXarrow = useXarrow();

    const { data: status } = useQuery(
        ['fullStatus', addr],
        async () => await fetchFullStatus(addr)
    );
    const {
        data: blockData,
        error,
        isLoading,
    } = useQuery(['latestBlocks', addr, height], async () => await fetchBlockStatus(addr, height));

    const { rows, knownProducerSet } = useMemo(() => {
        if (status && blockData) {
            const knownProducerSet = new Set<string>();
            for (const producer of status.detailed_debug_status!.network_info.known_producers) {
                knownProducerSet.add(producer.account_id);
            }

            const data = blockData.status_response.BlockStatus;
            const rows = sortBlocksAndDetermineBlockGraphLayout(
                data.blocks,
                data.missed_heights,
                data.head,
                data.header_head
            );
            return { rows, knownProducerSet };
        }
        return { rows: [], knownProducerSet: new Set<string>() };
    }, [status, blockData]);

    // Compute missing blocks and chunks statistics (whenever rows changes).
    const { numCanonicalBlocks, canonicalHeightCount, numChunksSkipped } = useMemo(() => {
        let firstCanonicalHeight = 0;
        let lastCanonicalHeight = 0;
        let numCanonicalBlocks = 0;
        const numChunksSkipped = [];
        for (const row of rows) {
            if (!('block' in row)) {
                continue;
            }
            const block = row.block;
            if (!block.is_on_canonical_chain) {
                continue;
            }
            if (firstCanonicalHeight === 0) {
                firstCanonicalHeight = block.block_height;
            }
            lastCanonicalHeight = block.block_height;
            numCanonicalBlocks++;
            for (let i = 0; i < row.chunkSkipped.length; i++) {
                while (numChunksSkipped.length < i + 1) {
                    numChunksSkipped.push(0);
                }
                if (row.chunkSkipped[i]) {
                    numChunksSkipped[i]++;
                }
            }
        }
        return {
            numCanonicalBlocks,
            canonicalHeightCount: firstCanonicalHeight - lastCanonicalHeight + 1,
            numChunksSkipped,
        };
    }, [rows]);

    const goToHeightCallback = useCallback(() => {
        const height = parseInt(heightInInput);
        setHeight(height);
    }, [heightInInput]);

    return (
        <Xwrapper>
            <div className="latest-blocks-view">
                <div className="height-controller">
                    <span className="prompt">
                        {height == null
                            ? 'Displaying most recent blocks'
                            : `Displaying blocks from height ${height}`}
                    </span>
                    <input
                        type="text"
                        placeholder="enter block number"
                        value={heightInInput}
                        onChange={(e) => setHeightInInput(e.target.value)}
                    />
                    <button onClick={goToHeightCallback}>Go</button>
                    <button onClick={() => setHeight(null)}>Show HEADER_HEAD</button>
                </div>
                <div className="explanation">Skipped chunks have grey background.</div>
                <div className="explanation">
                    Red text means that we don&apos;t know this producer (it&apos;s not present in
                    our announce account list).
                </div>
                {!!error && <div className="error">{(error as Error).stack}</div>}
                <div className="missed-blocks">
                    Missing blocks: {canonicalHeightCount - numCanonicalBlocks} {}
                    Produced: {numCanonicalBlocks} {}
                    Missing Rate:{' '}
                    {(
                        ((canonicalHeightCount - numCanonicalBlocks) / canonicalHeightCount) *
                        100
                    ).toFixed(2)}
                    %
                </div>
                <button
                    onClick={() => {
                        setExpandAll((value) => !value);
                        updateXarrow();
                    }}>
                    {expandAll ? "Don't expand all" : 'Expand all'}
                </button>
                <button
                    onClick={() => {
                        setHideMissingHeights((value) => !value);
                        updateXarrow();
                    }}>
                    {hideMissingHeights ? 'Show missing heights' : 'Hide missing heights'}
                </button>
                <button
                    onClick={() => {
                        setShowMissingChunksStats((value) => !value);
                        updateXarrow();
                    }}>
                    {showMissingChunksStats
                        ? 'Hide missing chunks stats'
                        : 'Show missing chunks stats'}
                </button>
                {showMissingChunksStats && (
                    <div className="missed-chunks">
                        {numChunksSkipped.map((numSkipped, shardId) => (
                            <div key={shardId}>
                                Shard {shardId}: Missing chunks: {numSkipped} {}
                                Produced: {numCanonicalBlocks - numSkipped} {}
                                Missing Rate: {((numSkipped / numCanonicalBlocks) * 100).toFixed(2)}
                                %
                            </div>
                        ))}
                    </div>
                )}
                {isLoading ? (
                    <div>Loading...</div>
                ) : (
                    <BlocksTable
                        rows={rows}
                        knownProducers={knownProducerSet}
                        expandAll={expandAll}
                        hideMissingHeights={hideMissingHeights}
                    />
                )}
            </div>
        </Xwrapper>
    );
};
