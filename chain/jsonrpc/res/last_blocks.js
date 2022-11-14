function ellipsify(str, maxLen) {
    if (str.length > maxLen) {
        return str.substring(0, maxLen - 3) + '...';
    }
    return str;
}

// Makes an element that when clicked, expands or ellipsifies the hash and creator.
function HashElement({ hashValue, creator, expandAll, knownProducers }) {
    let [expanded, setExpanded] = React.useState(false);
    let updateXarrow = reactXarrow.useXarrow();
    return <span
        className={`hash-element ${knownProducers.has(creator) ? '' : 'validator-unavailable'}`}
        onClick={() => {
            setExpanded((value) => !value);
            // xarrows need to be updated whenever graph dot positions may change.
            updateXarrow();
        }}>
        {expanded || expandAll
            ? `${hashValue} ${creator}`
            : `${ellipsify(hashValue, 8)} ${ellipsify(creator, 13)}`}
    </span>;
}

// Sorts the API response into easily displayable rows, and computes the graph layout.
//
// Inputs:
//   blocks: array of DebugBlockStatus
//   missedHeights: array of MissedHeightInfo
//   head: block hash of the chain's head
//   headerHead: block hash of the chain's header head
// Output: array of elements where each element is either {
//   block: DebugBlockStatus,
//   parentIndex: number?,  // the index of the parent block, or null if parent not included in the data
//   graphColumn: number,  // the column to display the graph node in
//   blockDelay: number?,  // number of seconds since parent's block timestamp, or null if parent not included in the data
//   chunkSkipped: boolean[],  // for each chunk, whether the chunk is the same as that chunk of parent block
//   isHead: boolean,
//   isHeaderHead: boolean,
// } or { missedHeight: MissedHeightInfo }
function sortBlocksAndDetermineBlockGraphLayout(blocks, missedHeights, head, headerHead) {
    const rows = [];
    for (let block of blocks) {
        rows.push({
            block,
            parentIndex: null,
            graphColumn: -1,
            blockDelay: null,
            chunkSkipped: block.chunks.map(() => false),
            isHead: head == block.block_hash,
            isHeaderHead: headerHead == block.block_hash,
        });
    }
    for (let missedHeight of missedHeights) {
        rows.push({ missedHeight });
    }

    function sortingKey(row) {
        if ('block' in row) {
            // some lousy tie-breaking for same-height rows.
            return row.block.block_height + (row.block.block_timestamp / 1e12 % 1);
        } else {
            return row.missedHeight.block_height;
        }
    }

    rows.sort((a, b) => sortingKey(b) - sortingKey(a));

    const rowIndexByHash = new Map();
    rows.forEach((row, rowIndex) => {
        if ('block' in row) {
            rowIndexByHash.set(row.block.block_hash, rowIndex);
        }
    });

    let highestNodeOnFirstColumn = rows.length;
    for (let i = rows.length - 1; i >= 0; i--) {
        let row = rows[i];
        if ('missedHeight' in row) {
            continue;
        }
        const block = row.block;

        // Look up parent index, and also compute things that depend on the parent block.
        if (rowIndexByHash.has(block.prev_block_hash)) {
            row.parentIndex = rowIndexByHash.get(block.prev_block_hash);
            const parent = rows[row.parentIndex];
            row.blockDelay = (block.block_timestamp - parent.block.block_timestamp) / 1e9;
            for (let j = 0;
                j < Math.min(block.chunks.length, parent.block.chunks.length);
                j++) {
                row.chunkSkipped[j] =
                    block.chunks[j].chunk_hash == parent.block.chunks[j].chunk_hash;
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
        if (row.parentIndex != null &&
            rows[row.parentIndex].graphColumn == 0 &&
            row.parentIndex > highestNodeOnFirstColumn) {
            column = 1;
        } else {
            highestNodeOnFirstColumn = i;
        }
        row.graphColumn = column;
    }
    return rows;
}

function BlocksTable({ rows, knownProducers, expandAll, hideMissingHeights }) {
    let numGraphColumns = 1;  // either 1 or 2; determines the width of leftmost td
    let numShards = 0;
    for (let row of rows) {
        if ('block' in row) {
            numGraphColumns = Math.max(numGraphColumns, row.graphColumn + 1);
            for (let chunk of row.block.chunks) {
                numShards = Math.max(numShards, chunk.shard_id + 1);
            }
        }
    }
    const header = <tr>
        <th>Chain</th>
        <th>Height</th>
        <th>{'Hash & creator'}</th>
        <th>Processing Time (ms)</th>
        <th>Block Delay (s)</th>
        <th>Gas price ratio</th>
        {[...Array(numShards).keys()].map(i =>
            <th key={i} colSpan="3">Shard {i} (hash/gas(Tgas)/time(ms))</th>)}
    </tr>;

    // One xarrow element per arrow (from block to block).
    const graphArrows = [];

    // One 'tr' element per row.
    const tableRows = [];
    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        if ('missedHeight' in row) {
            if (!hideMissingHeights) {
                tableRows.push(<tr key={row.missedHeight.block_height} className="missed-height">
                    <td className="graph-node-cell" />
                    <td>{row.missedHeight.block_height}</td>
                    <td colSpan={4 + numShards * 3}>{row.missedHeight.block_producer} missed block</td>
                </tr>);
            }
            continue;
        }
        let block = row.block;

        const chunkCells = [];
        block.chunks.forEach((chunk, shardId) => {
            chunkCells.push(<React.Fragment key={shardId}>
                <td className={row.chunkSkipped[shardId] ? 'skipped-chunk' : ''}>
                    <HashElement
                        hashValue={chunk.chunk_hash}
                        creator={chunk.chunk_producer}
                        expandAll={expandAll}
                        knownProducers={knownProducers} />
                </td>
                <td>{(chunk.gas_used / (1024 * 1024 * 1024 * 1024)).toFixed(1)}</td>
                <td>{chunk.processing_time_ms}</td>
            </React.Fragment>);
        });

        tableRows.push(
            <tr key={block.block_hash}
                className={`block-row ${row.block.is_on_canonical_chain ? '' : 'not-on-canonical-chain'}`}>
                <td className="graph-node-cell">
                    <div id={`graph-node-${i}`}
                        className={`graph-dot graph-dot-col-${row.graphColumn} graph-dot-total-${numGraphColumns}`}>
                    </div>
                </td>
                <td>
                    <span>{block.block_height}</span>
                    {row.isHead && <div className="head-label">HEAD</div>}
                    {row.isHeaderHead && <div className="header-head-label">HEADER HEAD</div>}
                </td>
                <td>
                    <HashElement
                        hashValue={block.block_hash}
                        creator={block.block_producer}
                        expandAll={expandAll}
                        knownProducers={knownProducers} />
                </td>
                <td>{block.processing_time_ms}</td>
                <td>{row.blockDelay ?? ''}</td>
                <td>{block.gas_price_ratio}</td>
                {block.full_block_missing && <td colSpan={numShards * 3}>header only</td>}
                {chunkCells}
            </tr>);
        if (row.parentIndex != null) {
            graphArrows.push(<reactXarrow.default
                key={i}
                start={`graph-node-${i}`}
                end={`graph-node-${row.parentIndex}`}
                color={row.block.is_on_canonical_chain ? 'black' : 'darkgray'}
                strokeWidth={row.block.is_on_canonical_chain ? 3 : 1}
                headSize="0"
                path="straight" />);
        }
    }
    return <div>
        {graphArrows}
        <table>
            <tbody>
                {header}
                {tableRows}
            </tbody>
        </table>
    </div>
}

function Page() {
    const [rows, setRows] = React.useState([]);
    const [error, setError] = React.useState(null);
    const [knownProducers, setKnownProducers] = React.useState(new Set());
    const [expandAll, setExpandAll] = React.useState(false);
    const [hideMissingHeights, setHideMissingHeights] = React.useState(false);
    const updateXarrow = reactXarrow.useXarrow();
    let blockStatusApiPath = '../api/block_status';
    const url = new URL(window.location.toString());
    let title = 'Most Recent Blocks';
    if (url.searchParams.has('height')) {
        blockStatusApiPath += '/' + url.searchParams.get('height');
        title = 'Blocks from ' + url.searchParams.get('height');
    }
    // useEffect with empty dependency list means to run this once at beginning.
    React.useEffect(() => {
        (async () => {
            try {
                let resp = await fetch('../api/status');
                if (resp.status == 405) {
                    throw new Error('Debug not allowed - did you set enable_debug_rpc: true in your config?');
                } else if (!resp.ok) {
                    throw new Error('Debug API call failed: ' + resp.statusText);
                }
                const { detailed_debug_status: { network_info: { known_producers } } } = await resp.json();
                const knownProducerSet = new Set();
                for (const producer of known_producers) {
                    knownProducerSet.add(producer.account_id);
                }
                setKnownProducers(knownProducerSet);

                resp = await fetch(blockStatusApiPath);
                if (!resp.ok) {
                    throw new Error('Could not fetch block debug status: ' + resp.statusText);
                }
                const { status_response: { BlockStatus: data } } = await resp.json();
                setRows(sortBlocksAndDetermineBlockGraphLayout(
                    data.blocks,
                    data.missed_heights,
                    data.head,
                    data.header_head));
            } catch (error) {
                setError(error);
            }
        })();
    }, []);

    // Compute missing blocks and chunks statistics (whenever rows changes).
    const { numCanonicalBlocks, canonicalHeightCount, numChunksSkipped } = React.useMemo(() => {
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
            if (firstCanonicalHeight == 0) {
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

    return <reactXarrow.Xwrapper>
        <h1>{title}</h1>
        <div className="explanation">Skipped chunks have grey background.</div>
        <div className="explanation">
            Red text means that we don't know this producer
            (it's not present in our announce account list).
        </div>
        {error && <div className="error">{error.stack}</div>}
        <div className="missed-blocks">
            Missing blocks: {canonicalHeightCount - numCanonicalBlocks} { }
            Produced: {numCanonicalBlocks} { }
            Missing Rate: {((canonicalHeightCount - numCanonicalBlocks) / canonicalHeightCount * 100).toFixed(2)}%
        </div>
        <div className="missed-chunks">
            {numChunksSkipped.map((numSkipped, shardId) =>
                <div key={shardId}>
                    Shard {shardId}: Missing chunks: {numSkipped} { }
                    Produced: {numCanonicalBlocks - numSkipped} { }
                    Missing Rate: {(numSkipped / numCanonicalBlocks * 100).toFixed(2)}%
                </div>)}
        </div>
        <button onClick={() => {
            setExpandAll(value => !value);
            updateXarrow();
        }}>
            {expandAll ? "Don't expand all" : 'Expand all'}
        </button>
        <button onClick={() => {
            setHideMissingHeights(value => !value);
            updateXarrow();
        }}>
            {hideMissingHeights ? 'Show missing heights' : 'Hide missing heights'}
        </button>
        <BlocksTable
            rows={rows}
            knownProducers={knownProducers}
            expandAll={expandAll}
            hideMissingHeights={hideMissingHeights} />

    </reactXarrow.Xwrapper>;
}

ReactDOM
    .createRoot(document.getElementById('react-container'))
    .render(<Page />);
