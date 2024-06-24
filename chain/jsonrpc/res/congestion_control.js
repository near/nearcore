function sortBlocks(blocks) {
    function sortingKey(row) {
        // Note: using the expression `row.block_timestamp / 1e12 % 1` reduces the timestamp
        // to a number between 0 and 1, making block_timestamp strictly weaker than block_height
        // in this comparison.
        return row.block_height + (row.block_timestamp / 1e12 % 1);
    }
    blocks.sort((a, b) => sortingKey(b) - sortingKey(a));
    return blocks;
}

function toTgas(gas) {
    return (gas / (1024 * 1024 * 1024 * 1024)).toFixed(2)
}

function toMiB(bytes) {
    return (bytes / (1024 * 1024)).toFixed(2)
}

function toCongestionLevelCell(level) {
    if (level == null) {
        return <td className="not_available">N/A</td>
    }
    return <td>{level.toFixed(2)}</td> 
}

function BlocksTable({ rows }) {
    let numShards = 0;
    for (let row of rows) {
            for (let chunk of row.chunks) {
                numShards = Math.max(numShards, chunk.shard_id + 1);
            }
    }
    const header = <tr>
        <th>Height</th>
        {[...Array(numShards).keys()].map(i =>
            <th key={i} colSpan="5">Shard {i} (congestion_level/delayed(Tgas)/buffered(Tgas)/receipt(MiB)/allowed(shard))</th>)}
    </tr>;

    // One 'tr' element per row.
    const tableRows = [];
    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];

        const chunkCells = [];
        row.chunks.forEach((chunk, shardId) => {
            if (chunk.congestion_info) {
                const info = chunk.congestion_info.V1;
                chunkCells.push(<React.Fragment key={shardId}>
                    {toCongestionLevelCell(chunk.congestion_level)}
                    <td>{toTgas(info.delayed_receipts_gas)}</td>
                    <td>{toTgas(info.buffered_receipts_gas)}</td>
                    <td>{toMiB(info.receipt_bytes)}</td>
                    <td>{info.allowed_shard}</td>
                </React.Fragment>);
            } else {
                chunkCells.push(<React.Fragment key={shardId}>
                    {toCongestionLevelCell(chunk.congestion_level)}
                    <td className="not_available">N/A</td>
                    <td className="not_available">N/A</td>
                    <td className="not_available">N/A</td>
                    <td className="not_available">N/A</td>
                </React.Fragment>);
            }
        });

        tableRows.push(
            <tr key={row.block_hash}>
                <td className="block_height">
                    <span>{row.block_height}</span>
                </td>
                {chunkCells}
            </tr>);
    }
    return <div>
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
    let blockStatusApiPath = '../api/block_status';
    const url = new URL(window.location.toString());
    let title = 'Congestion control';
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

                resp = await fetch(blockStatusApiPath);
                if (!resp.ok) {
                    throw new Error('Could not fetch block debug status: ' + resp.statusText);
                }
                const { status_response: { BlockStatus: data } } = await resp.json();
                setRows(sortBlocks(data.blocks));
            } catch (error) {
                setError(error);
            }
        })();
    }, []);

    return <div>
        <h1>{title}</h1>
        <div className="explanation">
            <b>delayed</b>: sum of gas in currently delayed receipts<br />
            <b>buffered</b>: sum of gas in currently buffered receipts<br />
            <b>receipt</b>: size of borsh serialized receipts stored in state because they were delayed, buffered, postponed, or yielded<br />
            <b>allowed</b>: if fully congested, only this shard can forward receipts<br />
        </div>
        {error && <div className="error">{error.stack}</div>}
        <h2>Blocks</h2>
        <BlocksTable
            rows={rows} />
    </div>;
}

ReactDOM
    .createRoot(document.getElementById('react-container'))
    .render(<Page />);
