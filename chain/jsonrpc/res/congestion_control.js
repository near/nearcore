function sortBlocks(blocks) {
    function sortingKey(row) {
        return row.block_height + (row.block_timestamp / 1e12 % 1);
    }
    blocks.sort((a, b) => sortingKey(b) - sortingKey(a));
    return blocks;
}

function toTgas(gas) {
    if (!gas) {
        return "N/A"
    }
    return (gas / (1024 * 1024 * 1024 * 1024)).toFixed(2)
}

function toMiB(bytes) {
    if (!bytes) {
        return "N/A"
    }
    return (bytes / (1024 * 1024)).toFixed(2)
}

function toShardIndex(shard) {
    if (!shard) {
        return "N/A"
    }
    return shard
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
            <th key={i} colSpan="4">Shard {i} (delayed(Tgas)/buffered(Tgas)/receipt(MiB)/allowed(shards))</th>)}
    </tr>;

    // One 'tr' element per row.
    const tableRows = [];
    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];

        const chunkCells = [];
        row.chunks.forEach((chunk, shardId) => {
            chunkCells.push(<React.Fragment key={shardId}>
                <td>{toTgas(chunk.congestion_info.delayed_receipts_gas)}</td>
                <td>{toTgas(chunk.congestion_info.buffered_receipts_gas)}</td>
                <td>{toMiB(chunk.congestion_info.receipt_bytes)}</td>
                <td>{toShardIndex(chunk.congestion_info.allowed_shard)}</td>
            </React.Fragment>);
        });

        tableRows.push(
            <tr key={row.block_hash}>
                <td>
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
