use crate::network;
use anyhow::Context;
use log::info;
use near_async::time;
use near_network::concurrency::ctx;
use near_network::concurrency::scope;
use near_primitives::hash::CryptoHash;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// run() fetches the chain (headers,blocks and chunks)
// starting with block having hash = <start_block_hash> and
// ending with the current tip of the chain (snapshotted once
// at the start of the routine, so that the amount of work
// is bounded).
pub async fn run(
    network: &Arc<network::Network>,
    start_block_hash: CryptoHash,
    block_limit: u64,
) -> anyhow::Result<()> {
    info!("SYNC start");
    let peers = network.info().await?;
    let target_height = peers.highest_height_peers[0].highest_block_height as i64;
    info!("SYNC target_height = {}", target_height);

    let start_time = ctx::time::now();
    let res = scope::run!(|s| async move {
        s.spawn_bg::<()>(async {
            loop {
                info!("stats = {:?}", network.stats);
                ctx::time::sleep(time::Duration::seconds(2)).await?;
            }
        });
        let mut last_hash = start_block_hash;
        let mut last_height = 0;
        let mut blocks_count = 0;
        while last_height < target_height {
            // Fetch the next batch of headers.
            let mut headers = network.fetch_block_headers(last_hash).await?;
            headers.sort_by_key(|h| h.height());
            let last_header = headers.last().context("no headers")?;
            last_hash = *last_header.hash();
            last_height = last_header.height() as i64;
            info!(
                "SYNC last_height = {}, {} headers left",
                last_height,
                target_height - last_height
            );
            for h in headers {
                blocks_count += 1;
                if blocks_count == block_limit {
                    return anyhow::Ok(());
                }
                let h = *h.hash();
                s.spawn(async move {
                    let block = network.fetch_block(h).await?;
                    for ch in block.chunks().iter() {
                        s.spawn(network.fetch_chunk(ch.clone()));
                    }
                    anyhow::Ok(())
                });
            }
        }
        anyhow::Ok(())
    });
    let stop_time = ctx::time::now();
    let total_time = stop_time - start_time;
    let t = total_time.as_seconds_f64();
    let sent = network.stats.msgs_sent.load(Ordering::Relaxed);
    let headers = network.stats.header_done.load(Ordering::Relaxed);
    let blocks = network.stats.block_done.load(Ordering::Relaxed);
    let chunks = network.stats.chunk_done.load(Ordering::Relaxed);
    info!("running time: {:.2}s", t);
    info!("average QPS: {:.2}", (sent as f64) / t);
    info!("fetched {} header batches ({:.2} per second)", headers, headers as f64 / t);
    info!("fetched {} blocks ({:.2} per second)", blocks, blocks as f64 / t);
    info!("fetched {} chunks ({:.2} per second)", chunks, chunks as f64 / t);
    return res;
}
