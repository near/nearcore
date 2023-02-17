use near_network::concurrency::ctx::Ctx;
use near_network::concurrency::scope;
use near_network::time;
use crate::network;
use anyhow::Context;
use log::info;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use near_primitives::hash::CryptoHash;

// run() fetches the chain (headers,blocks and chunks)
// starting with block having hash = <start_block_hash> and
// ending with the current tip of the chain (snapshotted once
// at the start of the routine, so that the amount of work
// is bounded).
pub async fn run(
    ctx: Ctx,
    network: &Arc<network::Network>,
    start_block_hash: CryptoHash,
    block_limit: u64,
) -> anyhow::Result<()> {
    info!("SYNC start");
    let peers = network.info(&ctx).await?;
    let target_height = peers.highest_height_peers[0].highest_block_height as i64;
    info!("SYNC target_height = {}", target_height);

    let start_time = ctx.clock().now();
    let res = scope::run!(&ctx, |s||ctx| async move {
        let service = s.new_service();
        service.spawn(|ctx:Ctx| async move {
            loop {
                info!("stats = {:?}", network.stats);
                ctx.wait(ctx.clock().sleep(time::Duration::seconds(2))).await?;
            }
        }).unwrap();

        let mut last_hash = start_block_hash;
        let mut last_height = 0;
        let mut blocks_count = 0;
        while last_height < target_height {
            // Fetch the next batch of headers.
            let mut headers = network.fetch_block_headers(&ctx, &last_hash).await?;
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
                s.spawn(|ctx| async move {
                    let block = network.fetch_block(&ctx, h.hash()).await?;
                    for ch in block.chunks().iter() {
                        let ch = ch.clone();
                        s.spawn(|ctx| async move {
                            network.fetch_chunk(&ctx, &ch).await?;
                            anyhow::Ok(())
                        });
                    }
                    anyhow::Ok(())
                });
            }
        }
        anyhow::Ok(())
    });
    let stop_time = ctx.clock().now();
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
