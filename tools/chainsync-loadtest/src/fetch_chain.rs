use std::sync::Arc;

use log::info;

use crate::concurrency::{Ctx, Scope};
use crate::network;
use anyhow::Context;
use tokio::time;

use near_primitives::hash::CryptoHash;

// run() fetches the chain (headers,blocks and chunks)
// starting with block having hash = <start_block_hash> and
// ending with the current tip of the chain (snapshotted once
// at the start of the routine, so that the amount of work
// is bounded).
pub async fn run(
    ctx: Ctx,
    network: Arc<network::Network>,
    start_block_hash: CryptoHash,
) -> anyhow::Result<()> {
    info!("SYNC start");
    let peers = network.info(&ctx).await?;
    let target_height = peers.highest_height_peers[0].chain_info.height as i64;
    info!("SYNC target_height = {}", target_height);
    Scope::run(&ctx, |ctx, s| async move {
        s.spawn_weak({
            let network = network.clone();
            |ctx| async move {
                let ctx = ctx.with_label("stats");
                loop {
                    info!("stats = {:?}", network.stats);
                    ctx.wait(time::Duration::from_secs(2)).await?;
                }
            }
        });

        let mut last_hash = start_block_hash;
        let mut last_height = 0;
        while last_height < target_height {
            // Fetch the next batch of headers.
            let mut headers = network.fetch_block_headers(&ctx, &last_hash).await?;
            headers.sort_by_key(|h| h.height());
            let last_header = headers.last().context("no headers")?;
            last_hash = last_header.hash().clone();
            last_height = last_header.height() as i64;
            info!(
                "SYNC last_height = {}, {} headers left",
                last_height,
                target_height - last_height
            );
            for h in headers {
                s.spawn({
                    let network = network.clone();
                    |ctx, s| async move {
                        let block = network.fetch_block(&ctx, h.hash()).await?;
                        for ch in block.chunks().iter() {
                            let ch = ch.clone();
                            let network = network.clone();
                            s.spawn(|ctx, _s| async move {
                                network.fetch_chunk(&ctx, &ch).await?;
                                anyhow::Ok(())
                            });
                        }
                        anyhow::Ok(())
                    }
                });
            }
        }
        anyhow::Ok(())
    })
    .await
}
