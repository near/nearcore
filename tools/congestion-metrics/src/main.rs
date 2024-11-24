use near_jsonrpc::primitives::types::chunks::ChunkReference;
use near_jsonrpc::primitives::types::congestion::RpcCongestionLevelRequest;
use near_jsonrpc_client::new_client;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use near_primitives::types::{BlockId, BlockReference};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create("data.csv").await?;
    file.write_all(b"block_height,shard_id,congestion_level\n").await?;

    let client = new_client("https://archival-rpc.mainnet.near.org");
    let start_height: u64 = 133453990;
    let end_height: u64 = 133454000;
    let mut current_height = end_height;

    while current_height >= start_height {
        let block =
            client.block(BlockReference::BlockId(BlockId::Height(current_height))).await.unwrap();
        for chunk in block.chunks {
            let congestion_level = client
                .EXPERIMENTAL_congestion_level(RpcCongestionLevelRequest {
                    chunk_reference: ChunkReference::BlockShardId {
                        block_id: BlockId::Height(current_height),
                        shard_id: chunk.shard_id,
                    },
                })
                .await
                .unwrap();
            file.write_all(
                format!(
                    "{},{},{}\n",
                    current_height, chunk.shard_id, congestion_level.congestion_level
                )
                .as_bytes(),
            )
            .await?;
        }
        current_height = block.header.prev_height.unwrap();
    }

    file.flush().await?;
    Ok(())
}
