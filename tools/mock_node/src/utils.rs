use near_jsonrpc::mock::MockHttpServer;
use near_primitives::types::BlockHeight;
use serde_json::Value;

pub fn json_block_header(block: &Value) -> anyhow::Result<&Value> {
    if let Some(values) = block.as_object() {
        match values.get("header") {
            Some(header) => Ok(header),
            None => Err(anyhow::anyhow!("get_latest_block() result has no header")),
        }
    } else {
        Err(anyhow::anyhow!("get_latest_block() result not an object"))
    }
}

pub fn json_block_height(block: &Value) -> anyhow::Result<BlockHeight> {
    match json_block_header(block)?.get("height") {
        Some(h) => {
            h.as_u64().ok_or(anyhow::anyhow!("get_latest_block() header has bad height: {}", h))
        }
        None => Err(anyhow::anyhow!("get_latest_block() header has no height")),
    }
}

pub async fn get_latest_height(server: &MockHttpServer) -> anyhow::Result<BlockHeight> {
    let b = match server.get_latest_block().await {
        Ok(b) => b,
        Err(e) => return Err(anyhow::anyhow!(format!("{:?}", e))),
    };
    json_block_height(&b)
}
