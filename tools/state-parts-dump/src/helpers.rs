mod rpc_requests;
mod cli;
mod metrics;
use near_primitives::types::{EpochId, StateRoot};
use near_primitives::hash::CryptoHash;
use std::str::FromStr;
use anyhow::anyhow;

pub struct DumpCheckIterInfo {
    pub latest_block_hash: String,
    pub latest_epoch_id: String,
    pub prev_epoch_id: String,
    pub prev_epoch_height: u64
}

pub(crate) fn get_processing_epoch_information(rpc_client: &crate::rpc_requests::RpcClient) -> anyhow::Result<DumpCheckIterInfo> {
    let latest_block_hash = rpc_client.get_latest_block_hash().or_else(anyhow!("get_latest_block_hash failed"))?;
    let latest_epoch_id = rpc_client.get_epoch_id(&latest_block_hash).or_else(anyhow!("get_epoch_id failed"))?;
    
    let prev_epoch_id_str = rpc_client.get_prev_epoch_id(&latest_epoch_id).or_else(anyhow!("get_prev_epoch_id failed"))?;
    let prev_epoch_height = rpc_client.get_epoch_height(&latest_epoch_id).or_else(anyhow!("get_epoch_height failed"))? - 1;
    
    Ok(DumpCheckIterInfo{
        latest_block_hash: latest_block_hash,
        latest_epoch_id: latest_epoch_id,
        prev_epoch_id: prev_epoch_id_str,
        prev_epoch_height: prev_epoch_height
    })
}

// TODO: run all 4 shards in parallel
pub(crate) async fn run_dump_check(
    rpc_client: &crate::rpc_requests::RpcClient,
    dump_check_iter_info: &DumpCheckIterInfo,
) -> anyhow::Result<crate::cli::StatePartsDumpCheckStatus> {
    println!( "start");
    let state_root_str = rpc_client.get_prev_epoch_state_root(&dump_check_iter_info.prev_epoch_id).or_else(anyhow!("get_prev_epoch_state_root failed"))?;

    let state_root: StateRoot = CryptoHash::from_str(&state_root_str)?;
    let prev_epoch_id = EpochId(CryptoHash::from_str(&dump_check_iter_info.prev_epoch_id)?);

    println!("latest_epoch_id: {}, latest_block_hash: {}, prev_epoch_id_str: {}, prev_epoch_height: {}, state_root_str: {}", dump_check_iter_info.latest_epoch_id, dump_check_iter_info.latest_block_hash, dump_check_iter_info.prev_epoch_id, dump_check_iter_info.prev_epoch_height, state_root_str);
    
    let dump_check = crate::cli::StatePartsDumpCheck::new(
        "mainnet".to_string(),
        prev_epoch_id,
        dump_check_iter_info.prev_epoch_height,
        3,
        state_root,
        None,
        None,
        None,
        Some("state-parts".to_string())
    );

    // let sys = actix::System::new();
    // sys.block_on(async move {
    //     dump_check.run().await;
    // });
    // sys.run().unwrap();
    dump_check.run().await
}