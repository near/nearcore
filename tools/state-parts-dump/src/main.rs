mod rpc_requests;
mod cli;
use near_primitives::types::{EpochId, StateRoot};
use near_primitives::hash::CryptoHash;
use std::str::FromStr;

fn main() {
    println!( "start");
    let rpc_client = crate::rpc_requests::RpcClient::new("mainnet");
    let latest_block_hash = rpc_client.get_latest_block_hash().unwrap();
    let latest_epoch_id = rpc_client.get_epoch_id(&latest_block_hash).unwrap();

    let epoch_id = EpochId(CryptoHash::from_str(&latest_epoch_id).unwrap());
    
    let prev_epoch_id_str = rpc_client.get_prev_epoch_id(&latest_epoch_id).unwrap();
    let prev_epoch_height = rpc_client.get_epoch_height(&latest_epoch_id).unwrap() - 1;
    let state_root_str = rpc_client.get_prev_epoch_state_root(&prev_epoch_id_str).unwrap();
    // //TODO: convert state_root from a String to StateRoot (CryptoHash)
    let state_root: StateRoot = CryptoHash::from_str(&state_root_str).unwrap();
    let prev_epoch_id = EpochId(CryptoHash::from_str(&prev_epoch_id_str).unwrap());

    println!("latest_epoch_id: {}, latest_block_hash: {}, prev_epoch_id_str: {}, prev_epoch_height: {}, state_root_str: {}", latest_epoch_id, latest_block_hash, prev_epoch_id_str, prev_epoch_height, state_root_str);
    
    let dump_check = crate::cli::StatePartsDumpCheck::new(
        "mainnet".to_string(),
        prev_epoch_id,
        prev_epoch_height,
        3,
        state_root,
        None,
        None,
        None,
        Some("state-parts".to_string())
    );

    
    let sys = actix::System::new();
    sys.block_on(async move {
        dump_check.run().await;
    });
    sys.run().unwrap();


}