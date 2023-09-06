mod rpc_requests;
mod cli;
mod metrics;
mod helpers;
use near_primitives::types::{EpochId, StateRoot};
use near_primitives::hash::CryptoHash;
use std::str::FromStr;
use crate::cli::StatePartsDumpCheckStatus;
use crate::helpers::{get_processing_epoch_information, run_dump_check};

// the script can query near rpc endpoint for latest epoch_height
// if the latest epoch_height > the last time we polled for epoch_height, and epoch_dump_check_progress = Done, then it means
// the check for last epoch was done, and we should check for last epoch_height + 1.
fn main() {

    let rpc_client = crate::rpc_requests::RpcClient::new("mainnet");
    
    let mut last_check_status = Ok(StatePartsDumpCheckStatus::WaitingForParts{epoch_height: 0});

    let mut last_check_iter_info;
    while true {
        let dump_check_iter_info_res = get_processing_epoch_information(&rpc_client);
        if let Err(_) = dump_check_iter_info_res {
            break;
        }
        let dump_check_iter_info = dump_check_iter_info_res.unwrap();
        
        if let Ok(StatePartsDumpCheckStatus::Done{epoch_height: epoch_height}) = last_check_status && epoch_height >= dump_check_iter_info.prev_epoch_height {
            // TODO: sleep 5 mins
            break;
        }

        last_check_iter_info = dump_check_iter_info;
        let check_result = run_dump_check(&rpc_client, &dump_check_iter_info).await;
        last_check_status = check_result;
    }



}