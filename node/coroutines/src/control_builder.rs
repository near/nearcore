//! Constructs control for TxFlow using the current Client state.
use client::Client;
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;
use txflow::txflow_task::{Control, State};

pub fn get_control(client: &Client, block_index: u64) -> Control<BeaconWitnessSelector> {
    let (owner_uid, uid_to_authority_map) =
        client.get_uid_to_authority_map(block_index);
    match owner_uid {
        None => Control::Stop,
        Some(owner_uid) => {
            let witness_selector = Box::new(BeaconWitnessSelector::new(
                uid_to_authority_map.keys().cloned().collect(),
                owner_uid,
            ));
            Control::Reset(State {
                owner_uid,
                starting_epoch: 0,
                gossip_size: 1, // TODO: Use adaptive gossip size.
                witness_selector,
            })
        }
    }
}
