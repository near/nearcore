use beacon::authority::{Authority, SelectedAuthority};
use beacon::types::SignedBeaconBlock;
use chain::{SignedBlock, SignedHeader};
use futures::sync::mpsc::{Receiver, Sender};
use futures::{Future, Sink, Stream};
use primitives::types::{AccountId, UID};
use std::collections::HashMap;
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;
use txflow::txflow_task::{Control, State};

pub fn spawn_authority_task(
    mut authority_handler: AuthorityHandler,
    new_block_rx: Receiver<SignedBeaconBlock>,
    authority_tx: Sender<HashMap<UID, SelectedAuthority>>,
    control_tx: Sender<Control<BeaconWitnessSelector>>,
) {
    let task = new_block_rx
        .map(move |block| {
            let index = block.header().index();
            authority_handler.authority.process_block_header(&block.header());
            // get authorities for the next block
            let next_authorities =
                authority_handler.authority.get_authorities(index + 1).unwrap_or_else(|_| {
                    panic!("failed to get authorities for block index {}", index + 1)
                });

            let mut uid_to_authority_map = HashMap::new();
            let mut owner_uid = None;
            for (index, authority) in next_authorities.into_iter().enumerate() {
                if authority.account_id == authority_handler.account_id {
                    owner_uid = Some(index as UID);
                }
                uid_to_authority_map.insert(index as UID, authority);
            }

            if let Some(owner_uid) = owner_uid {
                if !authority_handler.started {
                    authority_handler.started = true;
                    let witness_selector = Box::new(BeaconWitnessSelector::new(
                        uid_to_authority_map.keys().cloned().collect(),
                        owner_uid,
                    ));
                    let control = Control::Reset(State {
                        owner_uid,
                        starting_epoch: 0,
                        gossip_size: 1, // TODO: Use adaptive gossip size.
                        witness_selector,
                    });
                    let start_task = control_tx
                        .clone()
                        .send(control)
                        .map(|_| ())
                        .map_err(|err| error!("Error sending control to TxFlow {}", err));
                    tokio::spawn(start_task);
                }
            } else if authority_handler.started {
                authority_handler.started = false;
                let stop_task = control_tx
                    .clone()
                    .send(Control::Stop)
                    .map(|_| ())
                    .map_err(|err| error!("Error sending control to TxFlow {}", err));
                tokio::spawn(stop_task);
            }
            uid_to_authority_map
        })
        .forward(
            authority_tx.sink_map_err(|err| error!("Error sending payload down the sink: {}", err)),
        )
        .map(|_| ())
        .map_err(|_| error!("Error handling authority info from a block."));
    tokio::spawn(task);
}
pub struct AuthorityHandler {
    authority: Authority,
    account_id: AccountId,
    /// whether the node has started consensus
    started: bool,
}

impl AuthorityHandler {
    pub fn new(authority: Authority, account_id: AccountId) -> Self {
        AuthorityHandler { authority, account_id, started: false }
    }
}
