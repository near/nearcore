use beacon::authority::{Authority, AuthorityStake};
use beacon::types::SignedBeaconBlock;
use chain::{SignedBlock, SignedHeader};
use futures::sync::mpsc::{Receiver, Sender};
use futures::{Future, Sink, Stream};
use primitives::types::{AccountId, UID};
use std::collections::HashMap;
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;
use txflow::txflow_task::{Control, State};

/// Extracts control and uids that should be sent down control and authority channels.
fn authorities_to_control(
    account_id: AccountId,
    authorities: Vec<AuthorityStake>,
    block_index: u64,
) -> (Control<BeaconWitnessSelector>, HashMap<UID, AuthorityStake>) {
    let mut uid_to_authority_map = HashMap::new();
    let mut owner_uid = None;
    for (index, authority) in authorities.into_iter().enumerate() {
        if authority.account_id == account_id {
            owner_uid = Some(index as UID);
        }
        uid_to_authority_map.insert(index as UID, authority);
    }

    let control = if let Some(owner_uid) = owner_uid {
        let witness_selector = Box::new(BeaconWitnessSelector::new(
            uid_to_authority_map.keys().cloned().collect(),
            owner_uid,
        ));
        Control::Reset(State {
            owner_uid,
            starting_epoch: 0,
            gossip_size: 1, // TODO: Use adaptive gossip size.
            witness_selector,
            block_index: block_index,
        })
    } else {
        Control::Stop
    };
    (control, uid_to_authority_map)
}

pub fn spawn_authority_task(
    mut authority_handler: AuthorityHandler,
    new_block_rx: Receiver<SignedBeaconBlock>,
    authority_tx: Sender<HashMap<UID, AuthorityStake>>,
    control_tx: Sender<Control<BeaconWitnessSelector>>,
) {
    let (kickoff_control, kickoff_uid_map) = authorities_to_control(
        authority_handler.account_id,
        // TODO: Use the most recent slot, instead.
        authority_handler.authority.get_authorities(1).unwrap(),
        1,
    );
    let control_tx1 = control_tx.clone();
    let authority_tx1 = authority_tx.clone();
    let kickoff_task = futures::done(Ok(())).then(|_: Result<(), ()>| {
        let task = control_tx1.send(kickoff_control)
            .map(|_| ())
            .map_err(|err| error!("Error sending kickoff control to TxFlow {}", err));
        tokio::spawn(task);
        println!("Sending kickoff authorities: {:?}", kickoff_uid_map);
        authority_tx1.send(kickoff_uid_map).map(|_| println!("kickoff authorities sent"))
            .map_err(|err| error!("Error sending kickoff authority {}", err))
    });
    let listener_task = new_block_rx
        .map(move |block| {
            let index = block.header().index();
            authority_handler.authority.process_block_header(&block.header());
            // get authorities for the next block
            let next_authorities =
                authority_handler.authority.get_authorities(index + 1).unwrap_or_else(|_| {
                    panic!("failed to get authorities for block index {}", index + 1)
                });
            let (control, uid_to_authority_map) =
                authorities_to_control(authority_handler.account_id, next_authorities, index + 1);
            println!("Computed control: {:?}; started: {}", control, authority_handler.started);
            match control {
                c @ Control::Reset(_) => {
                    let start_task = control_tx
                        .clone()
                        .send(c)
                        .map(|_| println!("Control sent"))
                        .map_err(|err| error!("Error sending control to TxFlow {}", err));
                    tokio::spawn(start_task);
                }
                c @ Control::Stop => {
                    if authority_handler.started {
                        authority_handler.started = false;
                        let start_task = control_tx
                            .clone()
                            .send(c)
                            .map(|_| println!("Control sent"))
                            .map_err(|err| error!("Error sending control to TxFlow {}", err));
                        tokio::spawn(start_task);
                    }
                }
            }
            uid_to_authority_map
        })
        .forward(
            authority_tx.sink_map_err(|err| error!("Error sending payload down the sink: {}", err)),
        )
        .map(|_| ())
        .map_err(|_| error!("Error handling authority info from a block."));
    tokio::spawn(kickoff_task.then(|_| listener_task));
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
