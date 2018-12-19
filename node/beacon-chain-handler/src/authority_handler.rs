use std::collections::HashMap;
use beacon::authority::{Authority, SelectedAuthority};
use beacon::types::SignedBeaconBlock;
use primitives::types::{AccountId, UID};
use chain::{SignedBlock, SignedHeader};
use futures::{Stream, Sink, Future};
use futures::sync::mpsc::{Receiver, Sender};

pub fn spawn_authority_task(
    mut authority_handler: AuthorityHandler,
    new_block_rx: Receiver<SignedBeaconBlock>,
    authority_tx: Sender<HashMap<UID, SelectedAuthority>>,
) {
    tokio::spawn(
        new_block_rx.for_each(move |block| {
            let index = block.header().index();
            authority_handler.authority.process_block_header(&block.header());
            // get authorities for the next block
            let next_authorities = authority_handler.authority
                .get_authorities(index + 1)
                .unwrap_or_else(|_| panic!("failed to get authorities for block index {}", index + 1));

            let mut uid_to_authority_map = HashMap::new();
            let mut found_match = false;
            for (index, authority) in next_authorities.into_iter().enumerate() {
                if authority.account_id == authority_handler.account_id {
                    found_match = true;
                }
                uid_to_authority_map.insert(index as u64, authority);
            }
            if found_match && !authority_handler.started {
                authority_handler.started = true;
                // TODO: send start signal to consensus
            }
            else if !found_match && authority_handler.started {
                authority_handler.started = false;
                // TODO: send stop signal to consensus
            }
            let authority_map_tx = authority_tx.clone();
            tokio::spawn(
                authority_map_tx
                    .send(uid_to_authority_map)
                    .map(|_| ())
                    .map_err(|e| error!("Error sending authority map: {:?}", e))
            );
            Ok(())
        })
    );
}
pub struct AuthorityHandler {
    authority: Authority,
    account_id: AccountId,
    /// whether the node has started consensus
    started: bool
}

impl AuthorityHandler {
    pub fn new(authority: Authority, account_id: AccountId) -> Self {
        AuthorityHandler {
            authority,
            account_id,
            started: false,
        }
    }
}