use crate::setup::env::TestLoopEnv;
use crate::setup::network_rules::Action;
use near_chain::ChainStoreAccess;
use near_network::types::NetworkRequests;
use near_store::adapter::StoreAdapter;
use std::sync::atomic::Ordering;

/// The tag of the hold rule `delay_endorsements_propagation` installs on each account.
pub const ENDORSEMENT_DELAY_RULE: &str = "endorsement_delay";

impl TestLoopEnv {
    /// Set the endorsement propagation delay to `delay_height` blocks: an endorsement of a
    /// block at height `h` is held until the node has seen a block above `h + delay_height`,
    /// which it shows by broadcasting one or by approving one above it. Safe to call
    /// unconditionally; installs a hold rule on each node's account lazily on the first
    /// non-zero call, which a restart of the node keeps, and re-running it after `add_node`
    /// covers any account with no rule yet. With zero, new endorsements pass straight through
    /// and the held ones drain on the node's next block or approval.
    pub fn delay_endorsements_propagation(&self, delay_height: u64) {
        for node in &self.node_datas {
            node.set_expected_execution_delay(delay_height);
        }
        let rules = &self.shared_state.network_rules;
        if delay_height == 0 && rules.all_tagged(ENDORSEMENT_DELAY_RULE).is_empty() {
            return;
        }
        for node in &self.node_datas {
            if !rules.tagged(&node.account_id, ENDORSEMENT_DELAY_RULE).is_empty() {
                continue;
            }
            let delay = node.expected_execution_delay_handle();
            let chain_store = self
                .test_loop
                .data
                .get(&node.client_sender.actor_handle())
                .client
                .chain
                .chain_store
                .store()
                .chain_store();
            let matches = {
                let delay = delay.clone();
                move |request: &NetworkRequests| {
                    delay.load(Ordering::Relaxed) > 0
                        && matches!(request, NetworkRequests::SpiceChunkEndorsement(..))
                }
            };
            // The endorsed block's height comes from the node's store, so an endorsement of a
            // block the node has not received yet stays held until it has.
            let release = move |held: &NetworkRequests, seen_height| {
                let NetworkRequests::SpiceChunkEndorsement(_, endorsement) = held else {
                    return true;
                };
                let Ok(header) = chain_store.get_block_header(endorsement.block_hash()) else {
                    return false;
                };
                header.height() + delay.load(Ordering::Relaxed) < seen_height
            };
            rules.add_tagged(
                node.account_id.clone(),
                ENDORSEMENT_DELAY_RULE,
                matches,
                Action::Hold(Box::new(release)),
            );
        }
    }

    /// How many endorsements the delay held back so far, over every node.
    pub fn delayed_endorsements(&self) -> u64 {
        let rules = &self.shared_state.network_rules;
        rules.all_tagged(ENDORSEMENT_DELAY_RULE).iter().map(|handle| handle.matched()).sum()
    }
}
