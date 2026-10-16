use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::HandlerResult;
use crate::setup::state::{NodeExecutionData, SpiceEndorsementDelayState};
use near_async::messaging::CanSend as _;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_client::spice::chunk_validator_actor::SpiceChunkStateWitnessMessage;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::recv_permit::RecvMessagePermit;
use near_network::types::NetworkRequests;
use near_network::types::NetworkResponses;
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ReceiptProof;
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::types::{AccountId, BlockHeight, SpiceChunkId};
use near_store::DBCol;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl TestLoopEnv {
    /// Set the endorsement propagation delay to `delay_height` blocks. Safe
    /// to call unconditionally; installs the network handler lazily on the
    /// first non-zero call, and re-running it after `add_node` /
    /// `restart_node` instruments any node that doesn't have the handler yet.
    pub fn delay_endorsements_propagation(&mut self, delay_height: u64) {
        let state = self.shared_state.spice_endorsement_delay.clone();
        if state.lock().installed_for.is_empty() && delay_height == 0 {
            return;
        }
        for node in &self.node_datas {
            node.set_expected_execution_delay(delay_height);
        }
        // Refresh routing so handlers installed earlier can still deliver
        // endorsements to nodes added since.
        {
            let mut state = state.lock();
            state.senders = self
                .node_datas
                .iter()
                .map(|n| (n.account_id.clone(), n.spice_core_writer_sender.clone()))
                .collect();
        }
        for node in &self.node_datas {
            if !state.lock().installed_for.insert(node.identifier.clone()) {
                continue;
            }
            install_endorsement_delay_handler(&mut self.test_loop.data, node, state.clone());
        }
    }

    pub fn deliver_spice_data(&mut self) {
        if !cfg!(feature = "protocol_feature_spice") {
            return;
        }
        let node_datas = self.node_datas.clone();
        let stores: Vec<_> = (0..node_datas.len()).map(|idx| self.node(idx).store()).collect();
        let mut seen_keys: HashSet<(usize, Box<[u8]>)> = HashSet::new();
        let mut delivered: HashSet<(usize, SpiceChunkId)> = HashSet::new();
        let mut seen_receipt_keys: HashSet<(usize, Box<[u8]>)> = HashSet::new();
        let clock = self.test_loop.clock();
        let mut last_scan = clock.now();
        self.test_loop.set_every_event_callback(move |test_loop_data| {
            // Scanning every store on every event is too slow; data travels at network delay anyway.
            if clock.now() < last_scan + Duration::milliseconds(20) {
                return;
            }
            last_scan = clock.now();
            for (producer, store) in stores.iter().enumerate() {
                for (key, value) in store.iter(DBCol::receipt_proofs()) {
                    if !seen_receipt_keys.insert((producer, key.clone())) {
                        continue;
                    }
                    let block_hash = CryptoHash::try_from(&key[..32]).unwrap();
                    let receipt_proof: ReceiptProof = borsh::from_slice(&value).unwrap();
                    let to_shard_id = receipt_proof.1.to_shard_id;
                    for (target, target_store) in stores.iter().enumerate() {
                        if target == producer || target_store.exists(DBCol::receipt_proofs(), &key) {
                            continue;
                        }
                        // Only nodes that apply the destination shard in the next block need it.
                        let shard_tracker = &test_loop_data
                            .get(&node_datas[target].client_sender.actor_handle())
                            .client
                            .shard_tracker;
                        if !shard_tracker.cares_about_shard(&block_hash, to_shard_id)
                            && !shard_tracker.will_care_about_shard(&block_hash, to_shard_id)
                        {
                            continue;
                        }
                        let mut store_update = target_store.store_update();
                        store_update.set(DBCol::receipt_proofs(), &key, &value);
                        store_update.commit();
                    }
                }
                for (key, value) in store.iter(DBCol::witnesses()) {
                    if !seen_keys.insert((producer, key)) {
                        continue;
                    }
                    let witness: SpiceChunkStateWitness = borsh::from_slice(&value).unwrap();
                    let chunk_id = witness.chunk_id().clone();
                    let client =
                        &test_loop_data.get(&node_datas[producer].client_sender.actor_handle()).client;
                    let header = client.chain.get_block_header(&chunk_id.block_hash).unwrap();
                    // Only the chunk's validators, as the distributor would: any other node with a
                    // signer would endorse too, and every core writer rejects it as a non-validator.
                    let assignments = client
                        .epoch_manager
                        .get_chunk_validator_assignments(
                            header.epoch_id(),
                            chunk_id.shard_id,
                            header.height(),
                        )
                        .unwrap();
                    for (target, target_data) in node_datas.iter().enumerate() {
                        if target == producer
                            || !assignments.contains(&target_data.account_id)
                            || !delivered.insert((target, chunk_id.clone()))
                        {
                            continue;
                        }
                        tracing::info!(target: "test", ?chunk_id, producer, target, "delivering witness");
                        target_data.spice_chunk_validator_sender.send(
                            SpiceChunkStateWitnessMessage { witness: witness.clone(), raw_witness_size: 0 }
                                .span_wrap(),
                        );
                    }
                }
            }
        });
    }
}

fn install_endorsement_delay_handler(
    data: &mut TestLoopData,
    node: &NodeExecutionData,
    state: Arc<Mutex<SpiceEndorsementDelayState>>,
) {
    let delay = node.expected_execution_delay_handle();
    let block_heights: Arc<RwLock<HashMap<CryptoHash, BlockHeight>>> = Default::default();
    let delayed_endorsements: Arc<
        RwLock<VecDeque<(CryptoHash, AccountId, SpiceChunkEndorsement)>>,
    > = Default::default();
    node.register_override_handler(data, move |request| {
        let delay_height = delay.load(Ordering::Relaxed);
        match request {
            NetworkRequests::Block { ref block } => {
                block_heights.write().insert(*block.hash(), block.header().height());

                let mut delayed_endorsements = delayed_endorsements.write();
                loop {
                    let Some(front) = delayed_endorsements.front() else {
                        break;
                    };
                    let Some(&height) = block_heights.read().get(&front.0) else {
                        // Endorsed block not observed on this handler yet; wait
                        // until it arrives before deciding if the delay has
                        // elapsed.
                        break;
                    };
                    if height + delay_height >= block.header().height() {
                        break;
                    }
                    let (_, target, endorsement) = delayed_endorsements.pop_front().unwrap();
                    let Some(sender) = state.lock().senders.get(&target).cloned() else {
                        continue;
                    };
                    sender
                        .send(SpiceChunkEndorsementMessage(endorsement, RecvMessagePermit::none()));
                }
                HandlerResult::Unhandled(request)
            }
            NetworkRequests::SpiceChunkEndorsement(target, endorsement) => {
                if delay_height == 0 {
                    return HandlerResult::Unhandled(NetworkRequests::SpiceChunkEndorsement(
                        target,
                        endorsement,
                    ));
                }
                delayed_endorsements.write().push_back((
                    *endorsement.block_hash(),
                    target,
                    endorsement,
                ));
                HandlerResult::Handled(NetworkResponses::NoResponse)
            }
            _ => HandlerResult::Unhandled(request),
        }
    });
}
