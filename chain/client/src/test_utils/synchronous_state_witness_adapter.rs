use std::sync::{Arc, Mutex};

use near_async::messaging::CanSend;

use crate::stateless_validation::state_witness_actions::StateWitnessActions;
use crate::stateless_validation::state_witness_actor::DistributeStateWitnessRequest;

pub struct SynchronousStateWitnessAdapter {
    actions: Arc<Mutex<StateWitnessActions>>,
}

impl SynchronousStateWitnessAdapter {
    pub fn new(actions: StateWitnessActions) -> Self {
        Self { actions: Arc::new(Mutex::new(actions)) }
    }
}

impl CanSend<DistributeStateWitnessRequest> for SynchronousStateWitnessAdapter {
    fn send(&self, msg: DistributeStateWitnessRequest) {
        let mut actions = self.actions.lock().unwrap();
        actions.handle_distribute_state_witness_request(msg).unwrap();
    }
}
