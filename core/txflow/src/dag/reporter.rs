use primitives::types::*;

/// This structure is used to keep track of all violations detected on the network.
/// Not necessarily restricted to violations regarding TxFlow protocol,
/// but any kind of violation as enumerated in ViolationType
pub trait MisbehaviourReporter {
    fn new() -> Self;
    fn report(&mut self, violation: ViolationType);
}

pub struct DAGMisbehaviourReporter {
    pub violations: Vec<ViolationType>,
}

impl MisbehaviourReporter for DAGMisbehaviourReporter{
    fn new() -> Self {
        DAGMisbehaviourReporter { violations: vec![] }
    }

    /// Take ownership of the violation
    fn report(&mut self, violation: ViolationType) {
        self.violations.push(violation);
    }
}


/// MisbehaviourReporter that ignore all information stored
pub struct NoopMisbehaviourReporter{
}

impl MisbehaviourReporter for NoopMisbehaviourReporter{
    fn new() -> Self{
        Self {}
    }

    fn report(&mut self, violation: ViolationType) {

    }
}

/// TODO: Enumerate all violations and implement evidence to check that the
/// misbehaviour really took place.
#[derive(Debug)]
pub enum ViolationType {
    BadEpoch {
        message: TxFlowHash,
    },

    InvalidSignature,

    ForkAttempt {
        message_0: TxFlowHash,
        message_1: TxFlowHash,
    },
}
