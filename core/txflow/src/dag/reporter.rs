use primitives::types::*;

/// This structure is used to keep track of all violations detected on the network.
/// Not necessarily restricted to violations regarding TxFlow protocol,
/// but any kind of violation as enumerated in ViolationType
pub trait MisbehaviorReporter {
    fn new() -> Self;
    fn report(&mut self, violation: ViolationType);
}

pub struct DAGMisbehaviorReporter {
    pub violations: Vec<ViolationType>,
}

impl MisbehaviorReporter for DAGMisbehaviorReporter{
    fn new() -> Self {
        DAGMisbehaviorReporter { violations: vec![] }
    }

    /// Take ownership of the violation
    fn report(&mut self, violation: ViolationType) {
        self.violations.push(violation);
    }
}


/// MisbehaviorReporter that ignore all information stored
pub struct NoopMisbehaviorReporter{
}

impl MisbehaviorReporter for NoopMisbehaviorReporter{
    fn new() -> Self{
        Self {}
    }

    fn report(&mut self, _violation: ViolationType) {
    }
}

/// TODO: Enumerate all violations and implement evidence to check that the
/// misbehavior really took place.
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
