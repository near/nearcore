use primitives::types::*;

pub struct MisbehaviourReporter {
    violations: Vec<(UID, ViolationType)>,
}

impl MisbehaviourReporter {
    pub fn new() -> MisbehaviourReporter {
        MisbehaviourReporter { violations: vec![] }
    }

    /// Take ownership of uid and violation
    /// uid: participant the made the report
    fn report(&mut self, uid: UID, violation: ViolationType) {
        self.violations.push((uid, violation));
    }
}

/// TODO: Enumerate all violations and implement evidence to check that the
/// misbehaviour really took place.
pub enum ViolationType {
    BadEpoch {
        framed_participant: UID,
        message: StructHash,
        claimed_epoch: u64,
    },

    SignatureNotFound,

    ForkAttempt {
        framed_participant: UID,
        message_0: StructHash,
        message_1: StructHash,
    },
}
