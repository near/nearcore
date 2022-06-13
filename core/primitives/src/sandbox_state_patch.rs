use crate::state_record::StateRecord;

/// Changes to the state to be applied via sandbox-only state patching
/// functionality.
///
/// As we only expose this functionality for sandbox, we make sure that the
/// object is only constructable if sandbox feature is enabled.
pub struct SandboxStatePatch {
    records: Vec<StateRecord>,
}

impl SandboxStatePatch {
    // NB: it's crucial that all creation APIs are guarded with `#[cfg(feature = "sandbox")]`.
    #[cfg(feature = "sandbox")]
    pub fn new(records: Vec<StateRecord>) -> SandboxStatePatch {
        SandboxStatePatch { records }
    }

    pub fn into_records(self) -> Vec<StateRecord> {
        self.records
    }

    pub fn merge(&mut self, other: SandboxStatePatch) {
        self.records.extend(other.records)
    }
}
