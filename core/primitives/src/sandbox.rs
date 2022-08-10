#[cfg(feature = "sandbox")]
pub mod state_patch {
    use crate::state_record::StateRecord;

    /// Changes to the state to be applied via sandbox-only state patching
    /// functionality.
    ///
    /// As we only expose this functionality for sandbox, we make sure that the
    /// object is only constructable if sandbox feature is enabled.
    #[derive(Default)]
    pub struct SandboxStatePatch {
        records: Vec<StateRecord>,
    }

    impl SandboxStatePatch {
        pub fn new(records: Vec<StateRecord>) -> SandboxStatePatch {
            SandboxStatePatch { records }
        }

        pub fn is_empty(&self) -> bool {
            self.records.is_empty()
        }

        pub fn clear(&mut self) {
            self.records.clear();
        }

        pub fn take(&mut self) -> SandboxStatePatch {
            Self { records: core::mem::take(&mut self.records) }
        }

        pub fn merge(&mut self, other: SandboxStatePatch) {
            self.records.extend(other.records);
        }
    }

    impl IntoIterator for SandboxStatePatch {
        type Item = StateRecord;
        type IntoIter = std::vec::IntoIter<StateRecord>;

        fn into_iter(self) -> Self::IntoIter {
            self.records.into_iter()
        }
    }
}

#[cfg(not(feature = "sandbox"))]
pub mod state_patch {
    use crate::state_record::StateRecord;

    #[derive(Default)]
    pub struct SandboxStatePatch;

    impl SandboxStatePatch {
        pub const fn is_empty(&self) -> bool {
            true
        }
        pub const fn clear(&self) {}
        pub fn take(&mut self) -> Self {
            Self
        }
        pub const fn merge(&self, _other: SandboxStatePatch) {}
    }

    impl IntoIterator for SandboxStatePatch {
        type Item = StateRecord;
        type IntoIter = std::iter::Empty<StateRecord>;

        fn into_iter(self) -> Self::IntoIter {
            std::iter::empty()
        }
    }
}
