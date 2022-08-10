#[cfg(feature = "sandbox")]
pub mod state_patch {
    use crate::state_record::StateRecord;

    /// Changes to the state to be applied via sandbox-only state patching
    /// feature.
    ///
    /// As we only expose this functionality for sandbox, we make sure that the
    /// object can be non-empty only if `sandbox` feature is enabled.  On
    /// non-sandbox build, this struct is ZST and its methods are essentially
    /// short-circuited by treating the type as always empty.
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
        #[inline(always)]
        pub fn is_empty(&self) -> bool {
            true
        }
        #[inline(always)]
        pub fn clear(&self) {}
        #[inline(always)]
        pub fn take(&mut self) -> Self {
            Self
        }
        #[inline(always)]
        pub fn merge(&self, _other: SandboxStatePatch) {}
    }

    impl IntoIterator for SandboxStatePatch {
        type Item = StateRecord;
        type IntoIter = std::iter::Empty<StateRecord>;

        #[inline(always)]
        fn into_iter(self) -> Self::IntoIter {
            std::iter::empty()
        }
    }
}
