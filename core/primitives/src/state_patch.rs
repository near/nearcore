use crate::state_record::StateRecord;

pub struct StatePatch {
    #[cfg(feature = "sandbox")]
    records: Vec<StateRecord>,
}

impl StatePatch {
    pub fn into_records(self) -> Option<Vec<StateRecord>> {
        #[cfg(feature = "sandbox")]
        return Some(self.records);

        #[cfg(not(feature = "sandbox"))]
        return None;
    }
}
