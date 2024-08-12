use borsh::{BorshDeserialize, BorshSerialize};
use near_schema_checker_lib::ProtocolSchema;

#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Eq,
    PartialEq,
    Default,
    ProtocolSchema,
)]
pub struct ChunkEndorsementsBitmap(Vec<Vec<u8>>);

impl ChunkEndorsementsBitmap {
    /// Returns a clone of this bitmap with all the bits unset.
    pub fn clone_cleared(&self) -> Self {
        let mut clone = self.clone();
        clone.clear();
        clone
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}
