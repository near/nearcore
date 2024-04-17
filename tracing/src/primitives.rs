use bson::oid::ObjectId;
use bson::Binary;
use serde::{Deserialize, Serialize};

// The storage format in MongoDB.
#[derive(Serialize, Deserialize)]
pub struct SpanChunk {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _id: Option<ObjectId>,
    pub min_time: u64,
    pub max_time: u64,
    pub data: Binary,
}
