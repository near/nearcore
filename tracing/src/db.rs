use crate::primitives::RawTrace;
use bson::doc;
use mongodb::{Collection, IndexModel};

/// Simple wrapper on top of mongodb.
#[derive(Clone)]
pub struct Database {
    pub db: mongodb::Database,
}

impl Database {
    /// The collection where we dump all the traces received by the collector.
    pub fn raw_traces(&self) -> Collection<RawTrace> {
        self.db.collection("RawTrace")
    }

    pub async fn new(mongodb_uri: &str, initialize: bool) -> Self {
        let db = mongodb::Client::with_uri_str(mongodb_uri).await.unwrap().database("near");
        let db = Self { db };
        if initialize {
            db.create_indexes().await;
        }
        println!("Mongodb initialized");
        db
    }

    async fn create_indexes(&self) {
        // TODO: add a TTL to the collection so that data is automatically
        // garbage collected by MongoDB.
        let span_chunks = self.raw_traces();
        span_chunks
            .create_index(IndexModel::builder().keys(doc! {"min_time": 1}).build(), None)
            .await
            .unwrap();
        span_chunks
            .create_index(IndexModel::builder().keys(doc! {"max_time": 1}).build(), None)
            .await
            .unwrap();
    }
}
