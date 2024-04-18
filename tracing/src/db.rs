use crate::primitives::SpanChunk;
use bson::doc;
use mongodb::{Collection, IndexModel};

#[derive(Clone)]
pub struct Database {
    pub db: mongodb::Database,
}

impl Database {
    pub fn span_chunks(&self) -> Collection<SpanChunk> {
        self.db.collection("SpanChunk")
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
        let span_chunks = self.span_chunks();
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
