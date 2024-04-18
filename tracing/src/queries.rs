use crate::db::Database;
use actix_cors::Cors;
use actix_web::error::InternalError;
use actix_web::{get, post, web, App, Error, HttpResponse, HttpServer};
use bson::doc;
use mongodb::options::FindOptions;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::Span;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use tonic::codegen::tokio_stream::StreamExt;

pub struct QueryState {
    db: Database,
}

#[derive(Deserialize)]
pub struct Query {
    start_timestamp_unix_ms: i64,
    end_timestamp_unix_ms: i64,
    time_resolution_ms: i64,
    filter: QueryFilter,
    focus: Vec<QueryFocus>,
}

#[derive(Deserialize)]
pub struct QueryFilter {
    nodes: HashSet<String>,
    threads: HashSet<i64>, // TODO: filter by name, but the name doesn't exist nicely yet
}

struct QueryFilterResult {
    matches: bool,
    thread: i64,
    node: String,
}

impl QueryFilter {
    pub fn matches(&self, resource: &Resource, span: &Span) -> QueryFilterResult {
        let mut node_name = "(unknown)".to_owned();
        for attr in &resource.attributes {
            if attr.key == "service.name" {
                match &attr.value {
                    Some(value) => match &value.value {
                        Some(Value::StringValue(value)) => {
                            node_name = value.clone();
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
        }

        let mut thread_id = -1;
        for attr in &span.attributes {
            if attr.key == "thread.id" {
                match &attr.value {
                    Some(value) => match &value.value {
                        Some(Value::IntValue(value)) => {
                            thread_id = *value;
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
        }

        let nodes_match = self.nodes.is_empty() || self.nodes.contains(&node_name);
        let threads_match = self.threads.is_empty() || self.threads.contains(&thread_id);
        QueryFilterResult {
            matches: nodes_match && threads_match,
            thread: thread_id,
            node: node_name,
        }
    }
}

#[derive(Deserialize)]
pub struct QueryFocus {
    attributes: HashMap<String, String>,
    names: Vec<String>,
}

impl QueryFocus {
    pub fn matches(&self, _span: &Span) -> bool {
        true
    }
}

#[post("/query")]
async fn query(data: web::Data<QueryState>, req: web::Json<Query>) -> Result<HttpResponse, Error> {
    let col = data.db.span_chunks();
    let mut chunks = col
        .find(
            doc! {
                "max_time": {"$gt": req.start_timestamp_unix_ms * 1000000},
                "min_time": {"$lt": req.end_timestamp_unix_ms * 1000000},
            },
            Some(FindOptions::builder().batch_size(100).build()),
        )
        .await
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))?;

    let mut result = QueryResult::default();
    while let Some(chunk) = chunks.next().await {
        let mut chunk_bytes = chunk
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))?
            .data
            .bytes;
        let request = ExportTraceServiceRequest::decode(&chunk_bytes.as_slice()[..])
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))?;
        for resource_span in request.resource_spans {
            if let Some(resource) = resource_span.resource {
                for scope_span in resource_span.scope_spans {
                    for span in scope_span.spans {
                        let filter_match = req.filter.matches(&resource, &span);
                        if filter_match.matches {
                            result
                                .nodes
                                .entry(filter_match.node)
                                .or_default()
                                .threads
                                .entry(filter_match.thread)
                                .or_default()
                                .spans
                                .push(span);
                        }
                    }
                }
            }
        }
    }
    Ok(HttpResponse::Ok().json(result))
}

#[derive(Serialize, Default)]
pub struct QueryResult {
    nodes: BTreeMap<String, OneNodeResult>,
}

#[derive(Serialize, Default)]
pub struct OneNodeResult {
    threads: BTreeMap<i64, OneThreadResult>,
}

#[derive(Serialize, Default)]
pub struct OneThreadResult {
    spans: Vec<Span>,
}

pub async fn run_query_server(db: Database, port: u16) -> std::io::Result<()> {
    HttpServer::new(move || {
        App::new()
            .wrap(Cors::permissive())
            .app_data(web::Data::new(QueryState { db: db.clone() }))
            .service(query)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
