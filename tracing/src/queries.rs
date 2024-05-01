use crate::db::Database;
use crate::profile::{Category, Profile, ProfileMeta, StringTableBuilder, Thread};
use actix_cors::Cors;
use actix_web::middleware::Compress;
use actix_web::{post, web, App, Error, HttpResponse, HttpServer};
use bson::doc;
use mongodb::options::FindOptions;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::Span;
use prost::Message;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use tonic::codegen::tokio_stream::StreamExt;

/// Runs a server that allows trace data to be queried and returned as a
/// Firefox profile.
pub async fn run_query_server(db: Database, port: u16) -> std::io::Result<()> {
    HttpServer::new(move || {
        App::new()
            .wrap(Cors::permissive())
            .wrap(Compress::default())
            .app_data(web::Data::new(QueryState { db: db.clone() }))
            .service(raw_trace)
            .service(profile)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}

pub struct QueryState {
    db: Database,
}

#[derive(Deserialize)]
pub struct Query {
    start_timestamp_unix_ms: i64,
    end_timestamp_unix_ms: i64,
    filter: QueryFilter,
}

#[derive(Deserialize)]
pub struct QueryFilter {
    nodes: HashSet<String>,
    threads: HashSet<i64>, // TODO: filter by name, but the name doesn't exist nicely yet
}

pub struct QueryFilterResult {
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

#[post("/raw_trace")]
async fn raw_trace(
    data: web::Data<QueryState>,
    req: web::Json<Query>,
) -> Result<HttpResponse, Error> {
    // TODO: Set a limit on the duration of the request interval.
    let col = data.db.raw_traces();
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

    let mut result: Vec<ExportTraceServiceRequest> = Vec::new();
    while let Some(chunk) = chunks.next().await {
        let chunk_bytes = chunk
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))?
            .data
            .bytes;
        let request = ExportTraceServiceRequest::decode(&chunk_bytes.as_slice()[..])
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))?;
        result.push(request);
    }
    Ok(HttpResponse::Ok().json(result))
}

#[post("/profile")]
async fn profile(
    data: web::Data<QueryState>,
    req: web::Json<Query>,
) -> Result<HttpResponse, Error> {
    // TODO: Set a limit on the duration of the request interval.
    let col = data.db.raw_traces();
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
        let chunk_bytes = chunk
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

    let category =
        Category { color: "blue".to_string(), name: "Span".to_string(), subcategories: Vec::new() };

    let mut profile = Profile {
        libs: Vec::new(),
        meta: ProfileMeta {
            version: 29,
            preprocessed_profile_version: 48,
            interval: 1,
            start_time: req.start_timestamp_unix_ms,
            process_type: 0,
            product: "near-tracing".to_string(),
            stackwalk: 0,
            categories: vec![category],
            marker_schema: vec![],
        },
        threads: Vec::new(),
    };
    let mut thread = Thread {
        name: "spans".to_string(),
        process_name: "trace".to_string(),
        process_type: "default".to_string(),
        is_main_thread: true,
        process_startup_time: 0,
        show_markers_in_timeline: true,
        ..Default::default()
    };
    let mut strings = StringTableBuilder::new();

    for (node, node_result) in result.nodes {
        for (thread_id, thread_result) in node_result.threads {
            for span in thread_result.spans {
                thread.markers.add_interval_marker(
                    &mut strings,
                    &format!("{} ({})", node, thread_id),
                    (span.start_time_unix_nano as i64 - req.start_timestamp_unix_ms * 1000000)
                        as f64
                        / 1000000.0,
                    (span.end_time_unix_nano as i64 - req.start_timestamp_unix_ms * 1000000) as f64
                        / 1000000.0,
                    0,
                    span.attributes
                        .iter()
                        .map(|kv| (kv.key.clone(), stringify_value(kv.value.as_ref())))
                        .chain([("name".to_string(), span.name.clone())].into_iter())
                        .chain([("type".to_string(), "span".to_string())].into_iter())
                        .collect(),
                );
                for event in span.events {
                    thread.markers.add_instant_marker(
                        &mut strings,
                        &format!("{} ({})", node, thread_id),
                        (event.time_unix_nano as i64 - req.start_timestamp_unix_ms * 1000000)
                            as f64
                            / 1000000.0,
                        0,
                        event
                            .attributes
                            .iter()
                            .map(|kv| (kv.key.clone(), stringify_value(kv.value.as_ref())))
                            .chain([("msg".to_string(), event.name.clone())].into_iter())
                            .chain([("type".to_string(), "span-event".to_string())].into_iter())
                            .collect(),
                    );
                }
            }
        }
    }
    thread.string_array = strings.build();
    profile.threads.push(thread);

    Ok(HttpResponse::Ok().json(profile))
}

fn stringify_value(value: Option<&AnyValue>) -> String {
    match value {
        Some(value) => match value.value {
            Some(Value::StringValue(ref value)) => value.clone(),
            Some(Value::IntValue(value)) => value.to_string(),
            Some(Value::DoubleValue(value)) => value.to_string(),
            Some(Value::BoolValue(value)) => value.to_string(),
            Some(Value::ArrayValue(ref value)) => format!(
                "[{}]",
                value
                    .values
                    .iter()
                    .map(|v| stringify_value(Some(v)))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Some(Value::BytesValue(ref value)) => hex::encode(value),
            Some(Value::KvlistValue(ref value)) => format!(
                "{{{}}}",
                value
                    .values
                    .iter()
                    .map(|kv| format!("{}: {}", kv.key, stringify_value(kv.value.as_ref())))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            None => "".to_string(),
        },
        None => "".to_string(),
    }
}

#[derive(Default)]
struct QueryResult {
    nodes: BTreeMap<String, OneNodeResult>,
}

#[derive(Default)]
struct OneNodeResult {
    threads: BTreeMap<i64, OneThreadResult>,
}

#[derive(Default)]
struct OneThreadResult {
    spans: Vec<Span>,
}
