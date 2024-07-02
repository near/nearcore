use crate::db::Database;
use crate::primitives::RawTrace;
use bson::spec::BinarySubtype;
use bson::Binary;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tonic::{Request, Response, Status};

/// Runs an OpenTelemetry-compatible collector service. This service can accept
/// trace data from any OpenTelemetry client (such as neard), and will store the
/// data verbatim into a MongoDB database for later querying and processing.
pub struct Collector {
    db: Database,
}

impl Collector {
    pub fn new(db: Database) -> Self {
        Self { db }
    }
}

#[tonic::async_trait]
impl TraceService for Collector {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let request = request.into_inner();
        let serialized = prost::Message::encode_to_vec(&request);

        // We index the traces by min and max time to facilitate querying
        // by time intervals. Depending on need, we may want to index other
        // things too in the future.
        let mut min_time = None;
        let mut max_time = None;
        for resource_span in &request.resource_spans {
            for scope_span in &resource_span.scope_spans {
                for span in &scope_span.spans {
                    if min_time.is_none()
                        || min_time.is_some_and(|min_time| span.start_time_unix_nano < min_time)
                    {
                        min_time = Some(span.start_time_unix_nano);
                    }
                    if max_time.is_none()
                        || max_time.is_some_and(|max_time| span.end_time_unix_nano > max_time)
                    {
                        max_time = Some(span.end_time_unix_nano);
                    }
                }
            }
        }
        let (Some(min_time), Some(max_time)) = (min_time, max_time) else {
            return Err(Status::invalid_argument("Invalid trace request with no spans"));
        };
        println!("Persisting trace of size {} bytes between min_time = {} and max_time = {}", serialized.len(), min_time / 1_000_000, max_time / 1_000_000);

        // Insert into MongoDB.
        let span_chunk = RawTrace {
            _id: None,
            min_time,
            max_time,
            data: Binary { subtype: BinarySubtype::Generic, bytes: serialized },
        };
        self.db
            .raw_traces()
            .insert_one(span_chunk, None)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(ExportTraceServiceResponse { partial_success: None }))
    }
}
