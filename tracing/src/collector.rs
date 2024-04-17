use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tonic::{Request, Response, Status};

pub struct Collector {}

impl Collector {
    pub fn new() -> Self {
        Self {}
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
        println!("Received trace of size {} bytes", serialized.len());
        Ok(Response::new(ExportTraceServiceResponse { partial_success: None }))
    }
}
