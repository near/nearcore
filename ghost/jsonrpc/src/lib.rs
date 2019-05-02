use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::future;
use hyper::http::response::Builder;
use hyper::service::service_fn;
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use hyper::rt::{Future, Stream};
use log::{info, error};

use methods::RPCAPIHandler;
pub use server::JsonRpcServer;

mod codec;
mod connection;
pub mod methods;
mod server;
mod message;

type BoxFut = Box<Future<Item = Response<Body>, Error = hyper::Error> + Send>;

fn build_response() -> Builder {
    let mut builder = Response::builder();
    builder.header("Access-Control-Allow-Origin", "*").header(
        "Access-Control-Allow-Headers",
        "Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With",
    );
    builder
}

fn serve(rpc_api: Arc<RPCAPIHandler>, req: Request<Body>) -> BoxFut {
    match (req.method(), req.uri().path()) {
        (&Method::OPTIONS, _) => {
            // Pre-flight response for cross site access.
            Box::new(
                req.into_body()
                    .concat2()
                    .map(move |_| build_response().body(Body::empty()).unwrap()),
            )
        }
        (&Method::POST, "/") => Box::new(req.into_body().concat2().map(move |chunk| {
            match serde_json::from_slice(&chunk) {
                Ok(request) => build_response()
                    .body(Body::from(serde_json::to_string(&rpc_api.call(&request)).unwrap()))
                    .unwrap(),
                Err(e) => build_response()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from(e.to_string()))
                    .unwrap(),
            }
        })),

        _ => Box::new(future::ok(
            build_response().status(StatusCode::NOT_FOUND).body(Body::empty()).unwrap(),
        )),
    }
}

/// Start HTTP server listening for JSON RPC requests.
pub fn start_http(rpc_api: Arc<RPCAPIHandler>, addr: SocketAddr) {
    let service = move || {
        let rpc_api = rpc_api.clone();
        service_fn(move |req| {
            serve(rpc_api.clone(), req)
        })
    };
    let server = Server::bind(&addr)
        .serve(service)
        .map_err(|err| error!(target: "jsonrpc", "Server error: {}", err));
    hyper::rt::spawn(server);
    info!(target: "jsonrpc", "Started JSON RPC server: {}", addr);
}
