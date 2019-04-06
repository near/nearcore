extern crate futures;
extern crate hyper;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use futures::future;
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use hyper::http::response::Builder;
use hyper::rt::{Future, Stream};
use hyper::service::service_fn;

use crate::api::{HttpApi, RPCError};

type BoxFut = Box<Future<Item=Response<Body>, Error=hyper::Error> + Send>;

fn build_response() -> Builder {
    let mut builder = Response::builder();
    builder
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Headers", "Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With");
    builder
}

fn generate_error_response(error: RPCError) -> Response<Body> {
    let (body, error_code) = match error {
        RPCError::BadRequest(msg) => (Body::from(msg), StatusCode::BAD_REQUEST),
        RPCError::NotFound => (Body::from(""), StatusCode::NOT_FOUND),
        RPCError::ServiceUnavailable(msg) => (Body::from(msg), StatusCode::SERVICE_UNAVAILABLE),
    };
    build_response()
        .status(error_code)
        .body(body)
        .unwrap()
}

fn serve<T: Send + Sync + 'static>(http_api: Arc<HttpApi<T>>, req: Request<Body>) -> BoxFut {
    match (req.method(), req.uri().path()) {
        (&Method::OPTIONS, _) => {
            // Pre-flight response for cross site access.
            Box::new(req.into_body().concat2().map(move |_| {
                build_response()
                    .body(Body::empty())
                    .unwrap()
            }))
        }
        (&Method::POST, "/submit_transaction") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.submit_transaction(&data) {
                            Ok(response) => {
                                build_response()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => generate_error_response(e)
                        }
                    }
                    Err(e) => {
                        build_response()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }

        (&Method::POST, "/call_view_function") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.call_view_function(&data) {
                            Ok(response) => {
                                build_response()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => {
                                build_response()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(Body::from(e.to_string()))
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        build_response()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::POST, "/view_account") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.view_account(&data) {
                            Ok(response) => {
                                build_response()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => {
                                build_response()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(Body::from(e.to_string()))
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        build_response()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::POST, "/view_state") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.view_state(&data) {
                            Ok(response) => {
                                build_response()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => {
                                build_response()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(Body::from(e))
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        build_response()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::POST, "/view_latest_beacon_block") => {
            Box::new(future::ok(
                match http_api.view_latest_beacon_block() {
                    Ok(response) => {
                        build_response()
                            .body(Body::from(serde_json::to_string(&response).unwrap()))
                            .unwrap()
                    }
                    Err(_) => unreachable!()
                }
            ))
        }
        (&Method::POST, "/get_beacon_block_by_hash") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.get_beacon_block_by_hash(&data) {
                            Ok(response) => {
                                build_response()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => {
                                build_response()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(Body::from(e.to_string()))
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        build_response()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::POST, "/view_latest_shard_block") => {
            Box::new(future::ok(
                match http_api.view_latest_shard_block() {
                    Ok(response) => {
                        build_response()
                            .body(Body::from(serde_json::to_string(&response).unwrap()))
                            .unwrap()
                    }
                    Err(_) => unreachable!()
                }
            ))
        }
        (&Method::POST, "/get_shard_block_by_hash") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.get_shard_block_by_hash(&data) {
                            Ok(response) => {
                                build_response()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => {
                                build_response()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(Body::from(e.to_string()))
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        build_response()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::POST, "/get_beacon_blocks_by_index") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.get_beacon_blocks_by_index(&data) {
                            Ok(response) => {
                                build_response()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => {
                                build_response()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(Body::from(e.to_string()))
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        build_response()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::POST, "/get_shard_blocks_by_index") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.get_shard_blocks_by_index(&data) {
                            Ok(response) => {
                                build_response()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => {
                                build_response()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(Body::from(e.to_string()))
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        build_response()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::POST, "/get_transaction_info") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.get_transaction_info(&data) {
                            Ok(response) => {
                                build_response()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => generate_error_response(e)
                        }
                    }
                    Err(e) => {
                        build_response()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::POST, "/get_transaction_result") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.get_transaction_result(&data) {
                            Ok(response) => {
                                build_response()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(_) => unreachable!()
                        }
                    }
                    Err(e) => {
                        build_response()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::GET, "/healthz") => {
            // Assume that, if we can get a latest block, things are healthy
            Box::new(future::ok(
                match http_api.view_latest_beacon_block() {
                    Ok(_) => {
                        build_response()
                            .body(Body::from(""))
                            .unwrap()
                    }
                    Err(_) => unreachable!()
                }
            ))
        }

        _ => {
            Box::new(future::ok(
                build_response()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap()
            ))
        }
    }
}

pub fn spawn_server<T: Send + Sync + 'static>(http_api: HttpApi<T>, addr: Option<SocketAddr>) {
    let addr = addr.unwrap_or_else(|| {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 3030)
    });

    let http_api = Arc::new(http_api);
    let service = move || {
        let http_api = http_api.clone();
        service_fn(move |req| {
            serve(http_api.clone(), req)
        })
    };
    let server = Server::bind(&addr)
        .serve(service)
        .map_err(|e| eprintln!("server error: {}", e));
    hyper::rt::spawn(server);
}
