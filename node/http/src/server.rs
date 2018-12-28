extern crate futures;
extern crate hyper;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use futures::future;
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use hyper::rt::{Future, Stream};
use hyper::service::service_fn;

use api::HttpApi;

type BoxFut = Box<Future<Item=Response<Body>, Error=hyper::Error> + Send>;

fn serve(http_api: Arc<HttpApi>, req: Request<Body>) -> BoxFut {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/create_account") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.create_account(&data) {
                            Ok(response) => {
                                Response::builder()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(_) => unreachable!()
                        }
                    }
                    Err(e) => {
                        Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::POST, "/stake") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.stake(&data) {
                            Ok(response) => {
                                Response::builder()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(_) => unreachable!()
                        }
                    }
                    Err(e) => {
                        Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::POST, "/swap_key") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.swap_key(&data) {
                            Ok(response) => {
                                Response::builder()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(_) => unreachable!()
                        }
                    }
                    Err(e) => {
                        Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }

        (&Method::POST, "/deploy_contract") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.deploy_contract(data) {
                            Ok(response) => {
                                Response::builder()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(_) => unreachable!()
                        }
                    }
                    Err(e) => {
                        Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::POST, "/schedule_function_call") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.schedule_function_call(data) {
                            Ok(response) => {
                                Response::builder()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(_) => unreachable!()
                        }
                    }
                    Err(e) => {
                        Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::POST, "/send_money") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.send_money(&data) {
                            Ok(response) => {
                                Response::builder()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(_) => unreachable!()
                        }
                    }
                    Err(e) => {
                        Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(e.to_string()))
                            .unwrap()
                    }
                }
            }))
        }
        (&Method::POST, "/submit_transaction") => {
            Box::new(req.into_body().concat2().map(move |chunk| {
                match serde_json::from_slice(&chunk) {
                    Ok(data) => {
                        match http_api.submit_transaction(data) {
                            Ok(response) => {
                                Response::builder()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => {
                                Response::builder()
                                    .status(StatusCode::SERVICE_UNAVAILABLE)
                                    .body(Body::from(e.to_string()))
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        Response::builder()
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
                                Response::builder()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => {
                                Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(Body::from(e.to_string()))
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        Response::builder()
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
                                Response::builder()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => {
                                Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(Body::from(e.to_string()))
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        Response::builder()
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
                                Response::builder()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(_) => unreachable!()
                        }
                    }
                    Err(e) => {
                        Response::builder()
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
                        Response::builder()
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
                                Response::builder()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => {
                                Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(Body::from(e.to_string()))
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        Response::builder()
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
                        Response::builder()
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
                                Response::builder()
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap()
                            }
                            Err(e) => {
                                Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(Body::from(e.to_string()))
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        Response::builder()
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
                        Response::builder()
                            .body(Body::from(""))
                            .unwrap()
                    }
                    Err(_) => unreachable!()
                }
            ))
        }

        _ => {
            Box::new(future::ok(
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap()
            ))
        }
    }
}

pub fn spawn_server(http_api: HttpApi, addr: Option<SocketAddr>) {
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
