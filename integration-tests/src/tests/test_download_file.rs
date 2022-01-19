use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Barrier;

#[tokio::test]
async fn test_file_download() {
    let port = portpicker::pick_unused_port().expect("No ports free");

    async fn handle_request(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let data: [u8; 1024] = [42; 1024];
        Ok(Response::new(Body::from(data.to_vec())))
    }

    let barrier = Arc::new(Barrier::new(2));
    let server_barrier = barrier.clone();

    tokio::task::spawn(async move {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let make_svc =
            make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(handle_request)) });
        let server = Server::bind(&addr).serve(make_svc);

        server_barrier.wait().await;
        if let Err(e) = server.await {
            eprintln!("server error: {}", e);
        }
    });

    let tmp_downloaded_file = tempfile::NamedTempFile::new().unwrap();
    let tmp_downloaded_file_path = tmp_downloaded_file.path();

    barrier.wait().await;

    nearcore::config::download_file(
        &format!("http://localhost:{}", port),
        tmp_downloaded_file_path,
    )
    .await
    .unwrap();

    let downloaded_file_content = std::fs::read(tmp_downloaded_file_path).unwrap();
    assert_eq!(downloaded_file_content, [42; 1024].to_vec());
}
