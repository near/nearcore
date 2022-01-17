use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use std::convert::Infallible;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;

#[tokio::test]
async fn test_file_download() {
    let port = portpicker::pick_unused_port().expect("No ports free");

    async fn handle_request(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let data: [u8; 1024] = [42; 1024];
        Ok(Response::new(Body::from(data.to_vec())))
    }

    let (tx, rx) = tokio::sync::oneshot::channel();

    tokio::task::spawn(async move {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let make_svc =
            make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(handle_request)) });
        let server = Server::bind(&addr).serve(make_svc);

        tx.send(()).unwrap();
        if let Err(e) = server.await {
            eprintln!("server error: {}", e);
        }
    });

    let tmp_downloaded_file = tempfile::NamedTempFile::new().unwrap();
    let tmp_downloaded_file_path = tmp_downloaded_file.path();

    rx.await.unwrap();

    nearcore::config::download_file(
        &format!("http://localhost:{}", port),
        tmp_downloaded_file_path,
    )
    .await
    .unwrap();

    let mut downloaded_file = File::open(tmp_downloaded_file_path).unwrap();
    let mut downloaded_file_content: Vec<u8> = Vec::new();
    downloaded_file.read_to_end(&mut downloaded_file_content).unwrap();

    assert_eq!(downloaded_file_content, [42; 1024].to_vec());
}
