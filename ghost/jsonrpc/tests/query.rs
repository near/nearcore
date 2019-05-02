use std::sync::Arc;

use actix::{Actor, Recipient, System};
use actix::actors::mocker::Mocker;
use jsonrpc_client_core::{expand_params, jsonrpc_client};
use jsonrpc_client_http::HttpTransport;

use near_chain::test_utils::KeyValueRuntime;
use near_client::{ClientActor, ClientConfig};
use near_client::test_utils::setup_mock;
use near_jsonrpc::JsonRpcServer;
use near_network::{NetworkRequests, NetworkResponses};
use near_network::test_utils::open_port;
use near_store::test_utils::create_test_store;
use primitives::crypto::signer::InMemorySigner;
use primitives::test_utils::init_test_logger;
use std::thread;

jsonrpc_client!(pub struct TestClient {
    pub fn query(&mut self, path: String, data: Vec<u8>, height: String, proof: bool) -> RpcRequest<String>;
});

/// Connect to json rpc and query it.
#[test]
fn test_query() {
    init_test_logger();

    System::run(|| {
        let client_addr = setup_mock(
            vec!["test"],
            "test",
            true,
            Box::new(move |msg, _ctx, _| NetworkResponses::NoResponse),
        );

        let addr = format!("127.0.0.1:{}", open_port());
        let handler = Arc::new(near_jsonrpc::methods::RPCAPIHandler::new(client_addr));
        near_jsonrpc::start_http(handler, addr.parse().unwrap());

        let transport = HttpTransport::new().standalone().unwrap();
        let transport_handle = transport.handle(&format!("http://{}", addr)).unwrap();
        let mut client = TestClient::new(transport_handle);
        println!("1");
        let result = client
            .query("account/test".to_string(), vec![], "0".to_string(), false)
            .call()
            .unwrap();
        println!("res: {:?}", result);
        System::current().stop();
    })
    .unwrap();
}
