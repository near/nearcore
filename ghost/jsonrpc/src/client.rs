use jsonrpc_client_core::{expand_params, jsonrpc_client};
use jsonrpc_client_http::{HttpHandle, HttpTransport};

jsonrpc_client!(pub struct JsonRpcClient {
    pub fn broadcast_tx_sync(&mut self, tx: Vec<u8>) -> RpcRequest<()>;
    pub fn query(&mut self, path: String, data: Vec<u8>, height: String, proof: bool) -> RpcRequest<String>;
});

pub fn new_client(server_addr: &str) -> JsonRpcClient<HttpHandle> {
    let transport = HttpTransport::new().standalone().unwrap();
    let transport_handle = transport.handle(server_addr).unwrap();
    JsonRpcClient::new(transport_handle)
}
