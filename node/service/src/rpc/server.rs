use jsonrpc_core::IoHandler;
use jsonrpc_minihttp_server::cors::AccessControlAllowOrigin;
use jsonrpc_minihttp_server::{DomainsValidation, ServerBuilder};

pub fn run_server(io: IoHandler) {
    let server = ServerBuilder::new(io)
        .cors(DomainsValidation::AllowOnly(vec![
            AccessControlAllowOrigin::Null,
        ]))
        .start_http(&"127.0.0.1:3030".parse().unwrap())
        .expect("Unable to start RPC server");

    server.wait().unwrap();
}
