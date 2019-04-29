use futures::Future;
use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::SignedTransaction;
use protobuf::Message;
use reqwest::r#async::Client;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

pub struct RemoteNode {
    pub addr: SocketAddr,
    pub signers: Vec<Arc<InMemorySigner>>,
    pub nonces: Vec<u64>,
    pub url: String,
    client: Client,
}

impl RemoteNode {
    pub fn new(addr: SocketAddr, signers: &[&str], init_nonce: u64) -> Arc<RwLock<Self>> {
        let url = format!("http://{}", addr);
        let signers: Vec<_> =
            signers.iter().map(|s| Arc::new(InMemorySigner::from_seed(s, s))).collect();
        let nonces = vec![init_nonce; signers.len()];
        let client =
            Client::builder().use_rustls_tls().connect_timeout(CONNECT_TIMEOUT).build().unwrap();
        Arc::new(RwLock::new(Self { addr, signers, nonces, url, client }))
    }

    //    /// Attempts to initialize the client.
    //    fn client(&self) -> Result<Client, String> {
    //
    //            .map_err(|err| format!("{}", err))
    //    }

    pub fn add_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        let transaction: near_protos::signed_transaction::SignedTransaction = transaction.into();
        let tx_bytes = transaction.write_to_bytes().expect("write to bytes failed");
        let url = format!("{}{}", self.url, "/broadcast_tx_sync");
        //        let client = match self.client() {
        //            Ok(c) => c,
        //            Err(err) => return Box::new(futures::future::done(Err(err))),
        //        };
        let response = self
            .client
            .post(url.as_str())
            .form(&[("tx", format!("0x{}", hex::encode(&tx_bytes)))])
            .send()
            .map(|_| ())
            .map_err(|err| format!("{}", err));
        Box::new(response)
    }
}
