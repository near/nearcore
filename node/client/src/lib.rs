extern crate network;
extern crate primitives;

use network::protocol::Transaction;
use primitives::traits::GenericResult;
use primitives::types::TransactionBody;

#[derive(Default)]
pub struct Client;

impl Client {
    pub fn receive_transaction(&self, t: &TransactionBody) {
        println!("{:?}", t);
    }

    pub fn handle_signed_transaction<T: Transaction>(&self, t: &T) -> GenericResult {
        println!("{:?}", t);
        Ok(())
    }
}
