extern crate primitives;

use primitives::types::TransactionBody;

#[derive(Default, Clone, Copy)]
pub struct Client;

impl Client {
    pub fn receive_transaction(self, t: &TransactionBody) {
        println!("{:?}", t);
    }
}
