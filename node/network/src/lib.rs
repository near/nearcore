extern crate libp2p;
extern crate primitives;
extern crate substrate_network_libp2p;

extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate bytes;
extern crate futures;
extern crate parking_lot;
extern crate tokio;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate rand;

pub mod error;
mod io;
pub mod message;
pub mod protocol;
pub mod service;
pub mod test_utils;

use primitives::hash::CryptoHash;
use primitives::traits::{Block, Header};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MockBlockHeader {}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MockBlock {}

impl Header for MockBlockHeader {
    fn hash(&self) -> CryptoHash {
        CryptoHash { 0: [0; 32] }
    }
}

impl Block for MockBlock {
    type Header = MockBlockHeader;
    type Body = ();
    fn header(&self) -> &Self::Header {
        &MockBlockHeader {}
    }
    fn body(&self) -> &Self::Body {
        &()
    }
    fn deconstruct(self) -> (Self::Header, Self::Body) {
        (MockBlockHeader {}, ())
    }
    fn new(_header: Self::Header, _body: Self::Body) -> Self {
        MockBlock {}
    }
    fn hash(&self) -> CryptoHash {
        CryptoHash { 0: [0; 32] }
    }
}
