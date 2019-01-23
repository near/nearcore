#![allow(unused)]

extern crate storage;

use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use ::futures::{future, Future};
use ::futures::stream::Stream;
use ::futures::sync::mpsc::channel;
use ::parking_lot::RwLock;
use ::rand::Rng;
use ::tokio::timer::Interval;

use ::configs::AuthorityConfig;
use ::beacon::types::{BeaconBlockChain, SignedBeaconBlock, SignedBeaconBlockHeader};
use ::chain::{SignedBlock, SignedHeader};
use ::client::test_utils::get_client;
use ::primitives::hash::{CryptoHash, hash_struct};
use ::primitives::signature::get_key_pair;
use ::primitives::traits::GenericResult;
use ::primitives::types;
use ::transaction::{ChainPayload, SignedTransaction};

use crate::error::Error;
use crate::message::Message;
use crate::protocol::{CURRENT_VERSION, Protocol};

use self::storage::test_utils::create_memory_db;


pub fn fake_tx_message() -> Message {
    let tx = SignedTransaction::empty();
    Message::Transaction(Box::new(tx))
}

pub fn init_logger(debug: bool) {
    let mut builder = env_logger::Builder::new();
    if debug {
        builder.filter(Some("network"), log::LevelFilter::Debug);
    }
    builder.filter(None, log::LevelFilter::Info);
    builder.try_init().unwrap_or(());
}

pub fn get_noop_network_task() -> impl Future<Item = (), Error = ()> {
    Interval::new_interval(Duration::from_secs(1)).for_each(|_| Ok(())).then(|_| Ok(()))
}

pub fn get_test_protocol() -> Protocol {
    let (block_tx, _) = channel(1024);
    let (transaction_tx, _) = channel(1024);
    let (message_tx, _) = channel(1024);
    let (gossip_tx, _) = channel(1024);
    let client = Arc::new(get_client());
    Protocol::new(None, client, block_tx, transaction_tx, message_tx, gossip_tx)
}

pub fn get_test_authority_config(
    num_authorities: u32,
    epoch_length: u64,
    num_seats_per_slot: u64,
) -> AuthorityConfig {
    let mut initial_authorities = vec![];
    for i in 0..num_authorities {
        let (public_key, _) = get_key_pair();
        initial_authorities.push(types::AuthorityStake {
            account_id: i.to_string(),
            public_key,
            amount: 100,
        });
    }
    AuthorityConfig { initial_proposals: initial_authorities, epoch_length, num_seats_per_slot }
}
