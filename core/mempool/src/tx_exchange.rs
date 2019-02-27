use log::error;
use primitives::hash::CryptoHash;
use primitives::transaction::SignedTransaction;
use std::collections::HashMap;
use client::Client;
use std::sync::Arc;
use futures::sync::mpsc::{Receiver, Sender};
use primitives::chain::ChainPayload;
use std::sync::RwLock;
use futures::stream::Stream;
use futures::future::Future;
use primitives::types::{Gossip, GossipBody};
use std::collections::HashSet;
use primitives::types::UID;
use tokio::timer::Interval;
use std::time::Duration;
use primitives::block_traits::SignedBlock;
use primitives::signature::DEFAULT_SIGNATURE;
use primitives::types::SignedMessageData;
use primitives::types::MessageDataBody;
use futures::sink::Sink;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";
const TOKIO_RECV_ERR: &str = "Implementation of tokio Receiver should not return an error";
const TX_GOSSIP_NANOSECONDS: u32 = 200000000;

struct TxExchangeEntry {
    tx: SignedTransaction,
    known_to: HashSet<UID>,
}

impl TxExchangeEntry {
    fn new(tx: SignedTransaction) -> Self {
        TxExchangeEntry { tx, known_to: HashSet::new() }
    }
}

pub fn spawn_task(
    client: Arc<Client>,
    payload_receiver: Receiver<ChainPayload>,
    gossip_receiver: Receiver<Gossip<ChainPayload>>,
    gossip_sender: Sender<Gossip<ChainPayload>>
) {
    let entries: Arc<RwLock<HashMap<CryptoHash, TxExchangeEntry>>> = Arc::new(RwLock::new(HashMap::new()));
    let entries1 = entries.clone();
    let payload_recv_task = payload_receiver
        .for_each(move |payload| {
            error!("New payload");
            for transaction in payload.transactions.iter() {
                entries1.write().expect(POISONED_LOCK_ERR).insert(transaction.get_hash(), TxExchangeEntry::new(transaction.clone()));
            }
            Ok(()) })
        .map_err(|_| error!(target: "tx_exchange", "{}", TOKIO_RECV_ERR));
    tokio::spawn(payload_recv_task);

    let entries2 = entries.clone();
    let gossip_recv_task = gossip_receiver
        .for_each(move |gossip| {
            error!("New gossip");
            if let GossipBody::Unsolicited(msg) = gossip.body {
                for transaction in msg.body.payload.transactions.iter() {
                    entries2.write().expect(POISONED_LOCK_ERR).entry(transaction.get_hash()).or_insert(TxExchangeEntry::new(transaction.clone())).known_to.insert(gossip.sender_uid);
                }
            }
            Ok(())
        }).map_err(|_| error!(target: "tx_exchange", "{}", TOKIO_RECV_ERR));
    tokio::spawn(gossip_recv_task);

    let interval = Interval::new_interval(Duration::new(0, TX_GOSSIP_NANOSECONDS));
    let entries3 = entries.clone();
    let client3 = client.clone();
    let interval_task = interval.for_each(move |_| {
        let block_index = client.beacon_chain.chain.best_block().index() + 1;
        let (owner_uid, authority_map) = client3.get_uid_to_authority_map(block_index);

        let owner_uid = match owner_uid {
            None => { return Ok(()); },
            Some(x) => { x },
        };

        for (uid, authority) in authority_map.iter() {
            if *uid == owner_uid {
                continue;
            }

            let mut txs: Vec<SignedTransaction> = vec![];
            for tx_exchange_entry in entries3.write().expect(POISONED_LOCK_ERR).values_mut() {
                if !tx_exchange_entry.known_to.contains(uid) {
                    txs.push(tx_exchange_entry.tx.clone());
                    tx_exchange_entry.known_to.insert(*uid);
                }
            }

            if txs.len() > 0 {
                error!("Have transactions to send to {:?}!", uid);

                // TODO: nothing in MessageDataBody is used, change the Gossip to be directly Payload once TxFlow is removed
                let message_data_body = MessageDataBody {
                    owner_uid: owner_uid,
                    parents: HashSet::new(),
                    epoch: 0,
                    payload: ChainPayload { transactions: txs, receipts: vec![] },
                    endorsements: vec![],
                };

                let gossip_body = SignedMessageData {
                    owner_sig: DEFAULT_SIGNATURE, // TODO: Sign it.
                    hash: 0, // TODO: remove it
                    body: message_data_body,
                    beacon_block_index: block_index // TODO: unused, remove it
                };

                let gossip = Gossip {
                    sender_uid: owner_uid,
                    receiver_uid: *uid,
                    sender_sig: DEFAULT_SIGNATURE, // TODO: Sign it.
                    body: GossipBody::Unsolicited(gossip_body.clone()),
                };

                let copied_tx = gossip_sender.clone();
                tokio::spawn(copied_tx.send(gossip).map(|_| ()).map_err(|e| {
                    error!("Failure in the sub-task {:?}", e);
                }));
            }
        }
        Ok(())
    }).map_err(|_| error!(target: "tx_exchange", "{}", TOKIO_RECV_ERR));
    tokio::spawn(interval_task);
}
