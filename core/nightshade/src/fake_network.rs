use super::{Nightshade, Message, NSResult, AuthorityId};
use std::collections::HashSet;
use primitives::hash::CryptoHash;
use tokio::timer::Delay;
use futures::sync::mpsc;
use futures::{stream, Async, Future, Poll, Sink, Stream};
use futures::future::{join_all, lazy};
use std::time::{Duration, Instant};
use futures::try_ready;
use log::{info, debug, error};
use std::cmp::min;
use primitives::traits::Payload;

enum Control {
    Reset,
    Stop
}

const COOLDOWN_MS: u64 = 50;
const GOSSIP_SAMPLE_SIZE: usize = 3;

#[derive(Debug)]
enum GossipBody {
    Unsolicited(Message),
    Fetch(Vec<CryptoHash>),
    FetchReply(Vec<Message>),
}

#[derive(Debug)]
struct Gossip {
    sender_id: AuthorityId,
    receiver_id: AuthorityId,
    body: GossipBody,
}

struct ConsensusBlock {
    pub parent_hash: CryptoHash,
    pub messages: Vec<Message>,
}

struct NightshadeTask {
    owner_id: usize,
    num_authorities: usize,
    nightshade: Option<Nightshade>,
    control_receiver: mpsc::Receiver<Control>,
    gossip_receiver: mpsc::Receiver<Gossip>,
    gossip_sender: mpsc::Sender<Gossip>,
    consensus_sender: mpsc::Sender<ConsensusBlock>,

    /// Timer that determines the minimum time that we should not gossip after the given message
    /// for the sake of not spamming the network with small packages.
    cooldown_delay: Option<Delay>,
}

impl NightshadeTask {
    fn new(
        owner_id: usize,
        num_authorities: usize,
        control_receiver: mpsc::Receiver<Control>,
        gossip_receiver: mpsc::Receiver<Gossip>,
        gossip_sender: mpsc::Sender<Gossip>,
        consensus_sender: mpsc::Sender<ConsensusBlock>,
    ) -> Self {
        NightshadeTask {
            owner_id,
            num_authorities,
            nightshade: None,
            control_receiver,
            gossip_receiver,
            gossip_sender,
            consensus_sender,
            cooldown_delay: None,
        }
    }

    fn nightshade_as_ref(&self) -> &Nightshade {
        self.nightshade.as_ref().expect("Nightshade should be initialized")
    }

    fn nightshade_as_mut_ref(&mut self) -> &mut Nightshade {
        self.nightshade.as_mut().expect("Nightshade should be initialized")
    }

    fn init_nightshade(&mut self) {
        self.nightshade = Some(
            Nightshade::new(
                self.owner_id as usize, self.num_authorities as usize));
    }

    fn process_gossip(&mut self, gossip: Gossip) {
        if self.owner_id == 0 {
            println!("Receive gossip: {:?}", gossip);
        }
        match gossip.body {
            GossipBody::Unsolicited(message) => self.process_messages(gossip.sender_id, vec![message]),
            GossipBody::Fetch(hashes) => self.respond_fetch(gossip.sender_id, &hashes),
            GossipBody::FetchReply(messages) => self.process_messages(gossip.sender_id, messages),
        }
    }

    fn process_messages(&mut self, sender_id: AuthorityId, messages: Vec<Message>) {
        match self.nightshade_as_mut_ref().process_messages(messages) {
            NSResult::Finalize(hash) => self.send_consensus(hash),
            NSResult::Retrieve(hashes) => self.retrieve_messages(sender_id, hashes),
            _ => {}
        }
    }

    fn respond_fetch(&self, sender_id: AuthorityId, hashes: &Vec<CryptoHash>) {
        let reply_messages: Vec<_> = hashes
            .iter()
            .filter_map(|h| self.nightshade_as_ref().copy_message_data_by_hash(h))
            .collect();
        if sender_id == 0 {
            // println!("All nodes: {:?}", self.nightshade_as_ref().nodes);
            println!("Reply messages: {:?} {:?}", hashes, reply_messages);
        }
        let reply = Gossip {
            sender_id: self.owner_id,
            receiver_id: sender_id,
            body: GossipBody::FetchReply(reply_messages)
        };
        self.send_gossip(reply);
    }

    fn send_consensus(&self, hash: CryptoHash) {
        if self.owner_id == 0 {
            println!("Consensus: {:?}", hash);
        }
        info!("Consensus: {:?}", hash);
        let copied_tx = self.consensus_sender.clone();
        let consensus = ConsensusBlock {
            parent_hash: hash,
            messages: vec![],
        };
        tokio::spawn(copied_tx.send(consensus).map(|_| ()).map_err(|e| {
           error!("Failure in the sub-task {:?}", e);
        }));
    }

    fn retrieve_messages(&self, sender_id: AuthorityId, hashes: Vec<CryptoHash>) {
        let gossip = Gossip {
            sender_id: self.owner_id,
            receiver_id: sender_id,
            body: GossipBody::Fetch(hashes)
        };
        self.send_gossip(gossip);
    }

    /// Sends a gossip by spawning a separate task.
    fn send_gossip(&self, gossip: Gossip) {
        let copied_tx = self.gossip_sender.clone();
        tokio::spawn(copied_tx.send(gossip).map(|_| ()).map_err(|e| {
            error!("Failure in the sub-task {:?}", e);
        }));
    }

    fn gossip_message(&self, message: Message) {
        let mut random_authorities = HashSet::new();
        while random_authorities.len() < min(GOSSIP_SAMPLE_SIZE, self.num_authorities - 1) {
            let next = rand::random::<usize>() % self.num_authorities;
            if next != self.owner_id {
                random_authorities.insert(next);
            }
        }
        for w in &random_authorities {
            let gossip = Gossip {
                sender_id: self.owner_id,
                receiver_id: *w as usize,
                body: GossipBody::Unsolicited(message.clone())
            };
            self.send_gossip(gossip);
        }
    }
}

impl Stream for NightshadeTask {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match self.control_receiver.poll() {
                Ok(Async::Ready(Some(Control::Reset))) => {
                    info!("Control channel received Reset");
                    self.init_nightshade();
                    break;
                },
                Ok(Async::Ready(Some(Control::Stop))) => {
                    info!("Control channel received Stop");
                    if self.nightshade.is_some() {
                        self.nightshade = None;
                    }
                    return Ok(Async::Ready(Some(())));
                },
                Ok(Async::Ready(None)) => {
                    info!("Control channel was dropped");
                    return Ok(Async::Ready(None));
                }
                Ok(Async::NotReady) => {
                    if self.nightshade.is_none() {
                        return Ok(Async::NotReady);
                    }
                    break;
                },
                Err(err) => error!("Failed to read from the control channel {:?}", err),
            }
        }
        let mut end_of_messages = false;
        loop {
            match self.gossip_receiver.poll() {
                Ok(Async::Ready(Some(gossip))) => self.process_gossip(gossip),
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(None)) => {
                    // End of the stream that feeds the messages.
                    end_of_messages = true;
                    break;
                }
                Err(err) => error!("Failed to receive a message {:?}", err),
            }
        }

        // The following code should be executed only if the cooldown has passed.
        if let Some(ref mut d) = self.cooldown_delay {
            try_ready!(d.poll().map_err(|e| error!("Cooldown timer error {}", e)));
        }

        // TODO: add payload here.
        let (message, nightshade_result) = self.nightshade.as_mut().expect("Nightshade should be init").create_message(vec![]);
        match nightshade_result {
            NSResult::Finalize(hash) => self.send_consensus(hash),
            _ => {}
        };
        self.gossip_message(message);

        let now = Instant::now();
        self.cooldown_delay = Some(Delay::new(now + Duration::from_millis(COOLDOWN_MS)));

        if self.owner_id == 0 {
            println!("Cooldown");
        }
        if end_of_messages {
            Ok(Async::Ready(None))
        } else {
            Ok(Async::NotReady)
        }
    }
}

fn spawn_all(num_authorities: usize) {
    tokio::run(lazy(move || {
        let mut inc_gossip_tx_vec = vec![];
        let mut out_gossip_rx_vec = vec![];
        let mut consensus_rx_vec = vec![];
        let mut control_tx_vec = vec![];
        for owner_id in 0..num_authorities {
            let (control_tx, control_rx) = mpsc::channel(1024);
            let (inc_gossip_tx, inc_gossip_rx) = mpsc::channel(1024);
            let (out_gossip_tx, out_gossip_rx) = mpsc::channel(1024);
            let (consensus_tx, consensus_rx) = mpsc::channel(1024);

            inc_gossip_tx_vec.push(inc_gossip_tx);
            out_gossip_rx_vec.push(out_gossip_rx);
            consensus_rx_vec.push(consensus_rx);
            control_tx_vec.push(control_tx.clone());

            let task = NightshadeTask::new(
                owner_id,
                num_authorities,
                control_rx,
                inc_gossip_rx,
                out_gossip_tx,
                consensus_tx
            );
            tokio::spawn(task.for_each(|_| Ok(())));

            let start_task = control_tx.clone()
                .send(Control::Reset)
                .map(|_| ()).map_err(|e| error!("Error sending control {}", e));
            tokio::spawn(start_task);
            let control_tx2 = control_tx.clone();
            let stop_task = Delay::new(Instant::now() + Duration::from_secs(1)).then(|_| {
                control_tx2
                    .send(Control::Stop)
                    .map(|_| ()).map_err(|e| error!("Error sending control {}", e))
            });
            tokio::spawn(stop_task);
        }

        for out_message_rx in out_gossip_rx_vec.drain(..) {
            // Spawn the network itself.
            let inc_message_tx_vec_1 = inc_gossip_tx_vec.clone();
            let f = out_message_rx.map(move |gossip: Gossip| {
                // println!("Sending gossip: {:?}", gossip);
                let gossip_input = inc_message_tx_vec_1[gossip.receiver_id as usize].clone();
                tokio::spawn(gossip_input
                    .send(gossip)
                    .map(|_| ())
                    .map_err(|e| error!("Error relaying gossip {:?}", e)));
            });
            tokio::spawn(f.for_each(|_| Ok(())));
        }

//        for (i, consensus_rx) in consensus_rx_vec.drain(..).enumerate() {
//            let control_tx_vec_1 = control_tx_vec.clone();
//            let f = consensus_rx.map(move |consensus| {
//                let control_input = control_tx_vec_1[i].clone();
//                tokio::spawn(control_input
//                    .send(Control::Stop)
//                    .map(|_| ())
//                    .map_err(|e| error!("Error stopping Nightshade {:?}", e)));
//            });
//            tokio::spawn(f.for_each(|_| Ok(())));
//        }

        Ok(())
    }));
}

#[cfg(test)]
mod tests {
    use super::spawn_all;

    #[test]
    fn one_authority() {
        spawn_all(1);
    }

    #[test]
    fn ten_authority() {
        spawn_all(5);
    }
}