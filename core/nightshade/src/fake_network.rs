use super::{Nightshade, Message, NSResult};
use primitives::hash::CryptoHash;
use tokio::timer::Delay;
use futures::sync::mpsc;
use futures::{stream, Async, Future, Poll, Sink, Stream};
use futures::future::{join_all, lazy};
use std::time::{Duration, Instant};
use futures::try_ready;
use log::{info, debug, error};

struct NightshadeTask {
    nightshade: Nightshade,
    control_receiver: mpsc::Receiver<Control>,
    messages_receiver: mpsc::Receiver<Message>,
    messages_sender: mpsc::Sender<Message>,

    /// Timer that determines the minimum time that we should not gossip after the given message
    /// for the sake of not spamming the network with small packages.
    cooldown_delay: Option<Delay>,
}

enum Control {
    Reset,
    Stop
}

const COOLDOWN_MS: u64 = 500;

impl NightshadeTask {
    fn new(
        owner_id: u64,
        num_authorities: u64,
        control_receiver: mpsc::Receiver<Control>,
        messages_receiver: mpsc::Receiver<Message>,
        messages_sender: mpsc::Sender<Message>,
    ) -> Self {
        NightshadeTask {
            nightshade: Nightshade::new(owner_id as usize, num_authorities as usize),
            control_receiver,
            messages_receiver,
            messages_sender,
            cooldown_delay: None,
        }
    }

    fn process_message(&mut self, message: Message) {
        match self.nightshade.process_messages(vec![message]) {
            NSResult::Finalize(hash) => self.send_consensus(hash),
            NSResult::Retrieve(hashes) => self.retrieve_messages(hashes),
            _ => {}
        }
    }

    fn send_consensus(&self, hash: CryptoHash) {
        println!("Consensus: {}", hash);
    }

    fn retrieve_messages(&self, hashes: Vec<CryptoHash>) {
        println!("Fetch messages: {:?}", hashes);
    }

    /// Sends a gossip by spawning a separate task.
    fn send_gossip(&self, message: Message) {
        println!("Send message: {:?}", message);
        let copied_tx = self.messages_sender.clone();
        tokio::spawn(copied_tx.send(message).map(|_| ()).map_err(|e| {
            error!("Failure in the sub-task {:?}", e);
        }));
    }
}

impl Stream for NightshadeTask {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match self.control_receiver.poll() {
                Ok(Async::Ready(Some(Control::Reset))) => {
                    // TODO: reset.
                    info!("Control channel received Reset");
                    break;
                },
                Ok(Async::Ready(Some(Control::Stop))) => {
                    info!("Control channel received Stop");
                    return Ok(Async::Ready(Some(())));
                },
                Ok(Async::Ready(None)) => {
                    info!("Control channel was dropped");
                    return Ok(Async::Ready(None));
                }
                Ok(Async::NotReady) => {
                    return Ok(Async::NotReady);
                },
                Err(err) => error!("Failed to read from the control channel {:?}", err),
            }
        }
        let mut end_of_messages = false;
        loop {
            match self.messages_receiver.poll() {
                Ok(Async::Ready(Some(message))) => self.process_message(message),
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
        let (message, nightshade_result) = self.nightshade.create_message(vec![]);
        match nightshade_result {
            NSResult::Finalize(hash) => self.send_consensus(hash),
            _ => {}
        };
        self.send_gossip(message);

        let now = Instant::now();
        self.cooldown_delay = Some(Delay::new(now + Duration::from_millis(COOLDOWN_MS)));

        if end_of_messages {
            Ok(Async::Ready(None))
        } else {
            Ok(Async::NotReady)
        }
    }
}

fn spawn_all(num_authorities: u64) {
    tokio::run(lazy(move || {
        for owner_id in 0..num_authorities {
            let (control_tx, control_rx) = mpsc::channel(1024);
            let (inc_message_tx, inc_message_rx) = mpsc::channel(1024);
            let task = NightshadeTask::new(
                owner_id,
                num_authorities,
                control_rx,
                inc_message_rx,
                inc_message_tx,
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
}