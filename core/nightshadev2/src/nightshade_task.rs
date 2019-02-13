use std::time::Duration;
use std::time::Instant;

use futures::Async;
use futures::future::Future;
use futures::Poll;
use futures::sink::Sink;
use futures::Stream;
use futures::sync::mpsc;
use futures::try_ready;
use log::{error, info};
use tokio::timer::Delay;

use super::nightshade::{AuthorityId, Nightshade, State};

pub enum Control {
    Reset,
    Stop,
}

#[derive(Clone)]
pub struct Message {
    pub author: AuthorityId,
    pub receiver_id: AuthorityId,
    pub state: State,
}

const COOLDOWN_MS: u64 = 50;

pub struct NightshadeTask {
    owner_id: AuthorityId,
    num_authorities: usize,
    nightshade: Option<Nightshade>,
    state_receiver: mpsc::Receiver<Message>,
    state_sender: mpsc::Sender<Message>,
    control_receiver: mpsc::Receiver<Control>,
    consensus_sender: mpsc::Sender<AuthorityId>,
    consensus_reached: Option<AuthorityId>,
    /// Timer that determines the minimum time that we should not gossip after the given message
    /// for the sake of not spamming the network with small packages.
    cooldown_delay: Option<Delay>,
}

impl NightshadeTask {
    pub fn new(
        owner_id: AuthorityId,
        num_authorities: usize,
        state_receiver: mpsc::Receiver<Message>,
        state_sender: mpsc::Sender<Message>,
        control_receiver: mpsc::Receiver<Control>,
        consensus_sender: mpsc::Sender<AuthorityId>,
    ) -> Self {
        Self {
            owner_id,
            num_authorities,
            nightshade: None,
            state_receiver,
            state_sender,
            control_receiver,
            consensus_sender,
            consensus_reached: None,
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
            Nightshade::new(self.owner_id as AuthorityId, self.num_authorities as usize));
    }

    fn state(&self) -> State {
        self.nightshade_as_ref().state()
    }

    fn send_state(&self, message: Message) {
//        println!("Sending status from: {:?} to: {:?} - {:?}",
//                 self.owner_id, message.receiver_id, message.state.bare_state);

        let copied_tx = self.state_sender.clone();
        tokio::spawn(copied_tx.send(message).map(|_| ()).map_err(|e| {
            error!("Error sending state. {:?}", e);
        }));
    }

    #[allow(dead_code)]
    fn is_final(&self) -> bool {
        self.nightshade_as_ref().is_final()
    }

    fn process_message(&mut self, message: Message) {
        self.nightshade_as_mut_ref().update_state(message.author, message.state);
    }

    /// Sends gossip to random authority peers.
    fn gossip_state(&self) {
        let my_state = self.state();

        for i in 0..self.num_authorities {
            if i != self.owner_id {
                let message = Message {
                    author: self.owner_id,
                    receiver_id: i,
                    state: my_state.clone(),
                };
                self.send_state(message);
            }
        }
    }
}

impl Stream for NightshadeTask {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Control loop
        loop {
            match self.control_receiver.poll() {
                Ok(Async::Ready(Some(Control::Reset))) => {
                    info!(target: "nightshade", "Control channel received Reset");
                    self.init_nightshade();
                    break;
                }
                Ok(Async::Ready(Some(Control::Stop))) => {
                    info!(target: "nightshade", "Control channel received Stop");
                    if self.nightshade.is_some() {
                        self.nightshade = None;
                        // On the next call of poll if we still don't have the state this task will be
                        // parked because we will return NotReady.
                        return Ok(Async::Ready(Some(())));
                    }
                    // Otherwise loop until we encounter Reset command in the stream. If the stream
                    // is NotReady this will automatically park the task.
                }
                Ok(Async::Ready(None)) => {
                    info!(target: "nightshade", "Control channel was dropped");
                    return Ok(Async::Ready(None));
                }
                Ok(Async::NotReady) => {
                    if self.nightshade.is_none() {
                        // If there is no state then we cannot proceed, we return NotReady which
                        // will park the task and wait until we get the state over the control
                        // channel.
                        return Ok(Async::NotReady);
                    }
                    // If there is a state then we do not care about the control.
                    break;
                }
                Err(err) => error!(target: "nightshade", "Failed to read from the control channel {:?}", err),
            }
        }

        // Process new messages
        let mut end_of_messages = false;
        loop {
            match self.state_receiver.poll() {
                Ok(Async::Ready(Some(message))) => {
                    self.process_message(message);

                    // Report as soon as possible when an authority reach consensus on some outcome
                    if self.consensus_reached == None {
                        if let Some(outcome) = self.nightshade_as_ref().committed {
                            self.consensus_reached = Some(outcome);

                            let consensus_sender1 = self.consensus_sender.clone();

                            tokio::spawn(consensus_sender1.send(outcome)
                                .map(|_| ())
                                .map_err(|e| error!("Failed sending consensus: {:?}", e))
                            );
                        }
                    }
                }
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(None)) => {
                    // End of the stream that feeds the messages.
                    end_of_messages = true;
                    break;
                }
                Err(err) => error!(target: "nightshade", "Failed to receive a gossip {:?}", err),
            }
        }

        // Send your state if the cooldown has passed
        if let Some(ref mut d) = self.cooldown_delay {
            try_ready!(d.poll().map_err(|e| error!("Cooldown timer error {}", e)));
        }

        self.gossip_state();

        let now = Instant::now();
        self.cooldown_delay = Some(Delay::new(now + Duration::from_millis(COOLDOWN_MS)));

        if end_of_messages {
            Ok(Async::Ready(None))
        } else {
            Ok(Async::Ready(Some(())))
        }
    }
}