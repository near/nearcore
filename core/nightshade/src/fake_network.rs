use super::{Nightshade, Message};
use tokio::timer::Delay;
use futures::sync::mpsc;
use futures::{stream, Async, Future, Poll, Sink, Stream};
use futures::future::{join_all, lazy};

struct NightshadeTask {
    nightshade: Nightshade,
    messages_receiver: mpsc::Receiver<Message>,
    messages_sender: mpsc::Sender<Message>,
}

impl NightshadeTask {
    fn new(
        owner_id: u64,
        num_authorities: u64,
        messages_receiver: mpsc::Receiver<Message>,
        messages_sender: mpsc::Sender<Message>,
    ) -> Self {
        NightshadeTask {
            nightshade: Nightshade::new(owner_id as usize, num_authorities as usize),
            messages_receiver,
            messages_sender
        }
    }

    fn process_message(&self, message: Message) {

    }
}

impl Stream for NightshadeTask {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
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
            let (inc_message_tx, inc_message_rx) = mpsc::channel(1024);
            let task = NightshadeTask::new(
                owner_id,
                num_authorities,
                inc_message_rx,
                inc_message_tx,
            );
            tokio::spawn(task.for_each(|_| Ok(())));
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