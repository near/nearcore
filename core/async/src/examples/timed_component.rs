use crate::messaging::Sender;

pub(crate) struct TimedComponent {
    buffered_messages: Vec<String>,
    message_sender: Sender<Vec<String>>,
}

/// Mimics a component that has a specific function that is supposed to be
/// triggered by a timer.
impl TimedComponent {
    pub fn new(message_sender: Sender<Vec<String>>) -> Self {
        Self { buffered_messages: vec![], message_sender }
    }

    pub fn send_message(&mut self, msg: String) {
        self.buffered_messages.push(msg);
    }

    /// This is supposed to be triggered by a timer so it flushes the
    /// messages every tick.
    pub fn flush(&mut self) {
        if self.buffered_messages.is_empty() {
            return;
        }
        self.message_sender.send(self.buffered_messages.clone());
        self.buffered_messages.clear();
    }
}
