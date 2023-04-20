use super::{
    delay_sender::DelaySender,
    event_handler::{LoopEventHandler, TryIntoOrSelf},
};
use crate::messaging::CanSend;
use crate::time;
use std::fmt::Debug;

/// Any arbitrary logic that runs as part of the test loop.
///
/// This is not necessary (since one can just take the data and perform
/// arbitrary logic on it), but this is good for documentation and allows
/// the logs emitted as part of this function's execution to be segmented
/// in the TestLoop visualizer.
pub struct AdhocEvent<Data: 'static> {
    pub description: String,
    pub handler: Box<dyn FnOnce(&mut Data) + Send + 'static>,
}

impl<Data> Debug for AdhocEvent<Data> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.description)
    }
}

/// Allows DelaySender to be used to send or schedule adhoc events.
pub trait AdhocEventSender<Data: 'static> {
    fn send_adhoc_event(&self, description: &str, f: impl FnOnce(&mut Data) + Send + 'static);
    fn schedule_adhoc_event(
        &self,
        description: &str,
        f: impl FnOnce(&mut Data) + Send + 'static,
        delay: time::Duration,
    );
}

impl<Data: 'static, Event: From<AdhocEvent<Data>> + 'static> AdhocEventSender<Data>
    for DelaySender<Event>
{
    fn send_adhoc_event(&self, description: &str, f: impl FnOnce(&mut Data) + Send + 'static) {
        self.send(AdhocEvent { description: description.to_string(), handler: Box::new(f) })
    }
    fn schedule_adhoc_event(
        &self,
        description: &str,
        f: impl FnOnce(&mut Data) + Send + 'static,
        delay: time::Duration,
    ) {
        self.send_with_delay(
            AdhocEvent { description: description.to_string(), handler: Box::new(f) }.into(),
            delay,
        )
    }
}

/// Handler to handle adhoc events.
pub fn handle_adhoc_events<Data: 'static, Event: TryIntoOrSelf<AdhocEvent<Data>>>(
) -> LoopEventHandler<Data, Event> {
    LoopEventHandler::new(|event: Event, data, _ctx| {
        let event = event.try_into_or_self()?;
        (event.handler)(data);
        Ok(())
    })
}
