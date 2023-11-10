use crate::client::ClientSenderForNetworkMessage;
use crate::shards_manager::ShardsManagerRequestFromNetwork;
use crate::sink::Sink;
use near_async::messaging;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    ShardsManager(ShardsManagerRequestFromNetwork),
    Client(ClientSenderForNetworkMessage),
}

pub(crate) struct Fake {
    pub event_sink: Sink<Event>,
}

impl messaging::CanSend<ClientSenderForNetworkMessage> for Fake {
    fn send(&self, message: ClientSenderForNetworkMessage) {
        self.event_sink.push(Event::Client(message));
    }
}

impl messaging::CanSend<ShardsManagerRequestFromNetwork> for Fake {
    fn send(&self, message: ShardsManagerRequestFromNetwork) {
        self.event_sink.push(Event::ShardsManager(message.into()));
    }
}
