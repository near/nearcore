use std::time::Duration;

use derive_enum_from_into::{EnumFrom, EnumTryInto};

use crate::{
    messaging::IntoSender,
    test_loop::event_handler::{capture_events, periodic_interval, LoopEventHandler},
};

use super::timed_component::TimedComponent;

#[derive(Debug, Clone, PartialEq)]
struct Flush;

#[derive(Debug, EnumTryInto, EnumFrom)]
enum TestEvent {
    SendMessage(String),
    Flush(Flush),
    MessageSent(Vec<String>),
}

#[derive(derive_more::AsMut, derive_more::AsRef)]
struct TestData {
    component: TimedComponent,
    messages_sent: Vec<Vec<String>>,
}

struct ForwardSendMessage;

impl LoopEventHandler<TimedComponent, String> for ForwardSendMessage {
    fn handle(&mut self, event: String, data: &mut TimedComponent) -> Result<(), String> {
        data.send_message(event);
        Ok(())
    }
}

#[test]
fn test_timed_component() {
    let builder = crate::test_loop::TestLoopBuilder::<TestEvent>::new();
    let data = TestData {
        component: TimedComponent::new(builder.sender().into_sender()),
        messages_sent: vec![],
    };
    let sender = builder.sender();
    let mut test = builder.build(data);
    test.register_handler(ForwardSendMessage.widen());
    test.register_handler(
        periodic_interval(Duration::from_millis(100), Flush, |data: &mut TimedComponent| {
            data.flush()
        })
        .widen(),
    );
    test.register_handler(capture_events::<Vec<String>>().widen());

    sender.send_with_delay("Hello".to_string().into(), Duration::from_millis(10));
    sender.send_with_delay("World".to_string().into(), Duration::from_millis(20));
    // The timer fires at 100ms here and flushes "Hello" and "World".
    sender.send_with_delay("!".to_string().into(), Duration::from_millis(110));
    // The timer fires again at 200ms here and flushes "!"".
    // Further timer events do not send messages.

    test.run(Duration::from_secs(1));
    assert_eq!(
        test.data.messages_sent,
        vec![vec!["Hello".to_string(), "World".to_string()], vec!["!".to_string()]]
    );
}
