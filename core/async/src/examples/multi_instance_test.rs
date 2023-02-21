use std::time::Duration;

use derive_enum_from_into::{EnumFrom, EnumTryInto};

use crate::{
    examples::sum_numbers_test::ForwardSumRequest,
    messaging::{CanSend, IntoSender},
    test_loop::{
        delay_sender::DelaySender,
        event_handler::{capture_events, LoopEventHandler},
        TestLoopBuilder,
    },
};

use super::sum_numbers::{ReportSumMsg, SumNumbersComponent, SumRequest};

#[derive(derive_more::AsMut, derive_more::AsRef)]
struct TestData {
    summer: SumNumbersComponent,
    sums: Vec<ReportSumMsg>,
}

#[derive(Debug, EnumTryInto, EnumFrom)]
enum TestEvent {
    RemoteRequest(i64),
    LocalRequest(SumRequest),
    Sum(ReportSumMsg),
}

/// Let's pretend that when we send a remote request, the number gets sent to
/// every other instance in the setup as a local request.
pub struct ForwardRemoteRequestToOtherInstances {
    sender: Option<DelaySender<(usize, TestEvent)>>,
}

impl ForwardRemoteRequestToOtherInstances {
    pub fn new() -> Self {
        Self { sender: None }
    }
}

impl LoopEventHandler<Vec<TestData>, (usize, TestEvent)> for ForwardRemoteRequestToOtherInstances {
    fn init(&mut self, sender: DelaySender<(usize, TestEvent)>) {
        self.sender = Some(sender);
    }

    fn handle(
        &mut self,
        event: (usize, TestEvent),
        data: &mut Vec<TestData>,
    ) -> Result<(), (usize, TestEvent)> {
        if let TestEvent::RemoteRequest(number) = event.1 {
            for i in 0..data.len() {
                if i != event.0 {
                    self.sender
                        .as_ref()
                        .unwrap()
                        .send((i, TestEvent::LocalRequest(SumRequest::Number(number))))
                }
            }
            Ok(())
        } else {
            Err(event)
        }
    }
}

#[test]
fn test_multi_instance() {
    let builder = TestLoopBuilder::<(usize, TestEvent)>::new();
    // Build the SumNumberComponents so that it sends messages back to the test loop.
    let mut data = vec![];
    for i in 0..5 {
        data.push(TestData {
            // Multi-instance sender can be converted to a single-instance sender
            // so we can pass it into a component's constructor.
            summer: SumNumbersComponent::new(builder.sender().for_index(i).into_sender()),
            sums: vec![],
        });
    }
    let sender = builder.sender();
    let mut test = builder.build(data);
    test.register_handler(ForwardRemoteRequestToOtherInstances::new());
    for i in 0..5 {
        // Single-instance handlers can be reused for multi-instance tests.
        test.register_handler(ForwardSumRequest.widen().for_index(i));
        test.register_handler(capture_events::<ReportSumMsg>().widen().for_index(i));
    }

    // Send a RemoteRequest from each instance.
    sender.send((0, TestEvent::RemoteRequest(1)));
    sender.send((1, TestEvent::RemoteRequest(2)));
    sender.send((2, TestEvent::RemoteRequest(3)));
    sender.send((3, TestEvent::RemoteRequest(4)));
    sender.send((4, TestEvent::RemoteRequest(5)));

    // Then send a GetSum request for each instance; we use a delay so that we can ensure
    // these messages arrive later. (In a real test we wouldn't do this - the component would
    // automatically emit some events and we would assert on these events. But for this
    // contrived test we'll do it manually as a demonstration.)
    for i in 0..5 {
        sender.send_with_delay(
            (i, TestEvent::LocalRequest(SumRequest::GetSum)),
            Duration::from_millis(1),
        );
    }
    test.run(Duration::from_millis(2));
    assert_eq!(test.data[0].sums, vec![ReportSumMsg(14)]);
    assert_eq!(test.data[1].sums, vec![ReportSumMsg(13)]);
    assert_eq!(test.data[2].sums, vec![ReportSumMsg(12)]);
    assert_eq!(test.data[3].sums, vec![ReportSumMsg(11)]);
    assert_eq!(test.data[4].sums, vec![ReportSumMsg(10)]);
}
