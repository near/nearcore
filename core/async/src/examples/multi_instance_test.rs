use crate::time;
use derive_enum_from_into::{EnumFrom, EnumTryInto};

use crate::{
    examples::sum_numbers_test::forward_sum_request,
    messaging::{CanSend, IntoSender},
    test_loop::{
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
fn forward_remote_request_to_other_instances() -> LoopEventHandler<Vec<TestData>, (usize, TestEvent)>
{
    LoopEventHandler::new(|event: (usize, TestEvent), data: &mut Vec<TestData>, context| {
        if let TestEvent::RemoteRequest(number) = event.1 {
            for i in 0..data.len() {
                if i != event.0 {
                    context.sender.send((i, TestEvent::LocalRequest(SumRequest::Number(number))))
                }
            }
            Ok(())
        } else {
            Err(event)
        }
    })
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
    test.register_handler(forward_remote_request_to_other_instances());
    for i in 0..5 {
        // Single-instance handlers can be reused for multi-instance tests.
        test.register_handler(forward_sum_request().widen().for_index(i));
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
            time::Duration::milliseconds(1),
        );
    }
    test.run_for(time::Duration::milliseconds(2));
    assert_eq!(test.data[0].sums, vec![ReportSumMsg(14)]);
    assert_eq!(test.data[1].sums, vec![ReportSumMsg(13)]);
    assert_eq!(test.data[2].sums, vec![ReportSumMsg(12)]);
    assert_eq!(test.data[3].sums, vec![ReportSumMsg(11)]);
    assert_eq!(test.data[4].sums, vec![ReportSumMsg(10)]);
}
