use crate::time;
use derive_enum_from_into::{EnumFrom, EnumTryInto};

use crate::{
    messaging::{CanSend, IntoSender},
    test_loop::{
        adhoc::{handle_adhoc_events, AdhocEvent, AdhocEventSender},
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
    Request(SumRequest),
    Sum(ReportSumMsg),
}

// Handler that forwards SumRequest messages to the SumNumberComponent.
// Note that typically we would have a single handler like this, and it can
// be reused for any test that needs to send messages to this component.
pub fn forward_sum_request() -> LoopEventHandler<SumNumbersComponent, SumRequest> {
    LoopEventHandler::new_simple(|event, data: &mut SumNumbersComponent| {
        data.handle(event);
    })
}

#[test]
fn test_simple() {
    let builder = TestLoopBuilder::<TestEvent>::new();
    // Build the SumNumberComponents so that it sends messages back to the test loop.
    let data =
        TestData { summer: SumNumbersComponent::new(builder.sender().into_sender()), sums: vec![] };
    let mut test = builder.build(data);
    test.register_handler(forward_sum_request().widen());
    test.register_handler(capture_events::<ReportSumMsg>().widen());

    test.sender().send(SumRequest::Number(1));
    test.sender().send(SumRequest::Number(2));
    test.sender().send(SumRequest::GetSum);
    test.sender().send(SumRequest::Number(3));
    test.sender().send(SumRequest::Number(4));
    test.sender().send(SumRequest::Number(5));
    test.sender().send(SumRequest::GetSum);

    test.run_for(time::Duration::milliseconds(1));
    assert_eq!(test.data.sums, vec![ReportSumMsg(3), ReportSumMsg(12)]);
}

#[derive(Debug, EnumTryInto, EnumFrom)]
enum TestEventWithAdhoc {
    Request(SumRequest),
    Sum(ReportSumMsg),
    Adhoc(AdhocEvent<TestData>),
}

#[test]
fn test_simple_with_adhoc() {
    let builder = TestLoopBuilder::<TestEventWithAdhoc>::new();
    // Build the SumNumberComponents so that it sends messages back to the test loop.
    let data =
        TestData { summer: SumNumbersComponent::new(builder.sender().into_sender()), sums: vec![] };
    let mut test = builder.build(data);
    test.register_handler(forward_sum_request().widen());
    test.register_handler(capture_events::<ReportSumMsg>().widen());
    test.register_handler(handle_adhoc_events());

    // It is preferrable to put as much setup logic as possible into an adhoc
    // event (queued by .run below), so that as much logic as possible is
    // executed in the TestLoop context. This allows the setup logic to show
    // up in the visualizer too, with any of its logging shown under the
    // adhoc event.
    let sender = test.sender();
    test.sender().send_adhoc_event("initial events", move |_| {
        sender.send(SumRequest::Number(1));
        sender.send(SumRequest::Number(2));
        sender.send(SumRequest::GetSum);
        sender.send(SumRequest::Number(3));
        sender.send(SumRequest::Number(4));
        sender.send(SumRequest::Number(5));
        sender.send(SumRequest::GetSum);
    });

    test.run_instant();

    // We can put assertions inside an adhoc event as well. This is
    // especially useful if we had a multi-instance test, so that in the
    // visualizer we can easily see which assertion was problematic.
    //
    // Here, we queue these events after the first test.run call, so we
    // need to remember to call test.run again to actually execute them.
    // Alternatively we can use test.sender().schedule_adhoc_event to queue
    // the assertion events after the logic we expect to execute has been
    // executed; that way we only need to call test.run once. Either way,
    // don't worry if you forget to call test.run again; the test will
    // panic at the end if there are unhandled events.
    test.sender().send_adhoc_event("assertions", |data| {
        assert_eq!(data.sums, vec![ReportSumMsg(3), ReportSumMsg(12)]);
    });
    test.run_instant();
}
