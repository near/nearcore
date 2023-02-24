use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_primitives::time;

use crate::{
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
    let sender = builder.sender();
    let mut test = builder.build(data);
    test.register_handler(forward_sum_request().widen());
    test.register_handler(capture_events::<ReportSumMsg>().widen());

    sender.send(TestEvent::Request(SumRequest::Number(1)));
    sender.send(TestEvent::Request(SumRequest::Number(2)));
    sender.send(TestEvent::Request(SumRequest::GetSum));
    sender.send(TestEvent::Request(SumRequest::Number(3)));
    sender.send(TestEvent::Request(SumRequest::Number(4)));
    sender.send(TestEvent::Request(SumRequest::Number(5)));
    sender.send(TestEvent::Request(SumRequest::GetSum));

    test.run(time::Duration::milliseconds(1));
    assert_eq!(test.data.sums, vec![ReportSumMsg(3), ReportSumMsg(12)]);
}
