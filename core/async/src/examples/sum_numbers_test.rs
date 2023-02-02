use std::{sync::Arc, time::Duration};

use derive_enum_from_into::{EnumFrom, EnumTryInto};

use crate::{
    messaging::Sender,
    test_loop::{CaptureEvents, LoopEventHandler, TestLoopBuilder, TryIntoOrSelf},
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
pub struct ForwardSumRequest;

impl<Data: AsMut<SumNumbersComponent>, Event: TryIntoOrSelf<SumRequest>>
    LoopEventHandler<Data, Event> for ForwardSumRequest
{
    fn handle(&mut self, event: Event, data: &mut Data) -> Option<Event> {
        match event.try_into_or_self() {
            Ok(request) => {
                data.as_mut().handle(request);
                None
            }
            Err(event) => Some(event),
        }
    }
}

#[test]
fn test_simple() {
    let builder = TestLoopBuilder::<TestEvent>::new();
    // Build the SumNumberComponents so that it sends messages back to the test loop.
    let data =
        TestData { summer: SumNumbersComponent::new(Arc::new(builder.sender())), sums: vec![] };
    let sender = builder.sender();
    let mut test = builder.build(data);
    test.register_handler(ForwardSumRequest);
    test.register_handler(CaptureEvents::<ReportSumMsg>::new());

    sender.send(TestEvent::Request(SumRequest::Number(1)));
    sender.send(TestEvent::Request(SumRequest::Number(2)));
    sender.send(TestEvent::Request(SumRequest::GetSum));
    sender.send(TestEvent::Request(SumRequest::Number(3)));
    sender.send(TestEvent::Request(SumRequest::Number(4)));
    sender.send(TestEvent::Request(SumRequest::Number(5)));
    sender.send(TestEvent::Request(SumRequest::GetSum));

    test.run(Duration::from_millis(1));
    assert_eq!(test.data.sums, vec![ReportSumMsg(3), ReportSumMsg(12)]);
}
