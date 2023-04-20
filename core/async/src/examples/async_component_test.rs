use derive_enum_from_into::{EnumFrom, EnumTryInto};
use std::sync::Arc;

use crate::{
    messaging::{CanSend, IntoAsyncSender, IntoSender},
    test_loop::{
        event_handler::{capture_events, LoopEventHandler},
        futures::{drive_futures, MessageExpectingResponse, TestLoopFutureSpawner, TestLoopTask},
        TestLoopBuilder,
    },
};

use super::async_component::{
    InnerComponent, InnerRequest, InnerResponse, OuterComponent, OuterRequest, OuterResponse,
};

#[derive(derive_more::AsMut, derive_more::AsRef)]
struct TestData {
    dummy: (),                  // needed for any handlers that don't require data
    output: Vec<OuterResponse>, // needed for capture_events handler
    inner_component: InnerComponent,
    outer_component: OuterComponent,
}

#[derive(Debug, EnumTryInto, EnumFrom)]
enum TestEvent {
    OuterResponse(OuterResponse),
    OuterRequest(OuterRequest),
    // Requests that need responses need to use MessageExpectingResponse.
    InnerRequest(MessageExpectingResponse<InnerRequest, InnerResponse>),
    // Arc<TestLoopTask> is needed to support futures.
    Task(Arc<TestLoopTask>),
}

fn outer_request_handler(
    future_spawner: TestLoopFutureSpawner,
) -> LoopEventHandler<OuterComponent, OuterRequest> {
    LoopEventHandler::new_simple(move |event, data: &mut OuterComponent| {
        data.process_request(event, &future_spawner);
    })
}

fn inner_request_handler(
) -> LoopEventHandler<InnerComponent, MessageExpectingResponse<InnerRequest, InnerResponse>> {
    LoopEventHandler::new_simple(
        |event: MessageExpectingResponse<InnerRequest, InnerResponse>,
         data: &mut InnerComponent| {
            (event.responder)(data.process_request(event.message));
        },
    )
}

#[test]
fn test_async_component() {
    let builder = TestLoopBuilder::<TestEvent>::new();
    let sender = builder.sender();
    let future_spawner = builder.future_spawner();
    let mut test = builder.build(TestData {
        dummy: (),
        output: vec![],
        inner_component: InnerComponent,
        outer_component: OuterComponent::new(
            sender.clone().into_async_sender(),
            sender.clone().into_sender(),
        ),
    });
    test.register_handler(drive_futures().widen());
    test.register_handler(capture_events::<OuterResponse>().widen());
    test.register_handler(outer_request_handler(future_spawner).widen());
    test.register_handler(inner_request_handler().widen());

    sender.send(OuterRequest("hello".to_string()));
    test.run_instant();
    assert_eq!(test.data.output, vec![OuterResponse("hello!hello!".to_string())]);
}
