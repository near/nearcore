use std::{sync::Arc, time::Duration};

use derive_enum_from_into::{EnumFrom, EnumTryInto};

use crate::{
    messaging::{CanSend, IntoAsyncSender, IntoSender},
    test_loop::{
        delay_sender::DelaySender,
        event_handler::{capture_events, LoopEventHandler},
        futures::{DriveFutures, MessageExpectingResponse, TestLoopTask},
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

struct OuterRequestHandler {
    future_spawner: DelaySender<Arc<TestLoopTask>>,
}

impl LoopEventHandler<OuterComponent, OuterRequest> for OuterRequestHandler {
    fn handle(
        &mut self,
        event: OuterRequest,
        data: &mut OuterComponent,
    ) -> Result<(), OuterRequest> {
        data.process_request(event, &self.future_spawner);
        Ok(())
    }
}

struct InnerRequestHandler;

impl LoopEventHandler<InnerComponent, MessageExpectingResponse<InnerRequest, InnerResponse>>
    for InnerRequestHandler
{
    fn handle(
        &mut self,
        event: MessageExpectingResponse<InnerRequest, InnerResponse>,
        data: &mut InnerComponent,
    ) -> Result<(), MessageExpectingResponse<InnerRequest, InnerResponse>> {
        (event.responder)(data.process_request(event.message));
        Ok(())
    }
}

#[test]
fn test_async_component() {
    let builder = TestLoopBuilder::<TestEvent>::new();
    let sender = builder.sender();
    let mut test = builder.build(TestData {
        dummy: (),
        output: vec![],
        inner_component: InnerComponent,
        outer_component: OuterComponent::new(
            sender.clone().into_async_sender(),
            sender.clone().into_sender(),
        ),
    });
    test.register_handler(DriveFutures.widen());
    test.register_handler(capture_events::<OuterResponse>().widen());
    test.register_handler(OuterRequestHandler { future_spawner: sender.clone().narrow() }.widen());
    test.register_handler(InnerRequestHandler.widen());

    sender.send(OuterRequest("hello".to_string()));
    test.run(Duration::from_secs(1));
    assert_eq!(test.data.output, vec![OuterResponse("hello!hello!".to_string())]);
}
