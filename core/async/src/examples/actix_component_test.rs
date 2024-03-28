use super::actix_component::{
    ExampleComponent, ExampleComponentAdapterMessage, OuterComponent, PeriodicRequest,
};
use crate::futures::FutureSpawnerExt;
use crate::messaging::IntoSender;
use crate::test_loop::event_handler::{capture_events, LoopEventHandler};
use crate::test_loop::futures::{drive_futures, TestLoopDelayedActionEvent, TestLoopTask};
use crate::test_loop::TestLoopBuilder;
use derive_enum_from_into::{EnumFrom, EnumTryInto};
use std::sync::Arc;
use time::Duration;

#[derive(derive_more::AsMut, derive_more::AsRef)]
struct ExampleComponentTestData {
    dummy: (),
    example: ExampleComponent,
    outer: OuterComponent,
    periodic_requests_captured: Vec<PeriodicRequest>,
}

#[derive(Debug, EnumTryInto, EnumFrom)]
enum ExampleComponentTestEvent {
    PeriodicRequest(PeriodicRequest),
    ExampleRequest(ExampleComponentAdapterMessage),
    // Needed to support DelayedActionRunner on the ExampleComponent.
    DelayedAction(TestLoopDelayedActionEvent<ExampleComponent>),
    // Arc<TestLoopTask> is needed to support futures.
    Task(Arc<TestLoopTask>),
}

fn example_handler() -> LoopEventHandler<ExampleComponent, ExampleComponentAdapterMessage> {
    LoopEventHandler::new_simple(
        |event: ExampleComponentAdapterMessage, data: &mut ExampleComponent| match event {
            ExampleComponentAdapterMessage::_example(request) => {
                let response = data.process_request(request.message);
                (request.callback)(Ok(response));
            }
        },
    )
}

#[test]
fn test_actix_component() {
    let builder = TestLoopBuilder::<ExampleComponentTestEvent>::new();
    let data = ExampleComponentTestData {
        dummy: (),
        example: ExampleComponent::new(builder.sender().into_sender()),
        outer: OuterComponent::new(
            builder.sender().into_wrapped_multi_sender::<ExampleComponentAdapterMessage, _>(),
        ),
        periodic_requests_captured: vec![],
    };
    let mut test = builder.build(data);
    // This is to allow futures to be used in the test even though the
    // test itself is synchronous.
    test.register_handler(drive_futures().widen());
    // This is to allow the ExampleComponent to run delayed actions (timers).
    test.register_delayed_action_handler::<ExampleComponent>();
    // This is to capture the periodic requests sent by the ExampleComponent
    // so we can assert against it.
    test.register_handler(capture_events::<PeriodicRequest>().widen());
    // This is to handle the ExampleComponentAdapterMessage events by
    // forwarding them to the ExampleComponent.
    test.register_handler(example_handler().widen());

    // We need to redo whatever the ExampleActor does in its `started` method.
    test.data.example.start(&mut test.sender().into_delayed_action_runner(test.shutting_down()));
    // Send some requests; this can be done in the asynchronous context.
    test.future_spawner().spawn("wait for 5", {
        let res = test.data.outer.call_example_component_for_response(5);
        async move {
            assert_eq!(res.await, 5);
        }
    });
    test.future_spawner().spawn("wait for 6", {
        let res = test.data.outer.call_example_component_for_response(6);
        async move {
            assert_eq!(res.await, 6);
        }
    });
    // Run for 3 seconds (not real time, but in the test loop time).
    // It should result in sending 3 periodic requests.
    test.run_for(Duration::seconds(3));
    assert_eq!(
        test.data.periodic_requests_captured,
        vec![PeriodicRequest { id: 0 }, PeriodicRequest { id: 1 }, PeriodicRequest { id: 2 },]
    );

    test.shutdown_and_drain_remaining_events(Duration::seconds(1));
}
