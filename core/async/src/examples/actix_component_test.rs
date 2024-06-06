use super::actix_component::{
    ExampleComponent, ExampleComponentAdapterMessage, OuterComponent, PeriodicRequest,
};
use crate::futures::FutureSpawnerExt;
use crate::messaging::IntoSender;
use crate::test_loop::futures::{
    drive_delayed_action_runners, drive_futures, TestLoopDelayedActionEvent, TestLoopTask,
};
use crate::v2::{self};
use std::sync::Arc;
use time::Duration;

#[test]
fn test_actix_component() {
    let mut test = v2::TestLoop::new();
    let shutting_down = test.shutting_down();
    let periodic_request_stream = test.new_stream::<PeriodicRequest>();
    let example_adapter_stream = test.new_stream::<ExampleComponentAdapterMessage>();

    let example =
        test.add_data(ExampleComponent::new(periodic_request_stream.delay_sender().into_sender()));
    let outer = test.add_data(OuterComponent::new(
        example_adapter_stream
            .delay_sender()
            .into_wrapped_multi_sender::<ExampleComponentAdapterMessage, _>(),
    ));
    let periodic_requests_captured = test.add_data(Vec::<PeriodicRequest>::new());

    let futures_stream = test.new_stream::<Arc<TestLoopTask>>();
    futures_stream.handle0_legacy(&mut test, drive_futures());

    let example_component_delayed_actions =
        test.new_stream::<TestLoopDelayedActionEvent<ExampleComponent>>();
    example_component_delayed_actions.handle1_legacy(
        &mut test,
        example,
        drive_delayed_action_runners(
            example_component_delayed_actions.delay_sender(),
            shutting_down.clone(),
        ),
    );

    periodic_request_stream.handle1(&mut test, periodic_requests_captured, |event, data| {
        data.push(event);
        Ok(())
    });

    example_adapter_stream.handle1(&mut test, example, |event, data| match event {
        ExampleComponentAdapterMessage::_example(request) => {
            let response = data.process_request(request.message);
            (request.callback)(Ok(response));
            Ok(())
        }
    });

    // We need to redo whatever the ExampleActor does in its `started` method.
    test.data_mut(example).start(
        &mut example_component_delayed_actions
            .delay_sender()
            .into_delayed_action_runner(shutting_down),
    );
    // Send some requests; this can be done in the asynchronous context.
    futures_stream.delay_sender().spawn("wait for 5", {
        let res = test.data_mut(outer).call_example_component_for_response(5);
        async move {
            assert_eq!(res.await, 5);
        }
    });
    futures_stream.delay_sender().spawn("wait for 6", {
        let res = test.data_mut(outer).call_example_component_for_response(6);
        async move {
            assert_eq!(res.await, 6);
        }
    });
    // Run for 3 seconds (not real time, but in the test loop time).
    // It should result in sending 3 periodic requests.
    test.run_for(Duration::seconds(3));
    assert_eq!(
        test.data(periodic_requests_captured),
        &vec![PeriodicRequest { id: 0 }, PeriodicRequest { id: 1 }, PeriodicRequest { id: 2 },]
    );

    test.shutdown_and_drain_remaining_events(Duration::seconds(1));
}
