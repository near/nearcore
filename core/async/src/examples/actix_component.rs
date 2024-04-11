use crate as near_async; // only needed because we're in this crate itself
use crate::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use crate::messaging::{AsyncSender, SendAsync, Sender};
use crate::time::Duration;
use futures::future::BoxFuture;
use futures::FutureExt;
use near_async_derive::{MultiSend, MultiSendMessage, MultiSenderFrom};
use std::ops::{Deref, DerefMut};

#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "ExampleResponse")]
pub struct ExampleRequest {
    pub id: u32,
}

#[derive(actix::MessageResponse, Debug, Clone, PartialEq, Eq)]
pub struct ExampleResponse {
    pub id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeriodicRequest {
    pub id: u32,
}

/// An example component that represents the backing state of an actor.
///
/// It supports two functionalities: processing of ExampleRequest, as well as
/// sending a PeriodicRequest every second. We'll be testing both of these
/// functionalities.
pub struct ExampleComponent {
    next_periodic_request_id: u32,
    periodic_request_sender: Sender<PeriodicRequest>,
}

impl ExampleComponent {
    pub fn new(periodic_request_sender: Sender<PeriodicRequest>) -> Self {
        Self { next_periodic_request_id: 0, periodic_request_sender }
    }

    /// Example function that processes a request received by the actor.
    pub fn process_request(&mut self, request: ExampleRequest) -> ExampleResponse {
        ExampleResponse { id: request.id }
    }

    /// Example start function that is called at the start of the actor,
    /// to schedule timers.
    pub fn start(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.schedule_periodic_request(ctx);
    }

    fn schedule_periodic_request(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        ctx.run_later("periodic_request", Duration::seconds(1), |component, ctx| {
            component
                .periodic_request_sender
                .send(PeriodicRequest { id: component.next_periodic_request_id });
            component.next_periodic_request_id += 1;
            component.schedule_periodic_request(ctx);
        });
    }
}

/// Example actix Actor. Actors should have nothing but a shell that
/// forwards messages to the implementing component, because in the
/// TestLoop tests we aren't able to use this part at all.
struct ExampleActor {
    component: ExampleComponent,
}

// This Deref and DerefMut is used to support the DelayedActionRunner.
impl Deref for ExampleActor {
    type Target = ExampleComponent;

    fn deref(&self) -> &Self::Target {
        &self.component
    }
}

impl DerefMut for ExampleActor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.component
    }
}

impl actix::Actor for ExampleActor {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.component.start(ctx);
    }
}

impl actix::Handler<ExampleRequest> for ExampleActor {
    type Result = ExampleResponse;

    fn handle(&mut self, msg: ExampleRequest, _ctx: &mut Self::Context) -> Self::Result {
        self.component.process_request(msg)
    }
}

/// Typically an actor would handle multiple messages, and this is where
/// multisenders come in handy. For this example we have only message but
/// we still demonstrate the use of multisenders.
#[derive(MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
pub struct ExampleComponentAdapter {
    pub example: AsyncSender<ExampleRequest, ExampleResponse>,
}

/// Just another component that will send a request to the ExampleComponent.
pub struct OuterComponent {
    example: ExampleComponentAdapter,
}

impl OuterComponent {
    pub fn new(example: ExampleComponentAdapter) -> Self {
        Self { example }
    }

    pub fn call_example_component_for_response(&self, id: u32) -> BoxFuture<'static, u32> {
        let response = self.example.send_async(ExampleRequest { id });
        async move { response.await.unwrap().id }.boxed()
    }
}
