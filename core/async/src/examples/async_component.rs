use crate::{
    futures::FutureSpawner,
    messaging::{AsyncSender, Sender},
};

// For this test, we have an InnerComponent which handles an InnerRequest and
// responds with InnerResponse, and an OuterComponent which handles an
// OuterRequest, spawns a future to send a request to the InnerComponent, and
// then responds back with an OuterResponse (but not as an Actix response; just
// another message). This mimics how we use Actix in nearcore.

#[derive(Debug)]
pub(crate) struct InnerRequest(pub String);

#[derive(Debug)]
pub(crate) struct InnerResponse(pub String);

#[derive(Debug)]
pub(crate) struct OuterRequest(pub String);

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OuterResponse(pub String);

pub(crate) struct InnerComponent;

impl InnerComponent {
    pub fn process_request(&mut self, request: InnerRequest) -> InnerResponse {
        InnerResponse(request.0 + "!")
    }
}

pub(crate) struct OuterComponent {
    inner_sender: AsyncSender<InnerRequest, InnerResponse>,
    outer_response_sender: Sender<OuterResponse>,
}

impl OuterComponent {
    pub fn new(
        inner_sender: AsyncSender<InnerRequest, InnerResponse>,
        outer_response_sender: Sender<OuterResponse>,
    ) -> Self {
        Self { inner_sender, outer_response_sender }
    }

    pub fn process_request(&mut self, request: OuterRequest, future_spawner: &dyn FutureSpawner) {
        let inner_request = InnerRequest(request.0);
        let sender = self.inner_sender.clone();
        let response_sender = self.outer_response_sender.clone();

        // We're mimicing how we use Actix, and in an Actix handler context we don't have access
        // to async/await. So we use a FutureSpawner to do that.
        future_spawner.spawn("inner request", async move {
            let inner_response = sender.send_async(inner_request).await;
            let response = OuterResponse(inner_response.0.repeat(2));
            response_sender.send(response);
        });
    }
}
