use crate::{ClientSender, TxGenerator, TxGeneratorConfig, ViewClientSender};
use actix::Actor;
use near_async::actix_wrapper::ActixWrapper;
use near_async::futures::DelayedActionRunner;
use near_async::messaging::{self};

pub type TxGeneratorActor = ActixWrapper<GeneratorActorImpl>;

pub struct GeneratorActorImpl {
    tx_generator: TxGenerator,
}

impl messaging::Actor for GeneratorActorImpl {
    fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.start(ctx)
    }
}

impl GeneratorActorImpl {
    pub fn start(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {
        match self.tx_generator.start() {
            Err(err) => {
                tracing::error!(target: "transaction-generator", "Error: {err}");
            }
            Ok(_) => {
                tracing::info!(target: "transaction-generator",
                    tps=self.tx_generator.params.tps, "Started");
            }
        };
    }
}

pub fn start_tx_generator(
    config: TxGeneratorConfig,
    client_sender: ClientSender,
    view_client_sender: ViewClientSender,
) -> actix::Addr<TxGeneratorActor> {
    let arbiter = actix::Arbiter::new();
    let tx_generator = TxGenerator::new(config, client_sender, view_client_sender).unwrap();
    TxGeneratorActor::start_in_arbiter(&arbiter.handle(), move |_| {
        let actor_impl = GeneratorActorImpl { tx_generator };
        ActixWrapper::new(actor_impl)
    })
}
