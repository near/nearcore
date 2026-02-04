use crate::{ClientSender, Config, TxGenerator, ViewClientSender};
use near_async::ActorSystem;
use near_async::futures::DelayedActionRunner;
use near_async::messaging::{self};
use near_async::tokio::TokioRuntimeHandle;

pub struct GeneratorActor {
    tx_generator: TxGenerator,
}

impl messaging::Actor for GeneratorActor {
    fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.start(ctx)
    }
}

impl GeneratorActor {
    pub fn start(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {
        match self.tx_generator.start() {
            Err(err) => {
                tracing::error!(?err);
            }
            Ok(_) => {
                tracing::info!(schedule=?self.tx_generator.params.schedule, "started");
            }
        };
    }
}

pub fn start_tx_generator(
    actor_system: ActorSystem,
    config: Config,
    client_sender: ClientSender,
    view_client_sender: ViewClientSender,
) -> TokioRuntimeHandle<GeneratorActor> {
    let tx_generator = TxGenerator::new(config, client_sender, view_client_sender).unwrap();
    actor_system.spawn_tokio_actor(GeneratorActor { tx_generator })
}
