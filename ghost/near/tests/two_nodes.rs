use std::sync::Arc;
use std::time::Duration;

use actix::{Actor, AsyncContext, Context, System, WrapFuture};
use chrono::{DateTime, Utc};
use log::LevelFilter;

use futures::future::Future;
use near::{start_with_config, NearConfig};
use near_chain::test_utils::KeyValueRuntime;
use near_chain::{Block, BlockHeader, BlockStatus, Chain, Provenance, RuntimeAdapter};
use near_client::{BlockProducer, ClientActor, ClientConfig, GetBlock};
use near_network::{test_utils::convert_boot_nodes, NetworkConfig, PeerInfo, PeerManagerActor};
use near_store::test_utils::create_test_store;
use primitives::test_utils::init_test_logger;
use primitives::transaction::SignedTransaction;

/// Waits until condition or timeout.
struct WaitOrTimeout {
    f: Box<FnMut(&mut Context<WaitOrTimeout>)>,
    check_interval_ms: u64,
    max_wait_ms: u64,
    ms_slept: u64,
}

impl WaitOrTimeout {
    pub fn new(
        f: Box<FnMut(&mut Context<WaitOrTimeout>)>,
        check_interval_ms: u64,
        max_wait_ms: u64,
    ) -> Self {
        WaitOrTimeout { f, check_interval_ms, max_wait_ms, ms_slept: 0 }
    }

    fn wait_or_timeout(&mut self, ctx: &mut Context<Self>) {
        (self.f)(ctx);
        ctx.run_later(Duration::from_millis(self.check_interval_ms), move |act, ctx| {
            act.ms_slept += act.check_interval_ms;
            if act.ms_slept > act.max_wait_ms {
                println!("BBBB Slept {}; max_wait_ms {}", act.ms_slept, act.max_wait_ms);
                panic!("Timed out waiting for the condition");
            }
            act.wait_or_timeout(ctx);
        });
    }
}

impl Actor for WaitOrTimeout {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.wait_or_timeout(ctx);
    }
}

#[test]
fn two_nodes() {
    init_test_logger();

    let genesis_timestamp = Utc::now();
    let mut near1 = NearConfig::new(genesis_timestamp.clone(), "test1", 25123);
    near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", 25124)]);
    let mut near2 = NearConfig::new(genesis_timestamp, "test2", 25124);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", 25123)]);

    let system = System::new("NEAR");
    let client1 = start_with_config(near1);
    let _client2 = start_with_config(near2);

    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            actix::spawn(client1.send(GetBlock::Best).then(|res| {
                match &res {
                    Ok(Some(b)) if b.header.height > 2 => System::current().stop(),
                    Err(_) => return futures::future::err(()),
                    _ => {}
                };
                futures::future::ok(())
            }));
        }),
        1000,
        60000,
    )
    .start();

    system.run().unwrap();
}
