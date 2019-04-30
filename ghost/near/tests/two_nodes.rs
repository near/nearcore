use std::time::Duration;

use actix::{Actor, System};
use chrono::Utc;

use futures::future::Future;
use near::{start_with_config, NearConfig};
use near_client::GetBlock;
use near_network::test_utils::{convert_boot_nodes, WaitOrTimeout};
use primitives::test_utils::init_test_logger;
use primitives::transaction::SignedTransaction;

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
        100,
        60000,
    )
    .start();

    system.run().unwrap();
}
