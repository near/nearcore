use client::Client;
use futures::future::Future;
use futures::stream::Stream;
use std::sync::Arc;
use std::time::Duration;
use tokio::timer::Interval;
use client::chain::Chain;

// should take in a chain rather than a client
// https://github.com/nearprotocol/nearcore/issues/110
pub fn generate_produce_blocks_task(
    client: &Arc<Client>,
    interval: Duration,
) -> impl Future<Item = (), Error = ()> {
    Interval::new_interval(interval)
        .for_each({
            let client = client.clone();
            move |_| {
                // relies specifically on Chain trait
                let block = client.prod_block();
                // relies specifically on BeaconBlock Body type
                if !block.transactions.is_empty() {
                    info!(target: "service", "Transactions: {:?}", block.transactions);
                }
                Ok(())
            }
        }).then(|res| {
            match res {
                Ok(()) => (),
                Err(err) => error!("Error producing blocks: {:?}", err),
            };
            Ok(())
        })
}
