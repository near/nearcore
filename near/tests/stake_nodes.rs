use actix::{Actor, System};
use futures::future::Future;
use tempdir::TempDir;

use near::{load_test_config, start_with_config, GenesisConfig};
use near_client::Status;
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_network::NetworkClientMessages;
use near_primitives::serialize::BaseEncode;
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::{StakeTransaction, TransactionBody};

/// Runs one validator network, sends staking transaction for the second node and
/// waits until it becomes a validator.
#[test]
fn test_stake_nodes() {
    init_test_logger();

    let genesis_config = GenesisConfig::testing_spec(2, 1);
    let first_node = open_port();
    let near1 = load_test_config("near.0", first_node, &genesis_config);
    let mut near2 = load_test_config("near.1", open_port(), &genesis_config);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("near.0", first_node)]);

    let system = System::new("NEAR");

    let dir1 = TempDir::new("sync_nodes_1").unwrap();
    let (client1, _view_client1) = start_with_config(dir1.path(), near1);
    let dir2 = TempDir::new("sync_nodes_2").unwrap();
    let (client2, _view_client2) = start_with_config(dir2.path(), near2.clone());

    let tx = TransactionBody::Stake(StakeTransaction {
        nonce: 1,
        originator: "near.1".to_string(),
        amount: 50_000_000,
        public_key: near2.block_producer.clone().unwrap().signer.public_key().to_base(),
    })
    .sign(&*near2.block_producer.clone().unwrap().signer);
    actix::spawn(client1.send(NetworkClientMessages::Transaction(tx)).map(|_| ()).map_err(|_| ()));

    WaitOrTimeout::new(
        Box::new(move |_ctx| {
            actix::spawn(client2.send(Status {}).then(|res| {
                if res.unwrap().unwrap().validators.len() == 2 {
                    System::current().stop()
                }
                futures::future::ok(())
            }));
        }),
        100,
        60000,
    )
    .start();

    system.run().unwrap();
}
