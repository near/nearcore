use std::sync::Arc;

use actix::Addr;

use near_chain_configs::{Genesis, GenesisConfig};
use near_client::test_utils::setup_no_network_with_validity_period;
use near_client::ViewClientActor;
use near_jsonrpc::{start_http, RpcConfig};
use near_network::test_utils::open_port;
use near_primitives::account::Account;
use near_primitives::state_record::StateRecord;
use near_primitives::types::NumBlocks;

lazy_static::lazy_static! {
    pub static ref TEST_GENESIS_CONFIG: GenesisConfig =
        GenesisConfig::from_json(include_str!("../../../../neard/res/genesis_config.json"));
}

pub enum NodeType {
    Validator,
    NonValidator,
}

pub fn start_all(node_type: NodeType) -> (Addr<ViewClientActor>, String) {
    start_all_with_validity_period(node_type, 100, false)
}

pub fn start_all_with_validity_period(
    node_type: NodeType,
    transaction_validity_period: NumBlocks,
    enable_doomslug: bool,
) -> (Addr<ViewClientActor>, String) {
    let records = (0_u128..200)
        .map(|x| StateRecord::Account {
            account_id: format!("account-{}", x),
            account: Account {
                amount: x * 1_000_000,
                locked: 0,
                code_hash: Default::default(),
                storage_usage: 100,
            },
        })
        .collect::<Vec<_>>();
    let genesis = Genesis::new(TEST_GENESIS_CONFIG.clone(), records.into());
    let (client_addr, view_client_addr) = setup_no_network_with_validity_period(
        vec!["test1", "test2"],
        if let NodeType::Validator = node_type { "test1" } else { "other" },
        true,
        transaction_validity_period,
        enable_doomslug,
    );

    let addr = format!("127.0.0.1:{}", open_port());

    start_http(
        RpcConfig::new(&addr),
        Arc::new(genesis),
        client_addr.clone(),
        view_client_addr.clone(),
    );
    (view_client_addr, addr)
}

#[allow(unused_macros)] // Suppress Rustc warnings even though this macro is used.
macro_rules! test_with_client {
    ($node_type:expr, $client:ident, $block:expr) => {
        init_test_logger();

        System::run(|| {
            let (_view_client_addr, addr) = test_utils::start_all($node_type);

            let $client = new_client(&format!("http://{}", addr));

            actix::spawn(async move {
                $block.await;
                System::current().stop();
            });
        })
        .unwrap();
    };
}
