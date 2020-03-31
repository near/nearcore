use std::sync::Arc;

use actix::Addr;

use near_chain_configs::{Genesis, GenesisConfig};
use near_client::test_utils::setup_no_network_with_validity_period;
use near_client::ViewClientActor;
use near_jsonrpc::{start_http, RpcConfig};
use near_network::test_utils::open_port;
use near_primitives::state_record::StateRecord;
use near_primitives::types::NumBlocks;
use near_primitives::views::AccountView;

lazy_static::lazy_static! {
    pub static ref TEST_GENESIS_CONFIG: GenesisConfig =
        GenesisConfig::from_json(include_str!("../../../near/res/testnet_genesis_config.json"));
}

pub fn start_all(validator: bool) -> (Addr<ViewClientActor>, String) {
    start_all_with_validity_period(validator, 100, false)
}

pub fn start_all_with_validity_period(
    validator: bool,
    transaction_validity_period: NumBlocks,
    enable_doomslug: bool,
) -> (Addr<ViewClientActor>, String) {
    let records = (0_u128..200)
        .map(|x| StateRecord::Account {
            account_id: format!("account-{}", x),
            account: AccountView {
                amount: x * 1_000_000,
                locked: 0,
                code_hash: Default::default(),
                storage_usage: Default::default(),
                storage_paid_at: 0,
            },
        })
        .collect::<Vec<_>>();
    let genesis = Genesis::new(TEST_GENESIS_CONFIG.clone(), records.into());
    let (client_addr, view_client_addr) = setup_no_network_with_validity_period(
        vec!["test1", "test2"],
        if validator { "test1" } else { "other" },
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
    ($validator_status:expr, $client:ident, $block:expr) => {
        init_test_logger();

        System::run(|| {
            let (_view_client_addr, addr) = test_utils::start_all($validator_status == "validator");

            let $client = new_client(&format!("http://{}", addr));

            actix::spawn(async move {
                $block.await;
                System::current().stop();
            });
        })
        .unwrap();
    };
}
