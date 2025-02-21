use std::collections::BTreeMap;
use std::sync::Arc;

use near_async::time::Duration;
use near_chain_configs::test_genesis::{
    TestEpochConfigBuilder, TestGenesisBuilder, ValidatorsSpec,
};
use near_client::test_utils::test_loop::ClientQueries;
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_parameters::{ActionCosts, RuntimeConfigStore};
use near_primitives::action::{GlobalContractDeployMode, GlobalContractIdentifier};
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_runner::ContractCode;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::transactions::{self};
use crate::test_loop::utils::{ONE_NEAR, TGAS};

const INITIAL_BALANCE: Balance = 1000 * ONE_NEAR;
const GAS_PRICE: Balance = 1;

#[cfg_attr(not(feature = "nightly_protocol"), ignore)]
#[test]
fn test_global_contract_by_hash() {
    test_deploy_and_call_global_contract(GlobalContractDeployMode::CodeHash);
}

#[cfg_attr(not(feature = "nightly_protocol"), ignore)]
#[test]
fn test_global_contract_by_account_id() {
    test_deploy_and_call_global_contract(GlobalContractDeployMode::AccountId);
}

fn test_deploy_and_call_global_contract(deploy_mode: GlobalContractDeployMode) {
    let mut env = GlobalContractsTestEnv::setup();

    env.deploy_global_contract(deploy_mode.clone());
    let deploy_cost = INITIAL_BALANCE - env.get_account_balance(env.deploy_account.clone());
    assert_eq!(deploy_cost, env.deploy_global_contract_cost());

    for account in [env.account_shard_0.clone(), env.account_shard_1.clone()] {
        let identifier = env.global_contract_identifier(&deploy_mode);
        env.use_global_contract(&account, identifier);
        // TODO: check use cost
        env.call_global_contract(&account);
    }

    env.shutdown();
}

struct GlobalContractsTestEnv {
    env: TestLoopEnv,
    runtime_config_store: RuntimeConfigStore,
    contract: ContractCode,
    deploy_account: AccountId,
    account_shard_0: AccountId,
    account_shard_1: AccountId,
    rpc: AccountId,
    nonce: u64,
}

impl GlobalContractsTestEnv {
    fn setup() -> Self {
        init_test_logger();

        let [account_shard_0, account_shard_1, deploy_account, rpc] =
            ["account0", "account2", "account", "rpc"].map(|acc| acc.parse::<AccountId>().unwrap());

        let shard_layout = ShardLayout::simple_v1(&["account1"]);
        let block_and_chunk_producers = ["cp0", "cp1"];
        let chunk_validators_only = ["cv0", "cv1"];
        let validators_spec =
            ValidatorsSpec::desired_roles(&block_and_chunk_producers, &chunk_validators_only);

        let genesis = TestGenesisBuilder::new()
            .genesis_time_from_clock(&near_async::time::FakeClock::default().clock())
            .validators_spec(validators_spec.clone())
            .shard_layout(shard_layout.clone())
            .add_user_accounts_simple(
                &[account_shard_0.clone(), account_shard_1.clone(), deploy_account.clone()],
                INITIAL_BALANCE,
            )
            .gas_prices(GAS_PRICE, GAS_PRICE)
            .build();
        let epoch_config = TestEpochConfigBuilder::new()
            .shard_layout(shard_layout)
            .validators_spec(validators_spec)
            .build();
        let epoch_config_store = EpochConfigStore::test(BTreeMap::from([(
            genesis.config.protocol_version,
            Arc::new(epoch_config),
        )]));

        let clients = block_and_chunk_producers
            .iter()
            .chain(chunk_validators_only.iter())
            .map(|acc| acc.parse().unwrap())
            .chain(std::iter::once(rpc.clone()))
            .collect();
        let env = TestLoopBuilder::new()
            .genesis(genesis)
            .clients(clients)
            .epoch_config_store(epoch_config_store)
            .build();
        let contract = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);

        Self {
            env,
            runtime_config_store: RuntimeConfigStore::new(None),
            account_shard_0,
            account_shard_1,
            deploy_account,
            contract,
            rpc,
            nonce: 1,
        }
    }

    fn deploy_global_contract(&mut self, deploy_mode: GlobalContractDeployMode) {
        let tx = SignedTransaction::deploy_global_contract(
            self.next_nonce(),
            self.deploy_account.clone(),
            self.contract.code().to_vec(),
            &create_user_test_signer(&self.deploy_account),
            self.get_tx_block_hash(),
            deploy_mode,
        );
        self.run_tx(tx);
    }

    fn use_global_contract(&mut self, account: &AccountId, identifier: GlobalContractIdentifier) {
        let tx = SignedTransaction::use_global_contract(
            self.next_nonce(),
            &account,
            &create_user_test_signer(&account),
            self.get_tx_block_hash(),
            identifier,
        );
        self.run_tx(tx);
    }

    fn call_global_contract(&mut self, account: &AccountId) {
        let tx = SignedTransaction::call(
            self.next_nonce(),
            account.clone(),
            account.clone(),
            &create_user_test_signer(account),
            0,
            "log_something".to_owned(),
            vec![],
            300 * TGAS,
            self.get_tx_block_hash(),
        );
        self.run_tx(tx);
    }

    fn deploy_global_contract_cost(&self) -> Balance {
        let contract_size = self.contract.code().len();
        let runtime_config = self.runtime_config_store.get_config(PROTOCOL_VERSION);
        let fees = &runtime_config.fees;
        let gas_fees = fees.action_fees[ActionCosts::new_action_receipt].send_fee(true)
            + fees.action_fees[ActionCosts::deploy_global_contract_base].send_fee(true)
            + fees.action_fees[ActionCosts::deploy_global_contract_byte].send_fee(true)
                * contract_size as u64
            + fees.action_fees[ActionCosts::new_action_receipt].exec_fee()
            + fees.action_fees[ActionCosts::deploy_global_contract_base].exec_fee()
            + fees.action_fees[ActionCosts::deploy_global_contract_byte].exec_fee()
                * contract_size as u64;
        let storage_cost =
            runtime_config.fees.storage_usage_config.global_contract_storage_amount_per_byte
                * contract_size as u128;
        (gas_fees as Balance) * GAS_PRICE + storage_cost
    }

    fn get_account_balance(&mut self, account: AccountId) -> Balance {
        // Need to wait a bit for RPC node to catch up with the results
        // of previously submitted txs
        self.env.test_loop.run_for(Duration::seconds(2));
        let clients: Vec<&Client> = self
            .env
            .datas
            .iter()
            .map(|data| &self.env.test_loop.data.get(&data.client_sender.actor_handle()).client)
            .collect();
        clients.query_balance(&account)
    }

    fn next_nonce(&mut self) -> u64 {
        let ret = self.nonce;
        self.nonce += 1;
        ret
    }

    fn get_tx_block_hash(&self) -> CryptoHash {
        transactions::get_shared_block_hash(&self.env.datas, &self.env.test_loop.data)
    }

    fn run_tx(&mut self, tx: SignedTransaction) {
        transactions::run_tx(
            &mut self.env.test_loop,
            &self.rpc,
            tx,
            &self.env.datas,
            Duration::seconds(5),
        );
    }

    fn global_contract_identifier(
        &self,
        deploy_mode: &GlobalContractDeployMode,
    ) -> GlobalContractIdentifier {
        match deploy_mode {
            GlobalContractDeployMode::CodeHash => {
                GlobalContractIdentifier::CodeHash(*self.contract.hash())
            }
            GlobalContractDeployMode::AccountId => {
                GlobalContractIdentifier::AccountId(self.deploy_account.clone())
            }
        }
    }

    fn shutdown(self) {
        self.env.shutdown_and_drain_remaining_events(Duration::seconds(10));
    }
}
