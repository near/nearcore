use assert_matches::assert_matches;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_parameters::{ActionCosts, RuntimeConfigStore, RuntimeFeesConfig};
use near_primitives::action::{GlobalContractDeployMode, GlobalContractIdentifier};
use near_primitives::errors::{
    ActionError, ActionErrorKind, FunctionCallError, MethodResolveError, TxExecutionError,
};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, Gas, StorageUsage};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{
    AccountView, CallResult, ContractCodeView, FinalExecutionOutcomeView, FinalExecutionStatus,
    QueryRequest, QueryResponseKind,
};
use near_vm_runner::ContractCode;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::client_queries::ClientQueries;
use crate::utils::transactions::{self};
use crate::utils::{ONE_NEAR, TGAS};

const GAS_PRICE: Balance = 1;

#[test]
fn test_global_contract_by_hash() {
    test_deploy_and_call_global_contract(GlobalContractDeployMode::CodeHash);
}

#[test]
fn test_global_contract_by_account_id() {
    test_deploy_and_call_global_contract(GlobalContractDeployMode::AccountId);
}

#[test]
fn test_global_contract_deploy_insufficient_balance_for_storage() {
    let mut env = GlobalContractsTestEnv::setup(ONE_NEAR);

    let tx = env.deploy_global_contract_tx(GlobalContractDeployMode::CodeHash);
    let outcome = env.execute_tx(tx);
    assert_matches!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::LackBalanceForState { .. },
            index: _
        }))
    );

    env.shutdown();
}

#[test]
fn test_use_non_existent_global_contract() {
    let mut env = GlobalContractsTestEnv::setup(ONE_NEAR);

    let identifier = env.global_contract_identifier(&GlobalContractDeployMode::CodeHash);
    let tx = env.use_global_contract_tx(&env.account_shard_0.clone(), identifier);
    let outcome = env.execute_tx(tx);
    assert_matches!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::GlobalContractDoesNotExist { .. },
            index: _
        }))
    );

    env.shutdown();
}

#[test]
fn test_global_contract_update() {
    let mut env = GlobalContractsTestEnv::setup(1000 * ONE_NEAR);
    let use_accounts = [env.account_shard_0.clone(), env.account_shard_1.clone()];

    env.deploy_trivial_global_contract(GlobalContractDeployMode::AccountId);

    for account in &use_accounts {
        env.use_global_contract(
            account,
            GlobalContractIdentifier::AccountId(env.deploy_account.clone()),
        );

        // Currently deployed trivial contract doesn't have any methods,
        // so we expect any function call to fail with MethodNotFound error
        let call_tx = env.call_global_contract_tx(account);
        let call_outcome = env.execute_tx(call_tx);
        assert_matches!(
            call_outcome.status,
            FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
                kind: ActionErrorKind::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodNotFound
                )),
                index: _
            }))
        );
    }

    env.deploy_global_contract(GlobalContractDeployMode::AccountId);

    for account in &use_accounts {
        // Function call should be successful after deploying rs contract
        // containing the function we call here
        env.call_global_contract(account);
    }

    env.shutdown();
}

#[test]
fn test_global_contract_by_account_id_rpc_calls() {
    test_global_contract_rpc_calls(GlobalContractDeployMode::AccountId);
}

#[test]
fn test_global_contract_by_hash_rpc_calls() {
    test_global_contract_rpc_calls(GlobalContractDeployMode::CodeHash);
}

fn test_deploy_and_call_global_contract(deploy_mode: GlobalContractDeployMode) {
    const INITIAL_BALANCE: Balance = 1000 * ONE_NEAR;
    let mut env = GlobalContractsTestEnv::setup(INITIAL_BALANCE);

    env.deploy_global_contract(deploy_mode.clone());
    let deploy_cost = INITIAL_BALANCE - env.get_account_state(env.deploy_account.clone()).amount;
    assert_eq!(deploy_cost, env.deploy_global_contract_cost());

    for account in [env.account_shard_0.clone(), env.account_shard_1.clone()] {
        let identifier = env.global_contract_identifier(&deploy_mode);
        let baseline_storage_usage = env.get_account_state(account.clone()).storage_usage;

        env.use_global_contract(&account, identifier.clone());
        let account_state = env.get_account_state(account.clone());
        let use_cost = INITIAL_BALANCE - account_state.amount;
        assert_eq!(use_cost, env.use_global_contract_cost(&identifier));
        assert_eq!(
            account_state.storage_usage,
            baseline_storage_usage + identifier.len() as StorageUsage
        );

        env.call_global_contract(&account);

        // Deploy regular contract to check if storage usage is updated correctly
        env.deploy_regular_contract(&account);
        let account_state = env.get_account_state(account.clone());
        assert_eq!(
            account_state.storage_usage,
            baseline_storage_usage + env.contract.code().len() as StorageUsage
        );
    }

    env.shutdown();
}

fn test_global_contract_rpc_calls(deploy_mode: GlobalContractDeployMode) {
    let mut env = GlobalContractsTestEnv::setup(1000 * ONE_NEAR);
    env.deploy_global_contract(deploy_mode.clone());
    let target_account = env.account_shard_0.clone();
    let identifier = env.global_contract_identifier(&deploy_mode);
    env.use_global_contract(&target_account, identifier);
    env.env.test_loop.run_for(Duration::seconds(2));

    let view_call_result = env.view_call_global_contract(&target_account);
    assert_eq!(view_call_result.logs, vec!["hello".to_owned()]);

    let view_code_result = env.view_code(&target_account);
    assert_eq!(view_code_result.hash, *env.contract.hash());

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
    fn setup(initial_balance: Balance) -> Self {
        init_test_logger();

        let [account_shard_0, account_shard_1, deploy_account, rpc] =
            ["account0", "account2", "account", "rpc"].map(|acc| acc.parse::<AccountId>().unwrap());

        let shard_layout = ShardLayout::simple_v1(&["account1"]);
        let block_and_chunk_producers = ["cp0", "cp1"];
        let chunk_validators_only = ["cv0", "cv1"];
        let validators_spec =
            ValidatorsSpec::desired_roles(&block_and_chunk_producers, &chunk_validators_only);

        let genesis = TestLoopBuilder::new_genesis_builder()
            .validators_spec(validators_spec)
            .shard_layout(shard_layout)
            .add_user_accounts_simple(
                &[account_shard_0.clone(), account_shard_1.clone(), deploy_account.clone()],
                initial_balance,
            )
            .gas_prices(GAS_PRICE, GAS_PRICE)
            .build();
        let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);

        let clients = block_and_chunk_producers
            .iter()
            .chain(chunk_validators_only.iter())
            .map(|acc| acc.parse().unwrap())
            .chain(std::iter::once(rpc.clone()))
            .collect();
        let runtime_config_store = RuntimeConfigStore::new(None);
        let env = TestLoopBuilder::new()
            .genesis(genesis)
            .clients(clients)
            .epoch_config_store(epoch_config_store)
            .runtime_config_store(runtime_config_store.clone())
            .build()
            .warmup();
        let contract = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);

        Self {
            env,
            runtime_config_store,
            account_shard_0,
            account_shard_1,
            deploy_account,
            contract,
            rpc,
            nonce: 1,
        }
    }

    fn deploy_global_contract_custom_tx(
        &mut self,
        deploy_mode: GlobalContractDeployMode,
        contract_code: Vec<u8>,
    ) -> SignedTransaction {
        SignedTransaction::deploy_global_contract(
            self.next_nonce(),
            self.deploy_account.clone(),
            contract_code,
            &create_user_test_signer(&self.deploy_account),
            self.get_tx_block_hash(),
            deploy_mode,
        )
    }

    fn deploy_global_contract_tx(
        &mut self,
        deploy_mode: GlobalContractDeployMode,
    ) -> SignedTransaction {
        self.deploy_global_contract_custom_tx(deploy_mode, self.contract.code().to_vec())
    }

    fn deploy_global_contract(&mut self, deploy_mode: GlobalContractDeployMode) {
        let tx = self.deploy_global_contract_tx(deploy_mode);
        self.run_tx(tx);
    }

    fn deploy_trivial_global_contract(&mut self, deploy_mode: GlobalContractDeployMode) {
        let tx = self.deploy_global_contract_custom_tx(
            deploy_mode,
            near_test_contracts::trivial_contract().to_vec(),
        );
        self.run_tx(tx);
    }

    fn deploy_regular_contract(&mut self, account: &AccountId) {
        let tx = SignedTransaction::deploy_contract(
            self.next_nonce(),
            &account,
            self.contract.code().to_vec(),
            &create_user_test_signer(&account),
            self.get_tx_block_hash(),
        );
        self.run_tx(tx);
    }

    fn use_global_contract_tx(
        &mut self,
        account: &AccountId,
        identifier: GlobalContractIdentifier,
    ) -> SignedTransaction {
        SignedTransaction::use_global_contract(
            self.next_nonce(),
            &account,
            &create_user_test_signer(&account),
            self.get_tx_block_hash(),
            identifier,
        )
    }

    fn use_global_contract(&mut self, account: &AccountId, identifier: GlobalContractIdentifier) {
        let tx = self.use_global_contract_tx(account, identifier);
        self.run_tx(tx);
    }

    fn call_global_contract_tx(&mut self, account: &AccountId) -> SignedTransaction {
        SignedTransaction::call(
            self.next_nonce(),
            account.clone(),
            account.clone(),
            &create_user_test_signer(account),
            0,
            "log_something".to_owned(),
            vec![],
            300 * TGAS,
            self.get_tx_block_hash(),
        )
    }

    fn call_global_contract(&mut self, account: &AccountId) {
        let tx = self.call_global_contract_tx(account);
        self.run_tx(tx);
    }

    fn view_call_global_contract(&self, account: &AccountId) -> CallResult {
        let response = self.clients().runtime_query(
            account,
            QueryRequest::CallFunction {
                account_id: account.clone(),
                method_name: "log_something".to_owned(),
                args: Vec::new().into(),
            },
        );
        let QueryResponseKind::CallResult(call_result) = response.kind else { unreachable!() };
        call_result
    }

    fn deploy_global_contract_cost(&self) -> Balance {
        let contract_size = self.contract.code().len();
        let runtime_config = self.runtime_config_store.get_config(PROTOCOL_VERSION);
        let fees = &runtime_config.fees;
        let gas_fees = Self::total_action_cost(fees, ActionCosts::new_action_receipt)
            + Self::total_action_cost(fees, ActionCosts::deploy_global_contract_base)
            + Self::total_action_cost(fees, ActionCosts::deploy_global_contract_byte)
                * contract_size as Gas;
        let storage_cost =
            runtime_config.fees.storage_usage_config.global_contract_storage_amount_per_byte
                * contract_size as Balance;
        (gas_fees as Balance) * GAS_PRICE + storage_cost
    }

    fn use_global_contract_cost(&self, identifier: &GlobalContractIdentifier) -> Balance {
        let runtime_config = self.runtime_config_store.get_config(PROTOCOL_VERSION);
        let fees = &runtime_config.fees;
        let gas_fees = Self::total_action_cost(fees, ActionCosts::new_action_receipt)
            + Self::total_action_cost(fees, ActionCosts::use_global_contract_base)
            + Self::total_action_cost(fees, ActionCosts::use_global_contract_byte)
                * identifier.len() as Gas;
        (gas_fees as Balance) * GAS_PRICE
    }

    fn total_action_cost(fees: &RuntimeFeesConfig, cost: ActionCosts) -> Gas {
        let fee = &fees.action_fees[cost];
        fee.send_fee(true) + fee.exec_fee()
    }

    fn get_account_state(&mut self, account: AccountId) -> AccountView {
        // Need to wait a bit for RPC node to catch up with the results
        // of previously submitted txs
        self.env.test_loop.run_for(Duration::seconds(2));
        self.view_account(&account)
    }

    fn view_account(&self, account: &AccountId) -> AccountView {
        let response = self
            .clients()
            .runtime_query(account, QueryRequest::ViewAccount { account_id: account.clone() });
        let QueryResponseKind::ViewAccount(account_view) = response.kind else { unreachable!() };
        account_view
    }

    fn view_code(&self, account: &AccountId) -> ContractCodeView {
        let response = self
            .clients()
            .runtime_query(account, QueryRequest::ViewCode { account_id: account.clone() });
        let QueryResponseKind::ViewCode(contract_code_view) = response.kind else { unreachable!() };
        contract_code_view
    }

    fn next_nonce(&mut self) -> u64 {
        let ret = self.nonce;
        self.nonce += 1;
        ret
    }

    fn get_tx_block_hash(&self) -> CryptoHash {
        transactions::get_shared_block_hash(&self.env.node_datas, &self.env.test_loop.data)
    }

    fn execute_tx(&mut self, tx: SignedTransaction) -> FinalExecutionOutcomeView {
        transactions::execute_tx(
            &mut self.env.test_loop,
            &self.rpc,
            tx,
            &self.env.node_datas,
            Duration::seconds(5),
        )
        .unwrap()
    }

    fn run_tx(&mut self, tx: SignedTransaction) {
        transactions::run_tx(
            &mut self.env.test_loop,
            &self.rpc,
            tx,
            &self.env.node_datas,
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

    fn clients(&self) -> Vec<&Client> {
        self.env
            .node_datas
            .iter()
            .map(|data| &self.env.test_loop.data.get(&data.client_sender.actor_handle()).client)
            .collect()
    }

    fn shutdown(self) {
        self.env.shutdown_and_drain_remaining_events(Duration::seconds(10));
    }
}
