use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_parameters::{ActionCosts, RuntimeConfigStore, RuntimeFeesConfig};
use near_primitives::action::delegate::{DelegateAction, SignedDelegateAction};
use near_primitives::action::{
    Action, GlobalContractDeployMode, GlobalContractIdentifier, UseGlobalContractAction,
};
use near_primitives::errors::{
    ActionError, ActionErrorKind, FunctionCallError, MethodResolveError, TxExecutionError,
};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, BlockHeight, Gas, StorageUsage};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{
    AccountView, CallResult, ContractCodeView, FinalExecutionOutcomeView, FinalExecutionStatus,
    QueryRequest, QueryResponse, QueryResponseKind,
};
use near_vm_runner::ContractCode;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{
    create_account_ids, create_validators_spec, rpc_account_id, validators_spec_clients_with_rpc,
};
use crate::utils::node::TestLoopNode;
use crate::utils::transactions;

const GAS_PRICE: Balance = Balance::from_yoctonear(1);

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
    let mut env = GlobalContractsTestEnv::setup(Balance::from_near(1));

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
    let mut env = GlobalContractsTestEnv::setup(Balance::from_near(1));

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
    let mut env = GlobalContractsTestEnv::setup(Balance::from_near(1000));
    let use_accounts = [env.account_shard_0.clone(), env.account_shard_1.clone()];

    env.deploy_trivial_global_contract(GlobalContractDeployMode::AccountId);

    for account in &use_accounts {
        env.use_global_contract(
            account,
            GlobalContractIdentifier::AccountId(env.deploy_account.clone()),
        );

        // Currently deployed trivial contract doesn't have any methods,
        // so we expect any function call to fail with MethodNotFound error
        let call_tx = env.call_global_contract_tx(account.clone(), account.clone());
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
        env.assert_call_global_contract_success(account.clone(), account.clone());
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
    const INITIAL_BALANCE: Balance = Balance::from_near(1000);
    let mut env = GlobalContractsTestEnv::setup(INITIAL_BALANCE);

    env.deploy_global_contract(deploy_mode.clone());
    let deploy_cost = INITIAL_BALANCE
        .checked_sub(env.get_account_state(env.deploy_account.clone()).amount)
        .unwrap();
    assert_eq!(deploy_cost, env.deploy_global_contract_cost());

    for account in [env.account_shard_0.clone(), env.account_shard_1.clone()] {
        let identifier = env.global_contract_identifier(&deploy_mode);
        let baseline_storage_usage = env.get_account_state(account.clone()).storage_usage;

        env.use_global_contract(&account, identifier.clone());
        let account_state = env.get_account_state(account.clone());
        let use_cost = INITIAL_BALANCE.checked_sub(account_state.amount).unwrap();
        assert_eq!(use_cost, env.use_global_contract_cost(&identifier));
        assert_eq!(
            account_state.storage_usage,
            baseline_storage_usage + identifier.len() as StorageUsage
        );

        env.assert_call_global_contract_success(account.clone(), account.clone());

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
    let mut env = GlobalContractsTestEnv::setup(Balance::from_near(1000));
    env.deploy_global_contract(deploy_mode.clone());
    let target_account = env.account_shard_0.clone();
    let identifier = env.global_contract_identifier(&deploy_mode);
    env.use_global_contract(&target_account, identifier.clone());
    env.env.test_loop.run_for(Duration::seconds(2));

    let view_call_result = env.view_call_global_contract(&target_account);
    assert_eq!(view_call_result.logs, vec!["hello".to_owned()]);

    let view_code_result = env.view_code(&target_account);
    assert_eq!(view_code_result.hash, *env.contract.hash());

    let view_global_code_result = env.view_global_contract_code(identifier);
    assert_eq!(view_global_code_result.hash, *env.contract.hash());

    env.shutdown();
}

#[test]
fn test_use_global_contract_by_hash_delegate() {
    test_use_global_contract_delegate(GlobalContractDeployMode::CodeHash);
}

#[test]
fn test_use_global_contract_by_account_id_delegate() {
    test_use_global_contract_delegate(GlobalContractDeployMode::AccountId);
}

fn test_use_global_contract_delegate(deploy_mode: GlobalContractDeployMode) {
    let mut env = GlobalContractsTestEnv::setup(Balance::from_near(1000));
    env.deploy_global_contract(deploy_mode.clone());

    let user_account = env.zero_balance_account.clone();
    let relayer_account = env.account_shard_0.clone();
    let identifier = env.global_contract_identifier(&deploy_mode);
    env.use_global_contract_via_delegate_action(
        user_account.clone(),
        relayer_account.clone(),
        identifier,
    );

    // Using relayer's account to trigger function call since user account has
    // zero balance and cannot pay for the transaction.
    env.assert_call_global_contract_success(relayer_account, user_account);

    env.shutdown();
}

struct GlobalContractsTestEnv {
    env: TestLoopEnv,
    runtime_config_store: RuntimeConfigStore,
    contract: ContractCode,
    deploy_account: AccountId,
    zero_balance_account: AccountId,
    account_shard_0: AccountId,
    account_shard_1: AccountId,
    nonce: u64,
}

impl GlobalContractsTestEnv {
    fn setup(initial_balance: Balance) -> Self {
        init_test_logger();

        let [account_shard_0, account_shard_1, deploy_account, zero_balance_account] =
            create_account_ids(["account0", "account2", "account", "zero_balance_account"]);

        let boundary_accounts = create_account_ids(["account1"]).to_vec();
        let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
        let validators_spec = create_validators_spec(2, 2);
        let clients = validators_spec_clients_with_rpc(&validators_spec);

        let genesis = TestLoopBuilder::new_genesis_builder()
            .validators_spec(validators_spec)
            .shard_layout(shard_layout)
            .add_user_accounts_simple(
                &[account_shard_0.clone(), account_shard_1.clone(), deploy_account.clone()],
                initial_balance,
            )
            .add_user_account_simple(zero_balance_account.clone(), Balance::ZERO)
            .gas_prices(GAS_PRICE, GAS_PRICE)
            .build();

        let runtime_config_store = RuntimeConfigStore::new(None);
        let env = TestLoopBuilder::new()
            .genesis(genesis)
            .epoch_config_store_from_genesis()
            .clients(clients)
            .runtime_config_store(runtime_config_store.clone())
            .build()
            .warmup();

        Self {
            env,
            runtime_config_store,
            account_shard_0,
            account_shard_1,
            deploy_account,
            zero_balance_account,
            contract: ContractCode::new(near_test_contracts::rs_contract().to_vec(), None),
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

    fn use_global_contract_via_delegate_action(
        &mut self,
        user: AccountId,
        relayer: AccountId,
        contract_identifier: GlobalContractIdentifier,
    ) {
        let use_action =
            Action::UseGlobalContract(UseGlobalContractAction { contract_identifier }.into());
        let user_signer = create_user_test_signer(&user);
        let delegate_action = DelegateAction {
            sender_id: user.clone(),
            receiver_id: user.clone(),
            actions: vec![use_action.try_into().unwrap()],
            nonce: self.next_nonce(),
            max_block_height: BlockHeight::MAX,
            public_key: user_signer.public_key(),
        };
        let signed_delegate_action = SignedDelegateAction::sign(&user_signer, delegate_action);
        let tx = SignedTransaction::from_actions(
            self.next_nonce(),
            relayer.clone(),
            user,
            &create_user_test_signer(&relayer),
            vec![Action::Delegate(signed_delegate_action.into())],
            self.get_tx_block_hash(),
            0,
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

    fn call_global_contract_tx(
        &mut self,
        signer_id: AccountId,
        receiver_id: AccountId,
    ) -> SignedTransaction {
        let signer = create_user_test_signer(&signer_id);
        SignedTransaction::call(
            self.next_nonce(),
            signer_id,
            receiver_id,
            &signer,
            Balance::ZERO,
            "log_something".to_owned(),
            vec![],
            Gas::from_teragas(300),
            self.get_tx_block_hash(),
        )
    }

    fn assert_call_global_contract_success(
        &mut self,
        singer_id: AccountId,
        receiver_id: AccountId,
    ) {
        let tx = self.call_global_contract_tx(singer_id, receiver_id);
        self.run_tx(tx);
    }

    fn view_call_global_contract(&self, account: &AccountId) -> CallResult {
        let query = QueryRequest::CallFunction {
            account_id: account.clone(),
            method_name: "log_something".to_owned(),
            args: Vec::new().into(),
        };
        let response = self.runtime_query(account, query);
        let QueryResponseKind::CallResult(call_result) = response.kind else { unreachable!() };
        call_result
    }

    fn deploy_global_contract_cost(&self) -> Balance {
        let contract_size = self.contract.code().len();
        let runtime_config = self.runtime_config_store.get_config(PROTOCOL_VERSION);
        let fees = &runtime_config.fees;
        let gas_fees = Self::total_action_cost(fees, ActionCosts::new_action_receipt)
            .checked_add(Self::total_action_cost(fees, ActionCosts::deploy_global_contract_base))
            .unwrap()
            .checked_add(Gas::from_gas(
                Self::total_action_cost(fees, ActionCosts::deploy_global_contract_byte).as_gas()
                    * contract_size as u64,
            ))
            .unwrap();
        let storage_cost = runtime_config
            .fees
            .storage_usage_config
            .global_contract_storage_amount_per_byte
            .checked_mul(contract_size.try_into().unwrap())
            .unwrap();
        GAS_PRICE
            .checked_mul(u128::from(gas_fees.as_gas()))
            .unwrap()
            .checked_add(storage_cost)
            .unwrap()
    }

    fn use_global_contract_cost(&self, identifier: &GlobalContractIdentifier) -> Balance {
        let runtime_config = self.runtime_config_store.get_config(PROTOCOL_VERSION);
        let fees = &runtime_config.fees;
        let gas_fees = Self::total_action_cost(fees, ActionCosts::new_action_receipt)
            .checked_add(Self::total_action_cost(fees, ActionCosts::use_global_contract_base))
            .unwrap()
            .checked_add(Gas::from_gas(
                Self::total_action_cost(fees, ActionCosts::use_global_contract_byte).as_gas()
                    * identifier.len() as u64,
            ))
            .unwrap();
        GAS_PRICE.checked_mul(u128::from(gas_fees.as_gas())).unwrap()
    }

    fn total_action_cost(fees: &RuntimeFeesConfig, cost: ActionCosts) -> Gas {
        let fee = &fees.action_fees[cost];
        fee.send_fee(true).checked_add(fee.exec_fee()).unwrap()
    }

    fn get_account_state(&mut self, account: AccountId) -> AccountView {
        // Need to wait a bit for RPC node to catch up with the results
        // of previously submitted txs
        self.env.test_loop.run_for(Duration::seconds(2));
        self.view_account(&account)
    }

    fn view_account(&self, account: &AccountId) -> AccountView {
        let response =
            self.runtime_query(account, QueryRequest::ViewAccount { account_id: account.clone() });
        let QueryResponseKind::ViewAccount(account_view) = response.kind else { unreachable!() };
        account_view
    }

    fn view_code(&self, account: &AccountId) -> ContractCodeView {
        let response =
            self.runtime_query(account, QueryRequest::ViewCode { account_id: account.clone() });
        let QueryResponseKind::ViewCode(contract_code_view) = response.kind else { unreachable!() };
        contract_code_view
    }

    fn view_global_contract_code(&self, identifier: GlobalContractIdentifier) -> ContractCodeView {
        let query = match identifier {
            GlobalContractIdentifier::CodeHash(code_hash) => {
                QueryRequest::ViewGlobalContractCode { code_hash }
            }
            GlobalContractIdentifier::AccountId(account_id) => {
                QueryRequest::ViewGlobalContractCodeByAccountId { account_id }
            }
        };
        // account is required by `runtime_query` to resolve shard_id
        let account = self.account_shard_0.clone();
        let response = self.runtime_query(&account, query);
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
        TestLoopNode::for_account(&self.env.node_datas, &rpc_account_id())
            .execute_tx(&mut self.env.test_loop, tx, Duration::seconds(5))
            .unwrap()
    }

    fn run_tx(&mut self, tx: SignedTransaction) {
        TestLoopNode::for_account(&self.env.node_datas, &rpc_account_id()).run_tx(
            &mut self.env.test_loop,
            tx,
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

    fn runtime_query(&self, account_id: &AccountId, query: QueryRequest) -> QueryResponse {
        TestLoopNode::for_account(&self.env.node_datas, &rpc_account_id()).runtime_query(
            self.env.test_loop_data(),
            account_id,
            query,
        )
    }

    fn shutdown(self) {
        self.env.shutdown_and_drain_remaining_events(Duration::seconds(10));
    }
}
