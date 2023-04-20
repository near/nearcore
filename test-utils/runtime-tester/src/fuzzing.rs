use crate::run_test::{BlockConfig, NetworkConfig, RuntimeConfig, Scenario, TransactionConfig};
use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_primitives::{
    account::{AccessKey, AccessKeyPermission, FunctionCallPermission},
    transaction::{
        Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
        DeployContractAction, FunctionCallAction, TransferAction,
    },
    types::{AccountId, Balance, BlockHeight, Nonce},
};
use nearcore::config::{NEAR_BASE, TESTING_INIT_BALANCE};

use libfuzzer_sys::arbitrary::{Arbitrary, Result, Unstructured};

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

pub type ContractId = usize;

pub const MAX_BLOCKS: usize = 250;
pub const MAX_TXS: usize = 50;
pub const MAX_TX_DIFF: usize = 10;
pub const MAX_ACCOUNTS: usize = 100;
pub const MAX_ACTIONS: usize = 100;

const GAS_1: u64 = 300_000_000_000_000;

impl Arbitrary<'_> for Scenario {
    fn arbitrary(u: &mut Unstructured<'_>) -> Result<Self> {
        let num_accounts = u.int_in_range(2..=MAX_ACCOUNTS)?;

        let seeds: Vec<String> = (0..num_accounts).map(|i| format!("test{}", i)).collect();

        let mut scope = Scope::from_seeds(&seeds);

        let network_config = NetworkConfig { seeds };
        let runtime_config = RuntimeConfig {
            max_total_prepaid_gas: GAS_1 * 100,
            gas_limit: (GAS_1 as f64 * *u.choose(&[0.01, 0.1, 1., 10., 100.])?) as u64,
            epoch_length: *u.choose(&[5, 10, 100, 500])? as u64,
        };

        let mut blocks = vec![];

        while blocks.len() < MAX_BLOCKS && u.len() > BlockConfig::size_hint(0).0 {
            blocks.push(BlockConfig::arbitrary(u, &mut scope)?);
        }
        Ok(Scenario { network_config, runtime_config, blocks, use_in_memory_store: true })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (1, Some(MAX_BLOCKS * BlockConfig::size_hint(0).1.unwrap()))
    }
}

impl BlockConfig {
    fn arbitrary(u: &mut Unstructured, scope: &mut Scope) -> Result<BlockConfig> {
        scope.inc_height();
        let mut block_config = BlockConfig::at_height(scope.height());

        let lower_bound = scope.last_tx_num.saturating_sub(MAX_TX_DIFF);
        let upper_bound = scope.last_tx_num.saturating_add(MAX_TX_DIFF);
        let max_tx_num = u.int_in_range(lower_bound..=std::cmp::min(MAX_TXS, upper_bound))?;
        scope.last_tx_num = max_tx_num;

        while block_config.transactions.len() < max_tx_num
            && u.len() > TransactionConfig::size_hint(0).0
        {
            block_config.transactions.push(TransactionConfig::arbitrary(u, scope)?)
        }

        Ok(block_config)
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (1, Some((MAX_TXS + 1) * TransactionConfig::size_hint(0).1.unwrap()))
    }
}

impl TransactionConfig {
    fn arbitrary(u: &mut Unstructured, scope: &mut Scope) -> Result<Self> {
        let mut options: Vec<fn(&mut Unstructured, &mut Scope) -> Result<TransactionConfig>> =
            vec![];

        scope.inc_nonce();

        assert!(scope.alive_accounts.contains(&0), "The only validator account got deleted.");

        // Transfer
        options.push(|u, scope| {
            let signer_account = scope.random_account(u)?;
            let receiver_account = {
                if u.arbitrary::<bool>()? {
                    scope.new_account(u)?
                } else {
                    scope.random_account(u)?
                }
            };
            let amount = u.int_in_range::<u128>(0..=signer_account.balance)?;

            let signer_idx = scope.usize_id(&signer_account);
            let receiver_idx = scope.usize_id(&receiver_account);
            scope.accounts[signer_idx].balance =
                scope.accounts[signer_idx].balance.saturating_sub(amount);
            scope.accounts[receiver_idx].balance =
                scope.accounts[receiver_idx].balance.saturating_add(amount);

            Ok(TransactionConfig {
                nonce: scope.nonce(),
                signer_id: signer_account.id.clone(),
                receiver_id: receiver_account.id,
                signer: scope.full_access_signer(u, &signer_account)?,
                actions: vec![Action::Transfer(TransferAction { deposit: amount })],
            })
        });

        /* This actually can create new block producers, and currently we only support one.
        // Stake
        options.push(|u, scope| {
            let signer_account = scope.random_account(u)?;
            let amount = u.int_in_range::<u128>(0..=signer_account.balance)?;
            let signer = InMemorySigner::from_seed(
                signer_account.id.clone(),
                KeyType::ED25519,
                signer_account.id.as_ref(),
            );
            let public_key = signer.public_key.clone();

            Ok(TransactionConfig {
                nonce: scope.nonce(),
                signer_id: signer_account.id.clone(),
                receiver_id: signer_account.id,
                signer,
                actions: vec![Action::Stake(StakeAction { stake: amount, public_key })],
            })
        });
         */

        // Create Account
        options.push(|u, scope| {
            let signer_account = scope.random_account(u)?;
            let new_account = scope.new_account(u)?;

            let signer = scope.full_access_signer(u, &signer_account)?;
            let new_public_key = InMemorySigner::from_seed(
                new_account.id.clone(),
                KeyType::ED25519,
                new_account.id.as_ref(),
            )
            .public_key;
            Ok(TransactionConfig {
                nonce: scope.nonce(),
                signer_id: signer_account.id,
                receiver_id: new_account.id,
                signer,
                actions: vec![
                    Action::CreateAccount(CreateAccountAction {}),
                    Action::AddKey(AddKeyAction {
                        public_key: new_public_key,
                        access_key: AccessKey {
                            nonce: 0,
                            permission: AccessKeyPermission::FullAccess,
                        },
                    }),
                    Action::Transfer(TransferAction { deposit: NEAR_BASE }),
                ],
            })
        });

        // Delete Account
        if scope.num_alive_accounts() > 1 {
            options.push(|u, scope| {
                let signer_account = scope.random_non_zero_account(u)?;
                let receiver_account = signer_account.clone();
                let beneficiary_id = {
                    if u.arbitrary::<bool>()? {
                        scope.new_account(u)?
                    } else {
                        scope.accounts[scope.random_alive_account_usize_id(u)?].clone()
                    }
                };

                let signer = scope.full_access_signer(u, &signer_account)?;

                scope.delete_account(scope.usize_id(&receiver_account));

                Ok(TransactionConfig {
                    nonce: scope.nonce(),
                    signer_id: signer_account.id.clone(),
                    receiver_id: receiver_account.id,
                    signer,
                    actions: vec![Action::DeleteAccount(DeleteAccountAction {
                        beneficiary_id: beneficiary_id.id,
                    })],
                })
            });
        }

        // Deploy Contract
        options.push(|u, scope| {
            let nonce = scope.nonce();

            let signer_account = scope.random_account(u)?;

            let max_contract_id = scope.available_contracts.len() - 1;
            let contract_id = u.int_in_range::<usize>(0..=max_contract_id)?;

            let signer = scope.full_access_signer(u, &signer_account)?;

            scope.deploy_contract(&signer_account, contract_id);

            Ok(TransactionConfig {
                nonce,
                signer_id: signer_account.id.clone(),
                receiver_id: signer_account.id.clone(),
                signer,
                actions: vec![Action::DeployContract(DeployContractAction {
                    code: scope.available_contracts[contract_id].code.clone(),
                })],
            })
        });

        // Multiple function calls
        options.push(|u, scope| {
            let nonce = scope.nonce();

            let signer_account = scope.random_account(u)?;
            let receiver_account = {
                let mut possible_receiver_accounts = vec![];
                for account in &scope.accounts {
                    if account.deployed_contract != None {
                        possible_receiver_accounts.push(account);
                    }
                }
                if possible_receiver_accounts.is_empty() {
                    signer_account.clone()
                } else {
                    (*u.choose(&possible_receiver_accounts)?).clone()
                }
            };

            let signer = scope.function_call_signer(u, &signer_account, &receiver_account.id)?;

            let mut receiver_functions = vec![];
            if let Some(contract_id) = receiver_account.deployed_contract {
                for function in &scope.available_contracts[contract_id].functions {
                    receiver_functions.push(function);
                }
            }

            if receiver_functions.is_empty() {
                return Ok(TransactionConfig {
                    nonce,
                    signer_id: signer_account.id.clone(),
                    receiver_id: receiver_account.id.clone(),
                    signer,
                    actions: vec![],
                });
            }

            let mut actions = vec![];
            let mut actions_num = u.int_in_range(0..=MAX_ACTIONS)?;
            if u.int_in_range(0..=10)? == 0 {
                actions_num = 1;
            }

            while actions.len() < actions_num && u.len() > Function::size_hint(0).1.unwrap() {
                let function = u.choose(&receiver_functions)?;
                actions.push(Action::FunctionCall(function.arbitrary(u)?));
            }

            Ok(TransactionConfig {
                nonce,
                signer_id: signer_account.id.clone(),
                receiver_id: receiver_account.id.clone(),
                signer,
                actions,
            })
        });

        // Add key
        options.push(|u, scope| {
            let nonce = scope.nonce();

            let signer_account = scope.random_account(u)?;

            let signer = scope.full_access_signer(u, &signer_account)?;

            Ok(TransactionConfig {
                nonce,
                signer_id: signer_account.id.clone(),
                receiver_id: signer_account.id.clone(),
                signer,
                actions: vec![Action::AddKey(scope.add_new_key(
                    u,
                    scope.usize_id(&signer_account),
                    nonce,
                )?)],
            })
        });

        // Delete key
        options.push(|u, scope| {
            let nonce = scope.nonce();

            let signer_account = scope.random_account(u)?;
            let signer = scope.full_access_signer(u, &signer_account)?;

            if signer_account.keys.is_empty() {
                return Ok(TransactionConfig {
                    nonce,
                    signer_id: signer_account.id.clone(),
                    receiver_id: signer_account.id.clone(),
                    signer,
                    actions: vec![],
                });
            }

            let public_key = scope.delete_random_key(u, &signer_account)?;

            Ok(TransactionConfig {
                nonce,
                signer_id: signer_account.id.clone(),
                receiver_id: signer_account.id.clone(),
                signer,
                actions: vec![Action::DeleteKey(DeleteKeyAction { public_key })],
            })
        });

        let f = u.choose(&options)?;
        f(u, scope)
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (7, Some(210))
    }
}

#[derive(Clone)]
pub struct Scope {
    accounts: Vec<Account>,
    alive_accounts: HashSet<usize>,
    nonce: Nonce,
    height: BlockHeight,
    available_contracts: Vec<Contract>,
    last_tx_num: usize,
    account_id_to_idx: HashMap<AccountId, usize>,
}

#[derive(Clone)]
pub struct Account {
    pub id: AccountId,
    pub balance: Balance,
    pub deployed_contract: Option<ContractId>,
    pub keys: HashMap<Nonce, Key>,
}

#[derive(Clone)]
pub struct Key {
    pub signer: InMemorySigner,
    pub access_key: AccessKey,
}

#[derive(Clone)]
pub struct Contract {
    pub code: Vec<u8>,
    pub functions: Vec<Function>,
}

#[derive(Clone)]
pub enum Function {
    // #################
    // # Test contract #
    // #################
    StorageUsage,
    BlockIndex,
    BlockTimestamp,
    PrepaidGas,
    RandomSeed,
    PredecessorAccountId,
    SignerAccountPk,
    SignerAccountId,
    CurrentAccountId,
    AccountBalance,
    AttachedDeposit,
    ValidatorTotalStake,
    ExtSha256,
    UsedGas,
    WriteKeyValue,
    WriteBlockHeight,
    // ########################
    // # Contract for fuzzing #
    // ########################
    SumOfNumbers,
    DataReceipt,
}

impl Scope {
    fn from_seeds(seeds: &[String]) -> Self {
        let accounts: Vec<Account> = seeds.iter().map(|id| Account::from_id(id.clone())).collect();
        let account_id_to_idx = accounts
            .iter()
            .enumerate()
            .map(|(i, account)| (account.id.clone(), i))
            .collect::<HashMap<_, _>>();
        Scope {
            accounts,
            alive_accounts: HashSet::from_iter(0..seeds.len()),
            nonce: 1_000_000,
            height: 0,
            available_contracts: Scope::construct_available_contracts(),
            last_tx_num: MAX_TXS,
            account_id_to_idx,
        }
    }

    fn construct_available_contracts() -> Vec<Contract> {
        vec![
            Contract {
                code: near_test_contracts::rs_contract().to_vec(),
                functions: vec![
                    Function::StorageUsage,
                    Function::BlockIndex,
                    Function::BlockTimestamp,
                    Function::PrepaidGas,
                    Function::RandomSeed,
                    Function::PredecessorAccountId,
                    Function::SignerAccountPk,
                    Function::SignerAccountId,
                    Function::CurrentAccountId,
                    Function::AccountBalance,
                    Function::AttachedDeposit,
                    Function::ValidatorTotalStake,
                    Function::ExtSha256,
                    Function::UsedGas,
                    Function::WriteKeyValue,
                    Function::WriteBlockHeight,
                ],
            },
            Contract {
                code: near_test_contracts::fuzzing_contract().to_vec(),
                functions: vec![Function::SumOfNumbers, Function::DataReceipt],
            },
        ]
    }

    pub fn inc_height(&mut self) {
        self.height += 1
    }

    pub fn height(&self) -> BlockHeight {
        self.height
    }

    pub fn inc_nonce(&mut self) {
        self.nonce += 1;
    }

    pub fn nonce(&self) -> Nonce {
        self.nonce
    }

    pub fn num_alive_accounts(&self) -> usize {
        self.alive_accounts.len()
    }

    pub fn random_non_zero_alive_account_usize_id(&self, u: &mut Unstructured) -> Result<usize> {
        let mut accounts = self.alive_accounts.clone();
        accounts.remove(&0);
        Ok(*u.choose(&accounts.iter().cloned().collect::<Vec<_>>())?)
    }

    pub fn random_alive_account_usize_id(&self, u: &mut Unstructured) -> Result<usize> {
        Ok(*u.choose(&self.alive_accounts.iter().cloned().collect::<Vec<_>>())?)
    }

    pub fn random_account(&self, u: &mut Unstructured) -> Result<Account> {
        Ok(self.accounts[self.random_alive_account_usize_id(u)?].clone())
    }

    pub fn random_non_zero_account(&self, u: &mut Unstructured) -> Result<Account> {
        Ok(self.accounts[self.random_non_zero_alive_account_usize_id(u)?].clone())
    }

    pub fn usize_id(&self, account: &Account) -> usize {
        self.account_id_to_idx[&account.id]
    }

    fn new_test_account(&mut self) -> Account {
        let new_id = format!("test{}", self.accounts.len());
        self.alive_accounts.insert(self.accounts.len());
        self.accounts.push(Account::from_id(new_id));
        self.accounts[self.accounts.len() - 1].clone()
    }

    fn new_implicit_account(&mut self, u: &mut Unstructured) -> Result<Account> {
        let mut new_id_vec = vec![];
        let mut chars = vec![];
        for x in b'a'..=b'f' {
            chars.push(x);
        }
        for x in b'0'..=b'9' {
            chars.push(x);
        }
        for _ in 0..64 {
            new_id_vec.push(*u.choose(&chars)?);
        }
        let new_id = String::from_utf8(new_id_vec).unwrap();
        self.alive_accounts.insert(self.accounts.len());
        self.accounts.push(Account::from_id(new_id));
        Ok(self.accounts[self.accounts.len() - 1].clone())
    }

    pub fn new_account(&mut self, u: &mut Unstructured) -> Result<Account> {
        let account = if u.arbitrary::<bool>()? {
            self.new_implicit_account(u)?
        } else {
            self.new_test_account()
        };
        self.account_id_to_idx.insert(account.id.clone(), self.accounts.len() - 1);
        Ok(account)
    }

    pub fn delete_account(&mut self, account_usize_id: usize) {
        self.alive_accounts.remove(&account_usize_id);
    }

    pub fn deploy_contract(&mut self, receiver_account: &Account, contract_id: usize) {
        let acc_id = self.usize_id(receiver_account);
        self.accounts[acc_id].deployed_contract = Some(contract_id);
    }

    pub fn add_new_key(
        &mut self,
        u: &mut Unstructured,
        account_id: usize,
        nonce: Nonce,
    ) -> Result<AddKeyAction> {
        let permission = {
            if u.arbitrary::<bool>()? {
                AccessKeyPermission::FullAccess
            } else {
                AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: self.random_account(u)?.id.into(),
                    method_names: vec![],
                })
            }
        };
        let signer = InMemorySigner::from_seed(
            self.accounts[account_id].id.clone(),
            KeyType::ED25519,
            format!("test{}.{}", account_id, nonce).as_str(),
        );
        self.accounts[account_id].keys.insert(
            nonce,
            Key {
                signer: signer.clone(),
                access_key: AccessKey { nonce, permission: permission.clone() },
            },
        );
        Ok(AddKeyAction {
            public_key: signer.public_key,
            access_key: AccessKey { nonce, permission },
        })
    }

    pub fn full_access_signer(
        &self,
        u: &mut Unstructured,
        account: &Account,
    ) -> Result<InMemorySigner> {
        let account_idx = self.usize_id(account);
        let possible_signers = self.accounts[account_idx].full_access_keys();
        if possible_signers.is_empty() {
            // this transaction will be invalid
            Ok(InMemorySigner::from_seed(
                self.accounts[account_idx].id.clone(),
                KeyType::ED25519,
                self.accounts[account_idx].id.as_ref(),
            ))
        } else {
            Ok(u.choose(&possible_signers)?.clone())
        }
    }

    pub fn function_call_signer(
        &self,
        u: &mut Unstructured,
        account: &Account,
        receiver_id: &str,
    ) -> Result<InMemorySigner> {
        let account_idx = self.usize_id(account);
        let possible_signers = self.accounts[account_idx].function_call_keys(receiver_id);
        if possible_signers.is_empty() {
            // this transaction will be invalid
            Ok(InMemorySigner::from_seed(
                self.accounts[account_idx].id.clone(),
                KeyType::ED25519,
                self.accounts[account_idx].id.as_ref(),
            ))
        } else {
            Ok(u.choose(&possible_signers)?.clone())
        }
    }

    pub fn delete_random_key(
        &mut self,
        u: &mut Unstructured,
        account: &Account,
    ) -> Result<PublicKey> {
        let account_idx = self.usize_id(account);
        let (nonce, key) = self.accounts[account_idx].random_key(u)?;
        let public_key = key.signer.public_key;
        self.accounts[account_idx].keys.remove(&nonce);
        Ok(public_key)
    }
}

impl Account {
    pub fn from_id(id: String) -> Self {
        let mut keys = HashMap::new();
        keys.insert(
            0,
            Key {
                signer: InMemorySigner::from_seed(
                    AccountId::from_str(id.as_str()).expect("Invalid account_id"),
                    KeyType::ED25519,
                    id.as_ref(),
                ),
                access_key: AccessKey { nonce: 0, permission: AccessKeyPermission::FullAccess },
            },
        );
        Self {
            id: AccountId::from_str(id.as_str()).expect("Invalid account_id"),
            balance: TESTING_INIT_BALANCE,
            deployed_contract: None,
            keys,
        }
    }

    pub fn full_access_keys(&self) -> Vec<InMemorySigner> {
        let mut full_access_keys = vec![];
        for (_, key) in &self.keys {
            if key.access_key.permission == AccessKeyPermission::FullAccess {
                full_access_keys.push(key.signer.clone());
            }
        }
        full_access_keys
    }

    pub fn function_call_keys(&self, receiver_id: &str) -> Vec<InMemorySigner> {
        let mut function_call_keys = vec![];
        for (_, key) in &self.keys {
            match &key.access_key.permission {
                AccessKeyPermission::FullAccess => function_call_keys.push(key.signer.clone()),
                AccessKeyPermission::FunctionCall(function_call_permission) => {
                    if function_call_permission.receiver_id == receiver_id {
                        function_call_keys.push(key.signer.clone())
                    }
                }
            }
        }
        function_call_keys
    }

    pub fn random_key(&self, u: &mut Unstructured) -> Result<(Nonce, Key)> {
        let (nonce, key) = *u.choose(&self.keys.iter().collect::<Vec<_>>())?;
        Ok((*nonce, key.clone()))
    }
}

impl Function {
    pub fn arbitrary(&self, u: &mut Unstructured) -> Result<FunctionCallAction> {
        let method_name;
        let mut args = Vec::new();
        match self {
            // #################
            // # Test contract #
            // #################
            Function::StorageUsage => {
                method_name = "ext_storage_usage";
            }
            Function::BlockIndex => {
                method_name = "ext_block_index";
            }
            Function::BlockTimestamp => {
                method_name = "ext_block_timestamp";
            }
            Function::PrepaidGas => {
                method_name = "ext_prepaid_gas";
            }
            Function::RandomSeed => {
                method_name = "ext_random_seed";
            }
            Function::PredecessorAccountId => {
                method_name = "ext_predecessor_account_id";
            }
            Function::SignerAccountPk => {
                method_name = "ext_signer_account_pk";
            }
            Function::SignerAccountId => {
                method_name = "ext_signer_account_id";
            }
            Function::CurrentAccountId => {
                method_name = "ext_current_account_id";
            }
            Function::AccountBalance => {
                method_name = "ext_account_balance";
            }
            Function::AttachedDeposit => {
                method_name = "ext_attached_deposit";
            }
            Function::ValidatorTotalStake => {
                method_name = "ext_validators_total_stake";
            }
            Function::ExtSha256 => {
                let len = u.int_in_range(0..=100)?;
                method_name = "ext_sha256";
                args = u.bytes(len)?.to_vec();
            }
            Function::UsedGas => {
                method_name = "ext_used_gas";
            }
            Function::WriteKeyValue => {
                let key = u.int_in_range::<u64>(0..=1_000)?.to_le_bytes();
                let value = u.int_in_range::<u64>(0..=1_000)?.to_le_bytes();
                method_name = "write_key_value";
                args = [&key[..], &value[..]].concat();
            }
            Function::WriteBlockHeight => {
                method_name = "write_block_height";
            }
            // ########################
            // # Contract for fuzzing #
            // ########################
            Function::SumOfNumbers => {
                method_name = "sum_of_numbers";
                args = u.int_in_range::<u64>(1..=10)?.to_le_bytes().to_vec();
            }
            Function::DataReceipt => {
                method_name = "data_receipt_with_size";
                args = u.choose(&[10u64, 100, 1000, 10000, 100000])?.to_le_bytes().to_vec();
            }
        };
        Ok(FunctionCallAction {
            method_name: method_name.to_string(),
            args: args,
            gas: GAS_1,
            deposit: 0,
        })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (0, Some(20))
    }
}
