use crate::run_test::{BlockConfig, NetworkConfig, RuntimeConfig, Scenario, TransactionConfig};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::{
    account::{AccessKey, AccessKeyPermission},
    transaction::{
        Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeployContractAction,
        FunctionCallAction, TransferAction,
    },
    types::{AccountId, Balance, BlockHeight, Nonce},
};
use nearcore::config::{NEAR_BASE, TESTING_INIT_BALANCE};

use byteorder::{ByteOrder, LittleEndian};
use libfuzzer_sys::arbitrary::{Arbitrary, Result, Unstructured};

use std::collections::HashSet;
use std::mem::size_of;
use std::str::FromStr;

pub type ContractId = usize;

pub const MAX_BLOCKS: usize = 2500;
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
        let runtime_config = RuntimeConfig { max_total_prepaid_gas: GAS_1 * 100 };

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
            let receiver_account = scope.random_account(u)?;
            let amount = u.int_in_range::<u128>(0..=signer_account.balance)?;

            scope.accounts[signer_account.usize_id()].balance =
                scope.accounts[signer_account.usize_id()].balance.saturating_sub(amount);
            scope.accounts[receiver_account.usize_id()].balance =
                scope.accounts[receiver_account.usize_id()].balance.saturating_add(amount);

            Ok(TransactionConfig {
                nonce: scope.nonce(),
                signer_id: signer_account.id.clone(),
                receiver_id: receiver_account.id.clone(),
                signer: InMemorySigner::from_seed(
                    signer_account.id.clone(),
                    KeyType::ED25519,
                    signer_account.id.as_ref(),
                ),
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
            let new_account = scope.new_account();

            let signer = InMemorySigner::from_seed(
                signer_account.id.clone(),
                KeyType::ED25519,
                signer_account.id.as_ref(),
            );
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
                let signer_account = scope.random_account(u)?;
                let receiver_account = scope.random_non_zero_account(u)?;
                let beneficiary_id = {
                    if u.arbitrary::<bool>()? {
                        scope.accounts.len() + 100
                    } else {
                        scope.random_alive_account_usize_id(u)?
                    }
                };

                let signer = InMemorySigner::from_seed(
                    signer_account.id.clone(),
                    KeyType::ED25519,
                    signer_account.id.as_ref(),
                );

                scope.delete_account(receiver_account.usize_id());

                Ok(TransactionConfig {
                    nonce: scope.nonce(),
                    signer_id: signer_account.id.clone(),
                    receiver_id: receiver_account.id.clone(),
                    signer,
                    actions: vec![Action::DeleteAccount(DeleteAccountAction {
                        beneficiary_id: AccountId::from_str(
                            format!("test{}", beneficiary_id).as_str(),
                        )
                        .expect("Invalid account_id"),
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

            let signer = InMemorySigner::from_seed(
                signer_account.id.clone(),
                KeyType::ED25519,
                signer_account.id.as_ref(),
            );

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
            let receiver_account = scope.random_account(u)?;

            let signer = InMemorySigner::from_seed(
                signer_account.id.clone(),
                KeyType::ED25519,
                signer_account.id.as_ref(),
            );

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
}

#[derive(Clone)]
pub struct Account {
    pub id: AccountId,
    pub balance: Balance,
    pub deployed_contract: Option<ContractId>,
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
        let accounts = seeds.iter().map(|id| Account::from_id(id.clone())).collect();
        Scope {
            accounts,
            alive_accounts: HashSet::from_iter(0..seeds.len()),
            nonce: 0,
            height: 0,
            available_contracts: Scope::construct_available_contracts(),
            last_tx_num: MAX_TXS,
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

    pub fn new_account(&mut self) -> Account {
        let new_id = format!("test{}", self.accounts.len());
        self.alive_accounts.insert(self.accounts.len());
        self.accounts.push(Account::from_id(new_id));
        self.accounts[self.accounts.len() - 1].clone()
    }

    pub fn delete_account(&mut self, account_usize_id: usize) {
        self.alive_accounts.remove(&account_usize_id);
    }

    pub fn deploy_contract(&mut self, receiver_account: &Account, contract_id: usize) {
        let acc_id = receiver_account.usize_id();
        self.accounts[acc_id].deployed_contract = Some(contract_id);
    }
}

impl Account {
    pub fn from_id(id: String) -> Self {
        Self {
            id: AccountId::from_str(id.as_str()).expect("Invalid account_id"),
            balance: TESTING_INIT_BALANCE,
            deployed_contract: None,
        }
    }

    pub fn usize_id(&self) -> usize {
        self.id.as_ref()[4..].parse::<usize>().unwrap()
    }
}

impl Function {
    pub fn arbitrary(&self, u: &mut Unstructured) -> Result<FunctionCallAction> {
        let mut res =
            FunctionCallAction { method_name: String::new(), args: vec![], gas: GAS_1, deposit: 0 };
        match self {
            // #################
            // # Test contract #
            // #################
            Function::StorageUsage => {
                res.method_name = "ext_storage_usage".to_string();
            }
            Function::BlockIndex => {
                res.method_name = "ext_block_index".to_string();
            }
            Function::BlockTimestamp => {
                res.method_name = "ext_block_timestamp".to_string();
            }
            Function::PrepaidGas => {
                res.method_name = "ext_prepaid_gas".to_string();
            }
            Function::RandomSeed => {
                res.method_name = "ext_random_seed".to_string();
            }
            Function::PredecessorAccountId => {
                res.method_name = "ext_predecessor_account_id".to_string();
            }
            Function::SignerAccountPk => {
                res.method_name = "ext_signer_account_pk".to_string();
            }
            Function::SignerAccountId => {
                res.method_name = "ext_signer_account_id".to_string();
            }
            Function::CurrentAccountId => {
                res.method_name = "ext_current_account_id".to_string();
            }
            Function::AccountBalance => {
                res.method_name = "ext_account_balance".to_string();
            }
            Function::AttachedDeposit => {
                res.method_name = "ext_attached_deposit".to_string();
            }
            Function::ValidatorTotalStake => {
                res.method_name = "ext_validators_total_stake".to_string();
            }
            Function::ExtSha256 => {
                const VALUES_LEN: usize = 20;
                let mut args = [0u8; VALUES_LEN * size_of::<u64>()];
                let mut values = vec![];
                for _ in 0..VALUES_LEN {
                    values.push(u.arbitrary::<u64>()?);
                }
                LittleEndian::write_u64_into(&values, &mut args);
                res.method_name = "ext_sha256".to_string();
                res.args = args.to_vec();
            }
            Function::UsedGas => {
                res.method_name = "ext_used_gas".to_string();
            }
            Function::WriteKeyValue => {
                let key = u.int_in_range::<u64>(0..=1_000)?;
                let value = u.int_in_range::<u64>(0..=1_000)?;
                let mut args = [0u8; 2 * size_of::<u64>()];
                LittleEndian::write_u64_into(&[key, value], &mut args);
                res.method_name = "write_key_value".to_string();
                res.args = args.to_vec();
            }
            Function::WriteBlockHeight => {
                res.method_name = "write_block_height".to_string();
            }
            // ########################
            // # Contract for fuzzing #
            // ########################
            Function::SumOfNumbers => {
                let args = u.int_in_range::<u64>(1..=10)?.to_le_bytes();
                res.method_name = "sum_of_numbers".to_string();
                res.args = args.to_vec();
            }
            Function::DataReceipt => {
                let args = (*u.choose(&[10, 100, 1000, 10000, 100000])? as u64).to_le_bytes();
                res.method_name = "data_receipt_with_size".to_string();
                res.args = args.to_vec();
            }
        };
        Ok(res)
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (0, Some(20))
    }
}
