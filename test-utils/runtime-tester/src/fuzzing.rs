use crate::run_test::{BlockConfig, NetworkConfig, Scenario, TransactionConfig};
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
use std::iter::FromIterator;
use std::mem::size_of;

pub type ContractId = usize;

pub const MAX_BLOCKS: usize = 2500;
pub const MAX_TXS: usize = 500;
pub const MAX_TX_DIFF: usize = 10;
pub const MAX_ACCOUNTS: usize = 100;

const GAS_1: u64 = 900_000_000_000_000;
const GAS_2: u64 = GAS_1 / 3;
// const GAS_3: u64 = GAS_2 / 3;

impl Arbitrary<'_> for Scenario {
    fn arbitrary(u: &mut Unstructured<'_>) -> Result<Self> {
        let num_accounts = u.int_in_range(2..=MAX_ACCOUNTS)?;

        let seeds: Vec<String> = (0..num_accounts).map(|i| format!("test{}", i)).collect();

        let mut scope = Scope::from_seeds(&seeds);

        let network_config = NetworkConfig { seeds };

        let mut blocks = vec![];

        while blocks.len() < MAX_BLOCKS && u.len() > BlockConfig::size_hint(0).1.unwrap() {
            blocks.push(BlockConfig::arbitrary(u, &mut scope)?);
        }
        Ok(Scenario { network_config, blocks })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (0, Some(MAX_BLOCKS * BlockConfig::size_hint(0).1.unwrap()))
    }
}

impl BlockConfig {
    fn arbitrary(u: &mut Unstructured, scope: &mut Scope) -> Result<BlockConfig> {
        scope.inc_height();
        let mut block_config = BlockConfig::at_height(scope.height());

        let lower_bound = scope.last_tx_num.checked_sub(MAX_TX_DIFF).or(Some(0)).unwrap();
        let upper_bound = scope.last_tx_num.checked_add(MAX_TX_DIFF).or(Some(MAX_TXS)).unwrap();
        let max_tx_num = u.int_in_range(lower_bound..=std::cmp::min(MAX_TXS, upper_bound))?;
        scope.last_tx_num = max_tx_num;

        while block_config.transactions.len() < max_tx_num
            && u.len() > TransactionConfig::size_hint(0).1.unwrap()
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

            scope.accounts[signer_account.usize_id()].balance = scope.accounts
                [signer_account.usize_id()]
            .balance
            .checked_sub(amount)
            .or(Some(0))
            .unwrap();
            scope.accounts[receiver_account.usize_id()].balance = scope.accounts
                [receiver_account.usize_id()]
            .balance
            .checked_add(amount)
            .or(Some(Balance::MAX))
            .unwrap();

            Ok(TransactionConfig {
                nonce: scope.nonce(),
                signer_id: signer_account.id.clone(),
                receiver_id: receiver_account.id.clone(),
                signer: InMemorySigner::from_seed(
                    &signer_account.id,
                    KeyType::ED25519,
                    &signer_account.id,
                ),
                actions: vec![Action::Transfer(TransferAction { deposit: amount })],
            })
        });

        /* This actually can create new block producers, and currently we only support one.
        // Stake
        options.push(|u, scope| {
            let signer_account = scope.random_account(u)?;
            let amount = u.int_in_range::<u128>(0..=signer_account.balance)?;
            let signer =
                InMemorySigner::from_seed(&signer_account.id, KeyType::ED25519, &signer_account.id);
            let public_key = signer.public_key.clone();

            Ok(TransactionConfig {
                nonce: scope.nonce(),
                signer_id: signer_account.id.clone(),
                receiver_id: signer_account.id.clone(),
                signer,
                actions: vec![Action::Stake(StakeAction { stake: amount, public_key })],
            })
        });
         */

        // Create Account
        options.push(|u, scope| {
            let signer_account = scope.random_account(u)?;
            let new_account = scope.new_account();

            let signer =
                InMemorySigner::from_seed(&signer_account.id, KeyType::ED25519, &signer_account.id);
            let new_public_key =
                InMemorySigner::from_seed(&new_account.id, KeyType::ED25519, &new_account.id)
                    .public_key;
            Ok(TransactionConfig {
                nonce: scope.nonce(),
                signer_id: signer_account.id.clone(),
                receiver_id: new_account.id.clone(),
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
                let account_usize_id = scope.random_non_zero_alive_account_usize_id(u)?;
                let signer_account = scope.accounts[account_usize_id].clone();

                let signer = InMemorySigner::from_seed(
                    &signer_account.id,
                    KeyType::ED25519,
                    &signer_account.id,
                );

                Ok(TransactionConfig {
                    nonce: scope.nonce(),
                    signer_id: signer_account.id.clone(),
                    receiver_id: signer_account.id.clone(),
                    signer,
                    actions: vec![Action::DeleteAccount(DeleteAccountAction {
                        beneficiary_id: signer_account.id.clone(),
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

            let signer =
                InMemorySigner::from_seed(&signer_account.id, KeyType::ED25519, &signer_account.id);

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

        // Call
        for acc in &scope.accounts {
            if acc.deployed_contracts.len() > 0 {
                options.push(|u, scope| {
                    let nonce = scope.nonce();

                    let mut accounts_with_contracts = vec![];
                    for acc in &scope.accounts {
                        if acc.deployed_contracts.len() > 0 {
                            accounts_with_contracts.push(acc.usize_id());
                        }
                    }

                    let signer_account = scope.random_account(u)?;
                    let receiver_account = &scope.accounts[*u.choose(&accounts_with_contracts)?];

                    let contract_id = *u.choose(
                        &receiver_account.deployed_contracts.iter().cloned().collect::<Vec<_>>(),
                    )?;
                    let function = u.choose(&scope.available_contracts[contract_id].functions)?;

                    let signer = InMemorySigner::from_seed(
                        &signer_account.id,
                        KeyType::ED25519,
                        &signer_account.id,
                    );

                    Ok(TransactionConfig {
                        nonce,
                        signer_id: signer_account.id.clone(),
                        receiver_id: receiver_account.id.clone(),
                        signer,
                        actions: vec![Action::FunctionCall(
                            function.arbitrary(u, &signer_account)?,
                        )],
                    })
                });
                break;
            }
        }

        let f = u.choose(&options)?;
        f(u, scope)
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (7, Some(14))
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
    pub deployed_contracts: HashSet<ContractId>,
}

#[derive(Clone)]
pub struct Contract {
    pub code: Vec<u8>,
    pub functions: Vec<Function>,
}

#[derive(Clone)]
pub enum Function {
    BubbleSort,
    CallPromise,
    WriteBlockHeight,
    ExtUsedGas,
    WriteKeyValue,
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
            /*
            Contract {
                code: near_test_contracts::load_contract().to_vec(),
                functions: vec![Function::BubbleSort],
            },
             */
            Contract {
                code: near_test_contracts::rs_contract().to_vec(),
                functions: vec![
                    Function::CallPromise,
                    Function::WriteBlockHeight,
                    Function::ExtUsedGas,
                    Function::WriteKeyValue,
                ],
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
        self.accounts[acc_id].deployed_contracts.insert(contract_id);
    }
}

impl Account {
    pub fn from_id(id: AccountId) -> Self {
        Self { id, balance: TESTING_INIT_BALANCE, deployed_contracts: HashSet::default() }
    }

    pub fn usize_id(&self) -> usize {
        self.id[4..].parse::<usize>().unwrap()
    }
}

impl Function {
    pub fn arbitrary(
        &self,
        u: &mut Unstructured,
        signer_account: &Account,
    ) -> Result<FunctionCallAction> {
        match self {
            Function::BubbleSort => Ok(FunctionCallAction {
                method_name: "bubble_sort".to_string(),
                args: vec![],
                gas: GAS_1,
                deposit: 1,
            }),
            Function::CallPromise => {
                let data = serde_json::json!([
                    {"create": {
                    "account_id": signer_account.id,
                    "method_name": "call_promise",
                    "arguments": [],
                    "amount": "0",
                    "gas": GAS_2,
                    }, "id": 0 }
                ]);
                Ok(FunctionCallAction {
                    method_name: "call_promise".to_string(),
                    args: serde_json::to_vec(&data).unwrap(),
                    gas: GAS_1,
                    deposit: 0,
                })
            }
            Function::WriteBlockHeight => Ok(FunctionCallAction {
                method_name: "write_block_height".to_string(),
                args: vec![],
                gas: GAS_1,
                deposit: 0,
            }),
            Function::ExtUsedGas => Ok(FunctionCallAction {
                method_name: "ext_used_gas".to_string(),
                args: vec![],
                gas: GAS_1,
                deposit: 0,
            }),
            Function::WriteKeyValue => {
                let key = u.int_in_range::<u64>(0..=1_000)?;
                let value = u.int_in_range::<u64>(0..=1_000)?;
                let mut args = [0u8; 2 * size_of::<u64>()];
                LittleEndian::write_u64_into(&[key, value], &mut args);
                Ok(FunctionCallAction {
                    method_name: "write_key_value".to_string(),
                    args: args.to_vec(),
                    gas: GAS_1,
                    deposit: 1,
                })
            }
        }
    }
}
