use std::convert::TryFrom;

use crate::run_test::{BlockConfig, NetworkConfig, Scenario, TransactionConfig};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::{
    account::{AccessKey, AccessKeyPermission},
    transaction::{Action, AddKeyAction, CreateAccountAction, StakeAction, TransferAction},
    types::{AccountId, Balance, BlockHeight, Nonce},
};
use nearcore::config::{NEAR_BASE, TESTING_INIT_BALANCE};

use libfuzzer_sys::arbitrary::{Arbitrary, Result, Unstructured};

pub const MAX_BLOCKS: usize = 2500;
pub const MAX_TXS: usize = 300;
pub const MAX_ACCOUNTS: usize = 100;

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

        let max_tx_num = u.int_in_range(0..=MAX_TXS)?;

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

        // Transfer
        options.push(|u, scope| {
            let signer_account = scope.random_account(u)?;
            let receiver_account = scope.random_account(u)?;
            let amount = u.int_in_range::<u128>(0..=signer_account.balance)?;

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
    nonce: Nonce,
    height: BlockHeight,
}

#[derive(Clone)]
pub struct Account {
    pub id: AccountId,
    pub balance: Balance,
}

impl Scope {
    fn from_seeds(seeds: &[String]) -> Self {
        let accounts = seeds.iter().map(|id| Account::from_id(id.parse().unwrap())).collect();
        Scope { accounts, nonce: 0, height: 0 }
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

    pub fn random_account(&self, u: &mut Unstructured) -> Result<Account> {
        Ok(u.choose(&self.accounts)?.clone())
    }

    pub fn new_account(&mut self) -> Account {
        let new_id = format!("test{}", self.accounts.len());
        self.accounts.push(Account::from_id(AccountId::try_from(new_id).unwrap()));
        self.accounts[self.accounts.len() - 1].clone()
    }
}

impl Account {
    pub fn from_id(id: AccountId) -> Self {
        Self { id, balance: TESTING_INIT_BALANCE }
    }
}
