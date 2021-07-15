use crate::run_test::{BlockConfig, NetworkConfig, Scenario, TransactionConfig};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::{
    account::{AccessKey, AccessKeyPermission},
    transaction::{Action, AddKeyAction, CreateAccountAction, StakeAction, TransferAction},
    types::{AccountId, Balance, BlockHeight, Nonce},
};
use nearcore::config::{NEAR_BASE, TESTING_INIT_BALANCE};

use libfuzzer_sys::arbitrary::{Arbitrary, Result, Unstructured};

impl Arbitrary<'_> for Scenario {
    fn arbitrary(u: &mut Unstructured<'_>) -> Result<Self> {
        let num_accounts = u.int_in_range(2..=100)?;

        let seeds: Vec<String> = (0..num_accounts).map(|i| format!("test{}", i)).collect();

        let mut scope = Scope::from_seeds(&seeds);

        let network_config = NetworkConfig { seeds };

        let num_blocks = u.int_in_range(1500..=2500)?;

        let mut blocks = vec![];

        for _ in 0..num_blocks {
            blocks.push(BlockConfig::arbitrary(u, &mut scope)?);
        }
        Ok(Scenario { network_config, blocks })
    }
}

impl BlockConfig {
    fn arbitrary(u: &mut Unstructured, scope: &mut Scope) -> Result<BlockConfig> {
        scope.inc_height();
        let mut block_config = BlockConfig::at_height(scope.get_height());

        let num_txs = u.int_in_range(0..=500)?;
        for _ in 0..num_txs {
            block_config.transactions.push(TransactionConfig::arbitrary(u, scope)?)
        }

        Ok(block_config)
    }
}

impl TransactionConfig {
    fn arbitrary(u: &mut Unstructured, scope: &mut Scope) -> Result<Self> {
        let mut options: Vec<fn(&mut Unstructured, &mut Scope) -> Result<TransactionConfig>> =
            vec![];

        scope.inc_nonce();

        // Transfer
        options.push(|u, scope| {
            let signer_account = scope.get_account(u)?;
            let receiver_account = scope.get_account(u)?;
            let amount = u.int_in_range::<u128>(0..=signer_account.balance)?;

            Ok(TransactionConfig {
                nonce: scope.get_nonce(),
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

        // Stake
        options.push(|u, scope| {
            let signer_account = scope.get_account(u)?;
            let amount = u.int_in_range::<u128>(0..=signer_account.balance)?;
            let signer =
                InMemorySigner::from_seed(&signer_account.id, KeyType::ED25519, &signer_account.id);
            let public_key = signer.public_key.clone();

            Ok(TransactionConfig {
                nonce: scope.get_nonce(),
                signer_id: signer_account.id.clone(),
                receiver_id: signer_account.id.clone(),
                signer,
                actions: vec![Action::Stake(StakeAction { stake: amount, public_key })],
            })
        });

        // Create Account
        options.push(|u, scope| {
            let signer_account = scope.get_account(u)?;
            let new_account = scope.new_account();

            let signer =
                InMemorySigner::from_seed(&signer_account.id, KeyType::ED25519, &signer_account.id);
            let new_public_key =
                InMemorySigner::from_seed(&new_account.id, KeyType::ED25519, &new_account.id)
                    .public_key;
            Ok(TransactionConfig {
                nonce: scope.get_nonce(),
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

        let f = u.choose(&options)?;
        f(u, scope)
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
    fn from_seeds(seeds: &Vec<String>) -> Self {
        let accounts = seeds.iter().map(|id| Account::from_id(id.clone())).collect();
        Scope { accounts, nonce: 0, height: 0 }
    }

    pub fn inc_height(&mut self) {
        self.height += 1
    }

    pub fn get_height(&self) -> BlockHeight {
        self.height
    }

    pub fn inc_nonce(&mut self) {
        self.nonce += 1;
    }

    pub fn get_nonce(&self) -> Nonce {
        self.nonce
    }

    pub fn get_account(&self, u: &mut Unstructured) -> Result<Account> {
        Ok(u.choose(&self.accounts)?.clone())
    }

    pub fn new_account(&mut self) -> Account {
        let new_id = format!("test{}", self.accounts.len());
        self.accounts.push(Account::from_id(new_id));
        self.accounts[self.accounts.len() - 1].clone()
    }
}

impl Account {
    pub fn from_id(id: AccountId) -> Self {
        Self { id, balance: TESTING_INIT_BALANCE }
    }
}
