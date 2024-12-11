use std::fs;
use std::fs::create_dir;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time;

use crate::block_service::BlockService;
use crate::rpc::{new_request, view_access_key, ResponseCheckSeverity, RpcResponseHandler};
use clap::Args;
use log::info;
use near_crypto::{InMemorySigner, KeyType, SecretKey};
use near_crypto::{PublicKey, Signer};
use near_jsonrpc_client::JsonRpcClient;
use near_primitives::views::TxExecutionStatus;
use near_primitives::{
    account::{AccessKey, AccessKeyPermission},
    action::{Action, AddKeyAction, CreateAccountAction, TransferAction},
};
use near_primitives::{
    transaction::{Transaction, TransactionV0},
    types::AccountId,
};
use serde::{Deserialize, Serialize};

#[derive(Args, Debug)]
pub struct CreateSubAccountsArgs {
    /// TODO try to have single arg for all commands
    #[arg(long)]
    pub rpc_url: String,
    #[arg(long)]
    pub signer_key_path: PathBuf,
    /// Starting nonce > current_nonce to send transactions to create sub accounts.
    // TODO remove this field and get nonce from rpc
    #[arg(long, default_value_t = 1)]
    pub nonce: u64,
    /// Optional prefix for sub account names to avoid generating accounts that already exist on
    /// subsequent invocations.
    ///
    /// # Example
    ///
    /// The name of the `i`-th sub account will be:
    ///
    /// - `user_<i>.<signer_account_id>` if `sub_account_prefix == None`
    /// - `a_user_<i>.<signer_account_id>` if `sub_account_prefix == Some("a")`
    #[arg(long)]
    pub sub_account_prefix: Option<String>,
    /// Number of sub accounts to create.
    #[arg(long)]
    pub num_sub_accounts: u64,
    /// Amount to deposit with each sub-account.
    #[arg(long)]
    pub deposit: u128,
    #[arg(long)]
    /// Acts as upper bound on the number of concurrently open RPC requests.
    pub channel_buffer_size: usize,
    /// After each tick (in microseconds) a transaction is sent. If the hardware cannot keep up with
    /// that or if the NEAR node is congested, transactions are sent at a slower rate.
    #[arg(long)]
    pub interval_duration_micros: u64,
    /// Directory where created user account data (incl. key and nonce) is stored.
    #[arg(long)]
    pub user_data_dir: PathBuf,
}

pub fn new_create_subaccount_actions(public_key: PublicKey, deposit: u128) -> Vec<Action> {
    vec![
        Action::CreateAccount(CreateAccountAction {}),
        Action::AddKey(Box::new(AddKeyAction {
            access_key: AccessKey {
                nonce: 0, // This value will be ignored: https://github.com/near/nearcore/pull/4064
                permission: AccessKeyPermission::FullAccess,
            },
            public_key,
        })),
        Action::Transfer(TransferAction { deposit }),
    ]
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Account {
    #[serde(rename = "account_id")]
    pub id: AccountId,
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
    // New transaction must have a nonce bigger than this.
    pub nonce: u64,
}

impl Account {
    pub fn new(id: AccountId, secret_key: SecretKey, nonce: u64) -> Self {
        Self { id, public_key: secret_key.public_key(), secret_key, nonce }
    }

    pub fn from_file(path: &Path) -> anyhow::Result<Account> {
        let content = fs::read_to_string(path)?;
        let account = serde_json::from_str(&content)?;
        Ok(account)
    }

    pub fn write_to_dir(&self, dir: &Path) -> anyhow::Result<()> {
        if !dir.exists() {
            create_dir(dir)?;
        }

        let json = serde_json::to_string(self)?;
        let mut file_name = self.id.to_string();
        file_name.push_str(".json");
        let file_path = dir.join(file_name);
        fs::write(file_path, json)?;
        Ok(())
    }

    pub fn as_signer(&self) -> Signer {
        Signer::from(InMemorySigner::from_secret_key(self.id.clone(), self.secret_key.clone()))
    }
}

/// Tries to deserialize all json files in `dir` as [`Account`].
pub fn accounts_from_dir(dir: &Path) -> anyhow::Result<Vec<Account>> {
    if !dir.is_dir() {
        anyhow::bail!("{:?} is not a directory", dir);
    }

    let mut accounts = vec![];
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_file() {
            continue;
        }
        let path = entry.path();
        let file_extension = path.extension();
        if file_extension.is_none() || file_extension.unwrap() != "json" {
            continue;
        }
        let account = Account::from_file(&path)?;
        accounts.push(account);
    }

    Ok(accounts)
}

pub async fn create_sub_accounts(args: &CreateSubAccountsArgs) -> anyhow::Result<()> {
    let signer = InMemorySigner::from_file(&args.signer_key_path)?;

    let client = JsonRpcClient::connect(&args.rpc_url);
    let block_service = Arc::new(BlockService::new(client.clone()).await);
    block_service.clone().start().await;

    let mut interval = time::interval(Duration::from_micros(args.interval_duration_micros));
    let timer = Instant::now();

    let mut sub_accounts: Vec<Account> =
        Vec::with_capacity(args.num_sub_accounts.try_into().unwrap());

    // Before a request is made, a permit to send into the channel is awaited. Hence buffer size
    // limits the number of outstanding requests. This helps to avoid congestion.
    // TODO find reasonable buffer size.
    let (channel_tx, channel_rx) = mpsc::channel(args.channel_buffer_size);

    let wait_until = TxExecutionStatus::ExecutedOptimistic;
    let wait_until_channel = wait_until.clone();
    let num_expected_responses = args.num_sub_accounts;
    let response_handler_task = tokio::task::spawn(async move {
        let mut rpc_response_handler = RpcResponseHandler::new(
            channel_rx,
            wait_until_channel,
            ResponseCheckSeverity::Assert,
            num_expected_responses,
        );
        rpc_response_handler.handle_all_responses().await;
    });

    for i in 0..args.num_sub_accounts {
        let sub_account_key = SecretKey::from_random(KeyType::ED25519);
        let sub_account_id: AccountId = {
            let subname = if let Some(prefix) = &args.sub_account_prefix {
                format!("{prefix}_user_{i}")
            } else {
                format!("user_{i}")
            };
            format!("{subname}.{}", signer.account_id).parse()?
        };
        let tx = Transaction::V0(TransactionV0 {
            signer_id: signer.account_id.clone(),
            public_key: signer.public_key().clone(),
            nonce: args.nonce + i,
            receiver_id: sub_account_id.clone(),
            block_hash: block_service.get_block_hash(),
            actions: new_create_subaccount_actions(
                sub_account_key.public_key().clone(),
                args.deposit,
            ),
        });
        let request = new_request(tx, wait_until.clone(), signer.clone());

        interval.tick().await;
        let client = client.clone();
        // Await permit before sending the request to make channel buffer size a limit for the
        // number of outstanding requests.
        let permit = channel_tx.clone().reserve_owned().await.unwrap();
        // The spawned task starts running immediately. Assume with interval between spanning them
        // this leads to transaction nonces hitting the node in order.
        tokio::spawn(async move {
            let res = client.call(request).await;
            permit.send(res);
        });

        sub_accounts.push(Account::new(sub_account_id, sub_account_key, 0));
    }

    info!("Sent {} txs in {:.2} seconds", args.num_sub_accounts, timer.elapsed().as_secs_f64());

    // Ensure all rpc responses are handled.
    response_handler_task.await.expect("response handler tasks should succeed");

    info!("Querying nonces of newly created sub accounts.");

    // Nonces of new access keys are set by nearcore: https://github.com/near/nearcore/pull/4064
    // Query them from the rpc to write `Accounts` with valid nonces to disk.
    // TODO use `JoinSet`, e.g. by storing accounts in map instead of vec.
    let mut get_access_key_tasks = Vec::with_capacity(sub_accounts.len());
    // Use an interval to avoid overwhelming the node with requests.
    let mut interval = time::interval(Duration::from_micros(150));
    for account in sub_accounts.clone().into_iter() {
        interval.tick().await;
        let client = client.clone();
        get_access_key_tasks.push(tokio::spawn(async move {
            view_access_key(&client, account.id.clone(), account.public_key.clone()).await
        }))
    }

    for (i, task) in get_access_key_tasks.into_iter().enumerate() {
        let response = task.await.expect("join should succeed");
        let nonce = response?.nonce;
        let account = sub_accounts.get_mut(i).unwrap();
        account.nonce = nonce;
        account.write_to_dir(&args.user_data_dir)?;
    }

    Ok(())
}
