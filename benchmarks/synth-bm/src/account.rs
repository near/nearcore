use std::fs;
use std::fs::create_dir;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time;

use crate::block_service::BlockService;
use crate::rpc::{ResponseCheckSeverity, RpcResponseHandler, new_request, view_access_key};
use clap::Args;
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
use tracing::{debug, info, warn};

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
    /// Optional prefixes for sub account names to avoid generating accounts that already exist on
    /// subsequent invocations.
    /// Prefixes are separated by commas.
    ///
    /// # Example
    ///
    /// The name of the `i`-th sub account will be:
    ///
    /// - `user_<i>.<signer_account_id>` if `sub_account_prefixes == None`
    /// - `a_user_<i>.<signer_account_id>` if `sub_account_prefixes == Some("a")`
    /// - `a_user_<i>.<signer_account_id>,b_user_<i>.<signer_account_id>`
    ///   if `sub_account_prefixes == Some("a,b")`
    #[arg(long, alias = "sub-account-prefix", use_value_delimiter = true)]
    pub sub_account_prefixes: Option<Vec<String>>,
    /// Number of sub accounts to create.
    #[arg(long)]
    pub num_sub_accounts: u64,
    /// Amount to deposit with each sub-account.
    #[arg(long)]
    pub deposit: u128,
    #[arg(long)]
    /// Acts as upper bound on the number of concurrently open RPC requests.
    pub channel_buffer_size: usize,
    /// Upper bound on request rate to the network for transaction (and other auxiliary) calls. The actual rate may be lower in case of congestions.
    #[arg(long)]
    pub requests_per_second: u64,
    /// Directory where created user account data (incl. key and nonce) is stored.
    #[arg(long)]
    pub user_data_dir: PathBuf,
    /// Ignore RPC failures while creating accounts. If enabled, the tool will try to generate the target number of accounts, best-effort.
    #[arg(long)]
    pub ignore_failures: bool,
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

/// Updates accounts with the nonce values requested from the network and optionally writes the updated values to the disk.
pub async fn update_account_nonces(
    client: JsonRpcClient,
    mut accounts: Vec<Account>,
    rps_limit: u64,
    accounts_path: Option<&PathBuf>,
    ignore_failures: bool,
) -> anyhow::Result<Vec<Account>> {
    let mut tasks = JoinSet::new();
    let mut account_idxs_to_remove = Vec::new();

    let mut interval = time::interval(Duration::from_micros(1_000_000u64 / rps_limit));
    for (i, account) in accounts.iter().enumerate() {
        interval.tick().await;
        let client = client.clone();
        let (id, pk) = (account.id.clone(), account.public_key.clone());
        tasks.spawn(async move { (i, view_access_key(&client, id, pk).await) });
    }

    while let Some(res) = tasks.join_next().await {
        let (idx, response) = res.expect("join should succeed");

        let nonce = match response {
            Ok(resp) => Some(resp.nonce),
            Err(err) => {
                if ignore_failures {
                    warn!("Error while querying account: {err}");
                    account_idxs_to_remove.push(idx);
                    None
                } else {
                    return Err(err);
                }
            }
        };

        if let (Some(new_nonce), account) = (nonce, accounts.get_mut(idx).unwrap()) {
            if account.nonce != new_nonce {
                debug!(
                    name = "nonce updated",
                    user = account.id.to_string(),
                    nonce.old = account.nonce,
                    nonce.new = new_nonce,
                );
                account.nonce = new_nonce;
                if let Some(path) = accounts_path {
                    account.write_to_dir(path)?;
                }
            }
        }
    }

    // Remove accounts that we couldn't find in the RPC node.
    account_idxs_to_remove.sort_unstable();
    for idx in account_idxs_to_remove.into_iter().rev() {
        accounts.swap_remove(idx);
    }

    Ok(accounts)
}

pub async fn create_sub_accounts(args: &CreateSubAccountsArgs) -> anyhow::Result<()> {
    let signer = InMemorySigner::from_file(&args.signer_key_path)?;

    let client = JsonRpcClient::connect(&args.rpc_url);
    let block_service = Arc::new(BlockService::new(client.clone()).await);
    block_service.clone().start().await;

    let mut interval = time::interval(Duration::from_micros(1_000_000 / args.requests_per_second));
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
            // cspell:words subname
            let subname = if let Some(prefixes) = &args.sub_account_prefixes {
                let prefix = &prefixes[(i as usize) % prefixes.len()];
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
        // The spawned task starts running immediately. Assume an interval between spanning them
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
    // Query them from the rpc to write `Accounts` with valid nonces to disk
    sub_accounts = update_account_nonces(
        client.clone(),
        sub_accounts,
        args.requests_per_second,
        None,
        args.ignore_failures,
    )
    .await?;

    for account in sub_accounts.iter() {
        account.write_to_dir(&args.user_data_dir)?;
    }

    info!("Written {} accounts to disk", sub_accounts.len());

    Ok(())
}
