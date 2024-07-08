use crate::{
    tests::{
        utils::{self, codec},
        GET_NONCE, RLP_EXECUTE,
    },
    types::{Action, ExecuteResponse},
};
use aurora_engine_transactions::EthTransactionKind;
use aurora_engine_types::types::Wei;
use ethabi::Address;
use near_sdk::json_types::U64;
use near_workspaces::{
    network::Sandbox,
    result::ExecutionFinalResult,
    types::{KeyType, NearToken, PublicKey, SecretKey},
    Account, Contract, Worker,
};
use std::path::{Path, PathBuf};
use tokio::{process::Command, sync::Mutex};

const BASE_DIR: &str = std::env!("CARGO_MANIFEST_DIR");
const PACKAGE_NAME: &str = std::env!("CARGO_PKG_NAME");
const INITIAL_BALANCE: NearToken = NearToken::from_near(20);

// Prevents multiple tests from trying to compile the contracts at the same time.
static LOCK: Mutex<()> = Mutex::const_new(());

pub struct WalletContract {
    pub inner: Contract,
    pub sk: near_crypto::SecretKey,
}

impl WalletContract {
    pub async fn rlp_execute_with_receipts(
        &self,
        target: &str,
        tx: &EthTransactionKind,
    ) -> anyhow::Result<ExecutionFinalResult> {
        let result = self
            .inner
            .call(RLP_EXECUTE)
            .args_json(serde_json::json!({
                "target": target,
                "tx_bytes_b64": codec::encode_b64(&codec::rlp_encode(tx))
            }))
            .max_gas()
            .transact()
            .await?;

        Ok(result)
    }

    pub async fn rlp_execute_from(
        &self,
        caller: &Account,
        target: &str,
        tx: &EthTransactionKind,
        attached_deposit: NearToken,
    ) -> anyhow::Result<ExecuteResponse> {
        let result: ExecuteResponse = caller
            .call(self.inner.id(), RLP_EXECUTE)
            .args_json(serde_json::json!({
                "target": target,
                "tx_bytes_b64": codec::encode_b64(&codec::rlp_encode(tx))
            }))
            .max_gas()
            .deposit(attached_deposit)
            .transact()
            .await?
            .into_result()?
            .json()?;

        Ok(result)
    }

    pub async fn rlp_execute(
        &self,
        target: &str,
        tx: &EthTransactionKind,
    ) -> anyhow::Result<ExecuteResponse> {
        let result: ExecuteResponse =
            self.rlp_execute_with_receipts(target, tx).await?.into_result()?.json()?;

        Ok(result)
    }

    pub async fn get_nonce(&self) -> anyhow::Result<u64> {
        let nonce: U64 = self.inner.view(GET_NONCE).await?.json()?;
        Ok(nonce.0)
    }

    /// Add a new `FunctionCall` access key to the Wallet Contract.
    /// The idea is that this allows the relayer to submit transactions signed by
    /// the Wallet Contract directly.
    pub async fn register_relayer(
        &mut self,
        worker: &Worker<Sandbox>,
    ) -> anyhow::Result<PublicKey> {
        let relayer_account = worker.dev_create_account().await?;
        let relayer_key = SecretKey::from_random(KeyType::ED25519);
        let relayer_pk = relayer_key.public_key();

        let action = Action::AddKey {
            public_key_kind: 0,
            public_key: relayer_pk.key_data().to_vec(),
            nonce: 0,
            is_full_access: false,
            is_limited_allowance: false,
            allowance: 0,
            receiver_id: self.inner.id().to_string(),
            method_names: vec![RLP_EXECUTE.into()],
        };
        let nonce = self.get_nonce().await?;
        let signed_transaction =
            utils::create_signed_transaction(nonce, self.inner.id(), Wei::zero(), action, &self.sk);

        // Call the Wallet Contract from the relayer account to add the key
        let result: ExecuteResponse = relayer_account
            .call(self.inner.id(), RLP_EXECUTE)
            .args_json(serde_json::json!({
                "target": self.inner.id(),
                "tx_bytes_b64": codec::encode_b64(&codec::rlp_encode(&signed_transaction))
            }))
            .max_gas()
            .transact()
            .await?
            .into_result()?
            .json()?;

        assert!(result.success, "Adding Relayer's key failed: {:?}", result.error);

        // Tell near-workspaces to use this new key instead when
        // signing transactions from the Wallet Contract
        self.inner.as_account_mut().set_secret_key(relayer_key);

        Ok(relayer_pk)
    }
}

pub struct TestContext {
    pub worker: Worker<Sandbox>,
    pub wallet_contract: WalletContract,
    pub wallet_sk: near_crypto::SecretKey,
    pub wallet_address: Address,
    pub address_registrar: Contract,
    pub wallet_contract_bytes: Vec<u8>,
}

impl TestContext {
    pub async fn new() -> anyhow::Result<Self> {
        let _guard = LOCK.lock().await;
        let worker = near_workspaces::sandbox().await?;

        let registrar_id_path = address_registrar_account_id_path(BASE_DIR);
        let original_registrar_id = tokio::fs::read(&registrar_id_path).await?;
        let address_registrar = Self::deploy_address_registrar(&worker).await?;
        let wallet_contract_bytes = build_contract(BASE_DIR, PACKAGE_NAME).await?;
        // Restore address registrar account id file
        tokio::fs::write(registrar_id_path, &original_registrar_id).await?;

        let (wallet_contract, wallet_address) =
            Self::deploy_wallet(&worker, &wallet_contract_bytes).await?;
        let wallet_sk = wallet_contract.sk.clone();

        Ok(Self {
            worker,
            wallet_contract,
            wallet_sk,
            wallet_address,
            address_registrar,
            wallet_contract_bytes,
        })
    }

    async fn deploy_address_registrar(worker: &Worker<Sandbox>) -> anyhow::Result<Contract> {
        let base_dir = Path::new(BASE_DIR).parent().unwrap().join("address-registrar");
        let contract_bytes = build_contract(base_dir, "eth-address-registrar").await?;
        let contract = worker.dev_deploy(&contract_bytes).await?;

        // Initialize the contract
        contract.call("new").transact().await.unwrap().into_result().unwrap();

        // Update the file where the Wallet Contract gets the address registrar account id from
        tokio::fs::write(address_registrar_account_id_path(BASE_DIR), contract.id().as_bytes())
            .await?;

        Ok(contract)
    }

    pub async fn deploy_wallet(
        worker: &Worker<Sandbox>,
        contract_bytes: &[u8],
    ) -> anyhow::Result<(WalletContract, Address)> {
        let wallet_sk = near_crypto::SecretKey::from_random(near_crypto::KeyType::SECP256K1);
        let wallet_address = {
            let wallet_pk = wallet_sk.public_key();
            let hash = crate::internal::keccak256(wallet_pk.key_data());
            Address::from_slice(&hash[12..32])
        };
        let wallet_account = worker
            .root_account()?
            .create_subaccount(&format!("0x{}", hex::encode(wallet_address)))
            .keys(SecretKey::from_random(KeyType::ED25519))
            .initial_balance(INITIAL_BALANCE)
            .transact()
            .await?
            .result;
        let wallet_contract = WalletContract {
            inner: wallet_account.deploy(contract_bytes).await?.result,
            sk: wallet_sk,
        };

        Ok((wallet_contract, wallet_address))
    }
}

async fn build_contract<P: AsRef<Path>>(
    base_dir: P,
    package_name: &str,
) -> anyhow::Result<Vec<u8>> {
    let output = Command::new("cargo")
        .env("RUSTFLAGS", "-C link-arg=-s")
        .current_dir(base_dir.as_ref())
        .args(["build", "--target", "wasm32-unknown-unknown", "--release"])
        .output()
        .await?;

    if !output.status.success() {
        anyhow::bail!("Build failed: {}", String::from_utf8_lossy(&output.stderr));
    }

    let artifact_path = base_dir
        .as_ref()
        .parent()
        .unwrap()
        .join("target")
        .join("wasm32-unknown-unknown")
        .join("release")
        .join([package_name.replace('-', "_").as_str(), ".wasm"].concat());

    let bytes = tokio::fs::read(artifact_path).await?;
    Ok(bytes)
}

fn address_registrar_account_id_path(base_dir: &str) -> PathBuf {
    Path::new(base_dir).join("src").join("ADDRESS_REGISTRAR_ACCOUNT_ID")
}
