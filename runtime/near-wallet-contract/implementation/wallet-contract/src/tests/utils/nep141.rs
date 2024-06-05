use near_contract_standards::storage_management::StorageBalance;
use near_sdk::json_types::U128;
use near_workspaces::{network::Sandbox, types::NearToken, AccountId, Contract, Worker};

pub const STORAGE_DEPOSIT_AMOUNT: u128 = crate::NEP_141_STORAGE_DEPOSIT_AMOUNT.as_yoctonear();

pub struct Nep141 {
    pub contract: Contract,
}

impl Nep141 {
    pub async fn deploy(worker: &Worker<Sandbox>) -> anyhow::Result<Self> {
        let bytes = tokio::fs::read("src/tests/res/nep141.wasm").await?;
        let contract = worker.dev_deploy(&bytes).await?;

        contract
            .call("new")
            .args_json(serde_json::json!({
                "name": "TestToken",
                "symbol": "TTT",
                "decimals": 24,
            }))
            .max_gas()
            .transact()
            .await?
            .into_result()?;

        Ok(Self { contract })
    }

    pub async fn mint(&self, account_id: &AccountId, amount: u128) -> anyhow::Result<()> {
        self.contract
            .call("storage_deposit")
            .args_json(serde_json::json!({
                "account_id": account_id.as_str(),
            }))
            .deposit(NearToken::from_yoctonear(STORAGE_DEPOSIT_AMOUNT))
            .max_gas()
            .transact()
            .await?
            .into_result()?;

        self.contract
            .call("mint")
            .args_json(serde_json::json!({
                "account_id": account_id.as_str(),
                "amount": U128(amount),
            }))
            .max_gas()
            .transact()
            .await?
            .into_result()?;

        Ok(())
    }

    pub async fn ft_balance_of(&self, account_id: &AccountId) -> anyhow::Result<u128> {
        let result: U128 = self
            .contract
            .view("ft_balance_of")
            .args_json(serde_json::json!({
                "account_id": account_id.as_str(),
            }))
            .await?
            .json()?;
        Ok(result.0)
    }

    pub async fn storage_balance_of(
        &self,
        account_id: &AccountId,
    ) -> anyhow::Result<Option<StorageBalance>> {
        let result: Option<StorageBalance> = self
            .contract
            .view("storage_balance_of")
            .args_json(serde_json::json!({
                "account_id": account_id.as_str(),
            }))
            .await?
            .json()?;
        Ok(result)
    }
}
