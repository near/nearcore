use crate::{
    error::Error,
    types::{ExecuteResponse, ExecutionContext},
};
use error::{UnsupportedAction, UserError};
use near_contract_standards::storage_management::StorageBalance;
use near_sdk::{
    borsh::{BorshDeserialize, BorshSerialize},
    env,
    json_types::U64,
    near_bindgen, AccountId, Allowance, Gas, GasWeight, NearToken, Promise, PromiseOrValue,
    PromiseResult,
};
use types::{EthEmulationKind, TransactionKind};

pub mod error;
pub mod eth_emulation;
pub mod ethabi_utils;
pub mod internal;
pub mod near_action;
pub mod types;

#[cfg(test)]
mod tests;

const MICRO_NEAR: u128 = 10_u128.pow(18);
const ADDRESS_REGISTRAR_ACCOUNT_ID: &str = std::include_str!("ADDRESS_REGISTRAR_ACCOUNT_ID");
/// This storage deposit value is the one used by the standard NEP-141 implementation,
/// which essentially all tokens use. Therefore we hard-code it here instead of doing
/// the extra on-chain call to `storage_balance_bounds`. This also prevents malicious
/// token contracts with very high `storage_balance_bounds` from taking lots of $NEAR
/// from eth-wallet-contract users.
const NEP_141_STORAGE_DEPOSIT_AMOUNT: NearToken = NearToken::from_yoctonear(1_250 * MICRO_NEAR);
const NEP_141_STORAGE_DEPOSIT_GAS: Gas = Gas::from_tgas(5);
const NEP_141_STORAGE_BALANCE_OF_GAS: Gas = Gas::from_tgas(5);

#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
#[borsh(crate = "near_sdk::borsh")]
pub struct WalletContract {
    pub nonce: u64,
}

#[near_bindgen]
impl WalletContract {
    /// Return the nonce value currently stored in the contract.
    /// Following the Ethereum protocol, only transactions with nonce equal
    /// to the current value will be accepted.
    /// Additionally, the Ethereum protocol requires the nonce of an account increment
    /// by 1 each time a transaction with the correct nonce and a valid signature
    /// is submitted (even if that transaction eventually fails). In this way, each
    /// nonce value can only be used once (hence the name "nonce") and thus transaction
    /// replay is prevented.
    pub fn get_nonce(&self) -> U64 {
        U64(self.nonce)
    }

    /// This is the main entry point into this contract. It accepts an RLP-encoded
    /// Ethereum transaction signed by the private key associated with the address
    /// for the account where this contract is deployed. RLP is a binary format,
    /// so the argument is actually passed as a base64-encoded string.
    /// The Ethereum transaction represents a Near action the owner of the address
    /// wants to perform. This method decodes that action from the Ethereum transaction
    /// and crates a promise to perform that action.
    /// Actions on Near are sent to a particular account ID where they are supposed to
    /// be executed (for example, a `FunctionCall` action is sent to the contract
    /// which will execute the method). In the Ethereum transaction only the address
    /// of the target can be specified because it does not have a notion of named accounts
    /// like Near has. The `target` field of this method gives the actual account ID
    /// that the action will be sent to. The `target` must itself be an eth-implicit
    /// account and match the `to` address of the Ethereum transaction; or `target`
    /// must hash to the address given in the `to` field of the Ethereum transaction.
    /// The output of this function is an `ExecuteResponse` which gives the output
    /// of the Near action or an error message if there was a problem during the execution.
    #[payable]
    pub fn rlp_execute(
        &mut self,
        target: AccountId,
        tx_bytes_b64: String,
    ) -> PromiseOrValue<ExecuteResponse> {
        let current_account_id = env::current_account_id();
        let predecessor_account_id = env::predecessor_account_id();
        let result = inner_rlp_execute(
            current_account_id.clone(),
            predecessor_account_id,
            target,
            tx_bytes_b64,
            &mut self.nonce,
        );

        match result {
            Ok(promise) => PromiseOrValue::Promise(promise),
            Err(Error::Relayer(_)) if env::signer_account_id() == current_account_id => {
                let promise = create_ban_relayer_promise(current_account_id);
                PromiseOrValue::Promise(promise)
            }
            Err(e) => PromiseOrValue::Value(e.into()),
        }
    }

    /// Callback after checking if an address is contained in the registrar.
    /// This check happens when the target is another eth implicit account to
    /// confirm that the relayer really did check for a named account with that address.
    #[private]
    pub fn address_check_callback(
        &mut self,
        target: AccountId,
        action: near_action::Action,
    ) -> PromiseOrValue<ExecuteResponse> {
        let maybe_account_id: Option<AccountId> = match env::promise_result(0) {
            PromiseResult::Failed => {
                return PromiseOrValue::Value(ExecuteResponse {
                    success: false,
                    success_value: None,
                    error: Some("Call to Address Registrar contract failed".into()),
                });
            }
            PromiseResult::Successful(value) => serde_json::from_slice(&value)
                .unwrap_or_else(|_| env::panic_str("Unexpected response from account registrar")),
        };
        let current_account_id = env::current_account_id();
        let promise = if maybe_account_id.is_some() {
            if env::signer_account_id() == current_account_id {
                create_ban_relayer_promise(current_account_id)
            } else {
                return PromiseOrValue::Value(ExecuteResponse {
                    success: false,
                    success_value: None,
                    error: Some("Invalid target: target is address corresponding to existing named account_id".into()),
                });
            }
        } else {
            let ext = WalletContract::ext(current_account_id).with_unused_gas_weight(1);
            match action_to_promise(target, action).map(|p| p.then(ext.rlp_execute_callback())) {
                Ok(p) => p,
                Err(e) => {
                    return PromiseOrValue::Value(e.into());
                }
            }
        };
        PromiseOrValue::Promise(promise)
    }

    #[private]
    pub fn nep_141_storage_balance_callback(
        &mut self,
        token_id: AccountId,
        receiver_id: AccountId,
        action: near_action::Action,
    ) -> PromiseOrValue<ExecuteResponse> {
        let maybe_storage_balance: Option<StorageBalance> = match env::promise_result(0) {
            PromiseResult::Failed => {
                return PromiseOrValue::Value(ExecuteResponse {
                    success: false,
                    success_value: None,
                    error: Some(format!("Call to NEP-141 {token_id}::storage_balance_of failed")),
                });
            }
            PromiseResult::Successful(value) => {
                serde_json::from_slice(&value).unwrap_or_else(|_| {
                    env::panic_str("Unexpected response from NEP-141 storage_balance_of")
                })
            }
        };
        let current_account_id = env::current_account_id();
        let ext = WalletContract::ext(current_account_id).with_unused_gas_weight(1);
        let promise = match maybe_storage_balance {
            Some(_) => {
                // receiver_id is registered so we can send the transfer
                // without additional actions. Note: in the standard NEP-141
                // implementation it is impossible to have `Some` storage balance,
                // but have it be insufficient to transact.
                match action_to_promise(token_id, action)
                    .map(|p| p.then(ext.rlp_execute_callback()))
                {
                    Ok(p) => p,
                    Err(e) => {
                        return PromiseOrValue::Value(e.into());
                    }
                }
            }
            None => {
                // receiver_id is not registered so we must call `storage_deposit` first.
                let storage_deposit_args =
                    format!(r#"{{"account_id": "{receiver_id}"}}"#).into_bytes();
                let transfer_function_call = match action {
                    near_action::Action::FunctionCall(x) => x,
                    _ => {
                        env::panic_str("Expected function call action to perform NEP-141 transfer")
                    }
                };
                Promise::new(token_id)
                    .function_call(
                        "storage_deposit".into(),
                        storage_deposit_args,
                        NEP_141_STORAGE_DEPOSIT_AMOUNT,
                        NEP_141_STORAGE_DEPOSIT_GAS,
                    )
                    .function_call(
                        transfer_function_call.method_name,
                        transfer_function_call.args,
                        transfer_function_call.deposit,
                        transfer_function_call.gas,
                    )
                    .then(ext.rlp_execute_callback())
            }
        };
        PromiseOrValue::Promise(promise)
    }

    #[private]
    pub fn rlp_execute_callback(&mut self) -> ExecuteResponse {
        let n = env::promise_results_count();
        let mut success_value = None;
        for i in 0..n {
            match env::promise_result(i) {
                PromiseResult::Failed => {
                    return ExecuteResponse {
                        success: false,
                        success_value: None,
                        error: Some("Failed Near promise".into()),
                    };
                }
                PromiseResult::Successful(value) => success_value = Some(value),
            }
        }
        ExecuteResponse { success: true, success_value, error: None }
    }

    #[private]
    pub fn ban_relayer(&mut self) -> ExecuteResponse {
        ExecuteResponse {
            success: false,
            success_value: None,
            error: Some("Error: faulty relayer".into()),
        }
    }
}

fn inner_rlp_execute(
    current_account_id: AccountId,
    predecessor_account_id: AccountId,
    target: AccountId,
    tx_bytes_b64: String,
    nonce: &mut u64,
) -> Result<Promise, Error> {
    if *nonce == u64::MAX {
        return Err(Error::AccountNonceExhausted);
    }
    let context = ExecutionContext::new(
        current_account_id.clone(),
        predecessor_account_id,
        env::attached_deposit(),
    )?;

    let (action, transaction_kind) =
        internal::parse_rlp_tx_to_action(&tx_bytes_b64, &target, &context, nonce)?;
    let promise = match transaction_kind {
        TransactionKind::EthEmulation(EthEmulationKind::EOABaseTokenTransfer {
            address_check: Some(address),
        }) => {
            let ext = WalletContract::ext(current_account_id).with_unused_gas_weight(1);
            let address_registrar = {
                let account_id = ADDRESS_REGISTRAR_ACCOUNT_ID
                    .trim()
                    .parse()
                    .unwrap_or_else(|_| env::panic_str("Invalid address registrar"));
                ext_registrar::ext(account_id).with_static_gas(Gas::from_tgas(5))
            };
            let address = format!("0x{}", hex::encode(address));
            address_registrar.lookup(address).then(ext.address_check_callback(target, action))
        }
        TransactionKind::EthEmulation(EthEmulationKind::ERC20Transfer { receiver_id }) => {
            // In the case of the emulated ERC-20 transfer, the receiving account
            // might not be registered with the NEP-141 contract (per the NEP-145)
            // storage standard. Therefore we must create a multi-step promise where
            // first we check if the receiver is registered and then if not call
            // `storage_deposit` in addition to `ft_transfer`.
            let token_id = target;
            let ext: WalletContractExt =
                WalletContract::ext(current_account_id).with_unused_gas_weight(1);
            let storage_balance_args =
                format!(r#"{{"account_id": "{}"}}"#, receiver_id.as_str()).into_bytes();
            Promise::new(token_id.clone())
                .function_call(
                    "storage_balance_of".into(),
                    storage_balance_args,
                    NearToken::from_yoctonear(0),
                    NEP_141_STORAGE_BALANCE_OF_GAS,
                )
                .then(ext.nep_141_storage_balance_callback(token_id, receiver_id, action))
        }
        TransactionKind::EthEmulation(EthEmulationKind::SelfBaseTokenTransfer) => {
            // Base token transfers to self are no-ops on Near, so we do not need to
            // schedule an additional call. We can simply go straight to `rlp_execute_callback`.
            let ext: WalletContractExt =
                WalletContract::ext(current_account_id).with_unused_gas_weight(1);
            ext.rlp_execute_callback()
        }
        _ => {
            let ext = WalletContract::ext(current_account_id).with_unused_gas_weight(1);
            action_to_promise(target, action)?.then(ext.rlp_execute_callback())
        }
    };
    Ok(promise)
}

fn action_to_promise(target: AccountId, action: near_action::Action) -> Result<Promise, Error> {
    match action {
        near_action::Action::FunctionCall(action) => Ok(Promise::new(target).function_call(
            action.method_name,
            action.args,
            action.deposit,
            action.gas,
        )),
        near_action::Action::Transfer(action) => Ok(Promise::new(target).transfer(action.deposit)),
        near_action::Action::AddKey(action) => match action.access_key.permission {
            near_action::AccessKeyPermission::FullAccess => {
                Err(Error::User(UserError::UnsupportedAction(UnsupportedAction::AddFullAccessKey)))
            }
            near_action::AccessKeyPermission::FunctionCall(access) => Ok(Promise::new(target)
                .add_access_key_allowance_with_nonce(
                    action.public_key,
                    access.allowance.and_then(Allowance::limited).unwrap_or(Allowance::Unlimited),
                    access.receiver_id,
                    access.method_names.join(","),
                    action.access_key.nonce,
                )),
        },
        near_action::Action::DeleteKey(action) => {
            Ok(Promise::new(target).delete_key(action.public_key))
        }
    }
}

fn create_ban_relayer_promise(current_account_id: AccountId) -> Promise {
    let pk = env::signer_account_pk();
    Promise::new(current_account_id).delete_key(pk).function_call_weight(
        "ban_relayer".into(),
        Vec::new(),
        NearToken::from_yoctonear(0),
        Gas::from_tgas(1),
        GasWeight(1),
    )
}

#[near_sdk::ext_contract(ext_registrar)]
trait AddressRegistrar {
    fn lookup(&self, address: String) -> Option<AccountId>;
}
