use near_primitives::account::AccountContract;
use near_primitives::account::id::AccountType;
use near_primitives::action::GlobalContractIdentifier;
use near_primitives::errors::StorageError;
use near_primitives::global_contract::ContractIsLocalError;
use near_primitives::hash::CryptoHash;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::AccountId;
use near_store::trie::AccessOptions;
use near_store::{KeyLookupMode, TrieAccess as _, TrieUpdate};
use near_vm_runner::ContractCode;
use near_wallet_contract::{LegacyEthWallet, eth_wallet_global_contract_hash};

/// Identifies a resolved contract for execution.
///
/// Constructed via `resolve()` from an `AccountContract` and account context.
/// All special-case resolution (ETH implicit accounts, global contracts) is
/// performed at construction time so that `RuntimeContractExt` only needs
/// storage and identifier to implement `Contract`.
#[derive(Clone)]
pub(crate) enum RuntimeContractIdentifier {
    /// No contract deployed on the account.
    None,
    /// Regular local contract.
    AccountLocal { code_hash: CryptoHash, account_id: AccountId },
    /// Global contract. The code hash is always pre-resolved at construction
    /// time. The original `GlobalContractIdentifier` is preserved so that
    /// `record_contract_call` can construct the correct trie key.
    Global { code_hash: CryptoHash, identifier: GlobalContractIdentifier },
    /// Non-global legacy ETH wallet contract.
    /// Code comes from the built-in wallet contract, not from storage.
    LegacyEthWallet(LegacyEthWallet),
}

impl RuntimeContractIdentifier {
    /// Resolve a contract identifier from an account's contract field.
    ///
    /// Returns `RuntimeContractIdentifier::None` if the account has no contract deployed.
    pub(crate) fn resolve(
        account_id: &AccountId,
        account_contract: AccountContract,
        state_update: &TrieUpdate,
        config: &near_parameters::vm::Config,
        chain_id: &str,
        access: AccessOptions,
    ) -> Result<Self, StorageError> {
        let local_hash = match GlobalContractIdentifier::try_from(account_contract) {
            Ok(gci) => {
                let code_hash = gci.clone().hash(state_update, access)?;
                return Ok(RuntimeContractIdentifier::Global { code_hash, identifier: gci });
            }
            Err(ContractIsLocalError::NotDeployed) => return Ok(RuntimeContractIdentifier::None),
            Err(ContractIsLocalError::Deployed(local_hash)) => local_hash,
        };

        if account_id.get_account_type() == AccountType::EthImplicitAccount {
            // Accounts that look like eth implicit accounts and have existed prior to the
            // eth-implicit accounts protocol change (these accounts are discussed in the
            // description of #11606) may have something else deployed to them. Only return
            // something here if the accounts have a wallet contract hash. Otherwise use the
            // regular path to grab the deployed contract.
            if let Some(legacy) = LegacyEthWallet::resolve(local_hash) {
                // With EthImplicitGlobalContract, ETH implicit wallet accounts
                // switched to global contracts, including those created in old
                // protocol versions.
                let identifier = if config.eth_implicit_global_contract {
                    let global_hash = eth_wallet_global_contract_hash(chain_id);
                    RuntimeContractIdentifier::Global {
                        code_hash: global_hash,
                        identifier: GlobalContractIdentifier::CodeHash(global_hash),
                    }
                } else {
                    RuntimeContractIdentifier::LegacyEthWallet(legacy)
                };
                return Ok(identifier);
            }
        }

        Ok(RuntimeContractIdentifier::AccountLocal {
            code_hash: local_hash,
            account_id: account_id.clone(),
        })
    }

    /// Returns the code hash for this contract identifier.
    pub(crate) fn hash(&self) -> CryptoHash {
        match self {
            // Preserves existing behavior of using `CryptoHash::default()` value for
            // `code_hash` to indicate contract code absence in `AccountV1`.
            Self::None => CryptoHash::default(),
            Self::AccountLocal { code_hash, .. } | Self::Global { code_hash, .. } => *code_hash,
            Self::LegacyEthWallet(legacy) => *legacy.contract().hash(),
        }
    }
}

pub(crate) trait GlobalContractAccessExt {
    fn hash(self, store: &TrieUpdate, access: AccessOptions) -> Result<CryptoHash, StorageError>;
    fn code(self, store: &TrieUpdate) -> Result<Option<ContractCode>, StorageError>;
}

impl GlobalContractAccessExt for GlobalContractIdentifier {
    fn hash(self, store: &TrieUpdate, access: AccessOptions) -> Result<CryptoHash, StorageError> {
        if let GlobalContractIdentifier::CodeHash(hash) = self {
            return Ok(hash);
        }
        let key = TrieKey::GlobalContractCode { identifier: self.into() };
        let value_ref =
            store.get_ref(&key, KeyLookupMode::MemOrFlatOrTrie, access)?.ok_or_else(|| {
                let TrieKey::GlobalContractCode { identifier } = key else { unreachable!() };
                StorageError::StorageInconsistentState(format!(
                    "Global contract identifier not found {:?}",
                    identifier
                ))
            })?;
        Ok(value_ref.value_hash())
    }

    fn code(self, store: &TrieUpdate) -> Result<Option<ContractCode>, StorageError> {
        let key = TrieKey::GlobalContractCode { identifier: self.clone().into() };
        let code_hash = match self {
            GlobalContractIdentifier::AccountId(_) => None,
            GlobalContractIdentifier::CodeHash(hash) => Some(hash),
        };
        let code = store.get(&key, AccessOptions::DEFAULT)?;
        Ok(code.map(|code| ContractCode::new(code, code_hash)))
    }
}
