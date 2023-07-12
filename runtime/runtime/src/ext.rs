use near_primitives::contract::ContractCode;
use near_primitives::errors::{EpochError, StorageError};
use near_primitives::hash::CryptoHash;
use near_primitives::trie_key::{trie_key_parsers, TrieKey};
use near_primitives::types::{
    AccountId, Balance, EpochId, EpochInfoProvider, TrieCacheMode, TrieNodesCount,
};
use near_primitives::utils::create_data_id;
use near_primitives::version::ProtocolVersion;
use near_store::{get_code, KeyLookupMode, TrieUpdate, TrieUpdateValuePtr};
use near_vm_runner::logic::errors::{AnyError, VMLogicError};
use near_vm_runner::logic::{External, StorageGetMode, ValuePtr};

pub struct RuntimeExt<'a> {
    trie_update: &'a mut TrieUpdate,
    account_id: &'a AccountId,
    action_hash: &'a CryptoHash,
    data_count: u64,
    epoch_id: &'a EpochId,
    prev_block_hash: &'a CryptoHash,
    last_block_hash: &'a CryptoHash,
    epoch_info_provider: &'a dyn EpochInfoProvider,
    current_protocol_version: ProtocolVersion,
}

/// Error used by `RuntimeExt`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExternalError {
    /// Unexpected error which is typically related to the node storage corruption.
    /// It's possible the input state is invalid or malicious.
    StorageError(StorageError),
    /// Error when accessing validator information. Happens inside epoch manager.
    ValidatorError(EpochError),
}

impl From<ExternalError> for VMLogicError {
    fn from(err: ExternalError) -> Self {
        VMLogicError::ExternalError(AnyError::new(err))
    }
}

pub struct RuntimeExtValuePtr<'a>(TrieUpdateValuePtr<'a>);

impl<'a> ValuePtr for RuntimeExtValuePtr<'a> {
    fn len(&self) -> u32 {
        self.0.len()
    }

    fn deref(&self) -> ExtResult<Vec<u8>> {
        self.0.deref_value().map_err(wrap_storage_error)
    }
}

impl<'a> RuntimeExt<'a> {
    pub fn new(
        trie_update: &'a mut TrieUpdate,
        account_id: &'a AccountId,
        action_hash: &'a CryptoHash,
        epoch_id: &'a EpochId,
        prev_block_hash: &'a CryptoHash,
        last_block_hash: &'a CryptoHash,
        epoch_info_provider: &'a dyn EpochInfoProvider,
        current_protocol_version: ProtocolVersion,
    ) -> Self {
        RuntimeExt {
            trie_update,
            account_id,
            action_hash,
            data_count: 0,
            epoch_id,
            prev_block_hash,
            last_block_hash,
            epoch_info_provider,
            current_protocol_version,
        }
    }

    #[inline]
    pub fn account_id(&self) -> &'a AccountId {
        self.account_id
    }

    pub fn get_code(&self, code_hash: CryptoHash) -> Result<Option<ContractCode>, StorageError> {
        get_code(self.trie_update, self.account_id, Some(code_hash))
    }

    pub fn create_storage_key(&self, key: &[u8]) -> TrieKey {
        TrieKey::ContractData { account_id: self.account_id.clone(), key: key.to_vec() }
    }

    pub fn set_trie_cache_mode(&mut self, state: TrieCacheMode) {
        self.trie_update.set_trie_cache_mode(state);
    }

    #[inline]
    pub fn protocol_version(&self) -> ProtocolVersion {
        self.current_protocol_version
    }
}

fn wrap_storage_error(error: StorageError) -> VMLogicError {
    VMLogicError::from(ExternalError::StorageError(error))
}

type ExtResult<T> = ::std::result::Result<T, VMLogicError>;

impl<'a> External for RuntimeExt<'a> {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> ExtResult<()> {
        let storage_key = self.create_storage_key(key);
        self.trie_update.set(storage_key, Vec::from(value));
        Ok(())
    }

    fn storage_get<'b>(
        &'b self,
        key: &[u8],
        mode: StorageGetMode,
    ) -> ExtResult<Option<Box<dyn ValuePtr + 'b>>> {
        let storage_key = self.create_storage_key(key);
        let mode = match mode {
            StorageGetMode::FlatStorage => KeyLookupMode::FlatStorage,
            StorageGetMode::Trie => KeyLookupMode::Trie,
        };
        self.trie_update
            .get_ref(&storage_key, mode)
            .map_err(wrap_storage_error)
            .map(|option| option.map(|ptr| Box::new(RuntimeExtValuePtr(ptr)) as Box<_>))
    }

    fn storage_remove(&mut self, key: &[u8]) -> ExtResult<()> {
        let storage_key = self.create_storage_key(key);
        self.trie_update.remove(storage_key);
        Ok(())
    }

    fn storage_has_key(&mut self, key: &[u8], mode: StorageGetMode) -> ExtResult<bool> {
        let storage_key = self.create_storage_key(key);
        let mode = match mode {
            StorageGetMode::FlatStorage => KeyLookupMode::FlatStorage,
            StorageGetMode::Trie => KeyLookupMode::Trie,
        };
        self.trie_update
            .get_ref(&storage_key, mode)
            .map(|x| x.is_some())
            .map_err(wrap_storage_error)
    }

    fn storage_remove_subtree(&mut self, prefix: &[u8]) -> ExtResult<()> {
        let data_keys = self
            .trie_update
            .iter(&trie_key_parsers::get_raw_prefix_for_contract_data(self.account_id, prefix))
            .map_err(wrap_storage_error)?
            .map(|raw_key| {
                trie_key_parsers::parse_data_key_from_contract_data_key(&raw_key?, self.account_id)
                    .map_err(|_e| {
                        StorageError::StorageInconsistentState(
                            "Can't parse data key from raw key for ContractData".to_string(),
                        )
                    })
                    .map(Vec::from)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(wrap_storage_error)?;
        for key in data_keys {
            self.trie_update
                .remove(TrieKey::ContractData { account_id: self.account_id.clone(), key });
        }
        Ok(())
    }

    fn generate_data_id(&mut self) -> CryptoHash {
        let data_id = create_data_id(
            self.current_protocol_version,
            self.action_hash,
            self.prev_block_hash,
            self.last_block_hash,
            self.data_count as usize,
        );
        self.data_count += 1;
        data_id
    }

    fn get_trie_nodes_count(&self) -> TrieNodesCount {
        self.trie_update.trie().get_trie_nodes_count()
    }

    fn validator_stake(&self, account_id: &AccountId) -> ExtResult<Option<Balance>> {
        self.epoch_info_provider
            .validator_stake(self.epoch_id, self.prev_block_hash, account_id)
            .map_err(|e| ExternalError::ValidatorError(e).into())
    }

    fn validator_total_stake(&self) -> ExtResult<Balance> {
        self.epoch_info_provider
            .validator_total_stake(self.epoch_id, self.prev_block_hash)
            .map_err(|e| ExternalError::ValidatorError(e).into())
    }
}
