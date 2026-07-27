use crate::ApplyState;
use crate::contract_code::{GlobalContractAccessExt, RuntimeContractIdentifier};
use crate::ext::RuntimeExt;
use crate::function_call::execute_function_call;
use crate::pipelining::ReceiptPreparationPipeline;
use crate::receipt_manager::ReceiptManager;
use near_crypto::{KeyType, PublicKey, PublicKeyHandle};
use near_parameters::RuntimeConfigStore;
use near_primitives::account::{AccessKey, Account};
use near_primitives::action::GlobalContractIdentifier;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ActionReceipt, Receipt, ReceiptEnum, ReceiptV0, VersionedActionReceipt,
};
use near_primitives::transaction::FunctionCallAction;
use near_primitives::trie_key::TrieKey;
use near_primitives::trie_key::trie_key_parsers::{self, parse_key_handle_from_access_key_key};
use near_primitives::types::{
    AccountId, Balance, BlockHeight, EpochHeight, EpochId, EpochInfoProvider, Gas, Nonce, ShardId,
};
use near_primitives::version::assert_supported_protocol_version;
use near_primitives::views::{StateItem, ViewStateResult};
use near_primitives_core::config::ViewConfig;
use near_store::trie::AccessOptions;
use near_store::{
    Trie, TrieAccess as _, TrieUpdate, get_access_key, get_access_key_by_handle, get_account,
    get_gas_key_nonce,
};
use near_vm_runner::logic::{ProtocolVersion, ReturnData};
use near_vm_runner::{ContractCode, ContractRuntimeCache};
use std::num::NonZeroU32;
use std::ops::Bound;
use std::{str, sync::Arc, time::Instant};

pub mod errors;

/// State for the view call.
#[derive(Debug)]
pub struct ViewApplyState {
    /// Currently building block height.
    pub block_height: BlockHeight,
    /// Prev block hash
    pub prev_block_hash: CryptoHash,
    /// To which shard the applied chunk belongs.
    pub shard_id: ShardId,
    /// Current epoch id
    pub epoch_id: EpochId,
    /// Current epoch height
    pub epoch_height: EpochHeight,
    /// The current block timestamp (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    pub block_timestamp: u64,
    /// Current Protocol version when we apply the state transition
    pub current_protocol_version: ProtocolVersion,
    /// Cache for compiled contracts.
    pub cache: Option<Box<dyn ContractRuntimeCache>>,
}

/// Fallback cap on the number of access keys returned by `view_access_keys`,
/// used by `TrieViewer::default()`. Kept in sync with
/// `near_chain_configs::default_view_access_keys_limit()`.
const DEFAULT_VIEW_ACCESS_KEYS_LIMIT: u32 = 100;

/// Smallest byte string strictly greater than every string that has `prefix` as
/// a prefix, or `None` if `prefix` is all `0xFF` bytes (no such string exists).
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let trailing_ff = prefix.iter().rev().take_while(|&&byte| byte == u8::MAX).count();
    let (last, rest) = prefix[..prefix.len() - trailing_ff].split_last()?;
    let mut out = rest.to_vec();
    out.push(last + 1);
    Some(out)
}

/// Whether a trie key at nibble path `key_nibbles` still lies within the range of
/// keys that share `prefix_nibbles`: true while the path is on the descent toward
/// the prefix (a prefix of it) or inside its subtree (extends it), false once it
/// diverges. Used as the negation of the access-key iterator's prune condition.
fn nibbles_within_prefix(prefix_nibbles: &[u8], key_nibbles: &[u8]) -> bool {
    prefix_nibbles.starts_with(key_nibbles) || key_nibbles.starts_with(prefix_nibbles)
}

pub struct TrieViewer {
    runtime_config_store: RuntimeConfigStore,
    /// Upper bound of the byte size of contract state that is still viewable. None is no limit
    state_size_limit: Option<u64>,
    /// Upper bound on the number of access keys returned by `view_access_keys`.
    /// Applies to both paginated and unpaginated requests.
    access_keys_limit: u32,
    /// Gas limit used when handling call_function queries. If None, resolved
    /// from the runtime config for the current protocol version.
    max_gas_burnt_view: Option<Gas>,
}

impl Default for TrieViewer {
    fn default() -> Self {
        let runtime_config_store = RuntimeConfigStore::new(None);
        Self {
            runtime_config_store,
            state_size_limit: None,
            access_keys_limit: DEFAULT_VIEW_ACCESS_KEYS_LIMIT,
            max_gas_burnt_view: None,
        }
    }
}

impl TrieViewer {
    pub fn new(
        runtime_config_store: RuntimeConfigStore,
        state_size_limit: Option<u64>,
        access_keys_limit: u32,
        max_gas_burnt_view: Option<Gas>,
    ) -> Self {
        Self { runtime_config_store, state_size_limit, access_keys_limit, max_gas_burnt_view }
    }

    pub fn view_account(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
    ) -> Result<Account, errors::ViewAccountError> {
        get_account(state_update, account_id)?.ok_or_else(|| {
            errors::ViewAccountError::AccountDoesNotExist {
                requested_account_id: account_id.clone(),
            }
        })
    }

    pub fn view_account_contract_code(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
        current_protocol_version: ProtocolVersion,
        chain_id: &str,
    ) -> Result<ContractCode, errors::ViewContractCodeError> {
        assert_supported_protocol_version(current_protocol_version);
        let account = self.view_account(state_update, account_id)?;
        let contract_id = RuntimeContractIdentifier::resolve(
            account_id,
            account.contract().into_owned(),
            state_update,
            chain_id,
            AccessOptions::DEFAULT,
        )?;
        let maybe_code = match contract_id {
            RuntimeContractIdentifier::None => None,
            RuntimeContractIdentifier::AccountLocal { code_hash, account_id } => {
                let key = near_primitives::trie_key::TrieKey::ContractCode { account_id };
                let code = state_update.get(&key, AccessOptions::DEFAULT)?;
                code.map(|c| ContractCode::new(c, Some(code_hash)))
            }
            RuntimeContractIdentifier::Global { identifier, .. } => {
                identifier.code(state_update)?
            }
        };
        maybe_code.ok_or_else(|| errors::ViewContractCodeError::NoContractCode {
            contract_account_id: account_id.clone(),
        })
    }

    pub fn view_global_contract_code(
        &self,
        state_update: &TrieUpdate,
        identifier: GlobalContractIdentifier,
    ) -> Result<ContractCode, errors::ViewContractCodeError> {
        identifier
            .clone()
            .code(state_update)?
            .ok_or(errors::ViewContractCodeError::NoGlobalContractCode { identifier })
    }

    pub fn view_access_key(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKey, errors::ViewAccessKeyError> {
        get_access_key(state_update, account_id, public_key)?.ok_or_else(|| {
            errors::ViewAccessKeyError::AccessKeyDoesNotExist { public_key: public_key.clone() }
        })
    }

    /// Lists an account's access keys, optionally paginated.
    pub fn view_access_keys(
        &self,
        trie: &Trie,
        account_id: &AccountId,
        after: Option<&PublicKeyHandle>,
        limit: Option<NonZeroU32>,
    ) -> Result<
        (Vec<(PublicKeyHandle, AccessKey)>, Option<PublicKeyHandle>),
        errors::ViewAccessKeyError,
    > {
        let max = self.access_keys_limit;
        let paginated = after.is_some() || limit.is_some();

        let item_cap: Option<u32> = if paginated {
            // An explicit page size larger than the configured maximum is
            // clamped down rather than rejected; with no explicit page size we
            // fall back to the configured maximum.
            Some(limit.map_or(max, |requested| requested.get().min(max)))
        } else {
            None
        };

        let prefix = trie_key_parsers::get_raw_prefix_for_access_keys(account_id);
        // Bound iteration to this account's access-key range with a prune
        // condition. Unlike the per-node seek boundary, a prune condition
        // survives `seek`, so it keeps enforcing the range across the seeks we
        // use below to skip gas-key nonce blocks.
        let prefix_nibbles: Vec<u8> = prefix.iter().flat_map(|&b| [b >> 4, b & 0x0f]).collect();
        let mut iter =
            trie.disk_iter_with_prune_condition(Some(Box::new(move |key_nibbles: &Vec<u8>| {
                !nibbles_within_prefix(&prefix_nibbles, key_nibbles)
            })))?;

        match after {
            Some(handle) => {
                let after_key = TrieKey::access_key(account_id.clone(), handle.clone()).to_vec();
                match prefix_successor(&after_key) {
                    Some(next) => iter.seek(Bound::Included(next.as_slice()))?,
                    // Unreachable: access-key keys start with `col::ACCESS_KEY`,
                    // so they are never all-0xFF.
                    None => iter.seek(Bound::Excluded(after_key.as_slice()))?,
                }
            }
            None => iter.seek(Bound::Included(prefix.as_slice()))?,
        }

        let mut keys = Vec::new();
        let mut last_key = None;

        while let Some(item) = iter.next() {
            let (raw_key, _value) = item?;
            let key_handle =
                parse_key_handle_from_access_key_key(&raw_key, account_id).map_err(|_| {
                    errors::ViewAccessKeyError::InternalError {
                        error_message: "unexpected invalid access key from iterator".to_string(),
                    }
                })?;
            // A genuine access key beyond what we've already kept.
            if let Some(cap) = item_cap {
                if keys.len() as u64 >= u64::from(cap) {
                    // Page is full and at least one more key exists: emit a cursor
                    // at the last kept key so the caller can resume.
                    last_key = keys
                        .last()
                        .map(|(handle, _): &(PublicKeyHandle, AccessKey)| handle.clone());
                    break;
                }
            } else if keys.len() as u64 >= u64::from(max) {
                // Unpaginated request that exceeds the configured limit.
                return Err(errors::ViewAccessKeyError::TooManyAccessKeys {
                    requested_account_id: account_id.clone(),
                    limit: max,
                });
            }
            let access_key =
                get_access_key_by_handle(trie, account_id, &key_handle)?.ok_or_else(|| {
                    StorageError::StorageInconsistentState(format!(
                        "iterator yielded an access-key trie key with no value: {key_handle}"
                    ))
                })?;
            // A gas key's nonce rows sort immediately after its access-key row;
            // skip the whole block so we don't scan every nonce.
            let is_gas_key = access_key.gas_key_info().is_some();
            keys.push((key_handle, access_key));
            if is_gas_key {
                if let Some(next) = prefix_successor(&raw_key) {
                    iter.seek(Bound::Included(next.as_slice()))?;
                }
            }
        }
        Ok((keys, last_key))
    }

    pub fn view_gas_key_nonces(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Vec<Nonce>, errors::ViewGasKeyNoncesError> {
        let access_key =
            get_access_key(state_update, account_id, public_key)?.ok_or_else(|| {
                errors::ViewGasKeyNoncesError::GasKeyDoesNotExist { public_key: public_key.clone() }
            })?;
        // If the access key is not a gas key, treat as not found.
        let Some(gas_key_info) = access_key.gas_key_info() else {
            return Err(errors::ViewGasKeyNoncesError::GasKeyDoesNotExist {
                public_key: public_key.clone(),
            });
        };
        (0..gas_key_info.num_nonces)
            .map(|index| {
                get_gas_key_nonce(state_update, account_id, public_key, index)?.ok_or_else(|| {
                errors::ViewGasKeyNoncesError::InternalError {
                    error_message: format!(
                        "gas key nonce at index {} does not exist for account {} and public key {}",
                        index, account_id, public_key
                    ),
                }
            })
            })
            .collect()
    }

    pub fn view_state(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
        prefix: &[u8],
        after_key: Option<&[u8]>,
        limit: Option<NonZeroU32>,
        include_proof: bool,
    ) -> Result<ViewStateResult, errors::ViewStateError> {
        let paginated = limit.is_some() || after_key.is_some();
        if paginated && include_proof {
            return Err(errors::ViewStateError::ProofUnsupportedWithPagination);
        }
        if let Some(after_key) = after_key {
            if !after_key.starts_with(prefix) {
                return Err(errors::ViewStateError::AfterKeyOutsidePrefix);
            }
        }

        let Some(account) = get_account(state_update, account_id)? else {
            return Err(errors::ViewStateError::AccountDoesNotExist {
                requested_account_id: account_id.clone(),
            });
        };

        // Legacy per-account gate — paginated callers opt out of it.
        if !paginated {
            let code_len = state_update
                .get_code_len(
                    account_id.clone(),
                    account.local_contract_hash().unwrap_or_default(),
                )?
                .unwrap_or_default() as u64;
            if let Some(limit) = self.state_size_limit {
                if account.storage_usage().saturating_sub(code_len) > limit {
                    return Err(errors::ViewStateError::AccountStateTooLarge {
                        requested_account_id: account_id.clone(),
                    });
                }
            }
        }

        let query = trie_key_parsers::get_raw_prefix_for_contract_data(account_id, prefix);
        let acc_sep_len = query.len() - prefix.len();
        let mut iter = state_update.trie().disk_iter()?;
        iter.remember_visited_nodes(include_proof);

        match after_key {
            None => iter.seek_prefix(&query)?,
            Some(after_key) => {
                let mut full = query[..acc_sep_len].to_vec();
                full.extend_from_slice(after_key);
                iter.seek(Bound::Excluded(full))?;
            }
        }

        // Per-page caps, separate from the `trie_viewer_state_size_limit` that pagination skips.
        // The byte cap is soft: it's checked before each append, so a page can run one item over.
        const MAX_VIEW_STATE_PAGE_ITEMS: u32 = 10_000;
        const MAX_VIEW_STATE_PAGE_BYTES: u64 = 50_000;

        let (item_cap, byte_cap) = if paginated {
            let items = limit
                .map_or(MAX_VIEW_STATE_PAGE_ITEMS, NonZeroU32::get)
                .min(MAX_VIEW_STATE_PAGE_ITEMS);
            (Some(items), Some(MAX_VIEW_STATE_PAGE_BYTES))
        } else {
            (None, None)
        };

        // Pre-allocate only for an explicit `limit`; the default page size is too big to assume.
        let mut values = match (limit, item_cap) {
            (Some(_), Some(cap)) => Vec::with_capacity(cap as usize),
            _ => Vec::new(),
        };
        let mut used_bytes: u64 = 0;
        let mut last_key = None;

        for item in &mut iter {
            let (key, value) = item?;
            // `seek` (resumed pages) is not prefix-bounded — stop at the account edge.
            if !key.starts_with(&query) {
                break;
            }
            let hit_items = item_cap.is_some_and(|cap| values.len() as u64 >= u64::from(cap));
            let hit_bytes = byte_cap.is_some_and(|cap| used_bytes >= cap);
            if hit_items || hit_bytes {
                // At least one more item exists; resume after the last we kept.
                last_key = values.last().map(|it: &StateItem| it.key.clone());
                break;
            }
            used_bytes += (key.len() + value.len()) as u64;
            values.push(StateItem { key: key[acc_sep_len..].to_vec().into(), value: value.into() });
        }
        let proof = iter.into_visited_nodes();
        Ok(ViewStateResult { values, proof, last_key })
    }

    pub fn call_function(
        &self,
        mut state_update: TrieUpdate,
        view_state: ViewApplyState,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<Vec<u8>, errors::CallFunctionError> {
        assert_supported_protocol_version(view_state.current_protocol_version);
        let now = Instant::now();
        let root = *state_update.get_root();
        let account = get_account(&state_update, contract_id)?.ok_or_else(|| {
            errors::CallFunctionError::AccountDoesNotExist {
                requested_account_id: contract_id.clone(),
            }
        })?;
        // TODO(#1015): Add ability to pass public key and originator_id
        let originator_id = contract_id;
        let public_key = PublicKey::empty(KeyType::ED25519);
        let empty_hash = CryptoHash::default();
        let mut receipt_manager = ReceiptManager::default();
        let config = self.runtime_config_store.get_config(view_state.current_protocol_version);
        let apply_state = ApplyState {
            apply_reason: ApplyChunkReason::ViewTrackedShard,
            block_height: view_state.block_height,
            // Used for legacy reasons
            prev_block_hash: view_state.prev_block_hash,
            shard_id: view_state.shard_id,
            epoch_id: view_state.epoch_id,
            epoch_height: view_state.epoch_height,
            gas_price: Balance::ZERO,
            block_timestamp: view_state.block_timestamp,
            gas_limit: None,
            random_seed: root,
            current_protocol_version: view_state.current_protocol_version,
            config: Arc::clone(config),
            next_wasm_config: None,
            cache: view_state.cache,
            is_new_chunk: false,
            save_receipt_to_tx: false,
            congestion_info: Default::default(),
            bandwidth_requests: BlockBandwidthRequests::empty(),
            trie_access_tracker_state: Default::default(),
            on_post_state_ready: None,
        };
        let function_call = FunctionCallAction {
            method_name: method_name.to_string(),
            args: args.to_vec(),
            gas: self.max_gas_burnt_view(view_state.current_protocol_version),
            deposit: Balance::ZERO,
        };
        let action_receipt = ActionReceipt {
            signer_id: originator_id.clone(),
            signer_public_key: public_key,
            gas_price: Balance::ZERO,
            output_data_receivers: vec![],
            input_data_ids: vec![],
            actions: vec![function_call.clone().into()],
        };
        let receipt = Receipt::V0(ReceiptV0 {
            predecessor_id: contract_id.clone(),
            receiver_id: contract_id.clone(),
            receipt_id: empty_hash,
            receipt: ReceiptEnum::Action(action_receipt.clone()),
        });
        let pipeline = ReceiptPreparationPipeline::new(
            Arc::clone(config),
            apply_state.next_wasm_config.clone(),
            apply_state.cache.as_ref().map(|v| v.handle()),
            state_update.contract_storage().clone(),
            epoch_info_provider.chain_id(),
            apply_state.shard_id,
        );
        let max_gas_burnt_view = self.max_gas_burnt_view(view_state.current_protocol_version);
        let view_config = Some(ViewConfig { max_gas_burnt: max_gas_burnt_view });
        let contract_id_resolved = RuntimeContractIdentifier::resolve(
            contract_id,
            account.contract().into_owned(),
            &state_update,
            &epoch_info_provider.chain_id(),
            AccessOptions::DEFAULT,
        )?;
        let contract =
            pipeline.get_contract(&receipt, contract_id_resolved, 0, view_config.clone());

        let mut runtime_ext = RuntimeExt::new(
            &mut state_update,
            &mut receipt_manager,
            contract_id.clone(),
            account,
            empty_hash,
            view_state.epoch_id,
            view_state.block_height,
            epoch_info_provider,
            view_state.current_protocol_version,
            config.wasm_config.storage_get_mode,
            Arc::clone(&apply_state.trie_access_tracker_state),
            None,
        );
        let outcome = execute_function_call(
            contract,
            &apply_state,
            &mut runtime_ext,
            originator_id,
            &VersionedActionReceipt::from(action_receipt),
            [].into(),
            &function_call,
            &empty_hash,
            config,
            true,
            view_config,
        )
        .map_err(|e| errors::CallFunctionError::InternalError { error_message: e.to_string() })?;
        let elapsed = now.elapsed();
        let time_ms =
            (elapsed.as_secs() as f64 / 1_000.0) + f64::from(elapsed.subsec_nanos()) / 1_000_000.0;
        let time_str = format!("{:.*}ms", 2, time_ms);

        if let Some(err) = outcome.aborted {
            logs.extend(outcome.logs);
            let message = format!("wasm execution failed with error: {:?}", err);
            tracing::debug!(target: "runtime", %time_str, %message, "exec time and error message");
            let error: near_primitives::errors::FunctionCallError =
                crate::conversions::Convert::convert(err);
            Err(errors::CallFunctionError::VMError { error, error_message: message })
        } else {
            tracing::debug!(target: "runtime", %time_str, ?outcome, "exec time and result of execution");
            logs.extend(outcome.logs);
            let result = match outcome.return_data {
                ReturnData::Value(buf) => buf,
                ReturnData::ReceiptIndex(_) | ReturnData::None => vec![],
            };
            Ok(result)
        }
    }

    fn max_gas_burnt_view(&self, protocol_version: ProtocolVersion) -> Gas {
        self.max_gas_burnt_view.unwrap_or_else(|| {
            self.runtime_config_store
                .get_config(protocol_version)
                .wasm_config
                .limit_config
                .max_gas_burnt
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{nibbles_within_prefix, prefix_successor};

    #[test]
    fn test_prefix_successor() {
        assert_eq!(prefix_successor(b"a").as_deref(), Some(&b"b"[..]));
        // Trailing 0xFF bytes are stripped before incrementing.
        assert_eq!(prefix_successor(b"a\xff\xff").as_deref(), Some(&b"b"[..]));
        assert_eq!(prefix_successor(&[2, 5, 0xff]).as_deref(), Some(&[2, 6][..]));
        // No successor exists for an all-0xFF (or empty) prefix.
        assert_eq!(prefix_successor(b""), None);
        assert_eq!(prefix_successor(b"\xff\xff"), None);
    }

    #[test]
    fn test_nibbles_within_prefix() {
        let prefix = &[2, 0, 6, 1];
        // Descent path toward the prefix must be kept, not pruned. This is the
        // case a naive `key.starts_with(prefix)` predicate would break.
        assert!(nibbles_within_prefix(prefix, &[]));
        assert!(nibbles_within_prefix(prefix, &[2]));
        assert!(nibbles_within_prefix(prefix, &[2, 0, 6]));
        assert!(nibbles_within_prefix(prefix, prefix));
        // Inside the prefix's subtree must be kept.
        assert!(nibbles_within_prefix(prefix, &[2, 0, 6, 1, 9, 9]));
        // Divergence at any position must be excluded.
        assert!(!nibbles_within_prefix(prefix, &[3]));
        assert!(!nibbles_within_prefix(prefix, &[2, 1]));
        assert!(!nibbles_within_prefix(prefix, &[2, 0, 6, 2]));
    }
}
