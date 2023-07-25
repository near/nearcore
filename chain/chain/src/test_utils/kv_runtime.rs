use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};

use borsh::{BorshDeserialize, BorshSerialize};

use near_epoch_manager::types::BlockHeaderInfo;
use near_epoch_manager::{EpochManagerAdapter, RngSeed};
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::state_part::PartId;
use num_rational::Ratio;

use near_chain_configs::{ProtocolConfig, DEFAULT_GC_NUM_EPOCHS_TO_KEEP};
use near_chain_primitives::Error;
use near_crypto::{KeyType, PublicKey, SecretKey, Signature};
use near_pool::types::PoolIterator;
use near_primitives::account::{AccessKey, Account};
use near_primitives::block_header::{Approval, ApprovalInner};
use near_primitives::challenge::ChallengesResult;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::EpochConfig;
use near_primitives::epoch_manager::ValidatorSelectionConfig;
use near_primitives::errors::{EpochError, InvalidTxError};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum};
use near_primitives::shard_layout;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::sharding::ChunkHash;
use near_primitives::transaction::{
    Action, ExecutionMetadata, ExecutionOutcome, ExecutionOutcomeWithId, ExecutionStatus,
    SignedTransaction, TransferAction,
};
use near_primitives::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
use near_primitives::types::{
    AccountId, ApprovalStake, Balance, BlockHeight, EpochHeight, EpochId, Gas, Nonce, NumShards,
    ShardId, StateChangesForSplitStates, StateRoot, StateRootNode, ValidatorInfoIdentifier,
};
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};
use near_primitives::views::{
    AccessKeyInfoView, AccessKeyList, CallResult, ContractCodeView, EpochValidatorInfo,
    QueryRequest, QueryResponse, QueryResponseKind, ViewStateResult,
};
use near_store::{
    set_genesis_state_roots, DBCol, PartialStorage, ShardTries, Store, StoreUpdate, Trie,
    TrieChanges, WrappedTrieChanges,
};

use crate::types::{ApplySplitStateResult, ApplyTransactionResult, RuntimeAdapter};
use crate::BlockHeader;

use near_primitives::epoch_manager::ShardConfig;

use super::ValidatorSchedule;

/// Simple key value runtime for tests.
///
/// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
/// WARNING: If you choose to use KeyValueRuntime for your tests, BE PREPARED TO
/// HAVE A BAD TIME. Use it only if you understand it to its entirety. It has
/// implicit behavior, very specific partially implemented logic, and is generally
/// incorrect. USE NightshadeRuntime WHENEVER POSSIBLE. YOU HAVE BEEN WARNED.
/// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
///
/// Major differences with production `NightshadeRuntime`:
///   * Uses in-memory storage
///   * Doesn't have WASM runtime, so can only process simple transfer
///     transaction
///   * Uses hard-coded validator schedule instead of using `EpochManager` and
///     staking to assign block and chunk producers.
pub struct KeyValueRuntime {
    store: Store,
    tries: ShardTries,
    num_shards: NumShards,
    epoch_length: u64,
    no_gc: bool,

    // A mapping state_root => {account id => amounts}, for transactions and receipts
    state: RwLock<HashMap<StateRoot, KVState>>,
    state_size: RwLock<HashMap<StateRoot, u64>>,
    headers_cache: RwLock<HashMap<CryptoHash, BlockHeader>>,
}

/// DEPRECATED. DO NOT USE for new tests. Use the real EpochManager, familiarize
/// yourself with how block producers, chunk producers, epoch transitions, etc.
/// work, and write your test to be compatible with what's in production.
/// MockEpochManager is simpler, but it deviates considerably from the production
/// validator selection and epoch management behavior.
pub struct MockEpochManager {
    store: Store,
    num_shards: NumShards,
    epoch_length: u64,
    /// A pre determined list of validator sets. We rotate validator set in this list.
    /// Epoch i uses validators from `validators_by_valset[i % validators_by_valset.len()]`.
    validators_by_valset: Vec<EpochValidatorSet>,
    /// Maps from account id to validator stake for all validators, both block producers and
    /// chunk producers
    validators: HashMap<AccountId, ValidatorStake>,

    headers_cache: RwLock<HashMap<CryptoHash, BlockHeader>>,
    hash_to_epoch: RwLock<HashMap<CryptoHash, EpochId>>,
    hash_to_next_epoch_approvals_req: RwLock<HashMap<CryptoHash, bool>>,
    hash_to_next_epoch: RwLock<HashMap<CryptoHash, EpochId>>,
    /// Maps EpochId to index of `validators_by_valset` to determine validators for an epoch
    hash_to_valset: RwLock<HashMap<EpochId, u64>>,
    epoch_start: RwLock<HashMap<CryptoHash, u64>>,
}

/// Stores the validator information in an epoch.
/// Block producers are specified by `block_producers`
/// Chunk producers have two types, validators who are also block producers and chunk only producers.
/// Block producers are assigned to shards via `validator_groups`.
/// Each shard will have `block_producers.len() / validator_groups` of validators who are also block
/// producers
struct EpochValidatorSet {
    block_producers: Vec<ValidatorStake>,
    /// index of this list is shard_id
    chunk_producers: Vec<Vec<ValidatorStake>>,
}

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Ord, PartialOrd, Clone, Debug)]
struct AccountNonce(AccountId, Nonce);

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug)]
struct KVState {
    amounts: HashMap<AccountId, u128>,
    receipt_nonces: HashSet<CryptoHash>,
    tx_nonces: HashSet<AccountNonce>,
}

impl MockEpochManager {
    pub fn new(store: Store, epoch_length: u64) -> Arc<Self> {
        let vs =
            ValidatorSchedule::new().block_producers_per_epoch(vec![vec!["test".parse().unwrap()]]);
        Self::new_with_validators(store, vs, epoch_length)
    }

    pub fn new_with_validators(
        store: Store,
        vs: ValidatorSchedule,
        epoch_length: u64,
    ) -> Arc<Self> {
        let map_with_default_hash1 = HashMap::from([(CryptoHash::default(), EpochId::default())]);
        let map_with_default_hash2 = HashMap::from([(CryptoHash::default(), 0)]);
        let map_with_default_hash3 = HashMap::from([(EpochId::default(), 0)]);

        let mut validators = HashMap::new();
        #[allow(unused_mut)]
        let mut validators_by_valset: Vec<EpochValidatorSet> = vs
            .block_producers
            .iter()
            .map(|account_ids| {
                let block_producers: Vec<ValidatorStake> = account_ids
                    .iter()
                    .map(|account_id| {
                        let stake = ValidatorStake::new(
                            account_id.clone(),
                            SecretKey::from_seed(KeyType::ED25519, account_id.as_ref())
                                .public_key(),
                            1_000_000,
                        );
                        validators.insert(account_id.clone(), stake.clone());
                        stake
                    })
                    .collect();

                let validators_per_shard = block_producers.len() as ShardId / vs.validator_groups;
                let coef = block_producers.len() as ShardId / vs.num_shards;

                let chunk_producers: Vec<Vec<ValidatorStake>> = (0..vs.num_shards)
                    .map(|shard_id| {
                        let offset = (shard_id * coef / validators_per_shard * validators_per_shard)
                            as usize;
                        block_producers[offset..offset + validators_per_shard as usize].to_vec()
                    })
                    .collect();

                EpochValidatorSet { block_producers, chunk_producers }
            })
            .collect();

        if !vs.chunk_only_producers.is_empty() {
            assert_eq!(validators_by_valset.len(), vs.chunk_only_producers.len());
            for (epoch_idx, epoch_cops) in vs.chunk_only_producers.into_iter().enumerate() {
                assert_eq!(epoch_cops.len() as u64, vs.num_shards);
                for (shard_idx, shard_cops) in epoch_cops.into_iter().enumerate() {
                    for account_id in shard_cops {
                        let stake = ValidatorStake::new(
                            account_id.clone(),
                            SecretKey::from_seed(KeyType::ED25519, account_id.as_ref())
                                .public_key(),
                            1_000_000,
                        );
                        let prev = validators.insert(account_id, stake.clone());
                        assert!(prev.is_none(), "chunk only produced is also a block producer");
                        validators_by_valset[epoch_idx].chunk_producers[shard_idx].push(stake)
                    }
                }
            }
        }

        Arc::new(MockEpochManager {
            store,
            num_shards: vs.num_shards,
            epoch_length,
            validators,
            validators_by_valset,
            headers_cache: RwLock::new(HashMap::new()),
            hash_to_epoch: RwLock::new(HashMap::new()),
            hash_to_next_epoch_approvals_req: RwLock::new(HashMap::new()),
            hash_to_next_epoch: RwLock::new(map_with_default_hash1),
            hash_to_valset: RwLock::new(map_with_default_hash3),
            epoch_start: RwLock::new(map_with_default_hash2),
        })
    }

    /// Get epoch and index of validator set by the hash of previous block.
    /// Note that it also fills in-memory chain info and there is some
    /// assumption that it is called for all previous blocks.
    /// TODO (#8269): should we call it recursively for previous blocks if info is not found?
    fn get_epoch_and_valset(
        &self,
        prev_hash: CryptoHash,
    ) -> Result<(EpochId, usize, EpochId), EpochError> {
        if prev_hash == CryptoHash::default() {
            return Ok((EpochId(prev_hash), 0, EpochId(prev_hash)));
        }
        let prev_block_header = self
            .get_block_header(&prev_hash)?
            .ok_or_else(|| EpochError::MissingBlock(prev_hash))?;

        let mut hash_to_epoch = self.hash_to_epoch.write().unwrap();
        let mut hash_to_next_epoch_approvals_req =
            self.hash_to_next_epoch_approvals_req.write().unwrap();
        let mut hash_to_next_epoch = self.hash_to_next_epoch.write().unwrap();
        let mut hash_to_valset = self.hash_to_valset.write().unwrap();
        let mut epoch_start_map = self.epoch_start.write().unwrap();

        let prev_prev_hash = *prev_block_header.prev_hash();
        let prev_epoch = hash_to_epoch.get(&prev_prev_hash);
        let prev_next_epoch = hash_to_next_epoch.get(&prev_prev_hash).unwrap();
        let prev_valset = match prev_epoch {
            Some(prev_epoch) => Some(*hash_to_valset.get(prev_epoch).unwrap()),
            None => None,
        };

        let prev_epoch_start = *epoch_start_map.get(&prev_prev_hash).unwrap();

        let last_final_height = if prev_block_header.last_final_block() == &CryptoHash::default() {
            0
        } else {
            self.get_block_header(prev_block_header.last_final_block()).unwrap().unwrap().height()
        };

        let increment_epoch = prev_prev_hash == CryptoHash::default() // genesis is in its own epoch
            || last_final_height + 3 >= prev_epoch_start + self.epoch_length;

        let needs_next_epoch_approvals = !increment_epoch
            && last_final_height + 3 < prev_epoch_start + self.epoch_length
            && prev_block_header.height() + 3 >= prev_epoch_start + self.epoch_length;

        let (epoch, next_epoch, valset, epoch_start) = if increment_epoch {
            let new_valset = match prev_valset {
                None => 0,
                Some(prev_valset) => prev_valset + 1,
            };
            (
                prev_next_epoch.clone(),
                EpochId(prev_hash),
                new_valset,
                prev_block_header.height() + 1,
            )
        } else {
            (
                prev_epoch.unwrap().clone(),
                prev_next_epoch.clone(),
                prev_valset.unwrap(),
                prev_epoch_start,
            )
        };

        hash_to_next_epoch.insert(prev_hash, next_epoch.clone());
        hash_to_epoch.insert(prev_hash, epoch.clone());
        hash_to_next_epoch_approvals_req.insert(prev_hash, needs_next_epoch_approvals);
        hash_to_valset.insert(epoch.clone(), valset);
        hash_to_valset.insert(next_epoch.clone(), valset + 1);
        epoch_start_map.insert(prev_hash, epoch_start);

        Ok((epoch, valset as usize % self.validators_by_valset.len(), next_epoch))
    }

    fn get_block_producers(&self, valset: usize) -> &[ValidatorStake] {
        &self.validators_by_valset[valset].block_producers
    }

    fn get_chunk_producers(&self, valset: usize, shard_id: ShardId) -> Vec<ValidatorStake> {
        self.validators_by_valset[valset].chunk_producers[shard_id as usize].clone()
    }

    fn get_valset_for_epoch(&self, epoch_id: &EpochId) -> Result<usize, EpochError> {
        // conveniently here if the prev_hash is passed mistakenly instead of the epoch_hash,
        // the `unwrap` will trigger
        Ok(*self
            .hash_to_valset
            .read()
            .unwrap()
            .get(epoch_id)
            .ok_or_else(|| EpochError::EpochOutOfBounds(epoch_id.clone()))? as usize
            % self.validators_by_valset.len())
    }

    fn get_block_header(&self, hash: &CryptoHash) -> Result<Option<BlockHeader>, EpochError> {
        let mut headers_cache = self.headers_cache.write().unwrap();
        if headers_cache.get(hash).is_some() {
            return Ok(Some(headers_cache.get(hash).unwrap().clone()));
        }
        if let Some(result) = self.store.get_ser(DBCol::BlockHeader, hash.as_ref())? {
            headers_cache.insert(*hash, result);
            return Ok(Some(headers_cache.get(hash).unwrap().clone()));
        }
        Ok(None)
    }
}

impl KeyValueRuntime {
    pub fn new(store: Store, epoch_manager: &MockEpochManager) -> Arc<Self> {
        Self::new_with_no_gc(store, epoch_manager, false)
    }
    pub fn new_with_no_gc(
        store: Store,
        epoch_manager: &MockEpochManager,
        no_gc: bool,
    ) -> Arc<Self> {
        let num_shards = epoch_manager.num_shards(&EpochId::default()).unwrap();
        let epoch_length =
            epoch_manager.get_epoch_config(&EpochId::default()).unwrap().epoch_length;
        let tries = ShardTries::test(store.clone(), num_shards);
        let mut initial_amounts = HashMap::new();
        for (i, validator_stake) in epoch_manager
            .validators_by_valset
            .iter()
            .flat_map(|set| set.block_producers.iter())
            .enumerate()
        {
            initial_amounts.insert(validator_stake.account_id().clone(), (1000 + 100 * i) as u128);
        }

        let kv_state = KVState {
            amounts: initial_amounts,
            receipt_nonces: HashSet::default(),
            tx_nonces: HashSet::default(),
        };
        let data = kv_state.try_to_vec().unwrap();
        let data_len = data.len() as u64;
        // StateRoot is actually faked here.
        // We cannot do any reasonable validations of it in test_utils.
        let state = HashMap::from([(Trie::EMPTY_ROOT, kv_state)]);
        let state_size = HashMap::from([(Trie::EMPTY_ROOT, data_len)]);

        let mut store_update = store.store_update();
        let genesis_roots: Vec<CryptoHash> = (0..num_shards).map(|_| Trie::EMPTY_ROOT).collect();
        set_genesis_state_roots(&mut store_update, &genesis_roots);
        store_update.commit().expect("Store failed on genesis intialization");

        Arc::new(KeyValueRuntime {
            store,
            tries,
            no_gc,
            num_shards,
            epoch_length,
            headers_cache: RwLock::new(HashMap::new()),
            state: RwLock::new(state),
            state_size: RwLock::new(state_size),
        })
    }

    fn get_block_header(&self, hash: &CryptoHash) -> Result<Option<BlockHeader>, EpochError> {
        let mut headers_cache = self.headers_cache.write().unwrap();
        if headers_cache.get(hash).is_some() {
            return Ok(Some(headers_cache.get(hash).unwrap().clone()));
        }
        if let Some(result) = self.store.get_ser(DBCol::BlockHeader, hash.as_ref())? {
            headers_cache.insert(*hash, result);
            return Ok(Some(headers_cache.get(hash).unwrap().clone()));
        }
        Ok(None)
    }
}

pub fn account_id_to_shard_id(account_id: &AccountId, num_shards: NumShards) -> ShardId {
    let shard_layout = ShardLayout::v0(num_shards, 0);
    shard_layout::account_id_to_shard_id(account_id, &shard_layout)
}

#[derive(BorshSerialize, BorshDeserialize)]
struct ReceiptNonce {
    from: AccountId,
    to: AccountId,
    amount: Balance,
    nonce: Nonce,
}

fn create_receipt_nonce(
    from: AccountId,
    to: AccountId,
    amount: Balance,
    nonce: Nonce,
) -> CryptoHash {
    CryptoHash::hash_borsh(ReceiptNonce { from, to, amount, nonce })
}

impl EpochManagerAdapter for MockEpochManager {
    fn epoch_exists(&self, epoch_id: &EpochId) -> bool {
        self.hash_to_valset.write().unwrap().contains_key(epoch_id)
    }

    fn num_shards(&self, _epoch_id: &EpochId) -> Result<ShardId, EpochError> {
        Ok(self.num_shards)
    }

    fn num_total_parts(&self) -> usize {
        12 + (self.num_shards as usize + 1) % 50
    }

    fn num_data_parts(&self) -> usize {
        // Same as in Nightshade Runtime
        let total_parts = self.num_total_parts();
        if total_parts <= 3 {
            1
        } else {
            (total_parts - 1) / 3
        }
    }

    fn get_part_owner(&self, epoch_id: &EpochId, part_id: u64) -> Result<AccountId, EpochError> {
        let validators =
            &self.get_epoch_block_producers_ordered(epoch_id, &CryptoHash::default())?;
        // if we don't use data_parts and total_parts as part of the formula here, the part owner
        //     would not depend on height, and tests wouldn't catch passing wrong height here
        let idx = part_id as usize + self.num_data_parts() + self.num_total_parts();
        Ok(validators[idx as usize % validators.len()].0.account_id().clone())
    }

    fn account_id_to_shard_id(
        &self,
        account_id: &AccountId,
        _epoch_id: &EpochId,
    ) -> Result<ShardId, EpochError> {
        Ok(account_id_to_shard_id(account_id, self.num_shards))
    }

    fn shard_id_to_uid(
        &self,
        shard_id: ShardId,
        _epoch_id: &EpochId,
    ) -> Result<ShardUId, EpochError> {
        Ok(ShardUId { version: 0, shard_id: shard_id as u32 })
    }

    fn get_block_info(&self, _hash: &CryptoHash) -> Result<Arc<BlockInfo>, EpochError> {
        Ok(Default::default())
    }

    fn get_epoch_config(&self, _epoch_id: &EpochId) -> Result<EpochConfig, EpochError> {
        Ok(EpochConfig {
            epoch_length: self.epoch_length,
            num_block_producer_seats: 2,
            num_block_producer_seats_per_shard: vec![1, 1],
            avg_hidden_validator_seats_per_shard: vec![1, 1],
            block_producer_kickout_threshold: 0,
            chunk_producer_kickout_threshold: 0,
            validator_max_kickout_stake_perc: 0,
            online_min_threshold: Ratio::new(1i32, 4i32),
            online_max_threshold: Ratio::new(3i32, 4i32),
            fishermen_threshold: 1,
            minimum_stake_divisor: 1,
            protocol_upgrade_stake_threshold: Ratio::new(3i32, 4i32),
            shard_layout: ShardLayout::v1_test(),
            validator_selection_config: ValidatorSelectionConfig::default(),
        })
    }

    /// Return the epoch info containing the mocked data.
    /// Epoch id is unused.
    /// Available mocked data:
    /// - validators
    /// - block producers
    /// - chunk producers
    /// All the other fields have a hardcoded value or left empty.
    fn get_epoch_info(&self, _epoch_id: &EpochId) -> Result<Arc<EpochInfo>, EpochError> {
        let validators = self.validators.iter().map(|(_, stake)| stake.clone()).collect();
        let mut validator_to_index = HashMap::new();
        for (i, (account_id, _)) in self.validators.iter().enumerate() {
            validator_to_index.insert(account_id.clone(), i as u64);
        }
        let bp_settlement = self.validators_by_valset[0]
            .block_producers
            .iter()
            .map(|stake| *validator_to_index.get(stake.account_id()).unwrap())
            .collect();
        let cp_settlement = self.validators_by_valset[0]
            .chunk_producers
            .iter()
            .map(|vec| {
                vec.iter()
                    .map(|stake| *validator_to_index.get(stake.account_id()).unwrap())
                    .collect()
            })
            .collect();
        Ok(Arc::new(EpochInfo::new(
            10,
            validators,
            validator_to_index,
            bp_settlement,
            cp_settlement,
            vec![],
            vec![],
            HashMap::new(),
            BTreeMap::new(),
            HashMap::new(),
            HashMap::new(),
            1,
            1,
            1,
            RngSeed::default(),
        )))
    }

    fn get_shard_layout(&self, _epoch_id: &EpochId) -> Result<ShardLayout, EpochError> {
        Ok(ShardLayout::v0(self.num_shards, 0))
    }

    fn get_shard_config(&self, _epoch_id: &EpochId) -> Result<ShardConfig, EpochError> {
        panic!("get_shard_config not implemented for KeyValueRuntime");
    }

    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError> {
        if parent_hash == &CryptoHash::default() {
            return Ok(true);
        }
        let prev_block_header = self
            .get_block_header(parent_hash)?
            .ok_or_else(|| EpochError::MissingBlock(*parent_hash))?;
        let prev_prev_hash = *prev_block_header.prev_hash();
        Ok(self.get_epoch_and_valset(*parent_hash)?.0
            != self.get_epoch_and_valset(prev_prev_hash)?.0)
    }

    fn get_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        Ok(self.get_epoch_and_valset(*parent_hash)?.0)
    }

    fn get_epoch_height_from_prev_block(
        &self,
        _prev_block_hash: &CryptoHash,
    ) -> Result<EpochHeight, EpochError> {
        Ok(0)
    }

    fn get_next_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let (_, _, next_epoch_id) = self.get_epoch_and_valset(*block_hash)?;
        Ok(next_epoch_id)
    }

    fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        Ok(self.get_epoch_and_valset(*parent_hash)?.2)
    }

    fn get_prev_shard_ids(
        &self,
        _prev_hash: &CryptoHash,
        shard_ids: Vec<ShardId>,
    ) -> Result<Vec<ShardId>, Error> {
        Ok(shard_ids)
    }

    fn get_shard_layout_from_prev_block(
        &self,
        _parent_hash: &CryptoHash,
    ) -> Result<ShardLayout, EpochError> {
        Ok(ShardLayout::v0(self.num_shards, 0))
    }

    fn get_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let (epoch_id, _, _) = self.get_epoch_and_valset(*block_hash)?;
        Ok(epoch_id)
    }

    fn compare_epoch_id(
        &self,
        epoch_id: &EpochId,
        other_epoch_id: &EpochId,
    ) -> Result<Ordering, EpochError> {
        if epoch_id.0 == other_epoch_id.0 {
            return Ok(Ordering::Equal);
        }
        match (self.get_valset_for_epoch(epoch_id), self.get_valset_for_epoch(other_epoch_id)) {
            (Ok(index1), Ok(index2)) => Ok(index1.cmp(&index2)),
            _ => Err(EpochError::EpochOutOfBounds(epoch_id.clone())),
        }
    }

    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockHeight, EpochError> {
        let epoch_id = self.get_epoch_id(block_hash)?;
        match self.get_block_header(&epoch_id.0)? {
            Some(block_header) => Ok(block_header.height()),
            None => Ok(0),
        }
    }

    fn get_prev_epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        let mut candidate_hash = *prev_block_hash;
        loop {
            let header = self
                .get_block_header(&candidate_hash)?
                .ok_or_else(|| EpochError::MissingBlock(candidate_hash))?;
            candidate_hash = *header.prev_hash();
            if self.is_next_block_epoch_start(&candidate_hash)? {
                break Ok(self.get_epoch_and_valset(candidate_hash)?.0);
            }
        }
    }

    fn get_estimated_protocol_upgrade_block_height(
        &self,
        _block_hash: CryptoHash,
    ) -> Result<Option<BlockHeight>, EpochError> {
        Ok(None)
    }

    fn get_epoch_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, EpochError> {
        let validators = self.get_block_producers(self.get_valset_for_epoch(epoch_id)?);
        Ok(validators.iter().map(|x| (x.clone(), false)).collect())
    }

    fn get_epoch_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<(ApprovalStake, bool)>, EpochError> {
        let (_cur_epoch, cur_valset, next_epoch) = self.get_epoch_and_valset(*parent_hash)?;
        let mut validators = self
            .get_block_producers(cur_valset)
            .iter()
            .map(|x| x.get_approval_stake(false))
            .collect::<Vec<_>>();
        if *self.hash_to_next_epoch_approvals_req.write().unwrap().get(parent_hash).unwrap() {
            let validators_copy = validators.clone();
            validators.extend(
                self.get_block_producers(self.get_valset_for_epoch(&next_epoch)?)
                    .iter()
                    .filter(|x| {
                        !validators_copy.iter().any(|entry| &entry.account_id == x.account_id())
                    })
                    .map(|x| x.get_approval_stake(true)),
            );
        }
        let validators = validators.into_iter().map(|stake| (stake, false)).collect::<Vec<_>>();
        Ok(validators)
    }

    fn get_epoch_chunk_producers(
        &self,
        _epoch_id: &EpochId,
    ) -> Result<Vec<ValidatorStake>, EpochError> {
        tracing::warn!("not implemented, returning a dummy value");
        Ok(vec![])
    }

    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, EpochError> {
        let validators = self.get_block_producers(self.get_valset_for_epoch(epoch_id)?);
        Ok(validators[(height as usize) % validators.len()].account_id().clone())
    }

    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<AccountId, EpochError> {
        let valset = self.get_valset_for_epoch(epoch_id)?;
        let chunk_producers = self.get_chunk_producers(valset, shard_id);
        let index = (shard_id + height + 1) as usize % chunk_producers.len();
        Ok(chunk_producers[index].account_id().clone())
    }

    fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), EpochError> {
        let validators = &self.validators_by_valset[self.get_valset_for_epoch(epoch_id)?];
        for validator_stake in validators.block_producers.iter() {
            if validator_stake.account_id() == account_id {
                return Ok((validator_stake.clone(), false));
            }
        }
        for validator_stake in validators.chunk_producers.iter().flatten() {
            if validator_stake.account_id() == account_id {
                return Ok((validator_stake.clone(), false));
            }
        }
        Err(EpochError::NotAValidator(account_id.clone(), epoch_id.clone()))
    }

    fn get_fisherman_by_account_id(
        &self,
        epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), EpochError> {
        Err(EpochError::NotAValidator(account_id.clone(), epoch_id.clone()))
    }

    fn get_validator_info(
        &self,
        _epoch_id: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, EpochError> {
        Ok(EpochValidatorInfo {
            current_validators: vec![],
            next_validators: vec![],
            current_fishermen: vec![],
            next_fishermen: vec![],
            current_proposals: vec![],
            prev_epoch_kickout: vec![],
            epoch_start_height: 0,
            epoch_height: 1,
        })
    }

    fn add_validator_proposals(
        &self,
        _block_header_info: BlockHeaderInfo,
    ) -> Result<StoreUpdate, EpochError> {
        Ok(self.store.store_update())
    }

    fn get_epoch_minted_amount(&self, _epoch_id: &EpochId) -> Result<Balance, EpochError> {
        Ok(0)
    }

    fn get_epoch_protocol_version(
        &self,
        _epoch_id: &EpochId,
    ) -> Result<ProtocolVersion, EpochError> {
        Ok(PROTOCOL_VERSION)
    }

    fn get_epoch_sync_data(
        &self,
        _prev_epoch_last_block_hash: &CryptoHash,
        _epoch_id: &EpochId,
        _next_epoch_id: &EpochId,
    ) -> Result<
        (
            Arc<BlockInfo>,
            Arc<BlockInfo>,
            Arc<BlockInfo>,
            Arc<EpochInfo>,
            Arc<EpochInfo>,
            Arc<EpochInfo>,
        ),
        EpochError,
    > {
        Ok(Default::default())
    }

    fn epoch_sync_init_epoch_manager(
        &self,
        _prev_epoch_first_block_info: BlockInfo,
        _prev_epoch_last_block_info: BlockInfo,
        _prev_epoch_prev_last_block_info: BlockInfo,
        _prev_epoch_id: &EpochId,
        _prev_epoch_info: EpochInfo,
        _epoch_id: &EpochId,
        _epoch_info: EpochInfo,
        _next_epoch_id: &EpochId,
        _next_epoch_info: EpochInfo,
    ) -> Result<(), EpochError> {
        Ok(())
    }

    fn verify_block_vrf(
        &self,
        _epoch_id: &EpochId,
        _block_height: BlockHeight,
        _prev_random_value: &CryptoHash,
        _vrf_value: &near_crypto::vrf::Value,
        _vrf_proof: &near_crypto::vrf::Proof,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn verify_validator_signature(
        &self,
        _epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
        _account_id: &AccountId,
        _data: &[u8],
        _signature: &Signature,
    ) -> Result<bool, Error> {
        Ok(true)
    }

    fn verify_validator_or_fisherman_signature(
        &self,
        _epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
        _account_id: &AccountId,
        _data: &[u8],
        _signature: &Signature,
    ) -> Result<bool, Error> {
        Ok(true)
    }

    fn verify_header_signature(&self, header: &BlockHeader) -> Result<bool, Error> {
        let validator = self.get_block_producer(&header.epoch_id(), header.height())?;
        let validator_stake = &self.validators[&validator];
        Ok(header.verify_block_producer(validator_stake.public_key()))
    }

    fn verify_chunk_signature_with_header_parts(
        &self,
        _chunk_hash: &ChunkHash,
        _signature: &Signature,
        _epoch_id: &EpochId,
        _last_kown_hash: &CryptoHash,
        _height_created: BlockHeight,
        _shard_id: ShardId,
    ) -> Result<bool, Error> {
        Ok(true)
    }

    fn verify_approval(
        &self,
        _prev_block_hash: &CryptoHash,
        _prev_block_height: BlockHeight,
        _block_height: BlockHeight,
        _approvals: &[Option<Signature>],
    ) -> Result<bool, Error> {
        Ok(true)
    }

    fn verify_approvals_and_threshold_orphan(
        &self,
        epoch_id: &EpochId,
        can_approved_block_be_produced: &dyn Fn(
            &[Option<Signature>],
            &[(Balance, Balance, bool)],
        ) -> bool,
        prev_block_hash: &CryptoHash,
        prev_block_height: BlockHeight,
        block_height: BlockHeight,
        approvals: &[Option<Signature>],
    ) -> Result<(), Error> {
        let validators = self.get_block_producers(self.get_valset_for_epoch(epoch_id)?);
        let message_to_sign = Approval::get_data_for_sig(
            &if prev_block_height + 1 == block_height {
                ApprovalInner::Endorsement(*prev_block_hash)
            } else {
                ApprovalInner::Skip(prev_block_height)
            },
            block_height,
        );

        for (validator, may_be_signature) in validators.iter().zip(approvals.iter()) {
            if let Some(signature) = may_be_signature {
                if !signature.verify(message_to_sign.as_ref(), validator.public_key()) {
                    return Err(Error::InvalidApprovals);
                }
            }
        }
        let stakes = validators.iter().map(|stake| (stake.stake(), 0, false)).collect::<Vec<_>>();
        if !can_approved_block_be_produced(approvals, &stakes) {
            Err(Error::NotEnoughApprovals)
        } else {
            Ok(())
        }
    }

    fn cares_about_shard_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        // This `unwrap` here tests that in all code paths we check that the epoch exists before
        //    we check if we care about a shard. Please do not remove the unwrap, fix the logic of
        //    the calling function.
        let epoch_valset = self.get_epoch_and_valset(*parent_hash).unwrap();
        let chunk_producers = self.get_chunk_producers(epoch_valset.1, shard_id);
        for validator in chunk_producers {
            if validator.account_id() == account_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn cares_about_shard_next_epoch_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        // This `unwrap` here tests that in all code paths we check that the epoch exists before
        //    we check if we care about a shard. Please do not remove the unwrap, fix the logic of
        //    the calling function.
        let epoch_valset = self.get_epoch_and_valset(*parent_hash).unwrap();
        let chunk_producers = self
            .get_chunk_producers((epoch_valset.1 + 1) % self.validators_by_valset.len(), shard_id);
        for validator in chunk_producers {
            if validator.account_id() == account_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn will_shard_layout_change(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError> {
        // Copied from EpochManager (KeyValueRuntime is deprecated anyway).
        let epoch_id = self.get_epoch_id_from_prev_block(parent_hash)?;
        let next_epoch_id = self.get_next_epoch_id_from_prev_block(parent_hash)?;
        let shard_layout = self.get_shard_layout(&epoch_id)?;
        let next_shard_layout = self.get_shard_layout(&next_epoch_id)?;
        Ok(shard_layout != next_shard_layout)
    }
}

impl RuntimeAdapter for KeyValueRuntime {
    fn store(&self) -> &Store {
        &self.store
    }

    fn get_tries(&self) -> ShardTries {
        self.tries.clone()
    }

    fn get_trie_for_shard(
        &self,
        shard_id: ShardId,
        _block_hash: &CryptoHash,
        state_root: StateRoot,
        _use_flat_storage: bool,
    ) -> Result<Trie, Error> {
        Ok(self
            .tries
            .get_trie_for_shard(ShardUId { version: 0, shard_id: shard_id as u32 }, state_root))
    }

    fn get_flat_storage_manager(&self) -> Option<near_store::flat::FlatStorageManager> {
        None
    }

    fn get_view_trie_for_shard(
        &self,
        shard_id: ShardId,
        _block_hash: &CryptoHash,
        state_root: StateRoot,
    ) -> Result<Trie, Error> {
        Ok(self.tries.get_view_trie_for_shard(
            ShardUId { version: 0, shard_id: shard_id as u32 },
            state_root,
        ))
    }

    fn validate_tx(
        &self,
        _gas_price: Balance,
        _state_update: Option<StateRoot>,
        _transaction: &SignedTransaction,
        _verify_signature: bool,
        _epoch_id: &EpochId,
        _current_protocol_version: ProtocolVersion,
    ) -> Result<Option<InvalidTxError>, Error> {
        Ok(None)
    }

    fn prepare_transactions(
        &self,
        _gas_price: Balance,
        _gas_limit: Gas,
        _epoch_id: &EpochId,
        _shard_id: ShardId,
        _state_root: StateRoot,
        _next_block_height: BlockHeight,
        transactions: &mut dyn PoolIterator,
        _chain_validate: &mut dyn FnMut(&SignedTransaction) -> bool,
        _current_protocol_version: ProtocolVersion,
    ) -> Result<Vec<SignedTransaction>, Error> {
        let mut res = vec![];
        while let Some(iter) = transactions.next() {
            res.push(iter.next().unwrap());
        }
        Ok(res)
    }

    fn apply_transactions_with_optional_storage_proof(
        &self,
        shard_id: ShardId,
        state_root: &StateRoot,
        _height: BlockHeight,
        _block_timestamp: u64,
        _prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        receipts: &[Receipt],
        transactions: &[SignedTransaction],
        _last_validator_proposals: ValidatorStakeIter,
        gas_price: Balance,
        _gas_limit: Gas,
        _challenges: &ChallengesResult,
        _random_seed: CryptoHash,
        generate_storage_proof: bool,
        _is_new_chunk: bool,
        _is_first_block_with_chunk_of_version: bool,
        _state_patch: SandboxStatePatch,
        _use_flat_storage: bool,
    ) -> Result<ApplyTransactionResult, Error> {
        assert!(!generate_storage_proof);
        let mut tx_results = vec![];

        let mut state = self.state.read().unwrap().get(state_root).cloned().unwrap();

        let mut balance_transfers = vec![];

        for receipt in receipts.iter() {
            if let ReceiptEnum::Action(action) = &receipt.receipt {
                assert_eq!(account_id_to_shard_id(&receipt.receiver_id, self.num_shards), shard_id);
                if !state.receipt_nonces.contains(&receipt.receipt_id) {
                    state.receipt_nonces.insert(receipt.receipt_id);
                    if let Action::Transfer(TransferAction { deposit }) = action.actions[0] {
                        balance_transfers.push((
                            receipt.get_hash(),
                            receipt.predecessor_id.clone(),
                            receipt.receiver_id.clone(),
                            deposit,
                            0,
                        ));
                    }
                } else {
                    panic!("receipts should never be applied twice");
                }
            } else {
                unreachable!();
            }
        }

        for transaction in transactions {
            assert_eq!(
                account_id_to_shard_id(&transaction.transaction.signer_id, self.num_shards),
                shard_id
            );
            if transaction.transaction.actions.is_empty() {
                continue;
            }
            if let Action::Transfer(TransferAction { deposit }) = transaction.transaction.actions[0]
            {
                if !state.tx_nonces.contains(&AccountNonce(
                    transaction.transaction.receiver_id.clone(),
                    transaction.transaction.nonce,
                )) {
                    state.tx_nonces.insert(AccountNonce(
                        transaction.transaction.receiver_id.clone(),
                        transaction.transaction.nonce,
                    ));
                    balance_transfers.push((
                        transaction.get_hash(),
                        transaction.transaction.signer_id.clone(),
                        transaction.transaction.receiver_id.clone(),
                        deposit,
                        transaction.transaction.nonce,
                    ));
                } else {
                    balance_transfers.push((
                        transaction.get_hash(),
                        transaction.transaction.signer_id.clone(),
                        transaction.transaction.receiver_id.clone(),
                        0,
                        transaction.transaction.nonce,
                    ));
                }
            } else {
                unreachable!();
            }
        }

        let mut outgoing_receipts = vec![];

        for (hash, from, to, amount, nonce) in balance_transfers {
            let mut good_to_go = false;

            if account_id_to_shard_id(&from, self.num_shards) != shard_id {
                // This is a receipt, was already debited
                good_to_go = true;
            } else if let Some(balance) = state.amounts.get(&from) {
                if *balance >= amount {
                    let new_balance = balance - amount;
                    state.amounts.insert(from.clone(), new_balance);
                    good_to_go = true;
                }
            }

            if good_to_go {
                let new_receipt_hashes = if account_id_to_shard_id(&to, self.num_shards) == shard_id
                {
                    state.amounts.insert(to.clone(), state.amounts.get(&to).unwrap_or(&0) + amount);
                    vec![]
                } else {
                    assert_ne!(nonce, 0);
                    let receipt = Receipt {
                        predecessor_id: from.clone(),
                        receiver_id: to.clone(),
                        receipt_id: create_receipt_nonce(from.clone(), to.clone(), amount, nonce),
                        receipt: ReceiptEnum::Action(ActionReceipt {
                            signer_id: from.clone(),
                            signer_public_key: PublicKey::empty(KeyType::ED25519),
                            gas_price,
                            output_data_receivers: vec![],
                            input_data_ids: vec![],
                            actions: vec![Action::Transfer(TransferAction { deposit: amount })],
                        }),
                    };
                    let receipt_hash = receipt.get_hash();
                    outgoing_receipts.push(receipt);
                    vec![receipt_hash]
                };

                tx_results.push(ExecutionOutcomeWithId {
                    id: hash,
                    outcome: ExecutionOutcome {
                        status: ExecutionStatus::SuccessValue(vec![]),
                        logs: vec![],
                        receipt_ids: new_receipt_hashes,
                        gas_burnt: 0,
                        compute_usage: Some(0),
                        tokens_burnt: 0,
                        executor_id: to.clone(),
                        metadata: ExecutionMetadata::V1,
                    },
                });
            }
        }

        let data = state.try_to_vec()?;
        let state_size = data.len() as u64;
        let state_root = hash(&data);
        self.state.write().unwrap().insert(state_root, state);
        self.state_size.write().unwrap().insert(state_root, state_size);

        Ok(ApplyTransactionResult {
            trie_changes: WrappedTrieChanges::new(
                self.get_tries(),
                ShardUId { version: 0, shard_id: shard_id as u32 },
                TrieChanges::empty(state_root),
                Default::default(),
                *block_hash,
            ),
            new_root: state_root,
            outcomes: tx_results,
            outgoing_receipts,
            validator_proposals: vec![],
            total_gas_burnt: 0,
            total_balance_burnt: 0,
            proof: None,
            processed_delayed_receipts: vec![],
        })
    }

    fn check_state_transition(
        &self,
        _partial_storage: PartialStorage,
        _shard_id: ShardId,
        _state_root: &StateRoot,
        _height: BlockHeight,
        _block_timestamp: u64,
        _prev_block_hash: &CryptoHash,
        _block_hash: &CryptoHash,
        _receipts: &[Receipt],
        _transactions: &[SignedTransaction],
        _last_validator_proposals: ValidatorStakeIter,
        _gas_price: Balance,
        _gas_limit: Gas,
        _challenges: &ChallengesResult,
        _random_value: CryptoHash,
        _is_new_chunk: bool,
        _is_first_block_with_chunk_of_version: bool,
    ) -> Result<ApplyTransactionResult, Error> {
        unimplemented!();
    }

    fn query(
        &self,
        _shard_id: ShardUId,
        state_root: &StateRoot,
        block_height: BlockHeight,
        _block_timestamp: u64,
        _prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        _epoch_id: &EpochId,
        request: &QueryRequest,
    ) -> Result<QueryResponse, near_chain_primitives::error::QueryError> {
        match request {
            QueryRequest::ViewAccount { account_id, .. } => Ok(QueryResponse {
                kind: QueryResponseKind::ViewAccount(
                    Account::new(
                        self.state.read().unwrap().get(state_root).map_or_else(
                            || 0,
                            |state| *state.amounts.get(account_id).unwrap_or(&0),
                        ),
                        0,
                        CryptoHash::default(),
                        0,
                    )
                    .into(),
                ),
                block_height,
                block_hash: *block_hash,
            }),
            QueryRequest::ViewCode { .. } => Ok(QueryResponse {
                kind: QueryResponseKind::ViewCode(ContractCodeView {
                    code: vec![],
                    hash: CryptoHash::default(),
                }),
                block_height,
                block_hash: *block_hash,
            }),
            QueryRequest::ViewAccessKeyList { .. } => Ok(QueryResponse {
                kind: QueryResponseKind::AccessKeyList(AccessKeyList {
                    keys: vec![AccessKeyInfoView {
                        public_key: PublicKey::empty(KeyType::ED25519),
                        access_key: AccessKey::full_access().into(),
                    }],
                }),
                block_height,
                block_hash: *block_hash,
            }),
            QueryRequest::ViewAccessKey { .. } => Ok(QueryResponse {
                kind: QueryResponseKind::AccessKey(AccessKey::full_access().into()),
                block_height,
                block_hash: *block_hash,
            }),
            QueryRequest::ViewState { .. } => Ok(QueryResponse {
                kind: QueryResponseKind::ViewState(ViewStateResult {
                    values: Default::default(),
                    proof: vec![],
                }),
                block_height,
                block_hash: *block_hash,
            }),
            QueryRequest::CallFunction { .. } => Ok(QueryResponse {
                kind: QueryResponseKind::CallResult(CallResult {
                    result: Default::default(),
                    logs: Default::default(),
                }),
                block_height,
                block_hash: *block_hash,
            }),
        }
    }

    fn obtain_state_part(
        &self,
        _shard_id: ShardId,
        _block_hash: &CryptoHash,
        state_root: &StateRoot,
        part_id: PartId,
    ) -> Result<Vec<u8>, Error> {
        if part_id.idx != 0 {
            return Ok(vec![]);
        }
        let state = self.state.read().unwrap().get(state_root).unwrap().clone();
        let data = state.try_to_vec().expect("should never fall");
        Ok(data)
    }

    fn validate_state_part(&self, _state_root: &StateRoot, _part_id: PartId, _data: &[u8]) -> bool {
        // We do not care about deeper validation in test_utils
        true
    }

    fn apply_state_part(
        &self,
        _shard_id: ShardId,
        state_root: &StateRoot,
        part_id: PartId,
        data: &[u8],
        _epoch_id: &EpochId,
    ) -> Result<(), Error> {
        if part_id.idx != 0 {
            return Ok(());
        }
        let state = KVState::try_from_slice(data).unwrap();
        self.state.write().unwrap().insert(*state_root, state.clone());
        let data = state.try_to_vec()?;
        let state_size = data.len() as u64;
        self.state_size.write().unwrap().insert(*state_root, state_size);
        Ok(())
    }

    fn get_state_root_node(
        &self,
        _shard_id: ShardId,
        _block_hash: &CryptoHash,
        state_root: &StateRoot,
    ) -> Result<StateRootNode, Error> {
        let data = self
            .state
            .read()
            .unwrap()
            .get(state_root)
            .unwrap()
            .clone()
            .try_to_vec()
            .expect("should never fall")
            .into();
        let memory_usage = *self.state_size.read().unwrap().get(state_root).unwrap();
        Ok(StateRootNode { data, memory_usage })
    }

    fn validate_state_root_node(
        &self,
        _state_root_node: &StateRootNode,
        _state_root: &StateRoot,
    ) -> bool {
        // We do not care about deeper validation in test_utils
        true
    }

    fn get_gc_stop_height(&self, block_hash: &CryptoHash) -> BlockHeight {
        if !self.no_gc {
            // This code is 'incorrect' - as production one is always setting the GC to the
            // first block of the epoch.
            // Unfortunately many tests are depending on this and not setting epochs when
            // they produce blocks.
            let block_height = self
                .get_block_header(block_hash)
                .unwrap_or_default()
                .map(|h| h.height())
                .unwrap_or_default();
            block_height.saturating_sub(DEFAULT_GC_NUM_EPOCHS_TO_KEEP * self.epoch_length)
        /*  // TODO: use this version of the code instead - after we fix the block creation
            // issue in multiple tests.
        // We have to return the first block of the epoch T-DEFAULT_GC_NUM_EPOCHS_TO_KEEP.
        let mut current_header = self.get_block_header(block_hash).unwrap().unwrap();
        for _ in 0..DEFAULT_GC_NUM_EPOCHS_TO_KEEP {
            let last_block_of_prev_epoch = current_header.next_epoch_id();
            current_header =
                self.get_block_header(&last_block_of_prev_epoch.0).unwrap().unwrap();
        }
        loop {
            if current_header.next_epoch_id().0 == *current_header.prev_hash() {
                break;
            }
            current_header =
                self.get_block_header(current_header.prev_hash()).unwrap().unwrap();
        }
        current_header.height()*/
        } else {
            0
        }
    }

    fn get_protocol_config(&self, _epoch_id: &EpochId) -> Result<ProtocolConfig, Error> {
        unreachable!("get_protocol_config should not be called in KeyValueRuntime");
    }

    fn will_shard_layout_change_next_epoch(
        &self,
        _parent_hash: &CryptoHash,
    ) -> Result<bool, Error> {
        Ok(false)
    }

    fn apply_update_to_split_states(
        &self,
        _block_hash: &CryptoHash,
        _state_roots: HashMap<ShardUId, StateRoot>,
        _next_shard_layout: &ShardLayout,
        _state_changes: StateChangesForSplitStates,
    ) -> Result<Vec<ApplySplitStateResult>, Error> {
        Ok(vec![])
    }
}
