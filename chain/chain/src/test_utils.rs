mod validator_schedule;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use borsh::{BorshDeserialize, BorshSerialize};

use near_epoch_manager::EpochManagerAdapter;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::state_part::PartId;
use num_rational::Ratio;
use tracing::debug;

use near_chain_configs::{ProtocolConfig, DEFAULT_GC_NUM_EPOCHS_TO_KEEP};
use near_chain_primitives::Error;
use near_client_primitives::types::StateSplitApplyingStatus;
use near_crypto::{KeyType, PublicKey, SecretKey, Signature};
use near_pool::types::PoolIterator;
use near_primitives::account::{AccessKey, Account};
use near_primitives::block::Block;
use near_primitives::block_header::{Approval, ApprovalInner};
use near_primitives::challenge::ChallengesResult;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
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
    AccountId, ApprovalStake, Balance, BlockHeight, EpochHeight, EpochId, Gas, Nonce, NumBlocks,
    NumShards, ShardId, StateChangesForSplitStates, StateRoot, StateRootNode,
    ValidatorInfoIdentifier,
};
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};
use near_primitives::views::{
    AccessKeyInfoView, AccessKeyList, CallResult, ContractCodeView, EpochValidatorInfo,
    QueryRequest, QueryResponse, QueryResponseKind, ViewStateResult,
};
use near_store::test_utils::create_test_store;
use near_store::{
    DBCol, PartialStorage, ShardTries, Store, StoreUpdate, Trie, TrieChanges, WrappedTrieChanges,
};

use crate::block_processing_utils::BlockNotInPoolError;
use crate::chain::Chain;
use crate::store::ChainStoreAccess;
use crate::types::{
    AcceptedBlock, ApplySplitStateResult, ApplyTransactionResult, BlockHeaderInfo, ChainGenesis,
};
use crate::{BlockHeader, DoomslugThresholdMode, RuntimeAdapter};
use crate::{BlockProcessingArtifact, Doomslug, Provenance};
use near_primitives::epoch_manager::ShardConfig;
use near_primitives::time::Clock;
use near_primitives::utils::MaybeValidated;

pub use self::validator_schedule::ValidatorSchedule;

/// Wait for all blocks that started processing to be ready for postprocessing
/// Returns true if there are new blocks that are ready
pub fn wait_for_all_blocks_in_processing(chain: &mut Chain) -> bool {
    chain.blocks_in_processing.wait_for_all_blocks()
}

pub fn wait_for_block_in_processing(
    chain: &mut Chain,
    hash: &CryptoHash,
) -> Result<(), BlockNotInPoolError> {
    chain.blocks_in_processing.wait_for_block(hash)
}

/// Unlike Chain::start_process_block_async, this function blocks until the processing of this block
/// finishes
pub fn process_block_sync(
    chain: &mut Chain,
    me: &Option<AccountId>,
    block: MaybeValidated<Block>,
    provenance: Provenance,
    block_processing_artifacts: &mut BlockProcessingArtifact,
) -> Result<Vec<AcceptedBlock>, Error> {
    let block_hash = block.hash().clone();
    chain.start_process_block_async(
        me,
        block,
        provenance,
        block_processing_artifacts,
        Arc::new(|_| {}),
    )?;
    wait_for_block_in_processing(chain, &block_hash).unwrap();
    let (accepted_blocks, errors) =
        chain.postprocess_ready_blocks(me, block_processing_artifacts, Arc::new(|_| {}));
    // This is in test, we should never get errors when postprocessing blocks
    debug_assert!(errors.is_empty());
    Ok(accepted_blocks)
}

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Ord, PartialOrd, Clone, Debug)]
struct AccountNonce(AccountId, Nonce);

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug)]
struct KVState {
    amounts: HashMap<AccountId, u128>,
    receipt_nonces: HashSet<CryptoHash>,
    tx_nonces: HashSet<AccountNonce>,
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

/// Simple key value runtime for tests.
pub struct KeyValueRuntime {
    store: Store,
    tries: ShardTries,
    /// A pre determined list of validator sets. We rotate validator set in this list.
    /// Epoch i uses validators from `validators_by_valset[i % validators_by_valset.len()]`.
    validators_by_valset: Vec<EpochValidatorSet>,
    /// Maps from account id to validator stake for all validators, both block producers and
    /// chunk producers
    validators: HashMap<AccountId, ValidatorStake>,
    num_shards: NumShards,
    tracks_all_shards: bool,
    epoch_length: u64,
    no_gc: bool,

    // A mapping state_root => {account id => amounts}, for transactions and receipts
    state: RwLock<HashMap<StateRoot, KVState>>,
    state_size: RwLock<HashMap<StateRoot, u64>>,

    headers_cache: RwLock<HashMap<CryptoHash, BlockHeader>>,
    hash_to_epoch: RwLock<HashMap<CryptoHash, EpochId>>,
    hash_to_next_epoch_approvals_req: RwLock<HashMap<CryptoHash, bool>>,
    hash_to_next_epoch: RwLock<HashMap<CryptoHash, EpochId>>,
    /// Maps EpochId to index of `validators_by_valset` to determine validators for an epoch
    hash_to_valset: RwLock<HashMap<EpochId, u64>>,
    epoch_start: RwLock<HashMap<CryptoHash, u64>>,
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
    hash(&ReceiptNonce { from, to, amount, nonce }.try_to_vec().unwrap())
}

impl KeyValueRuntime {
    pub fn new(store: Store, epoch_length: u64) -> Self {
        let vs =
            ValidatorSchedule::new().block_producers_per_epoch(vec![vec!["test".parse().unwrap()]]);
        Self::new_with_validators(store, vs, epoch_length)
    }

    pub fn new_with_validators(store: Store, vs: ValidatorSchedule, epoch_length: u64) -> Self {
        Self::new_with_validators_and_no_gc(store, vs, epoch_length, false)
    }

    pub fn new_with_validators_and_no_gc(
        store: Store,
        vs: ValidatorSchedule,
        epoch_length: u64,
        no_gc: bool,
    ) -> Self {
        let tries = ShardTries::test(store.clone(), vs.num_shards);
        let mut initial_amounts = HashMap::new();
        for (i, validator) in vs.block_producers.iter().flatten().enumerate() {
            initial_amounts.insert(validator.clone(), (1000 + 100 * i) as u128);
        }

        let map_with_default_hash1 = HashMap::from([(CryptoHash::default(), EpochId::default())]);
        let map_with_default_hash2 = HashMap::from([(CryptoHash::default(), 0)]);
        let map_with_default_hash3 = HashMap::from([(EpochId::default(), 0)]);

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

        KeyValueRuntime {
            store,
            tries,
            validators,
            validators_by_valset,
            num_shards: vs.num_shards,
            tracks_all_shards: false,
            epoch_length,
            state: RwLock::new(state),
            state_size: RwLock::new(state_size),
            headers_cache: RwLock::new(HashMap::new()),
            hash_to_epoch: RwLock::new(HashMap::new()),
            hash_to_next_epoch_approvals_req: RwLock::new(HashMap::new()),
            hash_to_next_epoch: RwLock::new(map_with_default_hash1),
            hash_to_valset: RwLock::new(map_with_default_hash3),
            epoch_start: RwLock::new(map_with_default_hash2),
            no_gc,
        }
    }

    pub fn set_tracks_all_shards(&mut self, tracks_all_shards: bool) {
        self.tracks_all_shards = tracks_all_shards;
    }

    fn get_block_header(&self, hash: &CryptoHash) -> Result<Option<BlockHeader>, Error> {
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

    fn get_epoch_and_valset(
        &self,
        prev_hash: CryptoHash,
    ) -> Result<(EpochId, usize, EpochId), Error> {
        if prev_hash == CryptoHash::default() {
            return Ok((EpochId(prev_hash), 0, EpochId(prev_hash)));
        }
        let prev_block_header = self
            .get_block_header(&prev_hash)?
            .ok_or_else(|| Error::DBNotFoundErr(prev_hash.to_string()))?;

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

    fn get_valset_for_epoch(&self, epoch_id: &EpochId) -> Result<usize, Error> {
        // conveniently here if the prev_hash is passed mistakenly instead of the epoch_hash,
        // the `unwrap` will trigger
        Ok(*self
            .hash_to_valset
            .read()
            .unwrap()
            .get(epoch_id)
            .ok_or_else(|| Error::EpochOutOfBounds(epoch_id.clone()))? as usize
            % self.validators_by_valset.len())
    }

    pub fn get_chunk_only_producers_for_shard(
        &self,
        epoch_id: &EpochId,
        shard_id: ShardId,
    ) -> Result<Vec<&ValidatorStake>, Error> {
        let valset = self.get_valset_for_epoch(epoch_id)?;
        let block_producers = &self.validators_by_valset[valset].block_producers;
        let chunk_producers = &self.validators_by_valset[valset].chunk_producers[shard_id as usize];
        Ok(chunk_producers.iter().filter(|it| !block_producers.contains(it)).collect())
    }
}

impl EpochManagerAdapter for KeyValueRuntime {}

impl RuntimeAdapter for KeyValueRuntime {
    fn genesis_state(&self) -> (Store, Vec<StateRoot>) {
        (self.store.clone(), ((0..self.num_shards).map(|_| Trie::EMPTY_ROOT).collect()))
    }

    fn get_store(&self) -> Store {
        self.store.clone()
    }

    fn get_tries(&self) -> ShardTries {
        self.tries.clone()
    }

    fn get_trie_for_shard(
        &self,
        shard_id: ShardId,
        _block_hash: &CryptoHash,
        state_root: StateRoot,
    ) -> Result<Trie, Error> {
        Ok(self
            .tries
            .get_trie_for_shard(ShardUId { version: 0, shard_id: shard_id as u32 }, state_root))
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
        doomslug_threshold_mode: DoomslugThresholdMode,
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
        if !Doomslug::can_approved_block_be_produced(doomslug_threshold_mode, approvals, &stakes) {
            Err(Error::NotEnoughApprovals)
        } else {
            Ok(())
        }
    }

    fn get_epoch_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, Error> {
        let validators = self.get_block_producers(self.get_valset_for_epoch(epoch_id)?);
        Ok(validators.iter().map(|x| (x.clone(), false)).collect())
    }

    fn get_epoch_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<(ApprovalStake, bool)>, Error> {
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
    fn get_epoch_chunk_producers(&self, _epoch_id: &EpochId) -> Result<Vec<ValidatorStake>, Error> {
        tracing::warn!("not implemented, returning a dummy value");
        Ok(vec![])
    }

    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, Error> {
        let validators = self.get_block_producers(self.get_valset_for_epoch(epoch_id)?);
        Ok(validators[(height as usize) % validators.len()].account_id().clone())
    }

    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<AccountId, Error> {
        let valset = self.get_valset_for_epoch(epoch_id)?;
        let chunk_producers = self.get_chunk_producers(valset, shard_id);
        let index = (shard_id + height + 1) as usize % chunk_producers.len();
        Ok(chunk_producers[index].account_id().clone())
    }

    fn num_shards(&self, _epoch_id: &EpochId) -> Result<ShardId, Error> {
        Ok(self.num_shards)
    }

    fn get_shard_layout(&self, _epoch_id: &EpochId) -> Result<ShardLayout, Error> {
        Ok(ShardLayout::v0(self.num_shards, 0))
    }

    fn get_shard_config(&self, _epoch_id: &EpochId) -> Result<ShardConfig, Error> {
        panic!("get_shard_config not implemented for KeyValueRuntime");
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
    ) -> Result<ShardLayout, Error> {
        Ok(ShardLayout::v0(self.num_shards, 0))
    }

    fn shard_id_to_uid(&self, shard_id: ShardId, _epoch_id: &EpochId) -> Result<ShardUId, Error> {
        Ok(ShardUId { version: 0, shard_id: shard_id as u32 })
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

    fn account_id_to_shard_id(
        &self,
        account_id: &AccountId,
        _epoch_id: &EpochId,
    ) -> Result<ShardId, Error> {
        Ok(account_id_to_shard_id(account_id, self.num_shards))
    }

    fn get_part_owner(&self, epoch_id: &EpochId, part_id: u64) -> Result<AccountId, Error> {
        let validators =
            &self.get_epoch_block_producers_ordered(epoch_id, &CryptoHash::default())?;
        // if we don't use data_parts and total_parts as part of the formula here, the part owner
        //     would not depend on height, and tests wouldn't catch passing wrong height here
        let idx = part_id as usize + self.num_data_parts() + self.num_total_parts();
        Ok(validators[idx as usize % validators.len()].0.account_id().clone())
    }

    fn cares_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        _is_me: bool,
    ) -> bool {
        if self.tracks_all_shards {
            return true;
        }
        // This `unwrap` here tests that in all code paths we check that the epoch exists before
        //    we check if we care about a shard. Please do not remove the unwrap, fix the logic of
        //    the calling function.
        let epoch_valset = self.get_epoch_and_valset(*parent_hash).unwrap();
        let chunk_producers = self.get_chunk_producers(epoch_valset.1, shard_id);
        if let Some(account_id) = account_id {
            for validator in chunk_producers {
                if validator.account_id() == account_id {
                    return true;
                }
            }
        }
        false
    }

    fn will_care_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        _is_me: bool,
    ) -> bool {
        if self.tracks_all_shards {
            return true;
        }
        // This `unwrap` here tests that in all code paths we check that the epoch exists before
        //    we check if we care about a shard. Please do not remove the unwrap, fix the logic of
        //    the calling function.
        let epoch_valset = self.get_epoch_and_valset(*parent_hash).unwrap();
        let chunk_producers = self
            .get_chunk_producers((epoch_valset.1 + 1) % self.validators_by_valset.len(), shard_id);
        if let Some(account_id) = account_id {
            for validator in chunk_producers {
                if validator.account_id() == account_id {
                    return true;
                }
            }
        }
        false
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
    ) -> Result<(), Error> {
        Ok(())
    }

    fn add_validator_proposals(
        &self,
        _block_header_info: BlockHeaderInfo,
    ) -> Result<StoreUpdate, Error> {
        Ok(self.store.store_update())
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
    ) -> Result<ApplyTransactionResult, Error> {
        assert!(!generate_storage_proof);
        let mut tx_results = vec![];

        let mut state = self.state.read().unwrap().get(state_root).cloned().unwrap();

        let mut balance_transfers = vec![];

        for receipt in receipts.iter() {
            if let ReceiptEnum::Action(action) = &receipt.receipt {
                assert_eq!(
                    self.account_id_to_shard_id(&receipt.receiver_id, &EpochId::default())?,
                    shard_id
                );
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
                self.account_id_to_shard_id(
                    &transaction.transaction.signer_id,
                    &EpochId::default()
                )?,
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

            if self.account_id_to_shard_id(&from, &EpochId::default())? != shard_id {
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
                let new_receipt_hashes = if self.account_id_to_shard_id(&to, &EpochId::default())?
                    == shard_id
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

    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, Error> {
        if parent_hash == &CryptoHash::default() {
            return Ok(true);
        }
        let prev_block_header = self.get_block_header(parent_hash)?.ok_or_else(|| {
            Error::Other(format!("Missing block {} when computing the epoch", parent_hash))
        })?;
        let prev_prev_hash = *prev_block_header.prev_hash();
        Ok(self.get_epoch_and_valset(*parent_hash)?.0
            != self.get_epoch_and_valset(prev_prev_hash)?.0)
    }

    fn get_epoch_id_from_prev_block(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error> {
        Ok(self.get_epoch_and_valset(*parent_hash)?.0)
    }

    fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        Ok(self.get_epoch_and_valset(*parent_hash)?.2)
    }

    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockHeight, Error> {
        let epoch_id = self.get_epoch_and_valset(*block_hash)?.0;
        match self.get_block_header(&epoch_id.0)? {
            Some(block_header) => Ok(block_header.height()),
            None => Ok(0),
        }
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

    fn epoch_exists(&self, epoch_id: &EpochId) -> bool {
        self.hash_to_valset.write().unwrap().contains_key(epoch_id)
    }

    fn get_epoch_minted_amount(&self, _epoch_id: &EpochId) -> Result<Balance, Error> {
        Ok(0)
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
        Error,
    > {
        Ok(Default::default())
    }

    fn get_epoch_sync_data_hash(
        &self,
        _prev_epoch_last_block_hash: &CryptoHash,
        _epoch_id: &EpochId,
        _next_epoch_id: &EpochId,
    ) -> Result<CryptoHash, Error> {
        Ok(CryptoHash::default())
    }

    fn get_epoch_protocol_version(&self, _epoch_id: &EpochId) -> Result<ProtocolVersion, Error> {
        Ok(PROTOCOL_VERSION)
    }

    fn get_validator_info(
        &self,
        _epoch_id: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, Error> {
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

    fn compare_epoch_id(
        &self,
        epoch_id: &EpochId,
        other_epoch_id: &EpochId,
    ) -> Result<Ordering, Error> {
        if epoch_id.0 == other_epoch_id.0 {
            return Ok(Ordering::Equal);
        }
        match (self.get_valset_for_epoch(epoch_id), self.get_valset_for_epoch(other_epoch_id)) {
            (Ok(index1), Ok(index2)) => Ok(index1.cmp(&index2)),
            _ => Err(Error::EpochOutOfBounds(epoch_id.clone())),
        }
    }

    fn chunk_needs_to_be_fetched_from_archival(
        &self,
        _chunk_prev_block_hash: &CryptoHash,
        _header_head: &CryptoHash,
    ) -> Result<bool, Error> {
        Ok(false)
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

    fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error> {
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
        Err(Error::NotAValidator)
    }

    fn get_fisherman_by_account_id(
        &self,
        _epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
        _account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error> {
        Err(Error::NotAValidator)
    }

    fn get_protocol_config(&self, _epoch_id: &EpochId) -> Result<ProtocolConfig, Error> {
        unreachable!("get_protocol_config should not be called in KeyValueRuntime");
    }

    fn get_prev_epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        let mut candidate_hash = *prev_block_hash;
        loop {
            let header = self
                .get_block_header(&candidate_hash)?
                .ok_or_else(|| Error::DBNotFoundErr(candidate_hash.to_string()))?;
            candidate_hash = *header.prev_hash();
            if self.is_next_block_epoch_start(&candidate_hash)? {
                break Ok(self.get_epoch_and_valset(candidate_hash)?.0);
            }
        }
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

    fn build_state_for_split_shards(
        &self,
        _shard_uid: ShardUId,
        _state_root: &StateRoot,
        _next_epoch_shard_layout: &ShardLayout,
        _state_split_status: Arc<StateSplitApplyingStatus>,
    ) -> Result<HashMap<ShardUId, StateRoot>, Error> {
        Ok(HashMap::new())
    }

    fn get_protocol_upgrade_block_height(
        &self,
        _block_hash: CryptoHash,
    ) -> Result<Option<BlockHeight>, EpochError> {
        Ok(None)
    }

    fn get_epoch_height_from_prev_block(
        &self,
        _prev_block_hash: &CryptoHash,
    ) -> Result<EpochHeight, Error> {
        Ok(0)
    }
}

pub fn setup() -> (Chain, Arc<KeyValueRuntime>, Arc<InMemoryValidatorSigner>) {
    setup_with_tx_validity_period(100)
}

pub fn setup_with_tx_validity_period(
    tx_validity_period: NumBlocks,
) -> (Chain, Arc<KeyValueRuntime>, Arc<InMemoryValidatorSigner>) {
    let store = create_test_store();
    let epoch_length = 1000;
    let runtime = Arc::new(KeyValueRuntime::new(store, epoch_length));
    let chain = Chain::new(
        runtime.clone(),
        &ChainGenesis {
            time: Clock::utc(),
            height: 0,
            gas_limit: 1_000_000,
            min_gas_price: 100,
            max_gas_price: 1_000_000_000,
            total_supply: 1_000_000_000,
            gas_price_adjustment_rate: Ratio::from_integer(0),
            transaction_validity_period: tx_validity_period,
            epoch_length,
            protocol_version: PROTOCOL_VERSION,
        },
        DoomslugThresholdMode::NoApprovals,
        true,
    )
    .unwrap();
    let test_account = "test".parse::<AccountId>().unwrap();
    let signer = Arc::new(InMemoryValidatorSigner::from_seed(
        test_account.clone(),
        KeyType::ED25519,
        test_account.as_ref(),
    ));
    (chain, runtime, signer)
}

pub fn setup_with_validators(
    vs: ValidatorSchedule,
    epoch_length: u64,
    tx_validity_period: NumBlocks,
) -> (Chain, Arc<KeyValueRuntime>, Vec<Arc<InMemoryValidatorSigner>>) {
    let store = create_test_store();
    let signers = vs
        .all_block_producers()
        .map(|x| {
            Arc::new(InMemoryValidatorSigner::from_seed(x.clone(), KeyType::ED25519, x.as_ref()))
        })
        .collect();
    let runtime = Arc::new(KeyValueRuntime::new_with_validators(store, vs, epoch_length));
    let chain = Chain::new(
        runtime.clone(),
        &ChainGenesis {
            time: Clock::utc(),
            height: 0,
            gas_limit: 1_000_000,
            min_gas_price: 100,
            max_gas_price: 1_000_000_000,
            total_supply: 1_000_000_000,
            gas_price_adjustment_rate: Ratio::from_integer(0),
            transaction_validity_period: tx_validity_period,
            epoch_length,
            protocol_version: PROTOCOL_VERSION,
        },
        DoomslugThresholdMode::NoApprovals,
        true,
    )
    .unwrap();
    (chain, runtime, signers)
}

pub fn format_hash(hash: CryptoHash) -> String {
    let mut hash = hash.to_string();
    hash.truncate(6);
    hash
}

/// Displays chain from given store.
pub fn display_chain(me: &Option<AccountId>, chain: &mut Chain, tail: bool) {
    let runtime_adapter = chain.runtime_adapter();
    let chain_store = chain.mut_store();
    let head = chain_store.head().unwrap();
    debug!(
        "{:?} Chain head ({}): {} / {}",
        me,
        if tail { "tail" } else { "full" },
        head.height,
        head.last_block_hash
    );
    let mut headers = vec![];
    for (key, _) in chain_store.store().clone().iter(DBCol::BlockHeader).map(Result::unwrap) {
        let header = chain_store
            .get_block_header(&CryptoHash::try_from(key.as_ref()).unwrap())
            .unwrap()
            .clone();
        if !tail || header.height() + 10 > head.height {
            headers.push(header);
        }
    }
    headers.sort_by(|h_left, h_right| {
        if h_left.height() > h_right.height() {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    });
    for header in headers {
        if header.prev_hash() == &CryptoHash::default() {
            // Genesis block.
            debug!("{: >3} {}", header.height(), format_hash(*header.hash()));
        } else {
            let parent_header = chain_store.get_block_header(header.prev_hash()).unwrap().clone();
            let maybe_block = chain_store.get_block(header.hash()).ok();
            let epoch_id =
                runtime_adapter.get_epoch_id_from_prev_block(header.prev_hash()).unwrap();
            let block_producer =
                runtime_adapter.get_block_producer(&epoch_id, header.height()).unwrap();
            debug!(
                "{: >3} {} | {: >10} | parent: {: >3} {} | {}",
                header.height(),
                format_hash(*header.hash()),
                block_producer,
                parent_header.height(),
                format_hash(*parent_header.hash()),
                if let Some(block) = &maybe_block {
                    format!("chunks: {}", block.chunks().len())
                } else {
                    "-".to_string()
                }
            );
            if let Some(block) = maybe_block {
                for chunk_header in block.chunks().iter() {
                    let chunk_producer = runtime_adapter
                        .get_chunk_producer(
                            &epoch_id,
                            chunk_header.height_created(),
                            chunk_header.shard_id(),
                        )
                        .unwrap();
                    if let Ok(chunk) = chain_store.get_chunk(&chunk_header.chunk_hash()) {
                        debug!(
                            "    {: >3} {} | {} | {: >10} | tx = {: >2}, receipts = {: >2}",
                            chunk_header.height_created(),
                            format_hash(chunk_header.chunk_hash().0),
                            chunk_header.shard_id(),
                            chunk_producer,
                            chunk.transactions().len(),
                            chunk.receipts().len()
                        );
                    } else if let Ok(partial_chunk) =
                        chain_store.get_partial_chunk(&chunk_header.chunk_hash())
                    {
                        debug!(
                            "    {: >3} {} | {} | {: >10} | parts = {:?} receipts = {:?}",
                            chunk_header.height_created(),
                            format_hash(chunk_header.chunk_hash().0),
                            chunk_header.shard_id(),
                            chunk_producer,
                            partial_chunk.parts().iter().map(|x| x.part_ord).collect::<Vec<_>>(),
                            partial_chunk
                                .receipts()
                                .iter()
                                .map(|x| format!("{} => {}", x.0.len(), x.1.to_shard_id))
                                .collect::<Vec<_>>(),
                        );
                    }
                }
            }
        }
    }
}

impl ChainGenesis {
    pub fn test() -> Self {
        ChainGenesis {
            time: Clock::utc(),
            height: 0,
            gas_limit: 10u64.pow(15),
            min_gas_price: 0,
            max_gas_price: 1_000_000_000,
            total_supply: 1_000_000_000,
            gas_price_adjustment_rate: Ratio::from_integer(0),
            transaction_validity_period: 100,
            epoch_length: 5,
            protocol_version: PROTOCOL_VERSION,
        }
    }
}

#[cfg(test)]
mod test {
    use std::convert::TryFrom;

    use borsh::BorshSerialize;
    use rand::Rng;

    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::receipt::Receipt;
    use near_primitives::sharding::ReceiptList;
    use near_primitives::time::Clock;
    use near_primitives::types::{AccountId, NumShards};

    use crate::Chain;

    use near_primitives::shard_layout::{account_id_to_shard_id, ShardLayout};

    fn naive_build_receipt_hashes(
        receipts: &[Receipt],
        shard_layout: &ShardLayout,
    ) -> Vec<CryptoHash> {
        let mut receipts_hashes = vec![];
        for shard_id in 0..shard_layout.num_shards() {
            let shard_receipts: Vec<Receipt> = receipts
                .iter()
                .filter(|&receipt| {
                    account_id_to_shard_id(&receipt.receiver_id, shard_layout) == shard_id
                })
                .cloned()
                .collect();
            receipts_hashes
                .push(hash(&ReceiptList(shard_id, &shard_receipts).try_to_vec().unwrap()));
        }
        receipts_hashes
    }

    fn test_build_receipt_hashes_with_num_shard(num_shards: NumShards) {
        let shard_layout = ShardLayout::v0(num_shards, 0);
        let create_receipt_from_receiver_id =
            |receiver_id| Receipt::new_balance_refund(&receiver_id, 0);
        let mut rng = rand::thread_rng();
        let receipts = (0..3000)
            .map(|_| {
                let random_number = rng.gen_range(0, 1000);
                create_receipt_from_receiver_id(
                    AccountId::try_from(format!("test{}", random_number)).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let start = Clock::instant();
        let naive_result = naive_build_receipt_hashes(&receipts, &shard_layout);
        let naive_duration = start.elapsed();
        let start = Clock::instant();
        let prod_result = Chain::build_receipts_hashes(&receipts, &shard_layout);
        let prod_duration = start.elapsed();
        assert_eq!(naive_result, prod_result);
        // production implementation is at least 50% faster
        assert!(
            2 * naive_duration > 3 * prod_duration,
            "naive duration vs production {:?} {:?}",
            naive_duration,
            prod_duration
        );
    }

    #[test]
    #[ignore]
    /// Disabled, see more details in #5836
    fn test_build_receipt_hashes() {
        for num_shards in 1..10 {
            test_build_receipt_hashes_with_num_shard(num_shards);
        }
    }
}
