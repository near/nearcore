use crate::{SealsManager, NUM_PARTS_REQUESTED_IN_SEAL};

use near_chain::types::{ApplyTransactionResult, RuntimeAdapter};
use near_chain::Error;
use near_crypto::vrf::{Proof, Value};
use near_crypto::Signature;

use near_pool::types::PoolIterator;
use near_primitives::block::BlockHeader;
use near_primitives::challenge::{ChallengesResult, SlashedValidator};
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::BlockHeight;
use near_primitives::types::{
    AccountId, ApprovalStake, EpochId, ShardId, StateRoot, StateRootNode, ValidatorStake,
    ValidatorStats,
};
use near_primitives::views::{EpochValidatorInfo, QueryRequest, QueryResponse};

use near_store::{PartialStorage, ShardTries, Store, StoreUpdate, Trie};
use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

pub struct MockRuntimeAdapter {
    pub chunk_producer: AccountId,
    pub get_chunk_producer_call_count: Arc<Mutex<u8>>,
    pub epoch_id: EpochId,
    pub num_total_parts: usize,
    pub part_owner: AccountId,
    pub gc_stop_height: BlockHeight,
}
impl RuntimeAdapter for MockRuntimeAdapter {
    fn get_chunk_producer(
        &self,
        _epoch_id: &EpochId,
        _height: u64,
        _shard_id: u64,
    ) -> Result<AccountId, Error> {
        let mut count = self.get_chunk_producer_call_count.lock().unwrap();
        *count += 1;
        Ok(self.chunk_producer.clone())
    }

    fn get_epoch_id_from_prev_block(&self, _parent_hash: &CryptoHash) -> Result<EpochId, Error> {
        Ok(self.epoch_id.clone())
    }

    fn num_total_parts(&self) -> usize {
        self.num_total_parts
    }

    fn get_part_owner(&self, _parent_hash: &CryptoHash, _part_id: u64) -> Result<AccountId, Error> {
        Ok(self.part_owner.clone())
    }

    fn get_gc_stop_height(&self, _block_hash: &CryptoHash) -> Result<u64, Error> {
        Ok(self.gc_stop_height)
    }

    fn genesis_state(&self) -> (Arc<Store>, StoreUpdate, Vec<StateRoot>) {
        unimplemented!()
    }

    fn get_tries(&self) -> ShardTries {
        unimplemented!()
    }

    fn get_trie_for_shard(&self, _shard_id: u64) -> Arc<Trie> {
        unimplemented!()
    }

    fn verify_block_signature(&self, _header: &BlockHeader) -> Result<(), Error> {
        unimplemented!()
    }

    fn verify_block_vrf(
        &self,
        _epoch_id: &EpochId,
        _block_height: u64,
        _prev_random_value: &CryptoHash,
        _vrf_value: Value,
        _vrf_proof: Proof,
    ) -> Result<(), Error> {
        unimplemented!()
    }

    fn validate_tx(
        &self,
        _gas_price: u128,
        _state_root: StateRoot,
        _transaction: &SignedTransaction,
    ) -> Result<Option<InvalidTxError>, Error> {
        unimplemented!()
    }

    fn prepare_transactions(
        &self,
        _gas_price: u128,
        _gas_limit: u64,
        _shard_id: u64,
        _state_root: StateRoot,
        _max_number_of_transactions: usize,
        _pool_iterator: &mut dyn PoolIterator,
        _chain_validate: &mut dyn FnMut(&SignedTransaction) -> bool,
    ) -> Result<Vec<SignedTransaction>, Error> {
        unimplemented!()
    }

    fn verify_validator_signature(
        &self,
        _epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
        _account_id: &AccountId,
        _data: &[u8],
        _signature: &Signature,
    ) -> Result<bool, Error> {
        unimplemented!()
    }

    fn verify_validator_or_fisherman_signature(
        &self,
        _epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
        _account_id: &AccountId,
        _data: &[u8],
        _signature: &Signature,
    ) -> Result<bool, Error> {
        unimplemented!()
    }

    fn verify_header_signature(&self, _header: &BlockHeader) -> Result<bool, Error> {
        unimplemented!()
    }

    fn verify_chunk_header_signature(&self, _header: &ShardChunkHeader) -> Result<bool, Error> {
        unimplemented!()
    }

    fn verify_approval(
        &self,
        _prev_block_hash: &CryptoHash,
        _prev_block_height: u64,
        _block_height: u64,
        _approvals: &[Option<Signature>],
    ) -> Result<bool, Error> {
        unimplemented!()
    }

    fn get_epoch_block_producers_ordered(
        &self,
        _epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, Error> {
        unimplemented!()
    }

    fn get_epoch_block_approvers_ordered(
        &self,
        _parent_hash: &CryptoHash,
    ) -> Result<Vec<ApprovalStake>, Error> {
        unimplemented!()
    }

    fn get_block_producer(&self, _epoch_id: &EpochId, _height: u64) -> Result<AccountId, Error> {
        unimplemented!()
    }

    fn get_validator_by_account_id(
        &self,
        _epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
        _account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error> {
        unimplemented!()
    }

    fn get_fisherman_by_account_id(
        &self,
        _epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
        _account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error> {
        unimplemented!()
    }

    fn get_num_validator_blocks(
        &self,
        _epoch_id: &EpochId,
        _last_known_block_hash: &CryptoHash,
        _account_id: &AccountId,
    ) -> Result<ValidatorStats, Error> {
        unimplemented!()
    }

    fn num_shards(&self) -> u64 {
        unimplemented!()
    }

    fn num_data_parts(&self) -> usize {
        unimplemented!()
    }

    fn account_id_to_shard_id(&self, _account_id: &AccountId) -> u64 {
        unimplemented!()
    }

    fn cares_about_shard(
        &self,
        _account_id: Option<&AccountId>,
        _parent_hash: &CryptoHash,
        _shard_id: u64,
        _is_me: bool,
    ) -> bool {
        unimplemented!()
    }

    fn will_care_about_shard(
        &self,
        _account_id: Option<&AccountId>,
        _parent_hash: &CryptoHash,
        _shard_id: u64,
        _is_me: bool,
    ) -> bool {
        unimplemented!()
    }

    fn is_next_block_epoch_start(&self, _parent_hash: &CryptoHash) -> Result<bool, Error> {
        unimplemented!()
    }

    fn get_next_epoch_id_from_prev_block(
        &self,
        _parent_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        unimplemented!()
    }

    fn get_epoch_start_height(&self, _block_hash: &CryptoHash) -> Result<u64, Error> {
        unimplemented!()
    }

    fn epoch_exists(&self, _epoch_id: &EpochId) -> bool {
        unimplemented!()
    }

    fn get_epoch_minted_amount(&self, _epoch_id: &EpochId) -> Result<u128, Error> {
        unimplemented!()
    }

    fn add_validator_proposals(
        &self,
        _parent_hash: CryptoHash,
        _current_hash: CryptoHash,
        _rng_seed: CryptoHash,
        _height: u64,
        _last_finalized_height: u64,
        _proposals: Vec<ValidatorStake>,
        _slashed_validators: Vec<SlashedValidator>,
        _validator_mask: Vec<bool>,
        _total_supply: u128,
    ) -> Result<(), Error> {
        unimplemented!()
    }

    fn apply_transactions_with_optional_storage_proof(
        &self,
        _shard_id: u64,
        _state_root: &StateRoot,
        _height: u64,
        _block_timestamp: u64,
        _prev_block_hash: &CryptoHash,
        _block_hash: &CryptoHash,
        _receipts: &[Receipt],
        _transactions: &[SignedTransaction],
        _last_validator_proposals: &[ValidatorStake],
        _gas_price: u128,
        _gas_limit: u64,
        _challenges_result: &ChallengesResult,
        _generate_storage_proof: bool,
    ) -> Result<ApplyTransactionResult, Error> {
        unimplemented!()
    }

    fn check_state_transition(
        &self,
        _partial_storage: PartialStorage,
        _shard_id: u64,
        _state_root: &StateRoot,
        _height: u64,
        _block_timestamp: u64,
        _prev_block_hash: &CryptoHash,
        _block_hash: &CryptoHash,
        _receipts: &[Receipt],
        _transactions: &[SignedTransaction],
        _last_validator_proposals: &[ValidatorStake],
        _gas_price: u128,
        _gas_limit: u64,
        _challenges_result: &ChallengesResult,
    ) -> Result<ApplyTransactionResult, Error> {
        unimplemented!()
    }

    fn query(
        &self,
        _shard_id: u64,
        _state_root: &StateRoot,
        _block_height: u64,
        _block_timestamp: u64,
        _block_hash: &CryptoHash,
        _epoch_id: &EpochId,
        _request: &QueryRequest,
    ) -> Result<QueryResponse, Box<dyn std::error::Error>> {
        unimplemented!()
    }

    fn get_validator_info(&self, _block_hash: &CryptoHash) -> Result<EpochValidatorInfo, Error> {
        unimplemented!()
    }

    fn obtain_state_part(
        &self,
        _shard_id: u64,
        _state_root: &StateRoot,
        _part_id: u64,
        _num_parts: u64,
    ) -> Vec<u8> {
        unimplemented!()
    }

    fn validate_state_part(
        &self,
        _state_root: &StateRoot,
        _part_id: u64,
        _num_parts: u64,
        _data: &Vec<u8>,
    ) -> bool {
        unimplemented!()
    }

    fn confirm_state(
        &self,
        _shard_id: u64,
        _state_root: &StateRoot,
        _parts: &Vec<Vec<u8>>,
    ) -> Result<(), Error> {
        unimplemented!()
    }

    fn get_state_root_node(&self, _shard_id: u64, _state_root: &StateRoot) -> StateRootNode {
        unimplemented!()
    }

    fn validate_state_root_node(
        &self,
        _state_root_node: &StateRootNode,
        _state_root: &StateRoot,
    ) -> bool {
        unimplemented!()
    }

    fn compare_epoch_id(
        &self,
        _epoch_id: &EpochId,
        _other_epoch_id: &EpochId,
    ) -> Result<Ordering, Error> {
        unimplemented!()
    }
}

pub struct SealsManagerTestFixture {
    pub mock_chunk_producer: AccountId,
    pub mock_epoch_id: EpochId,
    pub mock_total_parts: usize,
    pub mock_part_owner: AccountId,
    pub mock_gc_height: BlockHeight,
    pub mock_me: Option<AccountId>,
    pub mock_shard_id: ShardId,
    pub mock_chunk_hash: ChunkHash,
    pub mock_parent_hash: CryptoHash,
    pub mock_height: BlockHeight,
}

impl Default for SealsManagerTestFixture {
    fn default() -> Self {
        let mock_chunk_producer: AccountId = "chunk_producer".to_string();
        let mock_epoch_id: EpochId = EpochId(CryptoHash::default());
        let mock_total_parts: usize = NUM_PARTS_REQUESTED_IN_SEAL * 3;
        let mock_part_owner: AccountId = "part_owner".to_string();
        let mock_gc_height: BlockHeight = 50;

        let mock_me: Option<AccountId> = Some("me".to_string());
        let mock_shard_id: ShardId = 1;
        let mock_chunk_hash = ChunkHash(CryptoHash::default());
        let mock_parent_hash = CryptoHash::default();
        let mock_height = 2 * mock_gc_height;

        Self {
            mock_chunk_producer,
            mock_epoch_id,
            mock_total_parts,
            mock_part_owner,
            mock_gc_height,
            mock_me,
            mock_shard_id,
            mock_chunk_hash,
            mock_parent_hash,
            mock_height,
        }
    }
}

impl SealsManagerTestFixture {
    pub fn create_seals_manager(&self) -> (Arc<MockRuntimeAdapter>, SealsManager) {
        let mock_runtime = MockRuntimeAdapter {
            chunk_producer: self.mock_chunk_producer.clone(),
            get_chunk_producer_call_count: Arc::new(Mutex::new(0)),
            epoch_id: self.mock_epoch_id.clone(),
            num_total_parts: self.mock_total_parts,
            part_owner: self.mock_part_owner.clone(),
            gc_stop_height: self.mock_gc_height,
        };
        let runtime = Arc::new(mock_runtime);

        (runtime.clone(), SealsManager::new(self.mock_me.clone(), runtime))
    }
}
