use log::info;
use near_chain::types::{ApplyTransactionResult, BlockHeaderInfo};
use near_chain::{
    Chain, ChainGenesis, ChainStore, ChainStoreAccess, DoomslugThresholdMode, Error, RuntimeAdapter,
};
use near_crypto::Signature;
use near_pool::types::PoolIterator;
use near_primitives::block_header::BlockHeader;
use near_primitives::challenge::SlashedValidator;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{
    ApprovalStake, Balance, BlockHeight, EpochId, Gas, NumBlocks, ShardId, StateRootNode,
    ValidatorStake,
};
use near_primitives::views::{EpochValidatorInfo, QueryRequest, QueryResponse};
use near_store::{PartialStorage, ShardTries, Store, StoreUpdate, Trie};
use neard::{NearConfig, NightshadeRuntime};
use std::cmp::Ordering;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct GCRuntime {
    runtime: NightshadeRuntime,
    latest_height: Arc<Mutex<BlockHeight>>,
}

impl GCRuntime {
    pub fn new(runtime: NightshadeRuntime, latest_height: BlockHeight) -> Self {
        Self { runtime, latest_height: Arc::new(Mutex::new(latest_height)) }
    }
}

impl RuntimeAdapter for GCRuntime {
    fn genesis_state(&self) -> (Arc<Store>, StoreUpdate, Vec<CryptoHash>) {
        self.runtime.genesis_state()
    }

    fn get_tries(&self) -> ShardTries {
        self.runtime.get_tries()
    }

    fn get_trie_for_shard(&self, shard_id: u64) -> Arc<Trie> {
        self.runtime.get_trie_for_shard(shard_id)
    }

    fn verify_block_signature(&self, header: &BlockHeader) -> Result<(), Error> {
        self.runtime.verify_block_signature(header)
    }

    fn verify_block_vrf(
        &self,
        epoch_id: &EpochId,
        block_height: u64,
        prev_random_value: &CryptoHash,
        vrf_value: &near_crypto::vrf::Value,
        vrf_proof: &near_crypto::vrf::Proof,
    ) -> Result<(), Error> {
        self.runtime.verify_block_vrf(
            epoch_id,
            block_height,
            prev_random_value,
            vrf_value,
            vrf_proof,
        )
    }

    fn validate_tx(
        &self,
        gas_price: u128,
        state_root: Option<CryptoHash>,
        transaction: &SignedTransaction,
    ) -> Result<Option<InvalidTxError>, Error> {
        self.runtime.validate_tx(gas_price, state_root, transaction)
    }

    fn prepare_transactions(
        &self,
        gas_price: u128,
        gas_limit: u64,
        shard_id: u64,
        state_root: CryptoHash,
        max_number_of_transactions: usize,
        pool_iterator: &mut dyn PoolIterator,
        chain_validate: &mut dyn FnMut(&SignedTransaction) -> bool,
    ) -> Result<Vec<SignedTransaction>, Error> {
        self.runtime.prepare_transactions(
            gas_price,
            gas_limit,
            shard_id,
            state_root,
            max_number_of_transactions,
            pool_iterator,
            chain_validate,
        )
    }

    fn verify_validator_signature(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &String,
        data: &[u8],
        signature: &Signature,
    ) -> Result<bool, Error> {
        self.runtime.verify_validator_signature(
            epoch_id,
            last_known_block_hash,
            account_id,
            data,
            signature,
        )
    }

    fn verify_validator_or_fisherman_signature(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &String,
        data: &[u8],
        signature: &Signature,
    ) -> Result<bool, Error> {
        self.runtime.verify_validator_or_fisherman_signature(
            epoch_id,
            last_known_block_hash,
            account_id,
            data,
            signature,
        )
    }

    fn verify_header_signature(&self, header: &BlockHeader) -> Result<bool, Error> {
        self.runtime.verify_header_signature(header)
    }

    fn verify_chunk_header_signature(&self, header: &ShardChunkHeader) -> Result<bool, Error> {
        self.runtime.verify_chunk_header_signature(header)
    }

    fn verify_approval(
        &self,
        prev_block_hash: &CryptoHash,
        prev_block_height: u64,
        block_height: u64,
        approvals: &[Option<Signature>],
    ) -> Result<bool, Error> {
        self.runtime.verify_approval(prev_block_hash, prev_block_height, block_height, approvals)
    }

    fn get_epoch_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, Error> {
        self.runtime.get_epoch_block_producers_ordered(epoch_id, last_known_block_hash)
    }

    fn get_epoch_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<ApprovalStake>, Error> {
        self.runtime.get_epoch_block_approvers_ordered(parent_hash)
    }

    fn get_block_producer(&self, epoch_id: &EpochId, height: u64) -> Result<String, Error> {
        self.runtime.get_block_producer(epoch_id, height)
    }

    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: u64,
        shard_id: u64,
    ) -> Result<String, Error> {
        self.runtime.get_chunk_producer(epoch_id, height, shard_id)
    }

    fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &String,
    ) -> Result<(ValidatorStake, bool), Error> {
        self.runtime.get_validator_by_account_id(epoch_id, last_known_block_hash, account_id)
    }

    fn get_fisherman_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &String,
    ) -> Result<(ValidatorStake, bool), Error> {
        self.runtime.get_fisherman_by_account_id(epoch_id, last_known_block_hash, account_id)
    }

    fn num_shards(&self) -> u64 {
        self.runtime.num_shards()
    }

    fn num_total_parts(&self) -> usize {
        self.runtime.num_total_parts()
    }

    fn num_data_parts(&self) -> usize {
        self.runtime.num_data_parts()
    }

    fn account_id_to_shard_id(&self, account_id: &String) -> u64 {
        self.runtime.account_id_to_shard_id(account_id)
    }

    fn get_part_owner(&self, parent_hash: &CryptoHash, part_id: u64) -> Result<String, Error> {
        self.runtime.get_part_owner(parent_hash, part_id)
    }

    fn cares_about_shard(
        &self,
        account_id: Option<&String>,
        parent_hash: &CryptoHash,
        shard_id: u64,
        is_me: bool,
    ) -> bool {
        self.runtime.cares_about_shard(account_id, parent_hash, shard_id, is_me)
    }

    fn will_care_about_shard(
        &self,
        account_id: Option<&String>,
        parent_hash: &CryptoHash,
        shard_id: u64,
        is_me: bool,
    ) -> bool {
        self.runtime.will_care_about_shard(account_id, parent_hash, shard_id, is_me)
    }

    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, Error> {
        self.runtime.is_next_block_epoch_start(parent_hash)
    }

    fn get_epoch_id_from_prev_block(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error> {
        self.runtime.get_epoch_id_from_prev_block(parent_hash)
    }

    fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        self.runtime.get_next_epoch_id_from_prev_block(parent_hash)
    }

    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<u64, Error> {
        self.runtime.get_epoch_start_height(block_hash)
    }

    fn get_gc_stop_height(&self, _block_hash: &CryptoHash) -> Result<u64, Error> {
        Ok(*self.latest_height.lock().unwrap())
    }

    fn epoch_exists(&self, epoch_id: &EpochId) -> bool {
        self.runtime.epoch_exists(epoch_id)
    }

    fn get_epoch_minted_amount(&self, epoch_id: &EpochId) -> Result<u128, Error> {
        self.runtime.get_epoch_minted_amount(epoch_id)
    }

    fn get_epoch_protocol_version(&self, epoch_id: &EpochId) -> Result<u32, Error> {
        self.runtime.get_epoch_protocol_version(epoch_id)
    }

    fn add_validator_proposals(&self, block_header_info: BlockHeaderInfo) -> Result<(), Error> {
        *self.latest_height.lock().unwrap() = block_header_info.height;
        self.runtime.add_validator_proposals(block_header_info)
    }

    fn apply_transactions_with_optional_storage_proof(
        &self,
        shard_id: ShardId,
        state_root: &CryptoHash,
        height: BlockHeight,
        block_timestamp: u64,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        receipts: &[Receipt],
        transactions: &[SignedTransaction],
        last_validator_proposals: &[ValidatorStake],
        gas_price: Balance,
        gas_limit: Gas,
        challenges_result: &Vec<SlashedValidator>,
        generate_storage_proof: bool,
    ) -> Result<ApplyTransactionResult, Error> {
        self.runtime.apply_transactions_with_optional_storage_proof(
            shard_id,
            state_root,
            height,
            block_timestamp,
            prev_block_hash,
            block_hash,
            receipts,
            transactions,
            last_validator_proposals,
            gas_price,
            gas_limit,
            challenges_result,
            generate_storage_proof,
        )
    }

    fn check_state_transition(
        &self,
        partial_storage: PartialStorage,
        shard_id: ShardId,
        state_root: &CryptoHash,
        height: BlockHeight,
        block_timestamp: u64,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        receipts: &[Receipt],
        transactions: &[SignedTransaction],
        last_validator_proposals: &[ValidatorStake],
        gas_price: Balance,
        gas_limit: Gas,
        challenges_result: &Vec<SlashedValidator>,
    ) -> Result<ApplyTransactionResult, Error> {
        self.runtime.check_state_transition(
            partial_storage,
            shard_id,
            state_root,
            height,
            block_timestamp,
            prev_block_hash,
            block_hash,
            receipts,
            transactions,
            last_validator_proposals,
            gas_price,
            gas_limit,
            challenges_result,
        )
    }

    fn query(
        &self,
        shard_id: ShardId,
        state_root: &CryptoHash,
        block_height: BlockHeight,
        block_timestamp: u64,
        block_hash: &CryptoHash,
        epoch_id: &EpochId,
        request: &QueryRequest,
    ) -> Result<QueryResponse, Box<dyn std::error::Error>> {
        self.runtime.query(
            shard_id,
            state_root,
            block_height,
            block_timestamp,
            block_hash,
            epoch_id,
            request,
        )
    }

    fn get_validator_info(&self, block_hash: &CryptoHash) -> Result<EpochValidatorInfo, Error> {
        self.runtime.get_validator_info(block_hash)
    }

    fn obtain_state_part(
        &self,
        shard_id: u64,
        state_root: &CryptoHash,
        part_id: u64,
        num_parts: u64,
    ) -> Vec<u8> {
        self.runtime.obtain_state_part(shard_id, state_root, part_id, num_parts)
    }

    fn validate_state_part(
        &self,
        state_root: &CryptoHash,
        part_id: u64,
        num_parts: u64,
        data: &Vec<u8>,
    ) -> bool {
        self.runtime.validate_state_part(state_root, part_id, num_parts, data)
    }

    fn confirm_state(
        &self,
        shard_id: u64,
        state_root: &CryptoHash,
        parts: &Vec<Vec<u8>>,
    ) -> Result<(), Error> {
        self.runtime.confirm_state(shard_id, state_root, parts)
    }

    fn get_state_root_node(&self, shard_id: u64, state_root: &CryptoHash) -> StateRootNode {
        self.runtime.get_state_root_node(shard_id, state_root)
    }

    fn validate_state_root_node(
        &self,
        state_root_node: &StateRootNode,
        state_root: &CryptoHash,
    ) -> bool {
        self.runtime.validate_state_root_node(state_root_node, state_root)
    }

    fn compare_epoch_id(
        &self,
        epoch_id: &EpochId,
        other_epoch_id: &EpochId,
    ) -> Result<Ordering, Error> {
        self.runtime.compare_epoch_id(epoch_id, other_epoch_id)
    }
}

pub fn garbage_collect(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
    num_blocks_to_gc: NumBlocks,
) {
    let chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);
    let latest_height = chain_store.head().unwrap().height;
    let runtime = NightshadeRuntime::new(
        &home_dir,
        store,
        Arc::clone(&near_config.genesis),
        near_config.client_config.tracked_accounts.clone(),
        near_config.client_config.tracked_shards.clone(),
    );
    let gc_runtime = GCRuntime::new(runtime, latest_height);
    let runtime_adapter: Arc<dyn RuntimeAdapter> = Arc::new(gc_runtime);
    let mut chain = Chain::new(
        runtime_adapter.clone(),
        &ChainGenesis::from(&near_config.genesis),
        DoomslugThresholdMode::NoApprovals,
    )
    .unwrap();
    info!("garbage collection start");
    chain.clear_data(runtime_adapter.get_tries(), num_blocks_to_gc).unwrap();
    info!("garbage collection end");
}
