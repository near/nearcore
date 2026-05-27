use crate::spice::chunk_validator_actor::send_spice_chunk_endorsement;
use crate::spice::data_distributor_actor::SpiceDataDistributorAdapter;
use crate::spice::data_distributor_actor::SpiceDistributorStateWitness;
use near_async::messaging::CanSend;
use near_async::messaging::IntoSender;
use near_async::messaging::Sender;
use near_chain::BlockHeader;
use near_chain::types::ApplyChunkResult;
use near_chain::{Block, Error, get_chunk_clone_from_header};
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::PeerManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ReceiptProof;
use near_primitives::sharding::ShardProof;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::spice::state_witness::SpiceChunkStateTransition;
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::spice::state_witness::compute_contract_accesses_hash;
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::{CodeHash, ContractUpdates};
use near_primitives::types::BlockHeight;
use near_primitives::types::ChunkExecutionResult;
use near_primitives::types::ChunkExecutionResultHash;
use near_primitives::types::NumBlocks;
use near_primitives::types::SpiceChunkId;
use near_primitives::types::{Gas, ShardId};
use near_primitives::utils::get_contract_accesses_key;
use near_primitives::utils::get_receipt_proof_key;
use near_primitives::utils::get_receipt_proof_target_shard_prefix;
use near_primitives::utils::get_witnesses_key;
use near_primitives::validator_signer::ValidatorSigner;
use near_store::DBCol;
use near_store::Store;
use near_store::StoreUpdate;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

/// Map a `DBNotFoundErr` to `None` and any other error through, so a "read that
/// may be absent" reads as `Result<Option<T>>` and composes with `?`. Lets the
/// executor/coordinator drop the repetitive `match … Err(DBNotFoundErr) => …` ladders.
pub(crate) fn optional<T>(res: Result<T, Error>) -> Result<Option<T>, Error> {
    match res {
        Ok(value) => Ok(Some(value)),
        Err(Error::DBNotFoundErr(_)) => Ok(None),
        Err(err) => Err(err),
    }
}

/// Scalar config a per-shard executor needs. The `save_*` flags are persistence
/// feature flags from the client config (they gate optional column writes backing
/// RPC / GC features); `transaction_validity_period` comes from the genesis config
/// and bounds how stale a transaction's `block_hash` may be.
#[derive(Clone, Debug)]
pub struct ChunkExecutorConfig {
    pub save_trie_changes: bool,
    pub save_tx_outcomes: bool,
    pub save_receipt_to_tx: bool,
    pub save_state_changes: bool,
    pub transaction_validity_period: NumBlocks,
}

/// Message with incoming unverified receipts corresponding to the block. Sent by
/// `SpiceDataDistributorActor` to the chunk-executor coordinator, which routes it
/// to the destination shard's `PerShardExecutor` for verification.
#[derive(Debug, PartialEq)]
pub struct ExecutorIncomingUnverifiedReceipts {
    pub block_hash: CryptoHash,
    pub receipt_proof: ReceiptProof,
}

pub(crate) fn new_execution_result(
    gas_limit: Gas,
    apply_result: &ApplyChunkResult,
    outgoing_receipts_root: CryptoHash,
) -> ChunkExecutionResult {
    let chunk_extra = apply_result.to_chunk_extra(gas_limit);
    ChunkExecutionResult { chunk_extra, outgoing_receipts_root }
}

/// Returns the chunk if it is a new, valid chunk. Returns `None` if the chunk is
/// not new, or if the chunk is invalid (malicious chunk producer).
pub(crate) fn get_new_chunk_if_valid(
    chain_store: &ChainStoreAdapter,
    chunk_header: &ShardChunkHeader,
    height: BlockHeight,
) -> Result<Option<ShardChunk>, Error> {
    if !chunk_header.is_new_chunk(height) {
        return Ok(None);
    }
    match get_chunk_clone_from_header(&chain_store.chunk_store(), chunk_header) {
        Ok(chunk) => Ok(Some(chunk)),
        Err(Error::ChunkMissing(_))
            if chain_store.chunk_store().is_invalid_chunk(chunk_header.chunk_hash()).is_some() =>
        {
            Ok(None)
        }
        Err(err) => Err(err),
    }
}

/// Builds the state witness + contract accesses for a shard's chunk.
pub(crate) fn create_chunk_execution_data(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
    apply_result: &ApplyChunkResult,
    shard_id: ShardId,
    execution_result_hash: ChunkExecutionResultHash,
) -> Result<(SpiceChunkStateWitness, HashSet<CodeHash>), Error> {
    let block_hash = block.header().hash();
    let epoch_id = epoch_manager.get_epoch_id(block_hash)?;
    let (transactions, proof_of_invalid_chunk) = {
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
        let shard_index = shard_layout.get_shard_index(shard_id)?;
        let chunk_headers = block.chunks();
        let chunk_header = chunk_headers.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?;
        match get_new_chunk_if_valid(chain_store, chunk_header, block.header().height())? {
            Some(chunk) => (chunk.into_transactions(), None),
            None => {
                let proof = if chunk_header.is_new_chunk(block.header().height()) {
                    // Chunk is new but invalid (malicious producer): include proof.
                    chain_store
                        .chunk_store()
                        .is_invalid_chunk(chunk_header.chunk_hash())
                        .map(|enc| Box::new(enc.content().clone()))
                } else {
                    None
                };
                (vec![], proof)
            }
        }
    };

    let applied_receipts_hash = apply_result.applied_receipts_hash;
    let ContractUpdates { contract_accesses, contract_deploys: _ } =
        apply_result.contract_updates.clone();

    let PartialState::TrieValues(base_state_values) = apply_result.proof.clone().unwrap().nodes;
    // Contract bytecodes are not included in the witness. They are sent separately
    // via SpiceChunkContractAccesses so validators can check their compiled
    // contract cache and only request missing ones.
    let main_transition = SpiceChunkStateTransition {
        base_state: PartialState::TrieValues(base_state_values),
        post_state_root: apply_result.new_root,
    };

    let source_receipt_proofs: HashMap<ShardId, ReceiptProof> = {
        let prev_block_hash = block.header().prev_hash();
        let (_, prev_block_shard_id, _) =
            epoch_manager.get_prev_shard_id_from_prev_hash(prev_block_hash, shard_id)?;
        get_receipt_proofs_for_shard(&chain_store.store(), prev_block_hash, prev_block_shard_id)
            .into_iter()
            .map(|proof| (proof.1.from_shard_id, proof))
            .collect()
    };

    // TODO(spice-resharding): Handle witness validation when resharding.
    let contract_accesses_hash = compute_contract_accesses_hash(&contract_accesses);
    let state_witness = SpiceChunkStateWitness::new(
        SpiceChunkId { block_hash: *block_hash, shard_id },
        main_transition,
        source_receipt_proofs,
        applied_receipts_hash,
        transactions,
        execution_result_hash,
        contract_accesses_hash,
        proof_of_invalid_chunk,
    );
    Ok((state_witness, contract_accesses))
}

/// Builds and durably saves this shard's state witness + contract accesses, then
/// hands them to the data distributor.
#[allow(clippy::too_many_arguments)]
pub(crate) fn distribute_witness(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    data_distributor_adapter: &SpiceDataDistributorAdapter,
    block: &Block,
    apply_result: &ApplyChunkResult,
    gas_limit: Gas,
    shard_id: ShardId,
    outgoing_receipts_root: CryptoHash,
) -> Result<(), Error> {
    let execution_result = new_execution_result(gas_limit, apply_result, outgoing_receipts_root);
    let execution_result_hash = execution_result.compute_hash();
    let (state_witness, contract_accesses) = create_chunk_execution_data(
        chain_store,
        epoch_manager,
        block,
        apply_result,
        shard_id,
        execution_result_hash,
    )?;
    save_witness_and_contract_accesses(
        chain_store,
        block.hash(),
        shard_id,
        &state_witness,
        &contract_accesses,
    );
    data_distributor_adapter
        .send(SpiceDistributorStateWitness { state_witness, contract_accesses });
    Ok(())
}

/// Builds this shard's chunk endorsement and sends it to peers + the core writer.
#[allow(clippy::too_many_arguments)]
pub(crate) fn send_chunk_endorsement(
    epoch_manager: &dyn EpochManagerAdapter,
    network_adapter: &PeerManagerAdapter,
    core_writer_sender: &Sender<SpiceChunkEndorsementMessage>,
    my_signer: &ValidatorSigner,
    block: &Block,
    apply_result: &ApplyChunkResult,
    gas_limit: Gas,
    shard_id: ShardId,
    outgoing_receipts_root: CryptoHash,
) {
    let execution_result = new_execution_result(gas_limit, apply_result, outgoing_receipts_root);
    let endorsement = SpiceChunkEndorsement::new(
        SpiceChunkId { block_hash: *block.hash(), shard_id },
        execution_result,
        my_signer,
    );
    send_spice_chunk_endorsement(
        endorsement.clone(),
        epoch_manager,
        &network_adapter.clone().into_sender(),
        my_signer,
    );
    core_writer_sender.send(SpiceChunkEndorsementMessage(endorsement));
}

// We depend on stored receipts for distribution, so we need to store receipt proof and not only
// Vec<Receipt>.
pub(crate) fn save_receipt_proof(
    store_update: &mut StoreUpdate,
    block_hash: &CryptoHash,
    receipt_proof: &ReceiptProof,
) {
    let &ReceiptProof(_, ShardProof { from_shard_id, to_shard_id, .. }) = receipt_proof;
    let key = get_receipt_proof_key(block_hash, from_shard_id, to_shard_id);
    let value = borsh::to_vec(&receipt_proof).unwrap();
    store_update.set(DBCol::receipt_proofs(), &key, &value);
}

fn set_witness(
    store_update: &mut StoreUpdate,
    block_hash: &CryptoHash,
    shard_id: ShardId,
    witness: &SpiceChunkStateWitness,
) {
    let key = get_witnesses_key(block_hash, shard_id);
    let value = borsh::to_vec(&witness).unwrap();
    store_update.set(DBCol::witnesses(), &key, &value);
}

pub(crate) fn get_receipt_proofs_for_shard(
    store: &Store,
    block_hash: &CryptoHash,
    to_shard_id: ShardId,
) -> Vec<ReceiptProof> {
    let prefix = get_receipt_proof_target_shard_prefix(block_hash, to_shard_id);
    store.iter_prefix_ser::<ReceiptProof>(DBCol::receipt_proofs(), &prefix).map(|kv| kv.1).collect()
}

pub fn get_witness(
    store: &Store,
    block_hash: &CryptoHash,
    shard_id: ShardId,
) -> Option<SpiceChunkStateWitness> {
    let key = get_witnesses_key(block_hash, shard_id);
    store.get_ser(DBCol::witnesses(), &key)
}

fn set_contract_accesses(
    store_update: &mut StoreUpdate,
    block_hash: &CryptoHash,
    shard_id: ShardId,
    contract_accesses: &HashSet<CodeHash>,
) {
    let key = get_contract_accesses_key(block_hash, shard_id);
    let value: Vec<CodeHash> = contract_accesses.iter().cloned().collect();
    let value = borsh::to_vec(&value).unwrap();
    store_update.set(DBCol::contract_accesses(), &key, &value);
}

/// Saves witness and contract accesses atomically in a single DB transaction.
pub(crate) fn save_witness_and_contract_accesses(
    chain_store: &ChainStoreAdapter,
    block_hash: &CryptoHash,
    shard_id: ShardId,
    witness: &SpiceChunkStateWitness,
    contract_accesses: &HashSet<CodeHash>,
) {
    let mut store_update = chain_store.store().store_update();
    set_witness(&mut store_update, block_hash, shard_id, witness);
    set_contract_accesses(&mut store_update, block_hash, shard_id, contract_accesses);
    store_update.commit();
}

pub(crate) fn get_contract_accesses(
    store: &Store,
    block_hash: &CryptoHash,
    shard_id: ShardId,
) -> Option<HashSet<CodeHash>> {
    let key = get_contract_accesses_key(block_hash, shard_id);
    let accesses: Arc<Vec<CodeHash>> = store.caching_get_ser(DBCol::contract_accesses(), &key)?;
    Some(accesses.iter().cloned().collect())
}

pub fn get_receipt_proof(
    store: &Store,
    block_hash: &CryptoHash,
    to_shard_id: ShardId,
    from_shard_id: ShardId,
) -> Option<ReceiptProof> {
    let key = get_receipt_proof_key(block_hash, from_shard_id, to_shard_id);
    store.get_ser(DBCol::receipt_proofs(), &key)
}

pub fn receipt_proof_exists(
    store: &Store,
    block_hash: &CryptoHash,
    to_shard_id: ShardId,
    from_shard_id: ShardId,
) -> bool {
    let key = get_receipt_proof_key(block_hash, from_shard_id, to_shard_id);
    store.exists(DBCol::receipt_proofs(), &key)
}

pub(crate) fn is_descendant_of_final_execution_head(
    chain_store: &ChainStoreAdapter,
    header: &BlockHeader,
) -> bool {
    let final_execution_head = match chain_store.spice_final_execution_head() {
        Ok(final_header) => final_header,
        // Without final execution head we are either executing on genesis and don't have it yet or
        // executing on top of non-spice blocks. In both cases we can assume that all blocks are on
        // top of final_execution_head until it's set.
        Err(Error::DBNotFoundErr(_)) => return true,
        Err(err) => panic!("failed to find final execution head: {err:?}"),
    };
    let mut height = header.height();
    if height <= final_execution_head.height {
        return false;
    }
    let mut prev_hash = *header.prev_hash();
    while height > final_execution_head.height {
        let header = chain_store.get_block_header(&prev_hash).unwrap();
        prev_hash = *header.prev_hash();
        height = header.height();
    }
    height == final_execution_head.height
}
