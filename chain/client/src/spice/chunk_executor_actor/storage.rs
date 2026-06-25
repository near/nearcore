//! DB read/write helpers for SPICE per-chunk artifacts (receipt proofs,
//! witnesses, contract accesses). Shared by the per-shard executor and, for the
//! read helpers, by the data distributor.

use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ReceiptProof, ShardProof};
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::types::ShardId;
use near_primitives::utils::{
    get_contract_accesses_key, get_receipt_proof_key, get_receipt_proof_target_shard_prefix,
    get_witnesses_key,
};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::{DBCol, Store, StoreUpdate};
use std::collections::HashSet;
use std::sync::Arc;

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
