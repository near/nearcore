//! The cache of external calls this node applied, found by the call's hash without its state init.
//! It lets `send_tx` find a call's outcome by that hash, and lets nodes drop a call that ran.

use crate::Error;
use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, BlockHeightDelta};
use near_runtime::ExternalCallInclusion;
use near_store::Store;
use near_store::adapter::StoreAdapter;
use parking_lot::Mutex;
use std::iter;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// For this many blocks after a contract accepts a call, a resend of the call does not run.
/// It is about 10 minutes at 600 ms blocks.
pub const EXTERNAL_CALL_REPEAT_WINDOW: BlockHeightDelta = 1_000;

/// How many calls the cache keeps. It covers the repeat window at up to 10 calls per block, and
/// each cache entry takes about 400 bytes.
const CAPACITY: NonZeroUsize = NonZeroUsize::new(10_000).unwrap();

/// An inclusion of a call, with the block that includes it.
#[derive(Clone)]
struct CachedInclusion {
    inclusion: ExternalCallInclusion,
    block_hash: CryptoHash,
    height: BlockHeight,
}

/// The inclusions the cache keeps for one call. Which of them is canonical is decided when the
/// cache is read, since a fork can orphan a block after its chunk was applied.
#[derive(Clone)]
struct CachedCall {
    /// The first inclusion the contract accepted, while it is inside the window.
    first_accepted: Option<CachedInclusion>,
    latest: CachedInclusion,
}

impl CachedCall {
    fn inclusions(&self) -> impl Iterator<Item = &CachedInclusion> {
        self.first_accepted.iter().chain(iter::once(&self.latest))
    }
}

/// The cache of applied external calls, kept in memory only. Its clones share one cache.
#[derive(Clone)]
pub struct ExternalCallCache {
    store: Store,
    /// Keyed by the call's hash without its state init.
    calls: Arc<Mutex<LruCache<CryptoHash, CachedCall>>>,
}

impl ExternalCallCache {
    /// An empty cache that reads blocks from `store`.
    pub fn new(store: Store) -> Self {
        Self { store, calls: Arc::new(Mutex::new(LruCache::new(CAPACITY))) }
    }

    /// Records the calls in the chunk of block `block_hash` at `height`.
    pub fn remember(
        &self,
        block_hash: &CryptoHash,
        height: BlockHeight,
        inclusions: &[ExternalCallInclusion],
    ) {
        let mut calls = self.calls.lock();
        for inclusion in inclusions {
            let latest =
                CachedInclusion { inclusion: inclusion.clone(), block_hash: *block_hash, height };
            let cached = calls.peek(&inclusion.hash_without_state_init);
            let earlier_accepted = cached.and_then(|call| call.first_accepted.clone());
            let first_accepted = match earlier_accepted {
                Some(earlier)
                    if height.saturating_sub(earlier.height) <= EXTERNAL_CALL_REPEAT_WINDOW =>
                {
                    Some(earlier)
                }
                _ => inclusion.accepted.then(|| latest.clone()),
            };
            calls.put(inclusion.hash_without_state_init, CachedCall { first_accepted, latest });
        }
    }

    /// The unique id of the inclusion that answers a `send_tx` of the call sent when the head was
    /// at `sent_at`, if this node has seen it.
    pub fn answer_to_send(
        &self,
        call_hash: &CryptoHash,
        sent_at: BlockHeight,
    ) -> Option<CryptoHash> {
        let call = self.calls.lock().get(call_hash).cloned()?;
        // Sends of the same call share its hash, so a cached inclusion may come from an earlier
        // send. Example: a refusal at 100 does not answer a retry sent at 105.
        let latest_first = [Some(&call.latest), call.first_accepted.as_ref()];
        // An inclusion after the send is this send's, accepted or refused. The latest one wins,
        // so a resend that landed before the first inclusion was final shows its own inclusion.
        for cached in latest_first.into_iter().flatten() {
            if cached.height > sent_at && self.is_canonical(cached) {
                return Some(cached.inclusion.unique_id);
            }
        }
        // An earlier inclusion answers only if it was accepted and is final, canonical and
        // inside the window, since nodes then drop this send as a repeat.
        let ran = call
            .inclusions()
            .find(|cached| cached.inclusion.accepted && self.ran_in_window(cached))?;
        Some(ran.inclusion.unique_id)
    }

    /// Whether the contract accepted the call in a final, canonical block inside the window.
    /// A failed store read means it did not.
    pub fn is_repeat(&self, call_hash: &CryptoHash) -> bool {
        let Some(call) = self.calls.lock().peek(call_hash).cloned() else {
            return false;
        };
        call.inclusions().any(|cached| cached.inclusion.accepted && self.ran_in_window(cached))
    }

    /// Whether the block of `cached` is final, canonical and at most the window below the final
    /// head.
    fn ran_in_window(&self, cached: &CachedInclusion) -> bool {
        match self.is_final_in_window(cached) {
            Ok(is_final_in_window) => is_final_in_window && self.is_canonical(cached),
            Err(error) => {
                tracing::debug!(target: "runtime", ?error, "repeat check failed");
                false
            }
        }
    }

    /// Whether the block of `cached` is final and at most the window below the final head.
    fn is_final_in_window(&self, cached: &CachedInclusion) -> Result<bool, Error> {
        let final_height = self.store.chain_store().final_head()?.height;
        Ok(cached.height <= final_height
            && final_height - cached.height <= EXTERNAL_CALL_REPEAT_WINDOW)
    }

    /// Whether the block of `cached` is on the canonical chain. If the canonical chain has no
    /// block at that height, the store read fails, and the answer is no.
    fn is_canonical(&self, cached: &CachedInclusion) -> bool {
        let canonical_hash = self.store.chain_store().get_block_hash_by_height(cached.height);
        canonical_hash.is_ok_and(|hash| hash == cached.block_hash)
    }
}
