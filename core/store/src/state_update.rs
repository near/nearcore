#![allow(unused)]
//! Next generation replacement for [`TrieUpdate`](crate::trie::update::TrieUpdate).
use crate::Trie;
use crate::trie::OptimizedValueRef;
use crate::{KeyLookupMode, trie::AccessOptions};
use borsh::BorshDeserialize;
use near_primitives::errors::StorageError;
use near_primitives::trie_key::TrieKey;
use near_primitives::trie_key::col::{ACCESS_KEY, CONTRACT_DATA};
use near_primitives::types::AccountId;
use parking_lot::Mutex;
use state_value::{StateValue, ValueAbsent};
use std::any::Any;
use std::collections::{BTreeMap, btree_map};
use std::sync::Arc;

type Clock = u64;

struct StateUpdateState {
    /// This is the version of the "database" which gets set whenever an update is committed.
    ///
    /// This field is primarily here to enable multi-thread safety for updates to a single
    /// `Transaction`. An example of a issue that this field mitigates is multiple threads
    /// obtaining multiple copies of [`Update`], making potentially conflicting modifications,
    /// potentially based on non-consistent view of the same data and then committing both of these
    /// updates in a non-deterministic ordering.
    ///
    /// Here are some practical examples:
    ///
    /// * T1 and T2 both obtain an `Update`;
    /// * T1 reads keys `a:0` and `b:0`;
    /// * T2 reads keys `c:0` and `b:0`;
    /// * Both threads use `b:0` to compute a new value (`d` and `e`);
    /// * This is okay, even though a concurrent access to a value has occurred, because `b` did
    ///   not change;
    ///
    /// However consider a situation where `b` *does* change!
    ///
    /// * T1 and T2 both obtain an `Update`;
    /// * T2 reads key `b:0`;
    /// * T1 reads keys `a:0` and `b:0`;
    /// * T1 commits back `b:1`;
    /// * T2 reads key `c:0`, and tries to compute its own updated `b`;
    /// * If T2's write of `b:1` is successful, it will erase changes made by T1!
    ///
    /// This means that effectively only a single concurrent commit can succeed. That said, if we
    /// can verify that two transactions are entirely disjoint, there are likely no conflicts
    /// between the two and both can be comitted concurrently. This can enable genuinely parallel
    /// storage writes that are known to be disjoint – such as receipt application for distinct
    /// accounts.
    max_clock: Clock,

    /// State that has been committed to the transaction.
    ///
    /// This contains information about both the values that have been read from the lower storage
    /// layer as well as the values that have been modified. Only the modified values will be
    /// pushed to the lower layer upon finalization.
    committed: BTreeMap<TrieKey, CommittedValue>,
}

struct CommittedValue {
    value: Arc<dyn StateValue>,
    last_operation_clock: Clock,
    operations: u8,
}

/// An "atomic" unit of finalized state updates to be written to the backing [`Trie`].
///
/// This type can be filled with changes by creating a [`StateOperations`] and committing it.
/// Multiple [`StateOperations`] can be constructed and operated on at once. At the commit time
/// this type will ensure that these [`StateOperations`] were accessing disjoint state, thus
/// ensuring absence of data races.
pub struct StateUpdate {
    trie: crate::Trie,
    state: Arc<Mutex<StateUpdateState>>,
}

/// A collection of state operations pending an inclusion into a [`StateUpdate`].
///
/// The changes include writes but also operations such as reads or key existence checks.
///
/// These changes can be rolled back, or discarded, via [`StateOperations::discard`]. This can be
/// useful in order to avoid side effects (though care must be taken,) or to roll-back changes in
/// a case of an error.
pub struct StateOperations<'su> {
    /// What version [`StateUpdate`] was at when [`StateOperations`] was created?
    ///
    /// See [`StateUpdateState::max_clock`]. In particular this state update will fail to commit if
    /// it reads or writes any KV which (at the time of commit) has clock greater than
    /// `clock_created`.
    clock_created: Clock,
    /// The containing [`StateUpdate`].
    state_update: &'su StateUpdate,
    /// All the operations made against the backing transaction or storage data.
    ///
    /// Can be reads, writes as well as other operations, which will affect how this value would
    /// get handled when committed or finalized.
    operations: BTreeMap<TrieKey, OperationValue>,
}

struct OperationValue {
    operations: u8,
    value: Arc<dyn StateValue>,
}

impl OperationValue {
    const READ: u8 = 1;
    const WRITTEN: u8 = 2;
    const UPDATES_CLOCK: u8 = Self::READ | Self::WRITTEN;
}

impl StateUpdate {
    pub fn new(trie: crate::Trie) -> Self {
        StateUpdate {
            trie,
            state: Arc::new(Mutex::new(StateUpdateState {
                max_clock: 0,
                committed: Default::default(),
            })),
        }
    }

    pub fn start_update(&self) -> StateOperations {
        let state_guard = self.state.lock();
        StateOperations {
            clock_created: state_guard.max_clock,
            state_update: self,
            operations: Default::default(),
        }
    }

    pub fn trie(&self) -> &crate::Trie {
        &self.trie
    }
}

impl<'su> StateOperations<'su> {
    /// Checks the key for presence.
    ///
    /// Does not read out the value or cache it. Unlike `get` this is also more relaxed with
    /// regards to the conflict detection.
    pub fn contains_key(&mut self, key: &TrieKey) -> Result<bool, StorageError> {
        self.contains_key_or(key, |t, k| t.contains_key(&k.to_vec(), AccessOptions::DEFAULT))
    }

    /// This is a more flexible version of [`Self::contains_key`], allowing custom [`Trie`] access.
    pub fn contains_key_or(
        &mut self,
        _key: &TrieKey,
        fallback: impl FnOnce(&Trie, &TrieKey) -> Result<bool, StorageError>,
    ) -> Result<bool, StorageError> {
        todo!()
    }

    /// Obtain a [`Trie`] value reference (if it hasn't been dereferenced already.)
    ///
    /// If the value has never been accessed or dereferenced for the lifetime of [`StateUpdate`],
    /// this method will walk the backing [`Trie`] to obtain the
    /// [`OptimizedValueRef`](crate::trie::OptimizedValueRef) and remember the reference for the
    /// lifetime of [`StateUpdateOperation`] (which, if later committed, will also remember it to
    /// [`StateUpdate`].
    ///
    /// The returned object will transparently memoize dereferences of this reference. Once the
    /// value is dereferenced for the first time, the [`StateUpdate`]/[`StateUpdateOperation`] will
    /// switch to storing the value itself.
    ///
    // FIXME: StateUpdate cannot serve `OptimizedValueRef`s. If the value is written-to, we cannot
    // materialize `OVR::AvailableValue()` variant of it as it would store serialized data. Making
    // `value_hash` not straightforward to compute. The only option I can think of is replacing
    // `OptimizedValueRef` with our own enum that would store either the `ValueRef` or the
    // deserialized value.
    pub fn get_ref(&mut self, key: TrieKey) -> Result<Option<&OptimizedValueRef>, StorageError> {
        self.get_ref_or(key, |t, k| {
            t.get_optimized_ref(&k.to_vec(), KeyLookupMode::MemOrFlatOrTrie, AccessOptions::DEFAULT)
        })
    }

    /// This is a more flexible version of [`Self::get_ref`], allowing custom [`Trie`] access.
    // FIXME: StateUpdate cannot serve `OptimizedValueRef`s. If the value is written-to, we cannot
    // materialize `OVR::AvailableValue()` variant of it as it would store serialized data. Making
    // `value_hash` not straightforward to compute. The only option I can think of is replacing
    // `OptimizedValueRef` with our own enum that would store either the `ValueRef` or the
    // deserialized value.
    pub fn get_ref_or(
        &mut self,
        key: TrieKey,
        fallback: impl FnOnce(&Trie, &TrieKey) -> Result<Option<OptimizedValueRef>, StorageError>,
    ) -> Result<Option<&OptimizedValueRef>, StorageError> {
        todo!()
    }

    /// Get a reference to a deserialized value stored at the specified key.
    ///
    /// If the value has been previously written or read using this [`StateUpdateOperation`] (or if
    /// such an operation has been committed to [`StateUpdate`]) the read will be served directly
    /// from it without accessing the underlying [`Trie`] in any way.
    ///
    /// If you need to customize how the [`Trie`] is accessed, see [`Self::get_or`].
    pub fn get<V>(&mut self, key: TrieKey) -> Result<Option<&V>, StorageError>
    where
        V: StateValue,
        Arc<V>: BorshDeserialize,
    {
        let value = self.operate_value::<V>(key);
        value.operations |= OperationValue::READ;
        let value: &dyn StateValue = &*value.value;
        let value = value as &dyn Any;
        if value.is::<ValueAbsent>() {
            Ok(None)
        } else {
            Ok(Some(value.downcast_ref().expect("TODO: type confusion??")))
        }
    }

    /// This is a more flexible version of [`Self::get`], allowing custom [`Trie`] access.
    pub fn get_or<V>(
        &mut self,
        key: TrieKey,
        fallback: impl FnOnce(&Trie) -> Result<Option<Vec<u8>>, StorageError>,
    ) -> Result<Option<&V>, StorageError>
    where
        V: StateValue,
        Arc<V>: BorshDeserialize,
    {
        todo!()
    }

    /// Mutate the value at the specified `key` in-place.
    ///
    /// This method returns a mutable reference to the value stored in this
    /// [`StateUpdateOperation`], loading it into this operation if the value has not been
    /// previously operated on in any way.
    pub fn mutate<'a, V>(&'a mut self, key: TrieKey) -> Result<Option<&'a mut V>, StorageError>
    where
        V: StateValue,
        Arc<V>: BorshDeserialize,
    {
        let value = self.operate_value::<V>(key);
        value.operations |= OperationValue::WRITTEN;
        if Arc::get_mut(&mut value.value).is_none() {
            let new_arc = value.value.arc();
            value.value = new_arc;
        }
        // NOTE: unfortunate that we probe Arc's reference counters twice in cases where the strong
        // count is known to be 1 (as would be the case if `Arc::get_mut` above returned `Some`.
        // However that quickly runs afoul of the borrow checker (and for a good reason) so this is
        // a straightforward work-around for now.
        let value = Arc::get_mut(&mut value.value).expect("cannot fail!");
        let value = value as &mut dyn Any;
        if value.is::<ValueAbsent>() {
            Ok(None)
        } else {
            Ok(Some(value.downcast_mut().expect("TODO: type confusion??")))
        }
    }

    /// Set the value at the specified key.
    ///
    /// The returned mutable reference can be used to modify the value further. Note that this
    /// operation will not access the underlying [`Trie`] until the [`StateUpdate`] is finalized
    /// and written out to the underlying storage.
    pub fn set<V: StateValue>(&mut self, key: TrieKey, value: V) -> Result<&mut V, StorageError> {
        let value = match self.operations.entry(key) {
            btree_map::Entry::Vacant(e) => {
                let value = value.arc();
                e.insert(OperationValue { operations: OperationValue::WRITTEN, value })
            }
            btree_map::Entry::Occupied(e) => {
                let opval = e.into_mut();
                opval.value = value.arc();
                opval.operations |= OperationValue::WRITTEN;
                opval
            }
        };
        let value = Arc::get_mut(&mut value.value).expect("cannot fail!");
        let value = value as &mut dyn Any;
        Ok(value.downcast_mut().expect("TODO: type confusion??"))
    }

    /// The value at the specified key should be removed.
    ///
    /// If you need access to the value stored at this key previously, use [`Self::take`] instead.
    pub fn remove(&mut self, key: TrieKey) {
        static_assertions::assert_eq_size!(ValueAbsent, ());
        self.set(key, ValueAbsent);
    }

    /// Remove and return the current value at the given key.
    pub fn take<V>(&mut self, key: TrieKey) -> Result<Option<V>, StorageError>
    where
        V: StateValue + Clone,
        Arc<V>: BorshDeserialize,
    {
        self.take_or(key, |t, k| todo!())
    }

    /// A more flexible version of [`Self::take`] that allows customizing [`Trie`] access when
    /// value is not yet part of [`StoreUpdate`].
    pub fn take_or<V>(
        &mut self,
        key: TrieKey,
        fallback: impl FnOnce(&Trie, &TrieKey) -> Result<Option<Vec<u8>>, StorageError>,
    ) -> Result<Option<V>, StorageError>
    where
        V: StateValue + Clone,
        Arc<V>: BorshDeserialize,
    {
        todo!()
        // let value = self.operate_value::<V>(key);
        // value.operations = OperationValue::READ | OperationValue::WRITTEN;
        // let original_value = std::mem::replace(&mut value.value, ValueAbsent.arc());
        // if (&*original_value as &dyn Any).is::<ValueAbsent>() {
        //     return None;
        // }
        // let arc = Arc::downcast::<V>(original_value).expect("TODO: type confusion??");
        // Some(Arc::unwrap_or_clone(arc))
    }

    /// Pulls in the value into this operation's state.
    ///
    /// Caller is responsible for setting appropriate `operations`.
    fn operate_value<V>(&mut self, key: TrieKey) -> &mut OperationValue
    where
        V: StateValue,
        Arc<V>: BorshDeserialize,
    {
        match self.operations.entry(key) {
            btree_map::Entry::Occupied(e) => e.into_mut(),
            btree_map::Entry::Vacant(e) => {
                let committed_value = {
                    let state_update_guard = self.state_update.state.lock();
                    state_update_guard.committed.get(e.key()).map(|v| Arc::clone(&v.value))
                };
                if let Some(value) = committed_value {
                    e.insert(OperationValue { operations: 0, value })
                } else {
                    // FIXME(nagisa): maybe we could avoid a heap allocation here? Maybe use a
                    // smallvec or something?
                    let key = e.key().to_vec();
                    let trie_ref = self
                        .state_update
                        .trie
                        .get_optimized_ref(
                            &key,
                            KeyLookupMode::MemOrFlatOrTrie,
                            AccessOptions::DEFAULT,
                        )
                        .expect("TODO: storage error");
                    match trie_ref {
                        Some(trie_ref) => {
                            // FIXME(nagisa): maybe we can make trie return references? Maybe
                            // Cow<'a, [u8]>? To avoid allocating data that might already be inside
                            // memtries/recorded storage and easily borrowable?
                            let value = self
                                .state_update
                                .trie
                                .deref_optimized(AccessOptions::DEFAULT, &trie_ref)
                                .expect("TODO: storage error");
                            let value = Arc::<V>::try_from_slice(&value)
                                .expect("TODO: deserialization error");
                            e.insert(OperationValue { value, operations: OperationValue::READ })
                        }
                        None => e.insert(OperationValue {
                            value: ValueAbsent.arc(),
                            operations: OperationValue::READ,
                        }),
                    }
                }
            }
        }
    }

    /// Read value "purely".
    ///
    /// Value read this way does not cause any observable side-effects on the underlying storage.
    pub fn pure_get<V>(&mut self, key: TrieKey) -> Result<Option<&V>, StorageError>
    where
        V: StateValue,
        Arc<V>: BorshDeserialize,
    {
        todo!()
    }

    /// Fetch the [`OptimizedValueRef`] "purely".
    ///
    /// Value read this way does not cause any observable side-effects on the underlying storage.
    //
    // FIXME: StateUpdate cannot serve `OptimizedValueRef`s. If the value is written-to, we cannot
    // materialize `OVR::AvailableValue()` variant of it as it would store serialized data. Making
    // `value_hash` not straightforward to compute. The only option I can think of is replacing
    // `OptimizedValueRef` with our own enum that would store either the `ValueRef` or the
    // deserialized value.
    pub fn pure_get_ref(
        &mut self,
        key: TrieKey,
    ) -> Result<Option<&OptimizedValueRef>, StorageError> {
        todo!()
    }

    /// Remove all values with the provided `TrieKey` prefix.
    pub fn remove_prefix(&mut self, _key: TrieKeyPrefix) -> Result<(), StorageError> {
        // static_assertions::assert_eq_size!(RemoveValue, ());
        // self.set(key, RemoveValue);
        todo!()
    }

    /// Commit the changes to the [`StateUpdate`].
    ///
    /// If you'd like to discard the changes accumulated in this type, see [`Self::discard`].
    pub fn commit(&mut self) -> Result<(), StorageError> {
        let Self { clock_created, state_update, operations } = self;
        let mut state_update_state = state_update.state.lock();
        let clock_at_create = *clock_created;
        let last_operation_clock = if state_update_state.max_clock <= clock_at_create {
            clock_at_create + 1
        } else {
            state_update_state.max_clock + 1
        };
        state_update_state.max_clock = last_operation_clock;
        *clock_created = last_operation_clock;

        // FIXME: this is not atomic, first should check all clocks for well-formedness, then write
        // values.
        for (key, value) in std::mem::take(operations) {
            match state_update_state.committed.entry(key) {
                btree_map::Entry::Vacant(ve) => {
                    ve.insert(CommittedValue {
                        value: value.value,
                        last_operation_clock,
                        operations: value.operations,
                    });
                }
                btree_map::Entry::Occupied(oe) => {
                    let committed_value = oe.into_mut();
                    let well_formed = committed_value.last_operation_clock <= clock_at_create;
                    if !well_formed {
                        panic!("commit is not well formed!")
                    }
                    // TODO: this currently updates the operation clock regardless of whether the
                    // value was mutated or not. However, this might be overzealous and we might
                    // not need to update the operation clock on values that have only been read
                    // (after all – any other concurrent `StateUpdateOperation`s would've seen the
                    // same and correct value.
                    if (value.operations & OperationValue::UPDATES_CLOCK) != 0 {
                        committed_value.last_operation_clock = last_operation_clock;
                    }
                    if (value.operations & OperationValue::WRITTEN) != 0 {
                        committed_value.value = value.value;
                    }
                    committed_value.operations |= value.operations;
                }
            }
        }
        Ok(())
    }

    pub fn discard(&mut self) {
        let Self { clock_created, state_update, operations } = self;
        let state_update_state = state_update.state.lock();
        operations.clear();
        *clock_created = state_update_state.max_clock;
    }

    /// Start another, parallel update operation.
    pub fn state_update(&self) -> &'su StateUpdate {
        self.state_update
    }

    pub fn trie(&self) -> &crate::Trie {
        &self.state_update.trie()
    }
}

// FIXME: these all need some other solution
impl<'su> StateOperations<'su> {
    pub fn record_contract_call(
        &mut self,
        _: AccountId,
        _: near_primitives::hash::CryptoHash,
        _: &near_primitives::account::AccountContract,
        _: near_primitives::apply::ApplyChunkReason,
    ) -> Result<(), StorageError> {
        todo!()
    }

    // FIXME: decouple from near_vm_runner!
    pub fn record_contract_deploy(&mut self, _: near_vm_runner::ContractCode) {
        todo!()
    }
}

#[cfg(debug_assertions)]
impl<'su> Drop for StateOperations<'su> {
    fn drop(&mut self) {
        if !self.operations.is_empty() {
            panic!("StateUpdateOperation is dropped without commiting or discarding contents");
        }
    }
}

#[repr(u8)]
pub enum TrieKeyPrefix {
    AccessKey { account_id: AccountId } = ACCESS_KEY,
    ContractData { account_id: AccountId } = CONTRACT_DATA,
}

pub mod state_value {
    use std::any::Any;
    use std::sync::Arc;

    use crate::trie::OptimizedValueRef;

    /// A value that can be eventually written out to the database.
    ///
    // FIXME: seal this so that implementing this is only possible in this module.
    pub trait StateValue: Any + Send + Sync {
        fn arc(&self) -> Arc<dyn StateValue>;
    }

    /// Value that does not exist. If written to storage – removes any existing value at the key.
    pub(super) struct ValueAbsent;

    impl StateValue for ValueAbsent {
        fn arc(&self) -> Arc<dyn StateValue> {
            Arc::new(ValueAbsent)
        }
    }

    /// Value exists at the key.
    ///
    /// This is something that we'd store for a `contains_key` query. Cannot be mixed with a write
    /// operation.
    pub(super) struct ValuePresent;

    impl StateValue for ValuePresent {
        fn arc(&self) -> Arc<dyn StateValue> {
            Arc::new(ValuePresent)
        }
    }

    // /// Value exists, but might not have been dereferenced from the trie.
    // pub(super) enum PossiblyTrieValueRef {
    //     TrieValueRef(OptimizedValueRef),
    //     Dereferenced(dyn Any + Send + Sync),
    // }

    // impl StateValue for TrieValueRef {
    //     fn arc(&self) -> Arc<dyn StateValue> {
    //         todo!()
    //     }
    // }
}

mod state_value_impls {
    use super::StateValue;
    use crate::trie::outgoing_metadata::{
        ReceiptGroup, ReceiptGroupsQueueData, ReceiptGroupsQueueDataV0,
    };
    use near_primitives::account::{AccessKey, Account};
    use near_primitives::bandwidth_scheduler::BandwidthSchedulerState;
    use near_primitives::hash::CryptoHash;
    use near_primitives::receipt::{
        BufferedReceiptIndices, DelayedReceiptIndices, PromiseYieldIndices, PromiseYieldTimeout,
        Receipt, ReceiptOrStateStoredReceipt, ReceivedData, TrieQueueIndices,
    };
    use std::sync::Arc;

    macro_rules! borsh_state_value {
        ($ty: ty) => {
            impl StateValue for $ty {
                fn arc(&self) -> Arc<dyn StateValue> {
                    Arc::new(self.clone())
                }
            }
        };
    }

    borsh_state_value!(Account);
    borsh_state_value!(BufferedReceiptIndices);
    borsh_state_value!(ReceiptGroupsQueueDataV0);
    borsh_state_value!(ReceiptGroupsQueueData);
    borsh_state_value!(TrieQueueIndices);
    borsh_state_value!(DelayedReceiptIndices);
    borsh_state_value!(AccessKey);
    borsh_state_value!(PromiseYieldIndices);
    borsh_state_value!(PromiseYieldTimeout);
    borsh_state_value!(ReceivedData);
    borsh_state_value!(Receipt);
    borsh_state_value!(ReceiptGroup);
    borsh_state_value!(u32);
    borsh_state_value!(CryptoHash);
    borsh_state_value!(ReceiptOrStateStoredReceipt<'static>);
    borsh_state_value!(BandwidthSchedulerState);

    impl StateValue for Vec<u8> {
        fn arc(&self) -> Arc<dyn StateValue> {
            Arc::new(self.clone())
        }
    }
}
