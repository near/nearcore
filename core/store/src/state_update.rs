#![allow(unused)]
//! Next generation replacement for [`TrieUpdate`](crate::trie::update::TrieUpdate).
use crate::trie::update::TrieUpdateResult;
use crate::trie::{OptimizedValueRef, ValueAccessToken};
use crate::{KeyLookupMode, trie::AccessOptions};
use crate::{Trie, TrieChanges};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::serialize;
use near_primitives::state::ValueRef;
use near_primitives::stateless_validation::contract_distribution::ContractUpdates;
use near_primitives::trie_key::col::{ACCESS_KEY, CONTRACT_DATA};
use near_primitives::trie_key::{SmallKeyVec, TrieKey, TrieKeyPrefix};
use near_primitives::types::{
    AccountId, RawStateChange, RawStateChangesWithTrieKey, StateChangeCause,
};
use parking_lot::RwLock;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use rayon::slice::ParallelSlice;
use state_value::{Deserializable, Deserialized};
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
    value: state_value::Type,
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
    state: Arc<RwLock<StateUpdateState>>,
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
    /// When `StateOperations` are dropped without manually committing or discarding, we can try
    /// doing a similar operation during drop.
    ///
    /// Ideally the code should manually handle the commits (as it is a fallible operation.)
    on_drop: OnDrop,
}

struct OperationValue {
    operations: u8,
    value: state_value::Type,
}

impl OperationValue {
    const READ: u8 = 1 << 0;
    const WRITTEN: u8 = 1 << 1;
    // Value is known to not exist in the underlying store.
    const ABSENT_IN_TRIE: u8 = 1 << 2;

    const UPDATES_CLOCK: u8 = Self::READ | Self::WRITTEN;

    const fn absent_in_trie() -> Self {
        Self { operations: Self::ABSENT_IN_TRIE, value: state_value::Type::Absent }
    }
}

impl StateUpdate {
    pub fn new(trie: crate::Trie) -> Self {
        StateUpdate {
            trie,
            state: Arc::new(RwLock::new(StateUpdateState {
                max_clock: 0,
                committed: Default::default(),
            })),
        }
    }

    pub fn start_update(&self) -> StateOperations {
        let state_guard = self.state.read();
        StateOperations {
            clock_created: state_guard.max_clock,
            state_update: self,
            operations: Default::default(),
            on_drop: Default::default(),
        }
    }

    pub fn trie(&self) -> &crate::Trie {
        &self.trie
    }

    pub fn clone_for_tx_preparation(&self) -> Self {
        Self { trie: self.trie.recording_reads_new_recorder(), state: self.state.clone() }
    }

    pub fn finalize(
        self,
        contract_updates: ContractUpdates,
        cause: StateChangeCause,
    ) -> Result<TrieUpdateResult, StorageError> {
        let state = self.state.read();

        let mut committed = state.committed.iter().collect::<Vec<_>>();
        let mut state_changes = committed
            .par_chunks(8)
            .flat_map_iter(|vs| vs)
            .filter_map(|&(k, v)| {
                let was_absent = (v.operations & OperationValue::ABSENT_IN_TRIE) != 0;
                let was_written = (v.operations & OperationValue::WRITTEN) != 0;

                let encoded_value = match &v.value {
                    // If we know the value is already absent in trie, we don't need to bother
                    // removing it.
                    state_value::Type::Absent if was_absent => {
                        return None;
                    }
                    state_value::Type::Absent => None,
                    state_value::Type::Present if was_absent || was_written => {
                        unreachable!("written but no value?")
                    }
                    // Don't have anything to write out here
                    state_value::Type::Present => return None,
                    state_value::Type::TrieValueRef(_) if was_absent || was_written => {
                        unreachable!("written but no value?")
                    }
                    state_value::Type::TrieValueRef(_) => return None,
                    state_value::Type::Serialized(_) if was_absent || was_written => {
                        unreachable!("written but no value?")
                    }
                    state_value::Type::Serialized(_) => return None,
                    state_value::Type::Deserialized(deserialized) if was_absent || was_written => {
                        Some(deserialized.serialize())
                    }
                    state_value::Type::Deserialized(_) => return None,
                };
                Some(RawStateChangesWithTrieKey {
                    trie_key: k.clone(),
                    changes: vec![RawStateChange {
                        cause: cause.clone(),
                        data: encoded_value.clone(),
                    }],
                })
            })
            .collect::<Vec<_>>();

        let trie_changes = self.trie.update2(&*state_changes)?;
        Ok(TrieUpdateResult { trie: self.trie, trie_changes, state_changes, contract_updates })
    }
}

pub enum StateValue {
    /// The value is present in the underlying Trie at a certain hash.
    TrieValueRef(ValueRef),
    /// The value was returned as a Vec<u8>.
    //
    // FIXME: must this be `Vec<u8>` here? Could we deal with something more optimal?
    Serialized(Arc<ValueAccessToken>),
    /// This is a deserialized form of the value stored at the given key.
    Deserialized(Arc<dyn Deserialized>),
}

impl StateValue {
    pub fn len(&self) -> usize {
        match self {
            StateValue::TrieValueRef(value_ref) => value_ref.len(),
            StateValue::Serialized(token) => token.len(),
            StateValue::Deserialized(deserialized) => {
                // FIXME(state_update): maybe also memoize serialized data for borsh types?
                deserialized.len()
            }
        }
    }

    pub fn value_hash_len(&self) -> (CryptoHash, usize) {
        match self {
            StateValue::TrieValueRef(value_ref) => (value_ref.hash, value_ref.len()),
            StateValue::Serialized(token) => (token.value_hash(), token.len()),
            StateValue::Deserialized(deserialized) => {
                // FIXME(state_update): maybe also memoize serialized data for borsh types?
                deserialized.value_hash_len()
            }
        }
    }
}

impl<'su> StateOperations<'su> {
    const NO_ON_PRESENT: Option<fn(&Trie, &TrieKey) -> Result<StateValue, StorageError>> = None;

    /// Pull in the value into this operation's state.
    ///
    /// The caller is responsible for setting appropriate `operations` or deserializing the value
    /// (if needed).
    fn pull_value<FromTrie, OnPresent>(
        &mut self,
        key: TrieKey,
        from_trie: FromTrie,
        on_present: Option<OnPresent>,
    ) -> Result<(&mut OperationValue, &Trie), StorageError>
    where
        FromTrie: FnOnce(&Trie, &TrieKey) -> Result<OperationValue, StorageError>,
        OnPresent: FnOnce(&Trie, &TrieKey) -> Result<StateValue, StorageError>,
    {
        match self.operations.entry(key) {
            btree_map::Entry::Occupied(e) => Ok(
                if let (state_value::Type::Present, Some(on_present)) = (&e.get().value, on_present)
                {
                    let new_value = on_present(self.state_update.trie(), e.key())?.into();
                    let op = e.into_mut();
                    op.value = new_value;
                    (op, self.state_update.trie())
                } else {
                    (e.into_mut(), self.state_update.trie())
                },
            ),
            btree_map::Entry::Vacant(e) => {
                let committed_value = {
                    let state_update_guard = self.state_update.state.read();
                    state_update_guard
                        .committed
                        .get(e.key())
                        .map(|v| state_value::Type::clone(&v.value))
                };
                let mut opval = if let Some(value) = committed_value {
                    OperationValue { operations: 0, value }
                } else {
                    from_trie(self.state_update.trie(), e.key())?
                };
                if let (state_value::Type::Present, Some(on_present)) = (&opval.value, on_present) {
                    opval.value = on_present(self.state_update.trie(), e.key())?.into();
                }
                Ok((e.insert(opval), self.state_update.trie()))
            }
        }
    }

    /// Check the key for presence.
    pub fn contains_key(&mut self, key: TrieKey) -> Result<bool, StorageError> {
        self.contains_key_or(key, |t, k| {
            let mut key_buf = SmallKeyVec::new_const();
            k.append_into(&mut key_buf);
            t.contains_key(&key_buf, AccessOptions::DEFAULT)
        })
    }

    /// Check the key for presence.
    ///
    /// This is a more flexible version of [`Self::contains_key`], allowing custom [`Trie`] access.
    pub fn contains_key_or(
        &mut self,
        key: TrieKey,
        lookup: impl FnOnce(&Trie, &TrieKey) -> Result<bool, StorageError>,
    ) -> Result<bool, StorageError> {
        let (opval, _) = self.pull_value(
            key,
            |trie, key| {
                let exists = lookup(trie, key)?;
                Ok(if exists {
                    OperationValue {
                        operations: OperationValue::READ,
                        value: state_value::Type::Present,
                    }
                } else {
                    OperationValue::absent_in_trie()
                })
            },
            Self::NO_ON_PRESENT,
        )?;
        Ok(opval.value.value_exists())
    }

    /// Obtain a [`Trie`] value reference or its dereferenced version.
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
    pub fn get_ref(
        &mut self,
        key: TrieKey,
        key_mode: KeyLookupMode,
        access_options: AccessOptions,
    ) -> Result<Option<StateValue>, StorageError> {
        // FIXME: this needs to handle pure accesses specially.
        self.get_ref_or(key, Self::default_fetch(key_mode, access_options))
    }

    /// This is a more flexible version of [`Self::get_ref`], allowing custom [`Trie`] access.
    pub fn get_ref_or(
        &mut self,
        key: TrieKey,
        fetch: impl Fn(&Trie, &TrieKey) -> Result<Option<StateValue>, StorageError>,
    ) -> Result<Option<StateValue>, StorageError> {
        let (opval, _) = self.pull_value(
            key,
            |trie: &Trie, key: &TrieKey| {
                let state_ref = fetch(trie, key)?;
                Ok(match state_ref {
                    None => OperationValue::absent_in_trie(),
                    Some(sr) => OperationValue { operations: 0, value: sr.into() },
                })
            },
            Some(|trie: &Trie, key: &TrieKey| match fetch(trie, key)? {
                None => todo!("storage inconsistent error"),
                Some(v) => Ok(v),
            }),
        )?;
        opval.operations |= OperationValue::READ;
        Ok(Some(match opval.value.clone() {
            state_value::Type::Absent => return Ok(None),
            state_value::Type::Present => todo!("unreachable, storage inconsistent error"),
            state_value::Type::TrieValueRef(value_ref) => StateValue::TrieValueRef(value_ref),
            state_value::Type::Serialized(serialized) => StateValue::Serialized(serialized),
            state_value::Type::Deserialized(deserialized) => StateValue::Deserialized(deserialized),
        }))
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
        V: Deserialized + Deserializable,
    {
        self.get_or(
            key,
            Self::default_fetch(KeyLookupMode::MemOrFlatOrTrie, AccessOptions::DEFAULT),
            AccessOptions::DEFAULT,
        )
    }

    /// This is a more flexible version of [`Self::get`], allowing custom [`Trie`] access.
    pub fn get_or<V>(
        &mut self,
        key: TrieKey,
        fetch: impl Fn(&Trie, &TrieKey) -> Result<Option<StateValue>, StorageError>,
        access_options: AccessOptions,
    ) -> Result<Option<&V>, StorageError>
    where
        V: Deserialized + Deserializable,
    {
        let (opval, trie) = self.pull_value(
            key,
            |trie: &Trie, key: &TrieKey| {
                let state_ref = fetch(trie, key)?;
                Ok(match state_ref {
                    None => OperationValue::absent_in_trie(),
                    Some(sr) => OperationValue { operations: 0, value: sr.into() },
                })
            },
            Some(|trie: &Trie, key: &TrieKey| match fetch(trie, key)? {
                None => todo!("storage inconsistent error"),
                Some(v) => Ok(v),
            }),
        )?;
        opval.operations |= OperationValue::READ;
        match &mut opval.value {
            state_value::Type::Absent => return Ok(None),
            state_value::Type::Present => todo!("unreachable, storage inconsistent error"),
            state_value::Type::Deserialized(v) => {
                let v: &dyn Deserialized = &**v;
                let downcast = <dyn Any>::downcast_ref::<V>(v);
                return Ok(Some(downcast.expect("TODO: type confusion??")));
            }
            this @ state_value::Type::Serialized(_) => {
                let serialized = std::mem::replace(this, state_value::Type::Present);
                let state_value::Type::Serialized(serialized) = serialized else { unreachable!() };
                // NOTE: it is fine if failing here leaves `Type::Present` rather than the original
                // value. It is not ideal, of course, but next time around if we request this key
                // again, we would refetch it from trie again.
                // That said, this is only okay so long as the only way to get `Serialized`
                // variants in the state is by reading from underlying trie.
                //
                // FIXME: have a method that converts an Arc<AccessValueToken> to a Cow<[u8]>
                // depending on whether there's just one owned reference or more.
                let serialized_bytes = trie.access_value(access_options, &serialized);
                // FIXME: have a method that deserializes optionally from a slice?
                match V::deserialize(serialized_bytes.to_vec()) {
                    Ok(v) => {
                        *this = state_value::Type::Deserialized(v);
                        let state_value::Type::Deserialized(v) = this else {
                            unreachable!();
                        };
                        let v: &dyn Deserialized = &**v;
                        let downcast = <dyn Any>::downcast_ref::<V>(v);
                        return Ok(Some(downcast.expect("TODO: unreachable type confusion??")));
                    }
                    Err(e) => {
                        *this = state_value::Type::Serialized(serialized);
                        return Err(e);
                    }
                }
            }
            this @ state_value::Type::TrieValueRef(_) => {
                // TODO: think how to deuglify this while keeping borrowcheck happy T_T
                let serialized_bytes = {
                    let state_value::Type::TrieValueRef(v) = this else {
                        unreachable!();
                    };
                    trie.retrieve_value(&v.hash, access_options)?
                };
                let deser = V::deserialize(serialized_bytes)?;
                *this = state_value::Type::Deserialized(deser);
                let state_value::Type::Deserialized(v) = this else {
                    unreachable!();
                };
                let v: &dyn Deserialized = &**v;
                let downcast = <dyn Any>::downcast_ref::<V>(v);
                return Ok(Some(downcast.expect("TODO: unreachable type confusion??")));
            }
        }
    }

    /// Get a reference to a deserialized value stored at the specified key.
    ///
    /// If the value has been previously written or read using this [`StateUpdateOperation`] (or if
    /// such an operation has been committed to [`StateUpdate`]) the read will be served directly
    /// from it without accessing the underlying [`Trie`] in any way.
    ///
    /// If you need to customize how the [`Trie`] is accessed, see [`Self::get_or`].
    pub fn mutate<V>(&mut self, key: TrieKey) -> Result<Option<&mut V>, StorageError>
    where
        V: Deserialized + Deserializable + Clone,
    {
        self.mutate_or(
            key,
            Self::default_fetch(KeyLookupMode::MemOrFlatOrTrie, AccessOptions::DEFAULT),
            AccessOptions::DEFAULT,
        )
    }

    /// This is a more flexible version of [`Self::get`], allowing custom [`Trie`] access.
    fn mutate_or<V>(
        &mut self,
        key: TrieKey,
        fetch: impl Fn(&Trie, &TrieKey) -> Result<Option<StateValue>, StorageError>,
        access_options: AccessOptions,
    ) -> Result<Option<&mut V>, StorageError>
    where
        V: Deserialized + Deserializable + Clone,
    {
        // TODO: this implementation is heavily shared with `get_or`. Think about how to
        // deduplicate.
        let (opval, trie) = self.pull_value(
            key,
            |trie: &Trie, key: &TrieKey| {
                let state_ref = fetch(trie, key)?;
                Ok(match state_ref {
                    None => OperationValue::absent_in_trie(),
                    Some(sr) => OperationValue { operations: 0, value: sr.into() },
                })
            },
            Some(|trie: &Trie, key: &TrieKey| match fetch(trie, key)? {
                None => todo!("storage inconsistent error"),
                Some(v) => Ok(v),
            }),
        )?;
        opval.operations |= OperationValue::READ | OperationValue::WRITTEN;
        match &mut opval.value {
            state_value::Type::Absent => return Ok(None),
            state_value::Type::Present => todo!("unreachable, storage inconsistent error"),
            state_value::Type::Deserialized(v) => {
                let downcast =
                    state_value::arc_make_mut_downcast::<V>(v).expect("TODO: type confusion??");
                return Ok(Some(downcast));
            }
            this @ state_value::Type::Serialized(_) => {
                let serialized = std::mem::replace(this, state_value::Type::Present);
                let state_value::Type::Serialized(serialized) = serialized else { unreachable!() };
                // NOTE: it is fine if failing here leaves `Type::Present` rather than the original
                // value. It is not ideal, of course, but next time around if we request this key
                // again, we would refetch it from trie again.
                // That said, this is only okay so long as the only way to get `Serialized`
                // variants in the state is by reading from underlying trie.
                //
                // FIXME: have a method that converts an Arc<AccessValueToken> to a Cow<[u8]>
                // depending on whether there's just one owned reference or more.
                let serialized_bytes = trie.access_value(access_options, &serialized);
                // FIXME: have a method that deserializes optionally from a slice?
                match V::deserialize(serialized_bytes.to_vec()) {
                    Ok(v) => {
                        *this = state_value::Type::Deserialized(v);
                        let state_value::Type::Deserialized(v) = this else {
                            unreachable!();
                        };
                        let downcast = state_value::arc_make_mut_downcast::<V>(v)
                            .expect("TODO: unreachable type confusion??");
                        return Ok(Some(downcast));
                    }
                    Err(e) => {
                        *this = state_value::Type::Serialized(serialized);
                        return Err(e);
                    }
                }
            }
            this @ state_value::Type::TrieValueRef(_) => {
                // TODO: think how to deuglify this while keeping borrowcheck happy T_T
                let serialized_bytes = {
                    let state_value::Type::TrieValueRef(v) = this else {
                        unreachable!();
                    };
                    trie.retrieve_value(&v.hash, access_options)?
                };
                let deser = V::deserialize(serialized_bytes)?;
                *this = state_value::Type::Deserialized(deser);
                let state_value::Type::Deserialized(v) = this else {
                    unreachable!();
                };
                let downcast = state_value::arc_make_mut_downcast::<V>(v)
                    .expect("TODO: unreachable type confusion??");
                return Ok(Some(downcast));
            }
        }
    }

    /// Set the value at the specified key.
    ///
    /// Note that this operation will not access the underlying [`Trie`] until the [`StateUpdate`]
    /// is finalized and written out to the underlying storage.
    pub fn set<V: Deserialized>(&mut self, key: TrieKey, value: V) -> () {
        let dbg_key = key.clone();
        let after = match self.operations.entry(key) {
            btree_map::Entry::Vacant(e) => {
                let value = state_value::Type::Deserialized(Arc::new(value));
                e.insert(OperationValue { operations: OperationValue::WRITTEN, value })
            }
            btree_map::Entry::Occupied(e) => {
                let opval = e.into_mut();
                opval.value = state_value::Type::Deserialized(Arc::new(value));
                opval.operations |= OperationValue::WRITTEN;
                opval
            }
        };
    }

    /// The value at the specified key should be removed.
    ///
    /// If you need access to the value stored at this key previously, use [`Self::take`] instead.
    pub fn remove(&mut self, key: TrieKey) {
        match self.operations.entry(key) {
            btree_map::Entry::Vacant(e) => {
                let value = state_value::Type::Absent;
                e.insert(OperationValue { operations: OperationValue::WRITTEN, value })
            }
            btree_map::Entry::Occupied(e) => {
                let opval = e.into_mut();
                opval.value = state_value::Type::Absent;
                opval.operations |= OperationValue::WRITTEN;
                opval
            }
        };
    }

    /// Remove and return the current value at the given key.
    pub fn take<V>(
        &mut self,
        key: TrieKey,
        mode: KeyLookupMode,
        options: AccessOptions,
    ) -> Result<Option<Arc<V>>, StorageError>
    where
        V: Deserialized + Deserializable,
    {
        // FIXME: this needs to take care to keep track of pure vs non-pure accesses. Pure accesses
        // cannot prevent future lookup that would record data into state witnesses or similar.
        self.take_or(key, Self::default_fetch(mode, options), options)
    }

    /// A more flexible version of [`Self::take`] that allows customizing [`Trie`] access.
    fn take_or<V>(
        &mut self,
        key: TrieKey,
        fetch: impl Fn(&Trie, &TrieKey) -> Result<Option<StateValue>, StorageError>,
        access_options: AccessOptions,
    ) -> Result<Option<Arc<V>>, StorageError>
    where
        V: Deserialized + Deserializable,
    {
        let (opval, trie) = self.pull_value(
            key,
            |trie: &Trie, key: &TrieKey| {
                let state_ref = fetch(trie, key)?;
                Ok(match state_ref {
                    None => OperationValue::absent_in_trie(),
                    Some(sr) => OperationValue { operations: 0, value: sr.into() },
                })
            },
            Some(|trie: &Trie, key: &TrieKey| match fetch(trie, key)? {
                None => todo!("storage inconsistent error"),
                Some(v) => Ok(v),
            }),
        )?;
        opval.operations |= OperationValue::READ | OperationValue::WRITTEN;
        match std::mem::replace(&mut opval.value, state_value::Type::Absent) {
            state_value::Type::Absent => return Ok(None),
            state_value::Type::Present => todo!("unreachable, storage inconsistent error"),
            state_value::Type::Deserialized(v) => {
                Ok(Some(<Arc<dyn Any + Send + Sync>>::downcast(v).expect("TODO: type confusion??")))
            }
            state_value::Type::Serialized(s) => {
                // TODO: access by value/cow (unwrap_or_clone) and optionally deserialize from ref.
                let serialized_bytes = trie.access_value(access_options, &s);
                Ok(Some(V::deserialize(serialized_bytes.to_vec())?))
            }
            this @ state_value::Type::TrieValueRef(valref) => {
                let serialized_bytes = trie.retrieve_value(&valref.hash, access_options)?;
                Ok(Some(V::deserialize(serialized_bytes)?))
            }
        }
    }

    /// Read value "purely".
    ///
    /// Value read this way does not cause any observable side-effects on the underlying storage.
    pub fn pure_get<V>(&mut self, key: TrieKey) -> Result<Option<&V>, StorageError>
    where
        V: Deserialized + Deserializable,
    {
        self.get_or(
            key,
            Self::default_fetch(KeyLookupMode::MemOrFlatOrTrie, AccessOptions::NO_SIDE_EFFECTS),
            AccessOptions::NO_SIDE_EFFECTS,
        )
    }

    /// Fetch the [`OptimizedValueRef`] "purely".
    ///
    /// Value read this way does not cause any observable side-effects on the underlying storage.
    pub fn pure_get_ref(&mut self, key: TrieKey) -> Result<Option<StateValue>, StorageError> {
        self.get_ref_or(
            key,
            Self::default_fetch(KeyLookupMode::MemOrFlatOrTrie, AccessOptions::NO_SIDE_EFFECTS),
        )
    }

    /// Remove all values with the provided `TrieKey` prefix.
    pub fn remove_prefix(&mut self, prefix: TrieKeyPrefix) -> Result<(), StorageError> {
        for (key, op) in self.operations.range_mut(prefix.range_start()..) {
            // FIXME: this is relatively inefficient, but we cannot construct invalid AccountIds
            // that may be otherwise necessary if we wanted to construct a `TrieKey` that
            // represents an end bound.
            if !prefix.contains(key) {
                break;
            }
            op.operations |= OperationValue::WRITTEN;
            op.value = state_value::Type::Absent;
        }
        {
            let mut committed_state = self.state_update.state.read();
            for (key, committed) in committed_state.committed.range(prefix.range_start()..) {
                if !prefix.contains(key) {
                    break;
                }
                self.remove(key.clone());
            }
        }

        let lock = self.trie().lock_for_iter();
        let mut iterator = lock.iter()?;
        let mut serialized_key = SmallKeyVec::new();
        prefix.append_into(&mut serialized_key);
        iterator.seek_prefix(&serialized_key);
        let mut to_remove = Vec::new();
        for result in iterator {
            let (key, value) = result?;
            let full_key = prefix
                .clone()
                .parse_full(&key)
                .map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?;
            to_remove.push(full_key);
        }
        drop(lock);

        // FIXME: This intermediate vector is unfortunate and could be avoided if `self.remove` did
        // not interact with the `self.trie()` borrow.
        for key in to_remove {
            self.remove(key);
        }

        Ok(())
    }

    pub fn ptr(&self) -> *const () {
        Arc::as_ptr(&self.state_update.state) as _
    }

    /// Commit the changes to the [`StateUpdate`].
    ///
    /// If you'd like to discard the changes accumulated in this type, see [`Self::discard`].
    pub fn in_place_commit(&mut self) -> Result<(), StorageError> {
        let Self { clock_created, state_update, operations, on_drop: _ } = self;
        let mut state_update_state = state_update.state.write();
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
            let state_update_state_ptr = (&*state_update_state) as *const StateUpdateState as usize;
            match state_update_state.committed.entry(key) {
                btree_map::Entry::Vacant(ve) => {
                    ve.insert(CommittedValue {
                        value: value.value,
                        last_operation_clock,
                        operations: value.operations,
                    });
                }
                btree_map::Entry::Occupied(oe) => {
                    let clock_upstream = oe.get().last_operation_clock;
                    let well_formed = clock_upstream <= clock_at_create;
                    if !well_formed {
                        // Hello. You have multiple `StateUpdate`s accessing the same data
                        // concurrently within a lifetime two conflicting `StateOperations`.
                        //
                        // `StateUpdate::max_clock` has some additional documentation about data
                        // race detection and the limitations (e.g. it will flag concurrent reads
                        // as conflicting as well.)
                        panic!(
                            r#"StateUpdate data races!
                            key: {:?}
                            clock upstream: {}
                            this operation read data at clock: {}
                            state update state: 0x{:x}
                            thread id: {:?}"#,
                            oe.key(),
                            clock_upstream,
                            clock_at_create,
                            state_update_state_ptr,
                            std::thread::current().id(),
                        );
                    }
                    let committed_value = oe.into_mut();
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

    pub fn commit(mut self) -> Result<(), StorageError> {
        self.in_place_commit()
    }

    /// Remove all pending operations from this instance and reset the transaction information.
    ///
    /// This is roughly equivalent to `discard`ing the current instance and creating a new one,
    /// only that it occurs in-place.
    pub fn reset(&mut self) {
        let Self { clock_created, state_update, operations, on_drop: _ } = self;
        let state_update_state = state_update.state.read();
        operations.clear();
        *clock_created = state_update_state.max_clock;
    }

    pub fn discard(mut self) {
        self.operations.clear();
    }

    pub fn trie(&self) -> &crate::Trie {
        &self.state_update.trie()
    }

    fn default_fetch(
        mode: KeyLookupMode,
        access_options: AccessOptions,
    ) -> impl Fn(&Trie, &TrieKey) -> Result<Option<StateValue>, StorageError> {
        move |trie, key| {
            let mut key_buf = SmallKeyVec::new_const();
            key.append_into(&mut key_buf);
            Ok(match trie.get_optimized_ref(&key_buf, mode, access_options)? {
                Some(OptimizedValueRef::Ref(r)) => Some(StateValue::TrieValueRef(r)),
                Some(OptimizedValueRef::AvailableValue(v)) => {
                    Some(StateValue::Serialized(Arc::new(v)))
                }
                None => None,
            })
        }
    }
}

#[derive(Default)]
enum OnDrop {
    /// [`OnDrop::Panic`] when debug assertions are enabled, [`OnDrop::Discard`] otherwise.
    ///
    /// This is the default.
    #[default]
    DebugPanicReleaseDiscard,
    /// Attempt to commit the changes into the `StateUpdate`.
    ///
    /// This should ideally be only limited to test use, as this operation is fallible. Any commits
    /// should ideally be executed with manual call to [`StateOperations::commit`].
    Commit,
    /// Discard the data.
    Discard,
    /// Always panic if a non-empty `StateOperations` is dropped.
    Panic,
}

impl<'su> StateOperations<'su> {
    /// Request that on drop of this collection of operations a commit attempt is made.
    pub fn commit_on_drop(mut self) -> Self {
        self.on_drop = OnDrop::Commit;
        self
    }

    pub fn discard_on_drop(mut self) -> Self {
        self.on_drop = OnDrop::Discard;
        self
    }

    pub fn panic_on_drop(mut self) -> Self {
        self.on_drop = OnDrop::Panic;
        self
    }
}

impl<'su> Drop for StateOperations<'su> {
    #[track_caller]
    fn drop(&mut self) {
        if !self.operations.is_empty() {
            match self.on_drop {
                OnDrop::DebugPanicReleaseDiscard if cfg!(debug_assertions) => {
                    if !std::thread::panicking() {
                        panic!(
                            "StateOperations is dropped without commiting or discarding contents"
                        )
                    }
                }
                OnDrop::DebugPanicReleaseDiscard | OnDrop::Discard => {}
                OnDrop::Commit => match self.in_place_commit() {
                    Ok(_) => {}
                    Err(_) if std::thread::panicking() => {}
                    Err(e) => panic!("StateOperations could not be committed during drop: {e}"),
                },
                OnDrop::Panic => {
                    if !std::thread::panicking() {
                        panic!(
                            "StateOperations is dropped without commiting or discarding contents"
                        )
                    }
                }
            }
        }
    }
}

pub mod state_value {
    use crate::trie::{OptimizedValueRef, ValueAccessToken};
    use near_primitives::errors::StorageError;
    use near_primitives::state::ValueRef;
    use std::any::{Any, type_name, type_name_of_val};
    use std::sync::Arc;

    #[derive(Clone)]
    pub enum Type {
        /// The value is not present in the storage at this layer.
        Absent,
        /// The value is present in the storage, but we were not forced to obtain the value yet.
        Present,
        /// The value is present in the underlying Trie at a certain hash.
        TrieValueRef(ValueRef),
        /// The value was returned as a Vec<u8>.
        //
        // FIXME: must Trie return `Vec<u8>`s here? Could it return something more optimal?
        Serialized(Arc<ValueAccessToken>),
        /// This is a deserialized form of the value stored at the given key.
        Deserialized(Arc<dyn Deserialized>),
    }

    impl Type {
        pub(super) fn value_exists(&self) -> bool {
            match self {
                Type::Absent => false,
                Type::Present
                | Type::TrieValueRef(_)
                | Type::Serialized(_)
                | Type::Deserialized(_) => true,
            }
        }
    }

    impl From<super::StateValue> for Type {
        fn from(value: super::StateValue) -> Self {
            use super::StateValue::*;
            match value {
                TrieValueRef(value_ref) => Self::TrieValueRef(value_ref),
                Serialized(serialized) => Self::Serialized(serialized),
                Deserialized(deserialized) => Self::Deserialized(deserialized),
            }
        }
    }

    /// A value that can be eventually written out to the database.
    ///
    // FIXME: seal this so that implementing this is only possible in this module.
    pub trait Deserialized: Any + Send + Sync {
        fn serialize(&self) -> Vec<u8>;
        fn len(&self) -> usize;
        fn value_hash_len(&self) -> (near_primitives::hash::CryptoHash, usize);
        fn type_name(&self) -> &'static str {
            std::any::type_name_of_val(self)
        }
    }

    pub trait Deserializable {
        fn deserialize(bytes: Vec<u8>) -> Result<Arc<Self>, StorageError>;
    }

    /// A combination of `Arc::make_mut` and `<dyn Any>::downcast_mut`.
    ///
    /// If given an `Arc` that has only a single strong reference, this will return a downcast
    /// mutable reference. If there are more than 1 strong reference, this function will, similarly
    /// to `make_mut`, make a clone of the downcast value in-place and then return a mutable
    /// reference to this value.
    #[track_caller]
    pub(super) fn arc_make_mut_downcast<T: Deserialized + Clone>(
        arc: &mut Arc<dyn Deserialized>,
    ) -> Option<&mut T> {
        if !<dyn Any>::is::<T>(&**arc) {
            panic!("&*arc isn't {}? It is {}", type_name::<T>(), type_name_of_val(&**arc));
            return None;
        }
        if Arc::get_mut(arc).is_none() {
            let value: &dyn Deserialized = &**arc;
            let value = <dyn Any>::downcast_ref::<T>(value).unwrap().clone();
            *arc = Arc::new(value);
        }
        // Could be made faster with `get_mut_unchecked`.
        let value: &mut dyn Deserialized = Arc::get_mut(arc).unwrap();
        <dyn Any + Send + Sync>::downcast_mut::<T>(value as _)
    }
}

mod state_value_impls {
    use super::Deserialized;
    use super::state_value::Deserializable;
    use crate::trie::outgoing_metadata::{
        ReceiptGroup, ReceiptGroupsQueueData, ReceiptGroupsQueueDataV0,
    };
    use near_primitives::account::{AccessKey, Account};
    use near_primitives::bandwidth_scheduler::BandwidthSchedulerState;
    use near_primitives::errors::StorageError;
    use near_primitives::hash::CryptoHash;
    use near_primitives::receipt::{
        BufferedReceiptIndices, DelayedReceiptIndices, PromiseYieldIndices, PromiseYieldTimeout,
        Receipt, ReceiptOrStateStoredReceipt, ReceivedData, TrieQueueIndices,
    };
    use std::sync::Arc;

    macro_rules! borsh_state_value {
        ($ty: ty) => {
            impl Deserialized for $ty {
                fn serialize(&self) -> Vec<u8> {
                    borsh::to_vec(self).expect("TODO(state_update): error handling?")
                }

                fn len(&self) -> usize {
                    // FIXME: maybe somehow memoize this serialization?
                    borsh::object_length(self).expect("shoudn't fail!")
                }

                fn value_hash_len(&self) -> (CryptoHash, usize) {
                    todo!("value_hash_len for borsh types")
                }
            }

            impl Deserializable for $ty {
                fn deserialize(bytes: Vec<u8>) -> Result<Arc<Self>, StorageError> {
                    borsh::de::from_slice(&bytes).map_err(|e| {
                        StorageError::StorageInconsistentState(format!(
                            "value could not be deserialized: {e}"
                        ))
                    })
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
    borsh_state_value!(i32);
    borsh_state_value!(CryptoHash);
    borsh_state_value!(ReceiptOrStateStoredReceipt<'static>);
    borsh_state_value!(BandwidthSchedulerState);

    impl Deserialized for Vec<u8> {
        fn serialize(&self) -> Vec<u8> {
            self.to_vec()
        }

        fn len(&self) -> usize {
            self.len()
        }

        fn value_hash_len(&self) -> (CryptoHash, usize) {
            (near_primitives::hash::hash(self), self.len())
        }
    }
    impl Deserializable for Vec<u8> {
        fn deserialize(bytes: Vec<u8>) -> Result<Arc<Self>, StorageError> {
            Ok(Arc::new(bytes))
        }
    }
}
