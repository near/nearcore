#![allow(unused)]
//! Next generation replacement for [`TrieUpdate`](crate::trie::update::TrieUpdate).
use crate::Trie;
use crate::trie::update::TrieUpdateResult;
use crate::trie::{OptimizedValueRef, ValueAccessToken};
use crate::{KeyLookupMode, trie::AccessOptions};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::serialize;
use near_primitives::state::ValueRef;
use near_primitives::trie_key::col::{ACCESS_KEY, CONTRACT_DATA};
use near_primitives::trie_key::{SmallKeyVec, TrieKey};
use near_primitives::types::AccountId;
use parking_lot::Mutex;
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

    pub fn clone_for_tx_preparation(&self) -> Self {
        Self {
            trie: self.trie.recording_reads_new_recorder(),
            state: self.state.clone(),
        }
    }

    pub fn finalize(self) -> Result<TrieUpdateResult, StorageError> {
        todo!()
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
                // FIXME(nagisa): this may not be necessary if the stored type is `Vec<u8>`. This
                // probably should be a method on `Deserialized`.
                todo!("serialize on the fly, take length")
            }
        }
    }

    pub fn value_hash_len(&self) -> (CryptoHash, usize) {
        match self {
            StateValue::TrieValueRef(value_ref) => (value_ref.hash, value_ref.len()),
            StateValue::Serialized(token) => (token.value_hash(), token.len()),
            StateValue::Deserialized(deserialized) => {
                todo!("serialize on the fly, hash and take length")
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
                    let state_update_guard = self.state_update.state.lock();
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
        self.get_ref_or(key, |t, k| {
            let mut key_buf = SmallKeyVec::new_const();
            k.append_into(&mut key_buf);
            let optref = t.get_optimized_ref(&*key_buf, key_mode, access_options);
            Ok(Some(match optref? {
                Some(OptimizedValueRef::Ref(r)) => StateValue::TrieValueRef(r),
                Some(OptimizedValueRef::AvailableValue(v)) => StateValue::Serialized(Arc::new(v)),
                None => return Ok(None),
            }))
        })
    }

    /// This is a more flexible version of [`Self::get_ref`], allowing custom [`Trie`] access.
    fn get_ref_or(
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
            |trie, key| {
                let mut key_buf = SmallKeyVec::new_const();
                key.append_into(&mut key_buf);
                let mode = KeyLookupMode::MemOrFlatOrTrie;
                Ok(match trie.get_optimized_ref(&key_buf, mode, AccessOptions::DEFAULT)? {
                    Some(OptimizedValueRef::Ref(r)) => Some(StateValue::TrieValueRef(r)),
                    Some(OptimizedValueRef::AvailableValue(v)) => {
                        Some(StateValue::Serialized(Arc::new(v)))
                    }
                    None => None,
                })
            },
            // FIXME: `deref_optimized` could be better if it didn't (potentially) clone the vector
            // inside...
            |trie, optref| trie.deref_optimized(AccessOptions::DEFAULT, &optref),
        )
    }

    /// This is a more flexible version of [`Self::get`], allowing custom [`Trie`] access.
    fn get_or<V>(
        &mut self,
        key: TrieKey,
        fetch: impl Fn(&Trie, &TrieKey) -> Result<Option<StateValue>, StorageError>,
        deref: impl Fn(&Trie, OptimizedValueRef) -> Result<Vec<u8>, StorageError>,
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
                return Ok(Some(<dyn Any>::downcast_ref(v).expect("TODO: type confusion??")));
            }
            this @ state_value::Type::Serialized(_) => {
                let serialized = std::mem::replace(this, state_value::Type::Present);
                let state_value::Type::Serialized(serialized) = serialized else { unreachable!() };
                let trie_ref = OptimizedValueRef::AvailableValue(Arc::unwrap_or_clone(serialized));
                // NOTE: it is fine if failing here leaves `Type::Present` rather than the original
                // value. It is not ideal, of course, but next time around if we request this key
                // again, we would refetch it from trie again.
                // That said, this is only okay so long as the only way to get `Serialized`
                // variants in the state is by reading from underlying trie.
                let serialized_bytes = deref(trie, trie_ref)?;
                let deser = V::deserialize(serialized_bytes)?;
                *this = state_value::Type::Deserialized(deser);
                let state_value::Type::Deserialized(v) = this else {
                    unreachable!();
                };
                return Ok(Some(<dyn Any>::downcast_ref(v).expect("TODO: type confusion??")));
            }
            this @ state_value::Type::TrieValueRef(_) => {
                let valref = std::mem::replace(this, state_value::Type::Present);
                let state_value::Type::TrieValueRef(valref) = valref else { unreachable!() };
                let trie_ref = OptimizedValueRef::Ref(valref);
                let serialized_bytes = deref(trie, trie_ref)?;
                let deser = V::deserialize(serialized_bytes)?;
                *this = state_value::Type::Deserialized(deser);
                let state_value::Type::Deserialized(v) = this else {
                    unreachable!();
                };
                return Ok(Some(<dyn Any>::downcast_ref(v).expect("TODO: type confusion??")));
            }
        }
    }

    /// Mutate the value at the specified `key` in-place.
    ///
    /// This method returns a mutable reference to the value stored in this
    /// [`StateUpdateOperation`], loading it into this operation if the value has not been
    /// previously operated on in any way.
    pub fn mutate<'a, V>(&'a mut self, key: TrieKey) -> Result<Option<&'a mut V>, StorageError>
    where
        V: Deserialized + Deserializable,
    {
        todo!()
        // let value = self.operate_value::<V>(key);
        // value.operations |= OperationValue::WRITTEN;
        // if Arc::get_mut(&mut value.value).is_none() {
        //     let new_arc = value.value.arc();
        //     value.value = new_arc;
        // }
        // // NOTE: unfortunate that we probe Arc's reference counters twice in cases where the strong
        // // count is known to be 1 (as would be the case if `Arc::get_mut` above returned `Some`.
        // // However that quickly runs afoul of the borrow checker (and for a good reason) so this is
        // // a straightforward work-around for now.
        // let value = Arc::get_mut(&mut value.value).expect("cannot fail!");
        // let value = value as &mut dyn Any;
        // todo!()
        // // if value.is::<ValueAbsent>() {
        // //     Ok(None)
        // // } else {
        // //     Ok(Some(value.downcast_mut().expect("TODO: type confusion??")))
        // // }
    }

    /// Set the value at the specified key.
    ///
    /// Note that this operation will not access the underlying [`Trie`] until the [`StateUpdate`]
    /// is finalized and written out to the underlying storage.
    pub fn set<V: Deserialized>(&mut self, key: TrieKey, value: V) -> () {
        match self.operations.entry(key) {
            btree_map::Entry::Vacant(e) => {
                let value = state_value::Type::Deserialized(value.arc());
                e.insert(OperationValue { operations: OperationValue::WRITTEN, value })
            }
            btree_map::Entry::Occupied(e) => {
                let opval = e.into_mut();
                opval.value = state_value::Type::Deserialized(value.arc());
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
        self.take_or(
            key,
            |trie, key| {
                let mut key_buf = SmallKeyVec::new_const();
                key.append_into(&mut key_buf);
                Ok(match trie.get_optimized_ref(&key_buf, mode, options)? {
                    Some(OptimizedValueRef::Ref(r)) => Some(StateValue::TrieValueRef(r)),
                    Some(OptimizedValueRef::AvailableValue(v)) => {
                        Some(StateValue::Serialized(Arc::new(v)))
                    }
                    None => None,
                })
            },
            // FIXME: `deref_optimized` could be better if it didn't (potentially) clone the vector
            // inside...
            |trie, optref| trie.deref_optimized(options, &optref),
        )
    }

    /// A more flexible version of [`Self::take`] that allows customizing [`Trie`] access.
    fn take_or<V>(
        &mut self,
        key: TrieKey,
        fetch: impl Fn(&Trie, &TrieKey) -> Result<Option<StateValue>, StorageError>,
        deref: impl Fn(&Trie, OptimizedValueRef) -> Result<Vec<u8>, StorageError>,
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
                let trie_ref = OptimizedValueRef::AvailableValue(Arc::unwrap_or_clone(s));
                let serialized_bytes = deref(trie, trie_ref)?;
                Ok(Some(V::deserialize(serialized_bytes)?))
            }
            this @ state_value::Type::TrieValueRef(r) => {
                let trie_ref = OptimizedValueRef::Ref(r);
                let serialized_bytes = deref(trie, trie_ref)?;
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
    pub fn in_place_commit(&mut self) -> Result<(), StorageError> {
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

    pub fn commit(mut self) -> Result<(), StorageError> {
        self.in_place_commit()
    }

    /// Remove all pending operations from this instance and reset the transaction information.
    ///
    /// This is roughly equivalent to `discard`ing the current instance and creating a new one,
    /// only that it occurs in-place.
    pub fn reset(&mut self) {
        let Self { clock_created, state_update, operations } = self;
        let state_update_state = state_update.state.lock();
        operations.clear();
        *clock_created = state_update_state.max_clock;
    }

    pub fn discard(self) {
        drop(self)
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
    use crate::trie::{OptimizedValueRef, ValueAccessToken};
    use near_primitives::errors::StorageError;
    use near_primitives::state::ValueRef;
    use std::any::Any;
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
        fn arc(&self) -> Arc<dyn Deserialized>;
    }

    /// A value that can be eventually written out to the database.
    ///
    // FIXME: seal this so that implementing this is only possible in this module.
    pub trait Deserializable {
        fn deserialize(bytes: Vec<u8>) -> Result<Arc<Self>, StorageError>;
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
                fn arc(&self) -> Arc<dyn Deserialized> {
                    Arc::new(self.clone())
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
        fn arc(&self) -> Arc<dyn Deserialized> {
            Arc::new(self.clone())
        }
    }
    impl Deserializable for Vec<u8> {
        fn deserialize(bytes: Vec<u8>) -> Result<Arc<Self>, StorageError> {
            Ok(Arc::new(bytes))
        }
    }
}
