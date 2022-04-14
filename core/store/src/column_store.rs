use std::io;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::db::DBTransaction;
use crate::{DBCol, Store};

pub trait Column {
    const RAW: DBCol;
    type Val: BorshSerialize + BorshDeserialize;
}

pub struct ColumnStore<CS> {
    raw: Store,
    _ghost: PhantomData<CS>,
}

// Note: ColumnStore is intentionally !Clone. We use Rust's ownership rule to
// mediate access to the state and avoid db-level data races.
//
// impl<CS> !Clone for ColumnStore<CS> {}

pub struct ColumnStoreUpdate<CS> {
    transaction: DBTransaction,
    _ghost: PhantomData<CS>,
}

pub trait ColumnSet {
    const COLUMNS: ColBitSet;
}

impl<CS: ColumnSet> ColumnStore<CS> {
    pub fn split(raw: &Store) -> Self {
        if raw.unclaimed_cols.claim(CS::COLUMNS).is_err() {
            panic!("failed to claim columns")
        }
        let raw = raw.clone();
        Self { raw, _ghost: PhantomData }
    }

    pub fn get<C>(&self, key: &[u8]) -> io::Result<Option<C::Val>>
    where
        C: Column,
    {
        match self.get_bytes::<C>(key)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(BorshDeserialize::try_from_slice(&bytes)?)),
        }
    }
    pub fn get_bytes<C>(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>>
    where
        C: Column,
    {
        let () = AssertHasColumn::<CS, C>::OK;
        self.raw.get_unchecked(C::RAW, key)
    }

    pub fn update(&self) -> ColumnStoreUpdate<CS> {
        ColumnStoreUpdate { transaction: DBTransaction::default(), _ghost: PhantomData }
    }

    pub fn commit(&mut self, update: ColumnStoreUpdate<CS>) -> io::Result<()> {
        self.raw.storage.write(update.transaction)?;
        Ok(())
    }
}

impl<CS: ColumnSet> ColumnStoreUpdate<CS> {
    pub fn set<C>(&mut self, key: &[u8], value: &C::Val)
    where
        C: Column,
    {
        let buf = value
            .try_to_vec()
            .unwrap_or_else(|err| panic!("failed to borsh-serialize a DB object: {err}"));
        self.set_bytes::<C>(key, buf)
    }
    pub fn set_bytes<C>(&mut self, key: &[u8], value: Vec<u8>)
    where
        C: Column,
    {
        let () = AssertHasColumn::<CS, C>::OK;
        self.transaction.insert(C::RAW, key.to_vec(), value)
    }
    pub fn delete<C>(&mut self, key: &[u8])
    where
        C: Column,
    {
        let () = AssertHasColumn::<CS, C>::OK;
        self.transaction.delete(C::RAW, key.to_vec())
    }
}

struct AssertHasColumn<CS: ColumnSet, C: Column>(CS, C);

impl<CS: ColumnSet, C: Column> AssertHasColumn<CS, C> {
    const OK: () = [()][!CS::COLUMNS.contains(C::RAW) as usize];
}

#[macro_export]
macro_rules! define_columns {
    ($($col:ident: $val_ty:ty),*$(,)?) => {$(
        pub enum $col {}
        impl $crate::column_store::Column for $col {
            const RAW: $crate::DBCol = $crate::DBCol::$col;
            type Val = $val_ty;
        }
    )*};
}

#[macro_export]
macro_rules! define_column_set {
    ($set:ident { $($col:ident),* $(,)?}) => {
        pub enum $set {}
        impl $crate::column_store::ColumnSet for $set {
            const COLUMNS: $crate::column_store::ColBitSet =
                $crate::column_store::ColBitSet::new(&[$($col::RAW),*]);
        }
    };
}

#[derive(Clone, Copy)]
pub struct ColBitSet {
    repr: u64,
}

impl ColBitSet {
    pub const fn new(cols: &[DBCol]) -> ColBitSet {
        let mut repr = 0;
        let mut i = 0;
        while i < cols.len() {
            repr |= ColBitSet::mask(cols[i]);
            i += 1;
        }
        ColBitSet { repr }
    }
    pub const fn contains(&self, col: DBCol) -> bool {
        let mask = ColBitSet::mask(col);
        self.repr & mask == mask
    }
    const fn mask(col: DBCol) -> u64 {
        1 << (col as u64)
    }
}

#[derive(Clone)]
pub(crate) struct AtomicColBitSet {
    repr: Arc<AtomicU64>,
}

impl Default for AtomicColBitSet {
    fn default() -> Self {
        Self { repr: Arc::new(AtomicU64::new(!0)) }
    }
}

impl AtomicColBitSet {
    pub(crate) fn contains(&self, col: DBCol) -> bool {
        let mask = ColBitSet::mask(col);
        self.repr.load(Ordering::SeqCst) & mask == mask
    }

    pub(crate) fn claim(&self, other: ColBitSet) -> Result<(), ()> {
        loop {
            let repr = self.repr.load(Ordering::SeqCst);
            if repr & other.repr != other.repr {
                return Err(());
            }
            let new_repr = repr & !other.repr;
            if self
                .repr
                .compare_exchange(repr, new_repr, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Ok(());
            }
        }
    }
}
