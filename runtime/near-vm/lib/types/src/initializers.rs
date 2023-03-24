use crate::indexes::{FunctionIndex, GlobalIndex, MemoryIndex, TableIndex};
use crate::lib::std::boxed::Box;

/// A WebAssembly table initializer.
#[derive(Clone, Debug, Hash, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct OwnedTableInitializer {
    /// The index of a table to initialize.
    pub table_index: TableIndex,
    /// Optionally, a global variable giving a base index.
    pub base: Option<GlobalIndex>,
    /// The offset to add to the base.
    pub offset: usize,
    /// The values to write into the table elements.
    pub elements: Box<[FunctionIndex]>,
}

/// A memory index and offset within that memory where a data initialization
/// should be performed.
#[derive(Clone, Debug, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct DataInitializerLocation {
    /// The index of the memory to initialize.
    pub memory_index: MemoryIndex,

    /// Optionally a Global variable base to initialize at.
    pub base: Option<GlobalIndex>,

    /// A constant offset to initialize at.
    pub offset: usize,
}

/// A data initializer for linear memory.
#[derive(Debug)]
pub struct DataInitializer<'data> {
    /// The location where the initialization is to be performed.
    pub location: DataInitializerLocation,

    /// The initialization data.
    pub data: &'data [u8],
}

/// As `DataInitializer` but owning the data rather than
/// holding a reference to it
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct OwnedDataInitializer {
    /// The location where the initialization is to be performed.
    pub location: DataInitializerLocation,

    /// The initialization owned data.
    pub data: Vec<u8>,
}

impl OwnedDataInitializer {
    /// Creates a new `OwnedDataInitializer` from a `DataInitializer`.
    pub fn new(borrowed: &DataInitializer<'_>) -> Self {
        Self { location: borrowed.location.clone(), data: borrowed.data.to_vec() }
    }
}

impl<'a> From<&'a OwnedDataInitializer> for DataInitializer<'a> {
    fn from(init: &'a OwnedDataInitializer) -> Self {
        DataInitializer { location: init.location.clone(), data: &*init.data }
    }
}

impl<'a> From<&'a ArchivedOwnedDataInitializer> for DataInitializer<'a> {
    fn from(init: &'a ArchivedOwnedDataInitializer) -> Self {
        DataInitializer {
            location: rkyv::Deserialize::deserialize(&init.location, &mut rkyv::Infallible)
                .expect("deserialization cannot fail"),
            data: &*init.data,
        }
    }
}

impl<'a> From<DataInitializer<'a>> for OwnedDataInitializer {
    fn from(init: DataInitializer<'a>) -> Self {
        Self { location: init.location.clone(), data: init.data.to_vec() }
    }
}
