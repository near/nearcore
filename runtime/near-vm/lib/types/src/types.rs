use crate::indexes::{FunctionIndex, GlobalIndex};
use crate::lib::std::fmt;
use crate::lib::std::format;
use crate::lib::std::string::{String, ToString};
use crate::lib::std::vec::Vec;
use crate::units::Pages;
use crate::values::{Value, WasmValueType};
use std::cell::UnsafeCell;
use std::rc::Rc;
use std::sync::Arc;

// Type Representations

// Value Types

/// A list of all possible value types in WebAssembly.
#[derive(
    Copy, Debug, Clone, Eq, PartialEq, Hash, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive,
)]
#[archive(as = "Self")]
pub enum Type {
    /// Signed 32 bit integer.
    I32,
    /// Signed 64 bit integer.
    I64,
    /// Floating point 32 bit integer.
    F32,
    /// Floating point 64 bit integer.
    F64,
    /// A 128 bit number.
    V128,
    /// A reference to opaque data in the Wasm instance.
    ExternRef, /* = 128 */
    /// A reference to a Wasm function.
    FuncRef,
}

impl Type {
    /// Returns true if `Type` matches any of the numeric types. (e.g. `I32`,
    /// `I64`, `F32`, `F64`, `V128`).
    pub fn is_num(self) -> bool {
        matches!(self, Self::I32 | Self::I64 | Self::F32 | Self::F64 | Self::V128)
    }

    /// Returns true if `Type` matches either of the reference types.
    pub fn is_ref(self) -> bool {
        matches!(self, Self::ExternRef | Self::FuncRef)
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Hash, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive,
)]
#[archive(as = "Self")]
/// The WebAssembly V128 type
pub struct V128(pub(crate) [u8; 16]);

impl V128 {
    /// Get the bytes corresponding to the V128 value
    pub fn bytes(&self) -> &[u8; 16] {
        &self.0
    }
    /// Iterate over the bytes in the constant.
    pub fn iter(&self) -> impl Iterator<Item = &u8> {
        self.0.iter()
    }

    /// Convert the immediate into a vector.
    pub fn to_vec(self) -> Vec<u8> {
        self.0.to_vec()
    }

    /// Convert the immediate into a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0[..]
    }
}

impl From<[u8; 16]> for V128 {
    fn from(array: [u8; 16]) -> Self {
        Self(array)
    }
}

impl From<&[u8]> for V128 {
    fn from(slice: &[u8]) -> Self {
        assert_eq!(slice.len(), 16);
        let mut buffer = [0; 16];
        buffer.copy_from_slice(slice);
        Self(buffer)
    }
}

// External Types

/// A list of all possible types which can be externally referenced from a
/// WebAssembly module.
///
/// This list can be found in [`ImportType`] or [`ExportType`], so these types
/// can either be imported or exported.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExternType {
    /// This external type is the type of a WebAssembly function.
    Function(FunctionType),
    /// This external type is the type of a WebAssembly global.
    Global(GlobalType),
    /// This external type is the type of a WebAssembly table.
    Table(TableType),
    /// This external type is the type of a WebAssembly memory.
    Memory(MemoryType),
}

macro_rules! accessors {
    ($(($variant:ident($ty:ty) $get:ident $unwrap:ident))*) => ($(
        /// Attempt to return the underlying type of this external type,
        /// returning `None` if it is a different type.
        pub fn $get(&self) -> Option<&$ty> {
            if let Self::$variant(e) = self {
                Some(e)
            } else {
                None
            }
        }

        /// Returns the underlying descriptor of this [`ExternType`], panicking
        /// if it is a different type.
        ///
        /// # Panics
        ///
        /// Panics if `self` is not of the right type.
        pub fn $unwrap(&self) -> &$ty {
            self.$get().expect(concat!("expected ", stringify!($ty)))
        }
    )*)
}

impl ExternType {
    accessors! {
        (Function(FunctionType) func unwrap_func)
        (Global(GlobalType) global unwrap_global)
        (Table(TableType) table unwrap_table)
        (Memory(MemoryType) memory unwrap_memory)
    }
}

// TODO: `shrink_to_fit` these or change it to `Box<[Type]>` if not using
// Cow or something else
/// The signature of a function that is either implemented
/// in a Wasm module or exposed to Wasm by the host.
///
/// WebAssembly functions can have 0 or more parameters and results.
#[derive(Debug, Clone, PartialEq, Eq, Hash, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct FunctionType {
    /// The parameters of the function
    params: Arc<[Type]>,
    /// The return values of the function
    results: Arc<[Type]>,
}

impl FunctionType {
    /// Creates a new Function Type with the given parameter and return types.
    pub fn new<Params, Returns>(params: Params, returns: Returns) -> Self
    where
        Params: Into<Arc<[Type]>>,
        Returns: Into<Arc<[Type]>>,
    {
        Self { params: params.into(), results: returns.into() }
    }

    /// Parameter types.
    pub fn params(&self) -> &[Type] {
        &self.params
    }

    /// Return types.
    pub fn results(&self) -> &[Type] {
        &self.results
    }
}

impl fmt::Display for FunctionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let params = self.params.iter().map(|p| format!("{:?}", p)).collect::<Vec<_>>().join(", ");
        let results =
            self.results.iter().map(|p| format!("{:?}", p)).collect::<Vec<_>>().join(", ");
        write!(f, "[{}] -> [{}]", params, results)
    }
}

// Macro needed until https://rust-lang.github.io/rfcs/2000-const-generics.html is stable.
// See https://users.rust-lang.org/t/how-to-implement-trait-for-fixed-size-array-of-any-size/31494
macro_rules! implement_from_pair_to_functiontype {
    ($($N:literal,$M:literal)+) => {
        $(
            impl From<([Type; $N], [Type; $M])> for FunctionType {
                fn from(pair: ([Type; $N], [Type; $M])) -> Self {
                    Self::new(&pair.0[..], &pair.1[..])
                }
            }
        )+
    }
}

implement_from_pair_to_functiontype! {
    0,0 0,1 0,2 0,3 0,4 0,5 0,6 0,7 0,8 0,9
    1,0 1,1 1,2 1,3 1,4 1,5 1,6 1,7 1,8 1,9
    2,0 2,1 2,2 2,3 2,4 2,5 2,6 2,7 2,8 2,9
    3,0 3,1 3,2 3,3 3,4 3,5 3,6 3,7 3,8 3,9
    4,0 4,1 4,2 4,3 4,4 4,5 4,6 4,7 4,8 4,9
    5,0 5,1 5,2 5,3 5,4 5,5 5,6 5,7 5,8 5,9
    6,0 6,1 6,2 6,3 6,4 6,5 6,6 6,7 6,8 6,9
    7,0 7,1 7,2 7,3 7,4 7,5 7,6 7,7 7,8 7,9
    8,0 8,1 8,2 8,3 8,4 8,5 8,6 8,7 8,8 8,9
    9,0 9,1 9,2 9,3 9,4 9,5 9,6 9,7 9,8 9,9
}

impl From<&Self> for FunctionType {
    fn from(as_ref: &Self) -> Self {
        as_ref.clone()
    }
}

/// Borrowed version of [`FunctionType`].
pub struct FunctionTypeRef<'a> {
    /// The parameters of the function
    params: &'a [Type],
    /// The return values of the function
    results: &'a [Type],
}

impl<'a> FunctionTypeRef<'a> {
    /// Create a new temporary function type.
    pub fn new(params: &'a [Type], results: &'a [Type]) -> Self {
        Self { params, results }
    }

    /// Parameter types.
    pub fn params(&self) -> &[Type] {
        self.params
    }

    /// Return types.
    pub fn results(&self) -> &[Type] {
        self.results
    }
}

impl<'a> From<&'a FunctionType> for FunctionTypeRef<'a> {
    fn from(FunctionType { params, results }: &'a FunctionType) -> Self {
        Self { params, results }
    }
}

impl<'a> From<&'a ArchivedFunctionType> for FunctionTypeRef<'a> {
    fn from(ArchivedFunctionType { params, results }: &'a ArchivedFunctionType) -> Self {
        Self { params: &**params, results: &**results }
    }
}

/// Indicator of whether a global is mutable or not
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive,
)]
#[archive(as = "Self")]
pub enum Mutability {
    /// The global is constant and its value does not change
    Const,
    /// The value of the global can change over time
    Var,
}

impl Mutability {
    /// Returns a boolean indicating if the enum is set to mutable.
    pub fn is_mutable(self) -> bool {
        match self {
            Self::Const => false,
            Self::Var => true,
        }
    }
}

/// WebAssembly global.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive,
)]
#[archive(as = "Self")]
pub struct GlobalType {
    /// The type of the value stored in the global.
    pub ty: Type,
    /// A flag indicating whether the value may change at runtime.
    pub mutability: Mutability,
}

// Global Types

/// A WebAssembly global descriptor.
///
/// This type describes an instance of a global in a WebAssembly
/// module. Globals are local to an `Instance` and are either
/// immutable or mutable.
impl GlobalType {
    /// Create a new Global variable
    /// # Usage:
    /// ```
    /// use near_vm_types::{GlobalType, Type, Mutability, Value};
    ///
    /// // An I32 constant global
    /// let global = GlobalType::new(Type::I32, Mutability::Const);
    /// // An I64 mutable global
    /// let global = GlobalType::new(Type::I64, Mutability::Var);
    /// ```
    pub fn new(ty: Type, mutability: Mutability) -> Self {
        Self { ty, mutability }
    }
}

impl fmt::Display for GlobalType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mutability = match self.mutability {
            Mutability::Const => "constant",
            Mutability::Var => "mutable",
        };
        write!(f, "{} ({})", self.ty, mutability)
    }
}

/// Globals are initialized via the `const` operators or by referring to another import.
#[derive(Debug, Clone, Copy, PartialEq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
#[archive(as = "Self")]
pub enum GlobalInit {
    /// An `i32.const`.
    I32Const(i32),
    /// An `i64.const`.
    I64Const(i64),
    /// An `f32.const`.
    F32Const(f32),
    /// An `f64.const`.
    F64Const(f64),
    /// A `v128.const`.
    V128Const(V128),
    /// A `global.get` of another global.
    GetGlobal(GlobalIndex),
    // TODO(reftypes): `ref.null func` and `ref.null extern` seem to be 2 different
    // things: we need to handle both. Perhaps this handled in context by the
    // global knowing its own type?
    /// A `ref.null`.
    RefNullConst,
    /// A `ref.func <index>`.
    RefFunc(FunctionIndex),
}

impl Eq for GlobalInit {}

impl GlobalInit {
    /// Get the `GlobalInit` from a given `Value`
    pub fn from_value<T: WasmValueType>(value: Value<T>) -> Self {
        match value {
            Value::I32(i) => Self::I32Const(i),
            Value::I64(i) => Self::I64Const(i),
            Value::F32(f) => Self::F32Const(f),
            Value::F64(f) => Self::F64Const(f),
            _ => unimplemented!("GlobalInit from_value for {:?}", value),
        }
    }
    /// Get the `Value` from the Global init value
    pub fn to_value<T: WasmValueType>(&self) -> Value<T> {
        match self {
            Self::I32Const(i) => Value::I32(*i),
            Self::I64Const(i) => Value::I64(*i),
            Self::F32Const(f) => Value::F32(*f),
            Self::F64Const(f) => Value::F64(*f),
            _ => unimplemented!("GlobalInit to_value for {:?}", self),
        }
    }
}

// Table Types

/// A descriptor for a table in a WebAssembly module.
///
/// Tables are contiguous chunks of a specific element, typically a `funcref` or
/// an `externref`. The most common use for tables is a function table through
/// which `call_indirect` can invoke other functions.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive,
)]
pub struct TableType {
    /// The type of data stored in elements of the table.
    pub ty: Type,
    /// The minimum number of elements in the table.
    pub minimum: u32,
    /// The maximum number of elements in the table.
    pub maximum: Option<u32>,
}

impl TableType {
    /// Creates a new table descriptor which will contain the specified
    /// `element` and have the `limits` applied to its length.
    pub fn new(ty: Type, minimum: u32, maximum: Option<u32>) -> Self {
        Self { ty, minimum, maximum }
    }
}

impl fmt::Display for TableType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(maximum) = self.maximum {
            write!(f, "{} ({}..{})", self.ty, self.minimum, maximum)
        } else {
            write!(f, "{} ({}..)", self.ty, self.minimum)
        }
    }
}

// Memory Types

/// A descriptor for a WebAssembly memory type.
///
/// Memories are described in units of pages (64KB) and represent contiguous
/// chunks of addressable memory.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive,
)]
pub struct MemoryType {
    /// The minimum number of pages in the memory.
    pub minimum: Pages,
    /// The maximum number of pages in the memory.
    pub maximum: Option<Pages>,
    /// Whether the memory may be shared between multiple threads.
    pub shared: bool,
}

impl MemoryType {
    /// Creates a new descriptor for a WebAssembly memory given the specified
    /// limits of the memory.
    pub fn new<IntoPages>(minimum: IntoPages, maximum: Option<IntoPages>, shared: bool) -> Self
    where
        IntoPages: Into<Pages>,
    {
        Self { minimum: minimum.into(), maximum: maximum.map(Into::into), shared }
    }
}

impl fmt::Display for MemoryType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let shared = if self.shared { "shared" } else { "not shared" };
        if let Some(maximum) = self.maximum {
            write!(f, "{} ({:?}..{:?})", shared, self.minimum, maximum)
        } else {
            write!(f, "{} ({:?}..)", shared, self.minimum)
        }
    }
}

// Import Types

/// A descriptor for an imported value into a wasm module.
///
/// This type is primarily accessed from the `Module::imports`
/// API. Each `ImportType` describes an import into the wasm module
/// with the module/name that it's imported from as well as the type
/// of item that's being imported.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Import<S = String, T = ExternType> {
    module: S,
    name: S,
    index: u32,
    ty: T,
}

impl<S: AsRef<str>, T> Import<S, T> {
    /// Creates a new import descriptor which comes from `module` and `name` and
    /// is of type `ty`.
    pub fn new(module: S, name: S, index: u32, ty: T) -> Self {
        Self { module, name, index, ty }
    }

    /// Returns the module name that this import is expected to come from.
    pub fn module(&self) -> &str {
        self.module.as_ref()
    }

    /// Returns the field name of the module that this import is expected to
    /// come from.
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    /// The index of the import in the module.
    pub fn index(&self) -> u32 {
        self.index
    }

    /// Returns the expected type of this import.
    pub fn ty(&self) -> &T {
        &self.ty
    }
}

// Export Types

/// A descriptor for an exported WebAssembly value.
///
/// This type is primarily accessed from the `Module::exports`
/// accessor and describes what names are exported from a wasm module
/// and the type of the item that is exported.
///
/// The `<T>` refefers to `ExternType`, however it can also refer to use
/// `MemoryType`, `TableType`, `FunctionType` and `GlobalType` for ease of
/// use.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExportType<T = ExternType> {
    name: String,
    ty: T,
}

impl<T> ExportType<T> {
    /// Creates a new export which is exported with the given `name` and has the
    /// given `ty`.
    pub fn new(name: &str, ty: T) -> Self {
        Self { name: name.to_string(), ty }
    }

    /// Returns the name by which this export is known by.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of this export.
    pub fn ty(&self) -> &T {
        &self.ty
    }
}

/// Fast gas counter with very simple structure, could be exposed to compiled code in the VM. For
/// instance by intrinsifying host functions responsible for gas metering.

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FastGasCounter {
    /// The following three fields must be put next to another to make sure
    /// generated gas counting code can use and adjust them.
    /// We will share counter to ensure we never miss synchronization.
    /// This could change and in such a case synchronization required between compiled WASM code
    /// and the host code.

    /// The amount of gas that was irreversibly used for contract execution.
    pub burnt_gas: u64,
    /// Hard gas limit for execution
    pub gas_limit: u64,
}

impl FastGasCounter {
    /// New fast gas counter.
    pub fn new(limit: u64) -> Self {
        Self { burnt_gas: 0, gas_limit: limit }
    }
    /// Amount of gas burnt, maybe load as atomic to avoid aliasing issues.
    pub fn burnt(&self) -> u64 {
        self.burnt_gas
    }
}

impl fmt::Display for FastGasCounter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "burnt: {} limit: {} ", self.burnt(), self.gas_limit,)
    }
}

/// External configuration of execution environment for Instance.
#[derive(Clone)]
pub struct InstanceConfig {
    /// External gas counter pointer.
    pub gas_counter: *mut FastGasCounter,
    default_gas_counter: Option<Rc<UnsafeCell<FastGasCounter>>>,
    /// Stack limit, in 8-byte slots.
    pub stack_limit: u32,
}

impl InstanceConfig {
    /// Create default instance configuration.
    pub fn with_stack_limit(stack_limit: u32) -> Self {
        let result = Rc::new(UnsafeCell::new(FastGasCounter { burnt_gas: 0, gas_limit: u64::MAX }));
        Self { gas_counter: result.get(), default_gas_counter: Some(result), stack_limit }
    }

    /// Create instance configuration with an external gas counter, unsafe as it creates
    /// an alias on raw memory of gas_counter. This memory could be accessed until
    /// instance configured with this `InstanceConfig` exists.
    pub unsafe fn with_counter(mut self, gas_counter: *mut FastGasCounter) -> Self {
        self.gas_counter = gas_counter;
        self.default_gas_counter = None;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VOID_TO_VOID: ([Type; 0], [Type; 0]) = ([], []);
    const I32_I32_TO_VOID: ([Type; 2], [Type; 0]) = ([Type::I32, Type::I32], []);
    const V128_I64_TO_I32: ([Type; 2], [Type; 1]) = ([Type::V128, Type::I64], [Type::I32]);
    const NINE_V128_TO_NINE_I32: ([Type; 9], [Type; 9]) = ([Type::V128; 9], [Type::I32; 9]);

    #[test]
    fn convert_tuple_to_functiontype() {
        let ty: FunctionType = VOID_TO_VOID.into();
        assert_eq!(ty.params().len(), 0);
        assert_eq!(ty.results().len(), 0);

        let ty: FunctionType = I32_I32_TO_VOID.into();
        assert_eq!(ty.params().len(), 2);
        assert_eq!(ty.params()[0], Type::I32);
        assert_eq!(ty.params()[1], Type::I32);
        assert_eq!(ty.results().len(), 0);

        let ty: FunctionType = V128_I64_TO_I32.into();
        assert_eq!(ty.params().len(), 2);
        assert_eq!(ty.params()[0], Type::V128);
        assert_eq!(ty.params()[1], Type::I64);
        assert_eq!(ty.results().len(), 1);
        assert_eq!(ty.results()[0], Type::I32);

        let ty: FunctionType = NINE_V128_TO_NINE_I32.into();
        assert_eq!(ty.params().len(), 9);
        assert_eq!(ty.results().len(), 9);
    }
}
