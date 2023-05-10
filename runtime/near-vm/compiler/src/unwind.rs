//! A `CompiledFunctionUnwindInfo` contains the function unwind information.
//!
//! The unwind information is used to determine which function
//! called the function that threw the exception, and which
//! function called that one, and so forth.
//!
//! [Learn more](https://en.wikipedia.org/wiki/Call_stack).
use crate::lib::std::vec::Vec;

/// Compiled function unwind information.
///
/// > Note: Windows OS have a different way of representing the [unwind info],
/// > That's why we keep the Windows data and the Unix frame layout in different
/// > fields.
///
/// [unwind info]: https://docs.microsoft.com/en-us/cpp/build/exception-handling-x64?view=vs-2019
#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Debug, Clone, PartialEq, Eq)]
pub enum CompiledFunctionUnwindInfo {
    /// Windows UNWIND_INFO.
    WindowsX64(Vec<u8>),

    /// The unwind info is added to the Dwarf section in `Compilation`.
    Dwarf,
}

/// See [`CompiledFunctionUnwindInfo`].
#[derive(Clone, Copy)]
pub enum CompiledFunctionUnwindInfoRef<'a> {
    /// Windows UNWIND_INFO.
    WindowsX64(&'a [u8]),
    /// Unwind info is added to the Dwarf section in `Compilation`.
    Dwarf,
}

impl<'a> From<&'a CompiledFunctionUnwindInfo> for CompiledFunctionUnwindInfoRef<'a> {
    fn from(uw: &'a CompiledFunctionUnwindInfo) -> Self {
        match uw {
            CompiledFunctionUnwindInfo::WindowsX64(d) => {
                CompiledFunctionUnwindInfoRef::WindowsX64(d)
            }
            CompiledFunctionUnwindInfo::Dwarf => CompiledFunctionUnwindInfoRef::Dwarf,
        }
    }
}

impl<'a> From<&'a ArchivedCompiledFunctionUnwindInfo> for CompiledFunctionUnwindInfoRef<'a> {
    fn from(uw: &'a ArchivedCompiledFunctionUnwindInfo) -> Self {
        match uw {
            ArchivedCompiledFunctionUnwindInfo::WindowsX64(d) => {
                CompiledFunctionUnwindInfoRef::WindowsX64(d)
            }
            ArchivedCompiledFunctionUnwindInfo::Dwarf => CompiledFunctionUnwindInfoRef::Dwarf,
        }
    }
}
