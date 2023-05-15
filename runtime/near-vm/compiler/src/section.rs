//! This module define the required structures to emit custom
//! Sections in a `Compilation`.
//!
//! The functions that access a custom [`CustomSection`] would need
//! to emit a custom relocation: `RelocationTarget::CustomSection`, so
//! it can be patched later by the engine (native or JIT).

use crate::lib::std::vec::Vec;
use crate::Relocation;
use near_vm_types::entity::entity_impl;

/// Index type of a Section defined inside a WebAssembly `Compilation`.
#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
#[archive_attr(derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug))]
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct SectionIndex(u32);

entity_impl!(SectionIndex);

entity_impl!(ArchivedSectionIndex);

/// Custom section Protection.
///
/// Determines how a custom section may be used.
#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Debug, Copy, Clone, PartialEq, Eq)]
pub enum CustomSectionProtection {
    /// A custom section with read permission.
    Read,

    /// A custom section with read and execute permissions.
    ReadExecute,
}

/// A Section for a `Compilation`.
///
/// This is used so compilers can store arbitrary information
/// in the emitted module.
#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Debug, Clone, PartialEq, Eq)]
pub struct CustomSection {
    /// Memory protection that applies to this section.
    pub protection: CustomSectionProtection,

    /// The bytes corresponding to this section.
    ///
    /// > Note: These bytes have to be at-least 8-byte aligned
    /// > (the start of the memory pointer).
    /// > We might need to create another field for alignment in case it's
    /// > needed in the future.
    pub bytes: SectionBody,

    /// Relocations that apply to this custom section.
    pub relocations: Vec<Relocation>,
}

/// See [`CustomSection`].
///
/// Note that this does not reference the relocation data.
#[derive(Clone, Copy)]
pub struct CustomSectionRef<'a> {
    /// See [`CustomSection::protection`].
    pub protection: CustomSectionProtection,

    /// See [`CustomSection::bytes`].
    pub bytes: &'a [u8],
}

impl<'a> From<&'a CustomSection> for CustomSectionRef<'a> {
    fn from(section: &'a CustomSection) -> Self {
        CustomSectionRef { protection: section.protection, bytes: section.bytes.as_slice() }
    }
}

impl<'a> From<&'a ArchivedCustomSection> for CustomSectionRef<'a> {
    fn from(section: &'a ArchivedCustomSection) -> Self {
        CustomSectionRef {
            protection: Result::<_, std::convert::Infallible>::unwrap(
                rkyv::Deserialize::deserialize(&section.protection, &mut rkyv::Infallible),
            ),
            bytes: &section.bytes.0[..],
        }
    }
}

/// The bytes in the section.
#[derive(
    rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Debug, Clone, PartialEq, Eq, Default,
)]
pub struct SectionBody(Vec<u8>);

impl SectionBody {
    /// Create a new section body with the given contents.
    pub fn new_with_vec(contents: Vec<u8>) -> Self {
        Self(contents)
    }

    /// Returns a raw pointer to the section's buffer.
    pub fn as_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }

    /// Returns the length of this section in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Dereferences into the section's buffer.
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Returns whether or not the section body is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
