use near_primitives::hash::CryptoHash;
use near_primitives::state::{FlatStateValue, ValueRef};

use super::FlexibleDataHeader;

/// Flexibly-sized data header for a trie value, representing either an inline
/// value, or a reference to a value stored in the State column.
///
/// The flexible part of the data is either the inlined value as a byte array,
/// or a CryptoHash representing the reference hash.
#[repr(C, packed(1))]
#[derive(Clone, Copy)]
pub struct EncodedValueHeader {
    // The high bit is 1 if the value is inlined, 0 if it is a reference.
    // The lower bits are the length of the value.
    length_and_inlined: u32,
}

impl EncodedValueHeader {
    const INLINED_MASK: u32 = 0x80000000;

    fn decode(&self) -> (u32, bool) {
        (
            self.length_and_inlined & !Self::INLINED_MASK,
            self.length_and_inlined & Self::INLINED_MASK != 0,
        )
    }
}

impl FlexibleDataHeader for EncodedValueHeader {
    type InputData = FlatStateValue;
    type View<'a> = ValueView<'a>;

    fn from_input(value: &FlatStateValue) -> Self {
        match value {
            FlatStateValue::Ref(value_ref) => {
                EncodedValueHeader { length_and_inlined: value_ref.length }
            }
            FlatStateValue::Inlined(v) => {
                assert!(v.len() as u32 & Self::INLINED_MASK == 0);
                EncodedValueHeader { length_and_inlined: Self::INLINED_MASK | v.len() as u32 }
            }
        }
    }

    fn flexible_data_length(&self) -> usize {
        let (length, inlined) = self.decode();
        if inlined {
            length as usize
        } else {
            std::mem::size_of::<CryptoHash>()
        }
    }

    unsafe fn encode_flexible_data(&self, value: FlatStateValue, ptr: *mut u8) {
        let (length, inlined) = self.decode();
        match value {
            FlatStateValue::Ref(value_ref) => {
                assert!(!inlined);
                *(ptr as *mut CryptoHash) = value_ref.hash;
            }
            FlatStateValue::Inlined(v) => {
                assert!(inlined);
                std::ptr::copy_nonoverlapping(v.as_ptr(), ptr, length as usize);
            }
        }
    }

    unsafe fn decode_flexible_data<'a>(&'a self, ptr: *const u8) -> ValueView<'a> {
        let (length, inlined) = self.decode();
        if inlined {
            ValueView::Inlined(std::slice::from_raw_parts(ptr, length as usize))
        } else {
            ValueView::Ref { length, hash: &*(ptr as *const CryptoHash) }
        }
    }

    unsafe fn drop_flexible_data(&self, _ptr: *mut u8) {}
}

// Efficient view of the encoded value.
#[derive(Debug, Clone)]
pub enum ValueView<'a> {
    Ref { length: u32, hash: &'a CryptoHash },
    Inlined(&'a [u8]),
}

impl<'a> ValueView<'a> {
    pub fn to_flat_value(self) -> FlatStateValue {
        match self {
            Self::Ref { length, hash } => FlatStateValue::Ref(ValueRef { length, hash: *hash }),
            Self::Inlined(data) => FlatStateValue::Inlined(data.to_vec()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Ref { length, .. } => *length as usize,
            Self::Inlined(data) => data.len(),
        }
    }
}
