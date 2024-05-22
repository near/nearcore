use super::arena::{ArenaSlice, ArenaSliceMut};

pub mod children;
pub mod encoding;
pub mod extension;
pub mod value;

/// This trait simplifies our programming against flexibly-sized structures.
/// A flexibly-sized structure is one whose memory layout depends on runtime
/// values. For example, we may use a flexibly-sized structure to encode two
/// variable-sized strings, like this:
///
///   0                4                 8         8+str1len    8+str1len+str2len
///   |  str1 length   |   str2 length   |    str1    |     str2    |
///
/// Why do we do this? Because this is a lot more memory-efficient than the
/// alternative of storing two heap-allocated Strings; it is a single memory
/// allocation rather than three, and it saves the need to encode two pointers.
///
/// In this string example, these two strings would each define a header struct,
/// say EncodedStringHeader, that implements FlexibleDataHeader. The struct
/// itself only contains the fixed-size part, i.e. the length of the string.
/// Then, we implement encode_flexible_data and decode_flexible_data to specify
/// how the flexibly-sized part of the data is encoded and decoded. These, of
/// course need to be consistent with each other.
///
/// This trait allows us to then encode and decode a flexibly-sized structure
/// with multiple flexibly-sized parts with relative ease.
pub trait FlexibleDataHeader {
    /// The type of the original form of data to be used for encoding.
    type InputData: ?Sized;
    /// The type of a view of the decoded data, which may reference the memory
    /// that we are decoding from, and therefore having a lifetime.
    type View<'a>;

    /// Derives the header (fixed-size part) from the original data.
    fn from_input(data: &Self::InputData) -> Self;

    /// Calculates the length of the flexibly-sized part of the data.
    /// This is used to allocate the right amount of memory for the containing
    /// flexibly-sized structure.
    fn flexible_data_length(&self) -> usize;

    /// Encodes the flexibly-sized part of the data into the given memory
    /// slice. This function must be implemented in a way that writes
    /// exactly `self.flexible_data_length()` bytes to the given memory
    /// slice. The caller must ensure that the memory slice is large enough.
    fn encode_flexible_data(&self, data: &Self::InputData, target: &mut ArenaSliceMut<'_>);

    /// Decodes the flexibly-sized part of the data from the given memory
    /// slice. This function must be implemented in a consistent manner
    /// with `encode_flexible_data`. It, of course, must only read
    /// `self.flexible_data_length()` bytes from the given memory slice,
    /// and the caller must ensure that the memory slice is the same one
    /// that was used to encode the data. The returned View has the same
    /// lifetime as the memory slice, and so may reference data from it.
    fn decode_flexible_data<'a>(&self, source: &ArenaSlice<'a>) -> Self::View<'a>;
}
