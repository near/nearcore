use near_vm_errors::{HostError, VMLogicError};

use crate::MemoryLike;

type Result<T> = ::std::result::Result<T, VMLogicError>;

/// Uses `,` separator to split `method_names` into a vector of method names.
/// Returns an empty vec if the empty slice is given.
/// Throws `HostError::EmptyMethodName` in case there is an empty method name inside.
pub(crate) fn split_method_names(method_names: &[u8]) -> Result<Vec<Vec<u8>>> {
    if method_names.is_empty() {
        Ok(vec![])
    } else {
        method_names
            .split(|c| *c == b',')
            .map(
                |v| {
                    if v.is_empty() {
                        Err(HostError::EmptyMethodName.into())
                    } else {
                        Ok(v.to_vec())
                    }
                },
            )
            .collect::<Result<Vec<_>>>()
    }
}

impl<'a> MemoryLike for &'a mut [u8] {
    fn fits_memory(&self, offset: u64, len: u64) -> bool {
        // We know self is allocated, so its size must be below 2^64
        offset.saturating_add(len) <= self.len() as u64
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) {
        let offset = offset as usize;
        buffer.copy_from_slice(&self[offset..offset + buffer.len()]);
    }

    fn read_memory_u8(&self, offset: u64) -> u8 {
        self[offset as usize]
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) {
        let offset = offset as usize;
        self[offset..offset + buffer.len()].copy_from_slice(buffer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_method_names_empty() {
        assert_eq!(split_method_names(b""), Ok(vec![]));
    }

    #[test]
    fn test_split_method_one_name() {
        assert_eq!(split_method_names(b"hello"), Ok(vec![b"hello".to_vec()]));
    }

    #[test]
    fn test_split_method_two_method_names() {
        assert_eq!(
            split_method_names(b"hello,world"),
            Ok(vec![b"hello".to_vec(), b"world".to_vec()])
        );
    }

    #[test]
    fn test_split_empty_method_name_inside() {
        assert_eq!(
            split_method_names(b"hello,,world"),
            Err(VMLogicError::HostError(HostError::EmptyMethodName))
        );
    }

    #[test]
    fn test_split_empty_method_name_front() {
        assert_eq!(
            split_method_names(b",world"),
            Err(VMLogicError::HostError(HostError::EmptyMethodName))
        );
    }

    #[test]
    fn test_split_empty_method_name_back() {
        assert_eq!(
            split_method_names(b"world,"),
            Err(VMLogicError::HostError(HostError::EmptyMethodName))
        );
    }

    #[test]
    fn test_split_empty_method_name_comma_only() {
        assert_eq!(
            split_method_names(b","),
            Err(VMLogicError::HostError(HostError::EmptyMethodName))
        );
    }
}
