use near_vm_errors::{HostError, VMLogicError};

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

/// Returns true if the account ID length is 64 characters and it's a hex representation.
pub fn is_account_id_64_len_hex(account_id: &str) -> bool {
    account_id.len() == 64
        && account_id.as_bytes().iter().all(|&b| match b {
            b'a'..=b'f' | b'0'..=b'9' => true,
            _ => false,
        })
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
