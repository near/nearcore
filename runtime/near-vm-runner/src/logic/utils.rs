use super::HostError;

/// Uses `,` separator to split `method_names` into a vector of method names.
/// Returns an empty vec if the empty slice is given.
/// Throws `HostError::EmptyMethodName` in case there is an empty method name inside.
pub(super) fn split_method_names(method_names: &[u8]) -> Result<Vec<Vec<u8>>, HostError> {
    if method_names.is_empty() {
        Ok(vec![])
    } else {
        method_names
            .split(|c| *c == b',')
            .map(|v| if v.is_empty() { Err(HostError::EmptyMethodName) } else { Ok(v.to_vec()) })
            .collect()
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
        assert_eq!(split_method_names(b"hello,,world"), Err(HostError::EmptyMethodName));
    }

    #[test]
    fn test_split_empty_method_name_front() {
        assert_eq!(split_method_names(b",world"), Err(HostError::EmptyMethodName));
    }

    #[test]
    fn test_split_empty_method_name_back() {
        assert_eq!(split_method_names(b"world,"), Err(HostError::EmptyMethodName));
    }

    #[test]
    fn test_split_empty_method_name_comma_only() {
        assert_eq!(split_method_names(b","), Err(HostError::EmptyMethodName));
    }
}
