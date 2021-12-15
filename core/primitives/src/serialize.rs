pub use near_primitives_core::serialize::*;

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::types::StoreKey;

    use super::*;

    #[derive(Deserialize, Serialize)]
    struct OptionBytesStruct {
        #[serde(with = "option_base64_format")]
        data: Option<Vec<u8>>,
    }

    #[derive(Deserialize, Serialize)]
    struct StoreKeyStruct {
        #[serde(with = "base64_format")]
        store_key: StoreKey,
    }

    #[test]
    fn test_serialize_some() {
        let s = OptionBytesStruct { data: Some(vec![10, 20, 30]) };
        let encoded = serde_json::to_string(&s).unwrap();
        assert_eq!(encoded, "{\"data\":\"ChQe\"}");
    }

    #[test]
    fn test_deserialize_some() {
        let encoded = "{\"data\":\"ChQe\"}";
        let decoded: OptionBytesStruct = serde_json::from_str(encoded).unwrap();
        assert_eq!(decoded.data, Some(vec![10, 20, 30]));
    }

    #[test]
    fn test_serialize_none() {
        let s = OptionBytesStruct { data: None };
        let encoded = serde_json::to_string(&s).unwrap();
        assert_eq!(encoded, "{\"data\":null}");
    }

    #[test]
    fn test_deserialize_none() {
        let encoded = "{\"data\":null}";
        let decoded: OptionBytesStruct = serde_json::from_str(encoded).unwrap();
        assert_eq!(decoded.data, None);
    }

    #[test]
    fn test_serialize_store_key() {
        let s = StoreKeyStruct { store_key: StoreKey::from(vec![10, 20, 30]) };
        let encoded = serde_json::to_string(&s).unwrap();
        assert_eq!(encoded, "{\"store_key\":\"ChQe\"}");
    }

    #[test]
    fn test_deserialize_store_key() {
        let encoded = "{\"store_key\":\"ChQe\"}";
        let decoded: StoreKeyStruct = serde_json::from_str(encoded).unwrap();
        assert_eq!(decoded.store_key, StoreKey::from(vec![10, 20, 30]));
    }
}
