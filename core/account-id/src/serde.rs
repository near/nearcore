use super::AccountId;

use serde::{de, ser};

impl ser::Serialize for AccountId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> de::Deserialize<'de> for AccountId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let account_id = Box::<str>::deserialize(deserializer)?;
        AccountId::validate(&account_id).map_err(|err| {
            de::Error::custom(format!("invalid value: \"{}\", {}", account_id, err))
        })?;
        Ok(AccountId(account_id))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::tests::{BAD_ACCOUNT_IDS, OK_ACCOUNT_IDS},
        AccountId,
    };

    use serde_json::json;

    #[test]
    fn test_is_valid_account_id() {
        for account_id in OK_ACCOUNT_IDS.iter() {
            let parsed_account_id = account_id.parse::<AccountId>().unwrap_or_else(|err| {
                panic!("Valid account id {:?} marked invalid: {}", account_id, err)
            });

            let deserialized_account_id: AccountId = serde_json::from_value(json!(account_id))
                .unwrap_or_else(|err| {
                    panic!("failed to deserialize account ID {:?}: {}", account_id, err)
                });
            assert_eq!(deserialized_account_id, parsed_account_id);

            let serialized_account_id = serde_json::to_value(&deserialized_account_id)
                .unwrap_or_else(|err| {
                    panic!("failed to serialize account ID {:?}: {}", account_id, err)
                });
            assert_eq!(serialized_account_id, json!(account_id));
        }

        for account_id in BAD_ACCOUNT_IDS.iter() {
            assert!(
                serde_json::from_value::<AccountId>(json!(account_id)).is_err(),
                "successfully deserialized invalid account ID {:?}",
                account_id
            );
        }
    }

    #[test]
    fn fuzz() {
        bolero::check!().for_each(|input: &[u8]| {
            if let Ok(account_id) = std::str::from_utf8(input) {
                if let Ok(account_id) = serde_json::from_value::<AccountId>(json!(account_id)) {
                    assert_eq!(
                        account_id,
                        serde_json::from_value(serde_json::to_value(&account_id).unwrap()).unwrap()
                    );
                }
            }
        });
    }
}
