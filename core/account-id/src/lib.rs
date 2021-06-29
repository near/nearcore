use std::{convert::TryFrom, str::FromStr};

#[cfg(feature = "borsh")]
use borsh::{BorshDeserialize, BorshSerialize};
#[cfg(feature = "serde")]
use serde::{self, de};

pub const MIN_ACCOUNT_ID_LEN: usize = 2;
pub const MAX_ACCOUNT_ID_LEN: usize = 64;

#[derive(Debug, thiserror::Error)]
pub enum ParseAccountError {
    #[error("the value is too long for account ID")]
    TooLong,
    #[error("the value is too short for account ID")]
    TooShort,
    #[error("the value has invalid characters for account ID")]
    Invalid,
}

/// Account identifier. Provides access to user's state.
///
/// *Note: Every owned Account ID has ensured its validity.*
#[derive(
    Eq,
    Ord,
    Hash,
    Clone,
    Debug,
    PartialEq,
    PartialOrd,
    derive_more::Into,
    derive_more::AsRef,
    derive_more::Display,
)]
#[as_ref(forward)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "borsh", derive(BorshSerialize))]
pub struct AccountId(
    #[cfg_attr(feature = "serde", serde(deserialize_with = "serde_validate_account_id"))] Box<str>,
);

impl AccountId {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn validate(account_id: impl AsRef<str>) -> Result<(), ParseAccountError> {
        let account_id = account_id.as_ref();

        if account_id.len() < MIN_ACCOUNT_ID_LEN {
            Err(ParseAccountError::TooShort)
        } else if account_id.len() > MAX_ACCOUNT_ID_LEN {
            Err(ParseAccountError::TooLong)
        } else {
            // NOTE: We don't want to use Regex here, because it requires extra time to compile it.
            // The valid account ID regex is /^(([a-z\d]+[-_])*[a-z\d]+\.)*([a-z\d]+[-_])*[a-z\d]+$/
            // Instead the implementation is based on the previous character checks.

            // We can safely assume that last char was a separator.
            let mut last_char_is_separator = true;

            for c in account_id.bytes() {
                let current_char_is_separator = match c {
                    b'a'..=b'z' | b'0'..=b'9' => false,
                    b'-' | b'_' | b'.' => true,
                    _ => return Err(ParseAccountError::Invalid),
                };
                if current_char_is_separator && last_char_is_separator {
                    return Err(ParseAccountError::Invalid);
                }
                last_char_is_separator = current_char_is_separator;
            }

            (!last_char_is_separator).then(|| ()).ok_or(ParseAccountError::Invalid)
        }
    }

    pub fn is_top_level_account_id(&self) -> bool {
        self.len() >= MIN_ACCOUNT_ID_LEN
            && self.len() <= MAX_ACCOUNT_ID_LEN
            && self.as_ref() != "system"
            && !self.as_ref().contains(".")
    }

    /// Returns true if the signer_id can create a direct sub-account with the given account Id.
    pub fn is_sub_account_of(&self, parent_account_id: &AccountId) -> bool {
        if parent_account_id.len() >= self.len() {
            return false;
        }
        // Will not panic, since valid account id is utf-8 only and the length is checked above.
        // e.g. when `near` creates `aa.near`, it splits into `aa.` and `near`
        let (prefix, suffix) = self.0.split_at(self.len() - parent_account_id.len());

        prefix.ends_with('.') && &suffix[..suffix.len()] == parent_account_id.as_ref()
    }

    /// Returns true if the account ID length is 64 characters and it's a hex representation.
    pub fn is_64_len_hex(account_id: impl AsRef<str>) -> bool {
        let account_id = account_id.as_ref();
        account_id.len() == 64
            && account_id.as_bytes().iter().all(|b| matches!(b, b'a'..=b'f' | b'0'..=b'9'))
    }

    /// Returns true if the account ID is suppose to be EVM machine.
    pub fn is_evm(account_id: impl AsRef<str>) -> bool {
        account_id.as_ref() == "evm"
    }

    /// Returns true if the account ID is the system account.
    pub fn is_system(account_id: impl AsRef<str>) -> bool {
        account_id.as_ref() == "system"
    }

    pub fn system_account() -> Self {
        "system".parse().unwrap()
    }

    pub fn test_account() -> Self {
        "test".parse().unwrap()
    }
}

impl std::borrow::Borrow<str> for AccountId {
    fn borrow(&self) -> &str {
        self.as_ref()
    }
}

impl FromStr for AccountId {
    type Err = ParseAccountError;

    fn from_str(account_id: &str) -> Result<Self, Self::Err> {
        Self::validate(account_id)?;
        Ok(Self(account_id.into()))
    }
}

impl TryFrom<String> for AccountId {
    type Error = ParseAccountError;

    fn try_from(account_id: String) -> Result<Self, Self::Error> {
        Self::validate(&account_id)?;
        Ok(Self(account_id.into()))
    }
}

impl From<AccountId> for String {
    fn from(account_id: AccountId) -> Self {
        account_id.0.into_string()
    }
}

impl TryFrom<&[u8]> for AccountId {
    type Error = ParseAccountError;

    fn try_from(account_id: &[u8]) -> Result<Self, Self::Error> {
        std::str::from_utf8(account_id).map_err(|_| Self::Error::Invalid)?.parse()
    }
}

#[cfg(feature = "serde")]
fn serde_validate_account_id<'de, D>(d: D) -> Result<Box<str>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let account_id = de::Deserialize::deserialize(d)?;
    AccountId::validate(&account_id).map_err(de::Error::custom)?;
    Ok(account_id)
}

#[cfg(feature = "borsh")]
impl BorshDeserialize for AccountId {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, std::io::Error> {
        let account_id = BorshDeserialize::deserialize(buf)?;
        Self::validate(&account_id)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        Ok(Self(account_id))
    }
}

#[cfg(feature = "paperclip")]
mod paperclip {
    use super::AccountId;
    use paperclip::v2::{models::DataType, schema::TypedData};
    impl TypedData for AccountId {
        fn data_type() -> DataType {
            DataType::String
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "borsh")]
    use borsh::{BorshDeserialize, BorshSerialize};
    #[cfg(feature = "serde")]
    use serde_json::json;

    #[test]
    fn test_is_valid_account_id() {
        let ok_account_ids = [
            "aa",
            "a-a",
            "a-aa",
            "100",
            "0o",
            "com",
            "near",
            "bowen",
            "b-o_w_e-n",
            "b.owen",
            "bro.wen",
            "a.ha",
            "a.b-a.ra",
            "system",
            "over.9000",
            "google.com",
            "illia.cheapaccounts.near",
            "0o0ooo00oo00o",
            "alex-skidanov",
            "10-4.8-2",
            "b-o_w_e-n",
            "no_lols",
            "0123456789012345678901234567890123456789012345678901234567890123",
            // Valid, but can't be created
            "near.a",
        ];
        for account_id in ok_account_ids.iter().cloned() {
            let _parsed_account_id = AccountId::from_str(account_id).unwrap_or_else(|err| {
                panic!("Valid account id {:?} marked invalid: {}", account_id, err)
            });

            #[cfg(feature = "serde")]
            {
                let deserialized_account_id: AccountId = serde_json::from_value(json!(account_id))
                    .unwrap_or_else(|err| {
                        panic!("failed to deserialize account ID {:?}: {}", account_id, err)
                    });
                assert_eq!(deserialized_account_id, _parsed_account_id);

                let serialized_account_id = serde_json::to_value(&deserialized_account_id)
                    .unwrap_or_else(|err| {
                        panic!("failed to serialize account ID {:?}: {}", account_id, err)
                    });
                assert_eq!(serialized_account_id, json!(account_id));
            };

            #[cfg(feature = "borsh")]
            {
                let str_serialized_account_id = account_id.try_to_vec().unwrap();

                let deserialized_account_id = AccountId::try_from_slice(&str_serialized_account_id)
                    .unwrap_or_else(|err| {
                        panic!("failed to deserialize account ID {:?}: {}", account_id, err)
                    });
                assert_eq!(deserialized_account_id, _parsed_account_id);

                let serialized_account_id =
                    deserialized_account_id.try_to_vec().unwrap_or_else(|err| {
                        panic!("failed to serialize account ID {:?}: {}", account_id, err)
                    });
                assert_eq!(serialized_account_id, str_serialized_account_id);
            };
        }

        let bad_account_ids = [
            "a",
            "A",
            "Abc",
            "-near",
            "near-",
            "-near-",
            "near.",
            ".near",
            "near@",
            "@near",
            "неар",
            "@@@@@",
            "0__0",
            "0_-_0",
            "0_-_0",
            "..",
            "a..near",
            "nEar",
            "_bowen",
            "hello world",
            "abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz",
            "01234567890123456789012345678901234567890123456789012345678901234",
            // `@` separators are banned now
            "some-complex-address@gmail.com",
            "sub.buy_d1gitz@atata@b0-rg.c_0_m",
        ];
        for account_id in bad_account_ids.iter().cloned() {
            assert!(
                AccountId::validate(account_id).is_err(),
                "Invalid account id {:?} marked valid",
                account_id
            );

            #[cfg(feature = "serde")]
            assert!(
                serde_json::from_value::<AccountId>(json!(account_id)).is_err(),
                "successfully deserialized invalid account ID {:?}",
                account_id
            );

            #[cfg(feature = "borsh")]
            {
                let str_serialized_account_id = account_id.try_to_vec().unwrap();

                assert!(
                    AccountId::try_from_slice(&str_serialized_account_id).is_err(),
                    "successfully deserialized invalid account ID {:?}",
                    account_id
                );
            };
        }
    }

    #[test]
    fn test_is_valid_top_level_account_id() {
        let ok_top_level_account_ids = &[
            "aa",
            "a-a",
            "a-aa",
            "100",
            "0o",
            "com",
            "near",
            "bowen",
            "b-o_w_e-n",
            "0o0ooo00oo00o",
            "alex-skidanov",
            "b-o_w_e-n",
            "no_lols",
            "0123456789012345678901234567890123456789012345678901234567890123",
        ];
        for account_id in ok_top_level_account_ids {
            assert!(
                account_id
                    .parse::<AccountId>()
                    .map_or(false, |account_id| account_id.is_top_level_account_id()),
                "Valid top level account id {:?} marked invalid",
                account_id
            );
        }

        let bad_top_level_account_ids = &[
            "near.a",
            "b.owen",
            "bro.wen",
            "a.ha",
            "a.b-a.ra",
            "some-complex-address@gmail.com",
            "sub.buy_d1gitz@atata@b0-rg.c_0_m",
            "over.9000",
            "google.com",
            "illia.cheapaccounts.near",
            "10-4.8-2",
            "a",
            "A",
            "Abc",
            "-near",
            "near-",
            "-near-",
            "near.",
            ".near",
            "near@",
            "@near",
            "неар",
            "@@@@@",
            "0__0",
            "0_-_0",
            "0_-_0",
            "..",
            "a..near",
            "nEar",
            "_bowen",
            "hello world",
            "abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz",
            "01234567890123456789012345678901234567890123456789012345678901234",
            // Valid regex and length, but reserved
            "system",
        ];
        for account_id in bad_top_level_account_ids {
            assert!(
                !account_id
                    .parse::<AccountId>()
                    .map_or(false, |account_id| account_id.is_top_level_account_id()),
                "Invalid top level account id {:?} marked valid",
                account_id
            );
        }
    }

    #[test]
    fn test_is_valid_sub_account_id() {
        let ok_pairs = &[
            ("test", "a.test"),
            ("test-me", "abc.test-me"),
            ("gmail.com", "abc.gmail.com"),
            ("gmail.com", "abc-lol.gmail.com"),
            ("gmail.com", "abc_lol.gmail.com"),
            ("gmail.com", "bro-abc_lol.gmail.com"),
            ("g0", "0g.g0"),
            ("1g", "1g.1g"),
            ("5-3", "4_2.5-3"),
        ];
        for (signer_id, sub_account_id) in ok_pairs {
            assert!(
                matches!(
                    (signer_id.parse::<AccountId>(), sub_account_id.parse::<AccountId>()),
                    (Ok(signer_id), Ok(sub_account_id)) if sub_account_id.is_sub_account_of(&signer_id)
                ),
                "Failed to create sub-account {:?} by account {:?}",
                sub_account_id,
                signer_id
            );
        }

        let bad_pairs = &[
            ("test", ".test"),
            ("test", "test"),
            ("test", "est"),
            ("test", ""),
            ("test", "st"),
            ("test5", "ббб"),
            ("test", "a-test"),
            ("test", "etest"),
            ("test", "a.etest"),
            ("test", "retest"),
            ("test-me", "abc-.test-me"),
            ("test-me", "Abc.test-me"),
            ("test-me", "-abc.test-me"),
            ("test-me", "a--c.test-me"),
            ("test-me", "a_-c.test-me"),
            ("test-me", "a-_c.test-me"),
            ("test-me", "_abc.test-me"),
            ("test-me", "abc_.test-me"),
            ("test-me", "..test-me"),
            ("test-me", "a..test-me"),
            ("gmail.com", "a.abc@gmail.com"),
            ("gmail.com", ".abc@gmail.com"),
            ("gmail.com", ".abc@gmail@com"),
            ("gmail.com", "abc@gmail@com"),
            ("test", "a@test"),
            ("test_me", "abc@test_me"),
            ("gmail.com", "abc@gmail.com"),
            ("gmail@com", "abc.gmail@com"),
            ("gmail.com", "abc-lol@gmail.com"),
            ("gmail@com", "abc_lol.gmail@com"),
            ("gmail@com", "bro-abc_lol.gmail@com"),
            ("gmail.com", "123456789012345678901234567890123456789012345678901234567890@gmail.com"),
            (
                "123456789012345678901234567890123456789012345678901234567890",
                "1234567890.123456789012345678901234567890123456789012345678901234567890",
            ),
            ("aa", "ъ@aa"),
            ("aa", "ъ.aa"),
        ];
        for (signer_id, sub_account_id) in bad_pairs {
            assert!(
                !matches!(
                    (signer_id.parse::<AccountId>(), sub_account_id.parse::<AccountId>()),
                    (Ok(signer_id), Ok(sub_account_id)) if sub_account_id.is_sub_account_of(&signer_id)
                ),
                "Invalid sub-account {:?} created by account {:?}",
                sub_account_id,
                signer_id
            );
        }
    }

    #[test]
    fn test_is_account_id_64_len_hex() {
        let valid_64_len_hex_account_ids = &[
            "0000000000000000000000000000000000000000000000000000000000000000",
            "6174617461746174617461746174617461746174617461746174617461746174",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "20782e20662e64666420482123494b6b6c677573646b6c66676a646b6c736667",
        ];
        for valid_account_id in valid_64_len_hex_account_ids {
            assert!(
                matches!(
                    valid_account_id.parse::<AccountId>(),
                    Ok(account_id) if AccountId::is_64_len_hex(&account_id)
                ),
                "Account ID {} should be valid 64-len hex",
                valid_account_id
            );
            assert!(
                AccountId::is_64_len_hex(valid_account_id),
                "Account ID {} should be valid 64-len hex",
                valid_account_id
            );
        }

        let invalid_64_len_hex_account_ids = &[
            "000000000000000000000000000000000000000000000000000000000000000",
            "6.74617461746174617461746174617461746174617461746174617461746174",
            "012-456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "fffff_ffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo",
            "00000000000000000000000000000000000000000000000000000000000000",
        ];
        for invalid_account_id in invalid_64_len_hex_account_ids {
            assert!(
                !matches!(
                    invalid_account_id.parse::<AccountId>(),
                    Ok(account_id) if AccountId::is_64_len_hex(&account_id)
                ),
                "Account ID {} should be invalid 64-len hex",
                invalid_account_id
            );
            assert!(
                !AccountId::is_64_len_hex(invalid_account_id),
                "Account ID {} should be invalid 64-len hex",
                invalid_account_id
            );
        }
    }
}
