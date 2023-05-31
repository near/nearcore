//! This crate provides a type for representing a syntactically valid, unique account identifier on the [NEAR](https://near.org) network.
//!
//! ## Account ID Rules
//!
//! - Minimum length is `2`
//! - Maximum length is `64`
//! - An **Account ID** consists of **Account ID parts** separated by `.`, example:
//!   - `root` ✔
//!   - `alice.near` ✔
//!   - `app.stage.testnet` ✔
//! - Must not start or end with separators (`_`, `-` or `.`):
//!   - `_alice.` ✗
//!   - `.bob.near-` ✗
//! - Each part of the **Account ID** consists of lowercase alphanumeric symbols separated either by `_` or `-`, example:
//!   - `ƒelicia.near` ✗ (`ƒ` is not `f`)
//!   - `1_4m_n0t-al1c3.near` ✔
//! - Separators are not permitted to immediately follow each other, example:
//!   - `alice..near` ✗
//!   - `not-_alice.near` ✗
//! - An **Account ID** that is 64 characters long and consists of lowercase hex characters is a specific **implicit account ID**
//!
//! Learn more here: <https://docs.near.org/docs/concepts/account#account-id-rules>
//!
//! Also see [Error kind precedence](AccountId#error-kind-precedence).
//!
//! ## Usage
//!
//! ```
//! use near_account_id::AccountId;
//!
//! let alice: AccountId = "alice.near".parse().unwrap();
//!
//! assert!("ƒelicia.near".parse::<AccountId>().is_err()); // (ƒ is not f)
//! ```

use std::{fmt, str::FromStr};

mod errors;

#[cfg(feature = "borsh")]
mod borsh;
#[cfg(feature = "serde")]
mod serde;

pub use errors::{ParseAccountError, ParseErrorKind};

/// NEAR Account Identifier.
///
/// This is a unique, syntactically valid, human-readable account identifier on the NEAR network.
///
/// [See the crate-level docs for information about validation.](index.html#account-id-rules)
///
/// Also see [Error kind precedence](AccountId#error-kind-precedence).
///
/// ## Examples
///
/// ```
/// use near_account_id::AccountId;
///
/// let alice: AccountId = "alice.near".parse().unwrap();
///
/// assert!("ƒelicia.near".parse::<AccountId>().is_err()); // (ƒ is not f)
/// ```
#[derive(Eq, Ord, Hash, Clone, Debug, PartialEq, PartialOrd)]
pub struct AccountId(Box<str>);

impl AccountId {
    /// Shortest valid length for a NEAR Account ID.
    pub const MIN_LEN: usize = 2;
    /// Longest valid length for a NEAR Account ID.
    pub const MAX_LEN: usize = 64;

    /// Returns a string slice of the entire Account ID.
    ///
    /// ## Examples
    ///
    /// ```
    /// use near_account_id::AccountId;
    ///
    /// let carol: AccountId = "carol.near".parse().unwrap();
    /// assert_eq!("carol.near", carol.as_str());
    /// ```
    pub fn as_str(&self) -> &str {
        self
    }

    /// Returns `true` if the `AccountId` is a top-level NEAR Account ID.
    ///
    /// See [Top-level Accounts](https://docs.near.org/docs/concepts/account#top-level-accounts).
    ///
    /// ## Examples
    ///
    /// ```
    /// use near_account_id::AccountId;
    ///
    /// let near_tla: AccountId = "near".parse().unwrap();
    /// assert!(near_tla.is_top_level());
    ///
    /// // "alice.near" is a sub account of "near" account
    /// let alice: AccountId = "alice.near".parse().unwrap();
    /// assert!(!alice.is_top_level());
    /// ```
    pub fn is_top_level(&self) -> bool {
        !self.is_system() && !self.contains('.')
    }

    /// Returns `true` if the `AccountId` is a direct sub-account of the provided parent account.
    ///
    /// See [Subaccounts](https://docs.near.org/docs/concepts/account#subaccounts).
    ///
    /// ## Examples
    ///
    /// ```
    /// use near_account_id::AccountId;
    ///
    /// let near_tla: AccountId = "near".parse().unwrap();
    /// assert!(near_tla.is_top_level());
    ///
    /// let alice: AccountId = "alice.near".parse().unwrap();
    /// assert!(alice.is_sub_account_of(&near_tla));
    ///
    /// let alice_app: AccountId = "app.alice.near".parse().unwrap();
    ///
    /// // While app.alice.near is a sub account of alice.near,
    /// // app.alice.near is not a sub account of near
    /// assert!(alice_app.is_sub_account_of(&alice));
    /// assert!(!alice_app.is_sub_account_of(&near_tla));
    /// ```
    pub fn is_sub_account_of(&self, parent: &AccountId) -> bool {
        self.strip_suffix(parent.as_str())
            .and_then(|s| s.strip_suffix('.'))
            .map_or(false, |s| !s.contains('.'))
    }

    /// Returns `true` if the `AccountId` is a 64 characters long hexadecimal.
    ///
    /// See [Implicit-Accounts](https://docs.near.org/docs/concepts/account#implicit-accounts).
    ///
    /// ## Examples
    ///
    /// ```
    /// use near_account_id::AccountId;
    ///
    /// let alice: AccountId = "alice.near".parse().unwrap();
    /// assert!(!alice.is_implicit());
    ///
    /// let rando = "98793cd91a3f870fb126f66285808c7e094afcfc4eda8a970f6648cdf0dbd6de"
    ///     .parse::<AccountId>()
    ///     .unwrap();
    /// assert!(rando.is_implicit());
    /// ```
    pub fn is_implicit(&self) -> bool {
        self.len() == 64 && self.as_bytes().iter().all(|b| matches!(b, b'a'..=b'f' | b'0'..=b'9'))
    }

    /// Returns `true` if this `AccountId` is the system account.
    ///
    /// See [System account](https://nomicon.io/DataStructures/Account.html?highlight=system#system-account).
    ///
    /// ## Examples
    ///
    /// ```
    /// use near_account_id::AccountId;
    ///
    /// let alice: AccountId = "alice.near".parse().unwrap();
    /// assert!(!alice.is_system());
    ///
    /// let system: AccountId = "system".parse().unwrap();
    /// assert!(system.is_system());
    /// ```
    pub fn is_system(&self) -> bool {
        self.as_str() == "system"
    }

    /// Validates a string as a well-structured NEAR Account ID.
    ///
    /// Checks Account ID validity without constructing an `AccountId` instance.
    ///
    /// ## Examples
    ///
    /// ```
    /// use near_account_id::{AccountId, ParseErrorKind};
    ///
    /// assert!(AccountId::validate("alice.near").is_ok());
    ///
    /// assert!(
    ///   matches!(
    ///     AccountId::validate("ƒelicia.near"), // fancy ƒ!
    ///     Err(err) if err.kind() == &ParseErrorKind::InvalidChar
    ///   )
    /// );
    /// ```
    ///
    /// ## Error kind precedence
    ///
    /// If an Account ID has multiple format violations, the first one would be reported.
    ///
    /// ### Examples
    ///
    /// ```
    /// use near_account_id::{AccountId, ParseErrorKind};
    ///
    /// assert!(
    ///   matches!(
    ///     AccountId::validate("A__ƒƒluent."),
    ///     Err(err) if err.kind() == &ParseErrorKind::InvalidChar
    ///   )
    /// );
    ///
    /// assert!(
    ///   matches!(
    ///     AccountId::validate("a__ƒƒluent."),
    ///     Err(err) if err.kind() == &ParseErrorKind::RedundantSeparator
    ///   )
    /// );
    ///
    /// assert!(
    ///   matches!(
    ///     AccountId::validate("aƒƒluent."),
    ///     Err(err) if err.kind() == &ParseErrorKind::InvalidChar
    ///   )
    /// );
    ///
    /// assert!(
    ///   matches!(
    ///     AccountId::validate("affluent."),
    ///     Err(err) if err.kind() == &ParseErrorKind::RedundantSeparator
    ///   )
    /// );
    /// ```
    pub fn validate(account_id: &str) -> Result<(), ParseAccountError> {
        if account_id.len() < AccountId::MIN_LEN {
            Err(ParseAccountError { kind: ParseErrorKind::TooShort, char: None })
        } else if account_id.len() > AccountId::MAX_LEN {
            Err(ParseAccountError { kind: ParseErrorKind::TooLong, char: None })
        } else {
            // Adapted from https://github.com/near/near-sdk-rs/blob/fd7d4f82d0dfd15f824a1cf110e552e940ea9073/near-sdk/src/environment/env.rs#L819

            // NOTE: We don't want to use Regex here, because it requires extra time to compile it.
            // The valid account ID regex is /^(([a-z\d]+[-_])*[a-z\d]+\.)*([a-z\d]+[-_])*[a-z\d]+$/
            // Instead the implementation is based on the previous character checks.

            // We can safely assume that last char was a separator.
            let mut last_char_is_separator = true;

            let mut this = None;
            for (i, c) in account_id.chars().enumerate() {
                this.replace((i, c));
                let current_char_is_separator = match c {
                    'a'..='z' | '0'..='9' => false,
                    '-' | '_' | '.' => true,
                    _ => {
                        return Err(ParseAccountError {
                            kind: ParseErrorKind::InvalidChar,
                            char: this,
                        });
                    }
                };
                if current_char_is_separator && last_char_is_separator {
                    return Err(ParseAccountError {
                        kind: ParseErrorKind::RedundantSeparator,
                        char: this,
                    });
                }
                last_char_is_separator = current_char_is_separator;
            }

            if last_char_is_separator {
                return Err(ParseAccountError {
                    kind: ParseErrorKind::RedundantSeparator,
                    char: this,
                });
            }
            Ok(())
        }
    }

    /// Creates an `AccountId` without any validation checks.
    ///
    /// Please note that this is restrictively for internal use only. Plus, being behind a feature flag,
    /// this could be removed later in the future.
    ///
    /// ## Safety
    ///
    /// Since this skips validation and constructs an `AccountId` regardless,
    /// the caller bears the responsibility of ensuring that the Account ID is valid.
    /// You can use the [`AccountId::validate`] function sometime after its creation but before it's use.
    ///
    /// ## Examples
    ///
    /// ```
    /// use near_account_id::AccountId;
    ///
    /// let alice = AccountId::new_unvalidated("alice.near".to_string());
    /// assert!(AccountId::validate(alice.as_str()).is_ok());
    ///
    /// let ƒelicia = AccountId::new_unvalidated("ƒelicia.near".to_string());
    /// assert!(AccountId::validate(ƒelicia.as_str()).is_err());
    /// ```
    #[doc(hidden)]
    #[cfg(feature = "internal_unstable")]
    #[deprecated = "AccountId construction without validation is illegal since #4440"]
    pub fn new_unvalidated(account_id: String) -> Self {
        Self(account_id.into_boxed_str())
    }
}

impl std::ops::Deref for AccountId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl AsRef<str> for AccountId {
    fn as_ref(&self) -> &str {
        self
    }
}

impl std::borrow::Borrow<str> for AccountId {
    fn borrow(&self) -> &str {
        self
    }
}

impl FromStr for AccountId {
    type Err = ParseAccountError;

    fn from_str(account_id: &str) -> Result<Self, Self::Err> {
        Self::validate(account_id)?;
        Ok(Self(account_id.into()))
    }
}

impl TryFrom<Box<str>> for AccountId {
    type Error = ParseAccountError;

    fn try_from(account_id: Box<str>) -> Result<Self, Self::Error> {
        Self::validate(&account_id)?;
        Ok(Self(account_id))
    }
}

impl TryFrom<String> for AccountId {
    type Error = ParseAccountError;

    fn try_from(account_id: String) -> Result<Self, Self::Error> {
        Self::validate(&account_id)?;
        Ok(Self(account_id.into_boxed_str()))
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<AccountId> for String {
    fn from(account_id: AccountId) -> Self {
        account_id.0.into_string()
    }
}

impl From<AccountId> for Box<str> {
    fn from(value: AccountId) -> Box<str> {
        value.0
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for AccountId {
    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (AccountId::MIN_LEN, Some(AccountId::MAX_LEN))
    }

    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let mut s = u.arbitrary::<&str>()?;
        loop {
            match s.parse::<AccountId>() {
                Ok(account_id) => break Ok(account_id),
                Err(ParseAccountError { char: Some((idx, _)), .. }) => {
                    s = &s[..idx];
                    continue;
                }
                _ => break Err(arbitrary::Error::IncorrectFormat),
            }
        }
    }

    fn arbitrary_take_rest(u: arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        <&str as arbitrary::Arbitrary>::arbitrary_take_rest(u)?
            .parse()
            .map_err(|_| arbitrary::Error::IncorrectFormat)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub const OK_ACCOUNT_IDS: [&str; 24] = [
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

    pub const BAD_ACCOUNT_IDS: [&str; 24] = [
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

    #[test]
    fn test_is_valid_account_id() {
        for account_id in OK_ACCOUNT_IDS.iter().cloned() {
            if let Err(err) = AccountId::validate(account_id) {
                panic!("Valid account id {:?} marked invalid: {}", account_id, err.kind());
            }
        }

        for account_id in BAD_ACCOUNT_IDS.iter().cloned() {
            if AccountId::validate(account_id).is_ok() {
                panic!("Invalid account id {:?} marked valid", account_id);
            }
        }
    }

    #[test]
    fn test_err_kind_classification() {
        let id = "ErinMoriarty.near".parse::<AccountId>();
        debug_assert!(
            matches!(
                id,
                Err(ParseAccountError { kind: ParseErrorKind::InvalidChar, char: Some((0, 'E')) })
            ),
            "{:?}",
            id
        );

        let id = "-KarlUrban.near".parse::<AccountId>();
        debug_assert!(
            matches!(
                id,
                Err(ParseAccountError {
                    kind: ParseErrorKind::RedundantSeparator,
                    char: Some((0, '-'))
                })
            ),
            "{:?}",
            id
        );

        let id = "anthonystarr.".parse::<AccountId>();
        debug_assert!(
            matches!(
                id,
                Err(ParseAccountError {
                    kind: ParseErrorKind::RedundantSeparator,
                    char: Some((12, '.'))
                })
            ),
            "{:?}",
            id
        );

        let id = "jack__Quaid.near".parse::<AccountId>();
        debug_assert!(
            matches!(
                id,
                Err(ParseAccountError {
                    kind: ParseErrorKind::RedundantSeparator,
                    char: Some((5, '_'))
                })
            ),
            "{:?}",
            id
        );
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
                    .map_or(false, |account_id| account_id.is_top_level()),
                "Valid top level account id {:?} marked invalid",
                account_id
            );
        }

        let bad_top_level_account_ids = &[
            "ƒelicia.near", // fancy ƒ!
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
                    .map_or(false, |account_id| account_id.is_top_level()),
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
            ("test", "a1.a.test"),
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
                    Ok(account_id) if account_id.is_implicit()
                ),
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
                    Ok(account_id) if account_id.is_implicit()
                ),
                "Account ID {} is not an implicit account",
                invalid_account_id
            );
        }
    }

    #[test]
    #[cfg(feature = "arbitrary")]
    fn test_arbitrary() {
        let corpus = [
            ("a|bcd", None),
            ("ab|cde", Some("ab")),
            ("a_-b", None),
            ("ab_-c", Some("ab")),
            ("a", None),
            ("miraclx.near", Some("miraclx.near")),
            ("01234567890123456789012345678901234567890123456789012345678901234", None),
        ];

        for (input, expected_output) in corpus {
            assert!(input.len() <= u8::MAX as usize);
            let data = [input.as_bytes(), &[input.len() as _]].concat();
            let mut u = arbitrary::Unstructured::new(&data);

            assert_eq!(u.arbitrary::<AccountId>().as_deref().ok(), expected_output);
        }
    }
}
