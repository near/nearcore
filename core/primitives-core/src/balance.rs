use near_token::NearToken;
use primitive_types::U256;

#[derive(
    borsh::BorshDeserialize,
    borsh::BorshSerialize,
    derive_more::Display,
    derive_more::FromStr,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Default,
    Hash,
    Debug,
)]
#[repr(transparent)]
pub struct Balance(NearToken);

const ONE_NEAR: u128 = 10_u128.pow(24);

impl Balance {
    pub const MAX: Balance = Balance::from_yoctonear(u128::MAX);
    pub const ZERO: Balance = Balance::from_yoctonear(0);
}

impl Balance {
    /// `from_yoctonear` is a function that takes value by a number of yocto-near.
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// assert_eq!( NearToken::from_yoctonear(10u128.pow(21)), NearToken::from_millinear(1))
    /// ```
    pub const fn from_yoctonear(inner: u128) -> Self {
        Self(NearToken::from_yoctonear(inner))
    }

    /// `from_millinear` is a function that takes value by a number of milli-near and converts it to an equivalent to the yocto-near.
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// assert_eq!(NearToken::from_millinear(1), NearToken::from_yoctonear(10u128.pow(21)))
    /// ```
    pub const fn from_millinear(inner: u128) -> Self {
        Self(NearToken::from_millinear(inner))
    }

    /// `from_near` is a function that takes value by a number of near and converts it to an equivalent to the yocto-near.
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// assert_eq!(NearToken::from_near(1), NearToken::from_yoctonear(10u128.pow(24)))
    /// ```
    pub const fn from_near(inner: u128) -> Self {
        Self(NearToken::from_near(inner))
    }

    /// `as_near` is a function that converts number of yocto-near to an equivalent to the near.
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// assert_eq!(NearToken::from_yoctonear(10u128.pow(24)).as_near(), 1)
    /// ```
    pub const fn as_near(&self) -> u128 {
        self.0.as_near()
    }

    /// `as_millinear` is a function that converts number of yocto-near to an equivalent to the milli-near.
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// assert_eq!(NearToken::from_yoctonear(10u128.pow(21)).as_millinear(), 1)
    /// ```
    pub const fn as_millinear(&self) -> u128 {
        self.0.as_millinear()
    }

    /// `as_yoctonear` is a function that shows a number of yocto-near.
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// assert_eq!(NearToken::from_yoctonear(10).as_yoctonear(), 10)
    /// ```
    pub const fn as_yoctonear(&self) -> u128 {
        self.0.as_yoctonear()
    }

    /// `is_zero` is a boolean function that checks `NearToken`
    /// if a `NearToken` inner is zero, returns true.
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// assert_eq!(NearToken::from_yoctonear(0).is_zero(), true)
    /// ```
    pub const fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    /// Checked integer addition. Computes self + rhs, returning None if overflow occurred.
    ///
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// use std::u128;
    /// assert_eq!(NearToken::from_yoctonear(u128::MAX -2).checked_add(NearToken::from_yoctonear(2)), Some(NearToken::from_yoctonear(u128::MAX)));
    /// assert_eq!(NearToken::from_yoctonear(u128::MAX -2).checked_add(NearToken::from_yoctonear(3)), None);
    /// ```
    pub const fn checked_add(self, rhs: Self) -> Option<Self> {
        if let Some(near) = self.as_yoctonear().checked_add(rhs.as_yoctonear()) {
            Some(Self::from_yoctonear(near))
        } else {
            None
        }
    }

    /// Checked integer subtraction. Computes self - rhs, returning None if overflow occurred.
    ///
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// assert_eq!(NearToken::from_yoctonear(2).checked_sub(NearToken::from_yoctonear(2)), Some(NearToken::from_yoctonear(0)));
    /// assert_eq!(NearToken::from_yoctonear(2).checked_sub(NearToken::from_yoctonear(3)), None);
    /// ```
    pub const fn checked_sub(self, rhs: Self) -> Option<Self> {
        if let Some(near) = self.as_yoctonear().checked_sub(rhs.as_yoctonear()) {
            Some(Self::from_yoctonear(near))
        } else {
            None
        }
    }

    /// Checked integer multiplication. Computes self * rhs, returning None if overflow occurred.
    ///
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// use std::u128;
    /// assert_eq!(NearToken::from_yoctonear(2).checked_mul(2), Some(NearToken::from_yoctonear(4)));
    /// assert_eq!(NearToken::from_yoctonear(u128::MAX).checked_mul(2), None)
    pub const fn checked_mul(self, rhs: u128) -> Option<Self> {
        if let Some(near) = self.as_yoctonear().checked_mul(rhs) {
            Some(Self::from_yoctonear(near))
        } else {
            None
        }
    }

    /// Checked integer division. Computes self / rhs, returning None if rhs == 0.
    ///
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// assert_eq!(NearToken::from_yoctonear(10).checked_div(2), Some(NearToken::from_yoctonear(5)));
    /// assert_eq!(NearToken::from_yoctonear(2).checked_div(0), None);
    /// ```
    pub const fn checked_div(self, rhs: u128) -> Option<Self> {
        if let Some(near) = self.as_yoctonear().checked_div(rhs) {
            Some(Self::from_yoctonear(near))
        } else {
            None
        }
    }

    /// Saturating integer addition. Computes self + rhs, saturating at the numeric bounds instead of overflowing.
    ///
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// assert_eq!(NearToken::from_yoctonear(5).saturating_add(NearToken::from_yoctonear(5)), NearToken::from_yoctonear(10));
    /// assert_eq!(NearToken::from_yoctonear(u128::MAX).saturating_add(NearToken::from_yoctonear(1)), NearToken::from_yoctonear(u128::MAX));
    /// ```
    pub const fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }

    /// Saturating integer subtraction. Computes self - rhs, saturating at the numeric bounds instead of overflowing.
    ///
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// assert_eq!(NearToken::from_yoctonear(5).saturating_sub(NearToken::from_yoctonear(2)), NearToken::from_yoctonear(3));
    /// assert_eq!(NearToken::from_yoctonear(1).saturating_sub(NearToken::from_yoctonear(2)), NearToken::from_yoctonear(0));
    /// ```
    pub const fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }

    /// Saturating integer multiplication. Computes self * rhs, saturating at the numeric bounds instead of overflowing.
    ///
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// use std::u128;
    /// assert_eq!(NearToken::from_yoctonear(2).saturating_mul(5), NearToken::from_yoctonear(10));
    /// assert_eq!(NearToken::from_yoctonear(u128::MAX).saturating_mul(2), NearToken::from_yoctonear(u128::MAX));
    /// ```
    pub const fn saturating_mul(self, rhs: u128) -> Self {
        Self(self.0.saturating_mul(rhs))
    }

    /// Saturating integer division. Computes self / rhs, saturating at the numeric bounds instead of overflowing.
    ///
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// assert_eq!(NearToken::from_yoctonear(10).saturating_div(2), NearToken::from_yoctonear(5));
    /// assert_eq!(NearToken::from_yoctonear(10).saturating_div(0), NearToken::from_yoctonear(0))
    /// ```
    pub const fn saturating_div(self, rhs: u128) -> Self {
        Self(self.0.saturating_div(rhs))
    }

    /// Formats the `NearToken` and displays the amount in NEAR or yoctoNEAR depending on the value.
    ///
    /// # Examples
    /// ```
    /// use near_token::NearToken;
    /// assert_eq!(NearToken::from_yoctonear(10_u128.pow(24)).exact_amount_display(), "1 NEAR");
    /// assert_eq!(NearToken::from_yoctonear(15 * 10_u128.pow(23)).exact_amount_display(), "1.5 NEAR");
    /// assert_eq!(NearToken::from_yoctonear(500).exact_amount_display(), "500 yoctoNEAR");
    /// assert_eq!(NearToken::from_yoctonear(0).exact_amount_display(), "0 NEAR");
    /// ```
    pub fn exact_amount_display(&self) -> String {
        let yoctonear = self.as_yoctonear();

        if yoctonear == 0 {
            "0 NEAR".to_string()
        } else if yoctonear <= 1_000 {
            format!("{} yoctoNEAR", yoctonear)
        } else if yoctonear % ONE_NEAR == 0 {
            format!("{} NEAR", yoctonear / ONE_NEAR)
        } else {
            format!(
                "{}.{} NEAR",
                yoctonear / ONE_NEAR,
                format!("{:0>24}", yoctonear % ONE_NEAR).trim_end_matches('0')
            )
        }
    }
}

impl From<NearToken> for Balance {
    fn from(token: NearToken) -> Self {
        Balance(token)
    }
}

impl From<Balance> for U256 {
    fn from(value: Balance) -> U256 {
        value.as_yoctonear().into()
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for Balance {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "NearToken".to_string().into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        NearToken::json_schema(generator)
    }
}
