use crate::errors::IntegerOverflowError;
use near_gas::NearGas;

/// Wrapper type over [`near_gas::NearGas`], which itself is a wrapper type over u64.
///
/// This wrapper exists to maintain JSON RPC compatibility. While [`NearGas`]
/// serializes to a JSON string for precision, we need to continue serializing
/// Gas values to JSON numbers for backward compatibility with existing clients.
///
/// Note: [`NearGas`] deserialization already handles both JSON numbers and JSON
/// strings, so we don't need to redefine deserialization behavior here.
#[derive(
    borsh::BorshDeserialize,
    borsh::BorshSerialize,
    derive_more::Display,
    derive_more::FromStr,
    serde::Deserialize,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Default,
    Hash,
)]
#[repr(transparent)]
pub struct Gas(NearGas);

impl Gas {
    /// Maximum value for Gas (u64::MAX)
    pub const MAX: Gas = Gas::from_gas(u64::MAX);
    /// Zero value for Gas
    pub const ZERO: Gas = Gas::from_gas(0);

    /// Creates a new `Gas` from the specified number of whole tera Gas.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///
    /// let tera_gas = Gas::from_teragas(5);
    ///
    /// assert_eq!(tera_gas.as_gas(), 5 * 1_000_000_000_000);
    /// ```
    pub const fn from_teragas(inner: u64) -> Self {
        Self(NearGas::from_tgas(inner))
    }

    /// Creates a new `Gas` from the specified number of whole giga Gas.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///    
    /// let giga_gas = Gas::from_gigagas(5);
    ///
    /// assert_eq!(giga_gas.as_gas(), 5 * 1_000_000_000);
    /// ```
    pub const fn from_gigagas(inner: u64) -> Self {
        Self(NearGas::from_ggas(inner))
    }

    /// Creates a new `Gas` from the specified number of whole Gas.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///
    /// let gas = Gas::from_gas(5 * 1_000_000_000_000);
    ///
    /// assert_eq!(gas.as_teragas(), 5);
    /// ```
    pub const fn from_gas(inner: u64) -> Self {
        Self(NearGas::from_gas(inner))
    }

    /// Returns the total number of whole Gas contained by this `Gas`.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///
    /// let gas = Gas::from_gas(12345);
    /// assert_eq!(gas.as_gas(), 12345);
    /// ```
    pub const fn as_gas(self) -> u64 {
        self.0.as_gas()
    }

    /// Returns the total number of a whole part of giga Gas contained by this `Gas`.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///
    /// let gas = Gas::from_gigagas(1);
    /// assert_eq!(gas.as_gigagas(), 1);
    /// ```
    pub const fn as_gigagas(self) -> u64 {
        self.0.as_ggas()
    }

    /// Returns the total number of a whole part of tera Gas contained by this `Gas`.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///
    /// let gas = Gas::from_gas(1 * 1_000_000_000_000);
    /// assert_eq!(gas.as_teragas(), 1);
    /// ```
    pub const fn as_teragas(self) -> u64 {
        self.0.as_tgas()
    }

    /// Checked integer addition. Computes self + rhs, returning None if overflow occurred.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///
    /// assert_eq!(Gas::from_gas(u64::MAX -2).checked_add(Gas::from_gas(2)), Some(Gas::MAX));
    /// assert_eq!(Gas::from_gas(u64::MAX -2).checked_add(Gas::from_gas(3)), None);
    /// ```
    pub const fn checked_add(self, rhs: Gas) -> Option<Self> {
        if let Some(result) = self.0.checked_add(rhs.0) { Some(Self(result)) } else { None }
    }

    pub fn checked_add_result(self, rhs: Gas) -> Result<Self, IntegerOverflowError> {
        self.checked_add(rhs).ok_or(IntegerOverflowError)
    }

    /// Checked integer subtraction. Computes self - rhs, returning None if overflow occurred.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///
    /// assert_eq!(Gas::from_gas(2).checked_sub(Gas::from_gas(2)), Some(Gas::ZERO));
    /// assert_eq!(Gas::from_gas(2).checked_sub(Gas::from_gas(3)), None);
    /// ```
    pub const fn checked_sub(self, rhs: Gas) -> Option<Self> {
        if let Some(result) = self.0.checked_sub(rhs.0) { Some(Self(result)) } else { None }
    }

    /// Checked integer multiplication. Computes self * rhs, returning None if overflow occurred.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///
    /// use std::u64;
    /// assert_eq!(Gas::from_gas(2).checked_mul(2), Some(Gas::from_gas(4)));
    /// assert_eq!(Gas::MAX.checked_mul(2), None)
    pub const fn checked_mul(self, rhs: u64) -> Option<Self> {
        if let Some(result) = self.0.checked_mul(rhs) { Some(Self(result)) } else { None }
    }

    /// Checked integer division. Computes self / rhs, returning None if rhs == 0.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///
    /// assert_eq!(Gas::from_gas(10).checked_div(2), Some(Gas::from_gas(5)));
    /// assert_eq!(Gas::from_gas(2).checked_div(0), None);
    /// ```
    pub const fn checked_div(self, rhs: u64) -> Option<Self> {
        if let Some(result) = self.0.checked_div(rhs) { Some(Self(result)) } else { None }
    }

    /// Saturating integer addition. Computes self + rhs, saturating at the numeric bounds instead of overflowing.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    /// assert_eq!(Gas::from_gas(5).saturating_add(Gas::from_gas(5)), Gas::from_gas(10));
    /// assert_eq!(Gas::MAX.saturating_add(Gas::from_gas(1)), Gas::MAX);
    /// ```
    pub const fn saturating_add(self, rhs: Gas) -> Gas {
        Self(self.0.saturating_add(rhs.0))
    }

    /// Saturating integer subtraction. Computes self - rhs, saturating at the numeric bounds instead of overflowing.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    /// assert_eq!(Gas::from_gas(5).saturating_sub(Gas::from_gas(2)), Gas::from_gas(3));
    /// assert_eq!(Gas::from_gas(1).saturating_sub(Gas::from_gas(2)), Gas::ZERO);
    /// ```
    pub const fn saturating_sub(self, rhs: Gas) -> Gas {
        Self(self.0.saturating_sub(rhs.0))
    }

    /// Saturating integer multiplication. Computes self * rhs, saturating at the numeric bounds instead of overflowing.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    /// use std::u64;
    /// assert_eq!(Gas::from_gas(2).saturating_mul(5), Gas::from_gas(10));
    /// assert_eq!(Gas::MAX.saturating_mul(2), Gas::MAX);
    /// ```
    pub const fn saturating_mul(self, rhs: u64) -> Gas {
        Self(self.0.saturating_mul(rhs))
    }

    /// Saturating integer division. Computes self / rhs, saturating at the numeric bounds instead of overflowing.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    /// assert_eq!(Gas::from_gas(10).saturating_div(2), Gas::from_gas(5));
    /// assert_eq!(Gas::from_gas(10).saturating_div(0), Gas::ZERO)
    /// ```
    pub const fn saturating_div(self, rhs: u64) -> Gas {
        if rhs == 0 {
            return Gas::ZERO;
        }
        Self(self.0.saturating_div(rhs))
    }
}

impl serde::Serialize for Gas {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.as_gas())
    }
}

impl core::fmt::Debug for Gas {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.as_gas())
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for Gas {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "NearGas".to_string().into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "format": "uint64",
            "minimum": 0,
            "type": "integer"
        })
    }
}
