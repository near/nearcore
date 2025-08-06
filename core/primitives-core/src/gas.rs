use near_gas::NearGas;

#[derive(
    borsh::BorshDeserialize,
    borsh::BorshSerialize,
    derive_more::Display,
    Debug,
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
pub struct Gas(NearGas);

impl Gas {
    /// Maximum value for Gas (u64::MAX)
    pub const MAX: Gas = Gas::from_gas(u64::MAX);
    /// Creates a new `Gas` from the specified number of whole tera Gas.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///
    /// let tera_gas = Gas::from_tgas(5);
    ///
    /// assert_eq!(tera_gas.as_gas(), 5 * 1_000_000_000_000);
    /// ```
    pub const fn from_tgas(inner: u64) -> Self {
        Self(NearGas::from_tgas(inner))
    }

    /// Creates a new `Gas` from the specified number of whole giga Gas.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///    
    /// let giga_gas = Gas::from_ggas(5);
    ///
    /// assert_eq!(giga_gas.as_gas(), 5 * 1_000_000_000);
    /// ```
    pub const fn from_ggas(inner: u64) -> Self {
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
    /// assert_eq!(gas.as_tgas(), 5);
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
    /// let gas = Gas::from_gas(1 * 1_000_000_000);
    /// assert_eq!(gas.as_ggas(), 1);
    /// ```
    pub const fn as_ggas(self) -> u64 {
        self.0.as_ggas()
    }

    /// Returns the total number of a whole part of tera Gas contained by this `Gas`.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///
    /// let gas = Gas::from_gas(1 * 1_000_000_000_000);
    /// assert_eq!(gas.as_tgas(), 1);
    /// ```
    pub const fn as_tgas(self) -> u64 {
        self.0.as_tgas()
    }

    /// Checked integer addition. Computes self + rhs, returning None if overflow occurred.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///
    /// assert_eq!(Gas::from_gas(u64::MAX -2).checked_add(Gas::from_gas(2)), Some(Gas::from_gas(u64::MAX)));
    /// assert_eq!(Gas::from_gas(u64::MAX -2).checked_add(Gas::from_gas(3)), None);
    /// ```
    pub const fn checked_add(self, rhs: Gas) -> Option<Self> {
        if let Some(result) = self.0.checked_add(rhs.0) { Some(Self(result)) } else { None }
    }

    /// Checked integer subtraction. Computes self - rhs, returning None if overflow occurred.
    ///
    /// # Examples
    /// ```
    /// use near_primitives_core::gas::Gas;
    ///
    /// assert_eq!(Gas::from_gas(2).checked_sub(Gas::from_gas(2)), Some(Gas::from_gas(0)));
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
    /// assert_eq!(Gas::from_gas(u64::MAX).checked_mul(2), None)
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

    pub const fn gas_limit() -> Gas {
        Gas::from_gas(u64::MAX)
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
