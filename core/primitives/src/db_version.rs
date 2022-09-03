/// Database version.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct DbVersion(pub u32);

/// Current version of the database.
pub const DB_VERSION: DbVersion = DbVersion(31);

impl DbVersion {
    /// Returns binary serialisation of the version.
    pub fn serialise(self) -> Vec<u8> {
        self.0.to_string().into_bytes()
    }

    /// Deserialises database version from binary data.
    ///
    /// This is an inverse of [`DbVersion::serialise`].
    pub fn deserialise(bytes: &[u8]) -> Result<Self, String> {
        let value = std::str::from_utf8(bytes)
            .map_err(|err| format!("invalid DbVersion (‘{bytes:x?}’): {err}"))?;
        let ver = u32::from_str_radix(value, 10)
            .map_err(|err| format!("invalid DbVersion (‘{value}’): {err}"))?;
        Ok(Self(ver))
    }
}

impl std::fmt::Debug for DbVersion {
    #[inline]
    fn fmt(&self, fmtr: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, fmtr)
    }
}

impl std::fmt::Display for DbVersion {
    #[inline]
    fn fmt(&self, fmtr: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, fmtr)
    }
}

impl From<u32> for DbVersion {
    #[inline]
    fn from(ver: u32) -> Self {
        Self(ver)
    }
}

impl From<DbVersion> for i64 {
    #[inline]
    fn from(ver: DbVersion) -> Self {
        i64::from(ver.0)
    }
}
