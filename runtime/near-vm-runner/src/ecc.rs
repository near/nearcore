//! Metadata for external contract calls (ECC).

use borsh::{BorshDeserialize, BorshSerialize};
use std::io::{Error, ErrorKind, Read, Result, Write};

/// Name of the custom section in which a contract lists its ECC-only functions.
#[cfg(any(feature = "prepare", test))]
pub(crate) const ECC_ONLY_FUNCTIONS_SECTION: &str = "ecc_only_functions";

/// Functions a contract marks as callable only through external contract calls
/// (ECC), read from the `ecc_only_functions` custom section.
///
/// Names are the original, unprefixed export names, kept sorted so lookups can
/// use binary search.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EccOnlyFunctions(Box<[Box<str>]>);

impl EccOnlyFunctions {
    /// `names` must be sorted and free of duplicates.
    #[cfg(any(feature = "prepare", test))]
    pub(crate) fn from_sorted(names: Box<[Box<str>]>) -> Self {
        debug_assert!(is_strictly_sorted(&names));
        Self(names)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.0.binary_search_by(|n| n.as_ref().cmp(name)).is_ok()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|n| n.as_ref())
    }
}

fn is_strictly_sorted(names: &[Box<str>]) -> bool {
    names.is_sorted_by(|a, b| a < b)
}

impl BorshSerialize for EccOnlyFunctions {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.0.as_ref().serialize(writer)
    }
}

impl BorshDeserialize for EccOnlyFunctions {
    /// Rejects lists that are not sorted or contain duplicates, so a corrupted
    /// cache entry cannot break the invariant [`EccOnlyFunctions::contains`]
    /// relies on.
    fn deserialize_reader<R: Read>(reader: &mut R) -> Result<Self> {
        let names = Vec::<Box<str>>::deserialize_reader(reader)?;
        if !is_strictly_sorted(&names) {
            return Err(Error::new(ErrorKind::InvalidData, "ecc-only functions are not sorted"));
        }
        Ok(Self(names.into_boxed_slice()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(names: &[&str]) -> Box<[Box<str>]> {
        names.iter().map(|&n| Box::from(n)).collect()
    }

    #[test]
    fn borsh_round_trip() {
        for list in [EccOnlyFunctions::default(), EccOnlyFunctions::from_sorted(names(&["a", "b"]))]
        {
            let bytes = borsh::to_vec(&list).unwrap();
            assert_eq!(borsh::from_slice::<EccOnlyFunctions>(&bytes).unwrap(), list);
        }
    }

    #[test]
    fn borsh_rejects_unsorted() {
        for list in [names(&["b", "a"]), names(&["a", "a"])] {
            let bytes = borsh::to_vec(&list).unwrap();
            assert!(borsh::from_slice::<EccOnlyFunctions>(&bytes).is_err());
        }
    }
}
