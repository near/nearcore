use crate::types::Gas;
use std::hash::Hash;

/// Defines value size threshold for flat state inlining.
/// It means that values having size greater than the threshold will be stored
/// in FlatState as `FlatStateValue::Ref`, otherwise the whole value will be
/// stored as `FlatStateValue::Inlined`.
/// See the following comment for reasoning behind the threshold value:
/// <https://github.com/near/nearcore/issues/8243#issuecomment-1523049994>
///
/// Note that this value then propagates to memtrie, and then to the "small read"
/// costs. As such, changing it is a protocol change, and it should be turned
/// into a protocol parameter if we ever want to change it.
pub const INLINE_DISK_VALUE_THRESHOLD: usize = 4000;

#[derive(
    Debug,
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    serde_repr::Serialize_repr,
    serde_repr::Deserialize_repr,
)]
#[repr(u8)]
pub enum AccountIdValidityRulesVersion {
    /// Skip account ID validation according to legacy rules.
    V0,
    /// Limit `receiver_id` in `FunctionCallPermission` to be a valid account ID.
    V1,
}

impl AccountIdValidityRulesVersion {
    pub fn v0() -> AccountIdValidityRulesVersion {
        AccountIdValidityRulesVersion::V0
    }
}

/// Configuration of view methods execution, during which no costs should be charged.
#[derive(Default, Clone, serde::Serialize, serde::Deserialize, Debug, Hash, PartialEq, Eq)]
pub struct ViewConfig {
    /// If specified, defines max burnt gas per view method.
    pub max_gas_burnt: Gas,
}
