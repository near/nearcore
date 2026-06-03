use near_primitives::version::{PROTOCOL_VERSION, ProtocolVersion};

/// Returns the rust test contract appropriate for `protocol_version`.
///
/// `rs_contract` imports the newest host functions (gated by `latest_protocol`)
/// and only links at the latest protocol version; below it we fall back to
/// `backwards_compatible_rs_contract`, which omits those imports.
pub(crate) fn rs_contract_for_protocol_version(protocol_version: ProtocolVersion) -> &'static [u8] {
    if protocol_version < PROTOCOL_VERSION {
        near_test_contracts::backwards_compatible_rs_contract()
    } else {
        near_test_contracts::rs_contract()
    }
}
