/// Protocol version type.
pub type ProtocolVersion = u32;

/// First protocol version backward compatibility started.
/// Changes 14 -> 15:
///   - Added `latest_protocol_version` into `BlockHeaderInnerRest`.
pub const PROTOCOL_VERSION_V14: ProtocolVersion = 14;

/// Current latest version of the protocol.
pub const PROTOCOL_VERSION: ProtocolVersion = 15;

pub const FIRST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION: ProtocolVersion = PROTOCOL_VERSION_V14;
