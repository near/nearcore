/// Conversion functions for the proto timestamp messages.
use near_async::time;
pub use protobuf::well_known_types::timestamp::Timestamp as ProtoTimestamp;

pub type ParseTimestampError = time::error::ComponentRange;

pub fn utc_to_proto(x: &time::Utc) -> ProtoTimestamp {
    ProtoTimestamp {
        seconds: x.unix_timestamp(),
        // x.nanosecond() is guaranteed to be in range [0,10^9).
        nanos: x.nanosecond() as i32,
        ..Default::default()
    }
}

pub fn utc_from_proto(x: &ProtoTimestamp) -> Result<time::Utc, ParseTimestampError> {
    time::Utc::from_unix_timestamp_nanos((x.seconds as i128 * 1_000_000_000) + (x.nanos as i128))
}
