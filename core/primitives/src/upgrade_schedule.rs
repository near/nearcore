use chrono::{DateTime, NaiveDateTime, ParseError, Utc};
use near_primitives_core::types::ProtocolVersion;
use std::str::FromStr;

/// Defines the point in time after which validators are expected to vote on the
/// new protocol version.
#[derive(Clone, Copy)]
pub struct ProtocolUpgradeVotingSchedule {
    timestamp: chrono::DateTime<Utc>,
}

impl Default for ProtocolUpgradeVotingSchedule {
    fn default() -> Self {
        let epoch = NaiveDateTime::from_timestamp(0, 0);
        Self { timestamp: DateTime::<Utc>::from_utc(epoch, Utc) }
    }
}

impl FromStr for ProtocolUpgradeVotingSchedule {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            timestamp: DateTime::<Utc>::from_utc(
                NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")?,
                Utc,
            ),
        })
    }
}

impl ProtocolUpgradeVotingSchedule {
    pub fn is_in_future(&self) -> bool {
        chrono::Utc::now() < self.timestamp
    }

    pub fn timestamp(&self) -> i64 {
        self.timestamp.timestamp()
    }
}

pub(crate) fn get_protocol_version_internal(
    // Protocol version that will be used in the next epoch.
    next_epoch_protocol_version: ProtocolVersion,
    // Latest protocol version supported by this client.
    client_protocol_version: ProtocolVersion,
    // Point in time when voting for client_protocol_version version is expected
    // to start.  Use `Default::default()` to start voting immediately.
    voting_start: ProtocolUpgradeVotingSchedule,
) -> ProtocolVersion {
    if next_epoch_protocol_version >= client_protocol_version {
        client_protocol_version
    } else if voting_start.is_in_future() {
        // Don't announce support for the latest protocol version yet.
        next_epoch_protocol_version
    } else {
        // The time has passed, announce the latest supported protocol version.
        client_protocol_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The tests call `get_protocol_version_internal()` with the following
    // parameters: No schedule:                  (X-2,X), (X,X), (X+2,X)
    // Before the scheduled upgrade: (X-2,X), (X,X), (X+2,X)
    // After the scheduled upgrade:  (X-2,X), (X,X), (X+2,X)

    #[test]
    fn test_no_upgrade_schedule() {
        // As no protocol upgrade voting schedule is set, always return the version
        // supported by the client.

        let client_protocol_version = 100;
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version - 2,
                client_protocol_version,
                Default::default(),
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version,
                client_protocol_version,
                Default::default()
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version + 2,
                client_protocol_version,
                Default::default(),
            )
        );
    }

    #[test]
    fn test_none_upgrade_schedule() {
        // As no protocol upgrade voting schedule is set, always return the version
        // supported by the client.

        let client_protocol_version = 100;

        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version - 2,
                client_protocol_version,
                Default::default(),
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version,
                client_protocol_version,
                Default::default(),
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version + 2,
                client_protocol_version,
                Default::default(),
            )
        );
    }

    #[test]
    fn test_before_scheduled_time() {
        let client_protocol_version = 100;
        let schedule = ProtocolUpgradeVotingSchedule::from_str("2050-01-01 00:00:00").unwrap();

        // The client supports a newer version than the version of the next epoch.
        // Upgrade voting will start in the far future, therefore don't announce the
        // newest supported version.
        let next_epoch_protocol_version = client_protocol_version - 2;
        assert_eq!(
            next_epoch_protocol_version,
            get_protocol_version_internal(
                next_epoch_protocol_version,
                client_protocol_version,
                schedule,
            )
        );

        // An upgrade happened before the scheduled time.
        let next_epoch_protocol_version = client_protocol_version;
        assert_eq!(
            next_epoch_protocol_version,
            get_protocol_version_internal(
                next_epoch_protocol_version,
                client_protocol_version,
                schedule,
            )
        );

        // Several upgrades happened before the scheduled time. Announce only the
        // currently supported protocol version.
        let next_epoch_protocol_version = client_protocol_version + 2;
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                next_epoch_protocol_version,
                client_protocol_version,
                schedule,
            )
        );
    }

    #[test]
    fn test_after_scheduled_time() {
        let client_protocol_version = 100;
        let schedule = ProtocolUpgradeVotingSchedule::from_str("1900-01-01 00:00:00").unwrap();

        // Regardless of the protocol version of the next epoch, return the version
        // supported by the client.
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version - 2,
                client_protocol_version,
                schedule,
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version,
                client_protocol_version,
                schedule,
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version + 2,
                client_protocol_version,
                schedule,
            )
        );
    }

    #[test]
    fn test_parse() {
        assert!(ProtocolUpgradeVotingSchedule::from_str("2001-02-03 23:59:59").is_ok());
        assert!(ProtocolUpgradeVotingSchedule::from_str("123").is_err());
    }

    #[test]
    fn test_is_in_future() {
        assert!(ProtocolUpgradeVotingSchedule::from_str("2999-02-03 23:59:59")
            .unwrap()
            .is_in_future());
        assert!(!ProtocolUpgradeVotingSchedule::from_str("1999-02-03 23:59:59")
            .unwrap()
            .is_in_future());
    }
}
