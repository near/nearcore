use chrono::{DateTime, NaiveDateTime, ParseResult, Utc};
use near_primitives_core::types::ProtocolVersion;

/// Defines the point in time after which validators are expected to vote on the new protocol version.
/// The parameter is a `str` to allow construction as constant evaluation, i.e. at the compile-time.
pub(crate) struct ProtocolUpgradeVotingSchedule(pub &'static str);

impl ProtocolUpgradeVotingSchedule {
    #[allow(dead_code)]
    pub fn is_valid(&self) -> bool {
        self.parse().is_ok()
    }

    pub fn get(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_utc(self.parse().unwrap(), Utc)
    }

    fn parse(&self) -> ParseResult<NaiveDateTime> {
        NaiveDateTime::parse_from_str(&self.0, "%Y-%m-%d %H:%M:%S")
    }
}

pub(crate) fn get_protocol_version_internal(
    // Protocol version that will be used in the next epoch.
    next_epoch_protocol_version: ProtocolVersion,
    // Latest protocol version supported by this client.
    client_protocol_version: ProtocolVersion,
    // Point in time when the client is expected to vote for upgrading to the
    // latest protocol version.
    // If None, the client votes for the latest protocol version immediately.
    schedule: &[(ProtocolVersion, Option<ProtocolUpgradeVotingSchedule>)],
) -> ProtocolVersion {
    if next_epoch_protocol_version >= client_protocol_version {
        return client_protocol_version;
    }
    for (version, voting_start) in schedule {
        if *version != client_protocol_version {
            continue;
        }
        if let Some(voting_start) = voting_start {
            if chrono::Utc::now() < voting_start.get() {
                // Don't announce support for the latest protocol version yet.
                return next_epoch_protocol_version;
            } else {
                // The time has passed, announce the latest supported protocol version.
                return client_protocol_version;
            }
        }
        return client_protocol_version;
    }
    return client_protocol_version;
}

#[cfg(test)]
mod tests {
    use super::*;

    // The tests call `get_protocol_version_internal()` with the following parameters:
    // No schedule:                  (X-2,X), (X,X), (X+2,X)
    // Before the scheduled upgrade: (X-2,X), (X,X), (X+2,X)
    // After the scheduled upgrade:  (X-2,X), (X,X), (X+2,X)

    #[test]
    fn test_no_upgrade_schedule() {
        // As no protocol upgrade voting schedule is set, always return the version supported by the client.

        let client_protocol_version = 100;
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version - 2,
                client_protocol_version,
                &[],
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(client_protocol_version, client_protocol_version, &[])
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version + 2,
                client_protocol_version,
                &[]
            )
        );
    }

    #[test]
    fn test_none_upgrade_schedule() {
        // As no protocol upgrade voting schedule is set, always return the version supported by the client.

        let client_protocol_version = 100;
        let schedule = &[
            (client_protocol_version, None),
            (
                client_protocol_version - 1,
                Some(ProtocolUpgradeVotingSchedule("2000-01-01 00:00:00")),
            ),
        ];

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
                schedule
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version + 2,
                client_protocol_version,
                schedule
            )
        );
    }

    #[test]
    fn test_before_scheduled_time() {
        let client_protocol_version = 100;
        let schedule = &[
            (client_protocol_version, Some(ProtocolUpgradeVotingSchedule("2050-01-01 00:00:00"))),
            (client_protocol_version - 10, None),
        ];

        // The client supports a newer version than the version of the next epoch.
        // Upgrade voting will start in the far future, therefore don't announce the newest supported version.
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

        // Several upgrades happened before the scheduled time. Announce only the currently supported protocol version.
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
        let schedule = &[
            (client_protocol_version, Some(ProtocolUpgradeVotingSchedule("1999-01-01 00:00:00"))),
            (client_protocol_version - 10, None),
        ];

        // Regardless of the protocol version of the next epoch, return the version supported by the client.
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
        assert!(ProtocolUpgradeVotingSchedule("2001-02-03 23:59:59").is_valid());
        assert!(!ProtocolUpgradeVotingSchedule("123").is_valid());
    }
}
