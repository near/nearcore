use chrono::{DateTime, NaiveDateTime, ParseResult, Utc};
use near_primitives_core::types::ProtocolVersion;

pub(crate) struct ProtocolUpgradeVotingSchedule(pub &'static str);
impl ProtocolUpgradeVotingSchedule {
    #[allow(dead_code)]
    pub fn is_valid(self) -> bool {
        self.parse().is_ok()
    }

    pub fn get(self) -> DateTime<Utc> {
        DateTime::<Utc>::from_utc(self.parse().unwrap(), Utc)
    }

    fn parse(self) -> ParseResult<NaiveDateTime> {
        NaiveDateTime::parse_from_str(&self.0, "%Y-%m-%d %H:%M:%S")
    }
}

pub(crate) fn get_protocol_version_internal(
    next_epoch_protocol_version: ProtocolVersion,
    client_protocol_version: ProtocolVersion,
    schedule: Option<ProtocolUpgradeVotingSchedule>,
) -> ProtocolVersion {
    if next_epoch_protocol_version >= client_protocol_version {
        client_protocol_version
    } else if let Some(voting_start) = schedule {
        if chrono::Utc::now() < voting_start.get() {
            // Don't announce support for the latest protocol version yet.
            next_epoch_protocol_version
        } else {
            // The time has passed, announce the latest supported protocol version.
            client_protocol_version
        }
    } else {
        // No voting schedule set, always announce the latest supported protocol version.
        client_protocol_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_upgrade_schedule() {
        // As no protocol upgrade voting schedule is set, always return the version supported by the client.

        let client_protocol_version = 100;
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version - 2,
                client_protocol_version,
                None
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(client_protocol_version, client_protocol_version, None)
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version + 2,
                client_protocol_version,
                None
            )
        );
    }

    #[test]
    fn test_before_scheduled_time() {
        let schedule = "2050-01-01 00:00:00";
        let client_protocol_version = 100;

        // The client supports a newer version than the version of the next epoch.
        // Upgrade voting will start in the far future, therefore don't announce the newest supported version.
        let next_epoch_protocol_version = client_protocol_version - 2;
        assert_eq!(
            next_epoch_protocol_version,
            get_protocol_version_internal(
                next_epoch_protocol_version,
                client_protocol_version,
                Some(ProtocolUpgradeVotingSchedule(schedule))
            )
        );

        // An upgrade happened before the scheduled time.
        let next_epoch_protocol_version = client_protocol_version;
        assert_eq!(
            next_epoch_protocol_version,
            get_protocol_version_internal(
                next_epoch_protocol_version,
                client_protocol_version,
                Some(ProtocolUpgradeVotingSchedule(schedule))
            )
        );

        // Several upgrades happened before the scheduled time. Announce only the currently supported protocol version.
        let next_epoch_protocol_version = client_protocol_version + 2;
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                next_epoch_protocol_version,
                client_protocol_version,
                Some(ProtocolUpgradeVotingSchedule(schedule))
            )
        );
    }

    #[test]
    fn test_after_scheduled_time() {
        let schedule = "1999-01-01 00:00:00";
        let client_protocol_version = 100;

        // Regardless of the protocol version of the next epoch, return the version supported by the client.
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version - 2,
                client_protocol_version,
                Some(ProtocolUpgradeVotingSchedule(schedule))
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version,
                client_protocol_version,
                Some(ProtocolUpgradeVotingSchedule(schedule))
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version + 2,
                client_protocol_version,
                Some(ProtocolUpgradeVotingSchedule(schedule))
            )
        );
    }
}
