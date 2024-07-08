use chrono::{DateTime, NaiveDateTime, Utc};
use near_primitives_core::types::ProtocolVersion;
use std::env;

const NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE: &str = "NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE";

#[derive(thiserror::Error, Clone, Debug)]
pub enum ProtocolUpgradeVotingScheduleError {
    #[error("The final upgrade must be the client protocol version! final version: {0}, client version: {1}")]
    InvalidFinalUpgrade(ProtocolVersion, ProtocolVersion),
    #[error("The upgrades must be sorted by datetime!")]
    InvalidDateTimeOrder,
    #[error("The upgrades must be sorted and increasing by one!")]
    InvalidProtocolVersionOrder,

    #[error("The environment override has an invalid format! Input: {0} Error: {1}")]
    InvalidOverrideFormat(String, String),
}

type ProtocolUpgradeVotingScheduleRaw = Vec<(chrono::DateTime<Utc>, ProtocolVersion)>;

/// Defines a schedule for validators to vote for the protocol version upgrades.
/// Multiple protocol version upgrades can be scheduled. The default schedule is
/// empty and in that case the node will always vote for the client protocol
/// version.
#[derive(Clone, Debug, PartialEq)]
pub struct ProtocolUpgradeVotingSchedule {
    // The highest protocol version supported by the client. This class will
    // check that the schedule ends with this version.
    client_protocol_version: ProtocolVersion,

    /// The schedule is a sorted list of (datetime, version) tuples. The node
    /// should vote for the highest version that is less than or equal to the
    /// current time.
    schedule: ProtocolUpgradeVotingScheduleRaw,
}

impl ProtocolUpgradeVotingSchedule {
    /// This method creates an instance of the ProtocolUpgradeVotingSchedule
    /// that will immediately vote for the client protocol version.
    pub fn new_immediate(client_protocol_version: ProtocolVersion) -> Self {
        Self { client_protocol_version, schedule: vec![] }
    }

    /// This method creates an instance of the ProtocolUpgradeVotingSchedule.
    ///
    /// It will first check if the NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE is set
    /// in the environment and if so this override will be used as schedule.
    /// This should only be used in tests, in particular in tests that in some
    /// way test neard upgrades.
    ///
    /// Otherwise it will use the provided schedule.
    pub fn new_from_env_or_schedule(
        client_protocol_version: ProtocolVersion,
        mut schedule: ProtocolUpgradeVotingScheduleRaw,
    ) -> Result<Self, ProtocolUpgradeVotingScheduleError> {
        let env_override = env::var(NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE);
        if let Ok(env_override) = env_override {
            schedule = Self::parse_override(&env_override)?;
            tracing::warn!(
                target: "protocol_upgrade",
                ?schedule,
                "Setting protocol upgrade override. This is fine in tests but should be avoided otherwise"
            );
        }

        // Sanity and invariant checks.

        // The final upgrade must be the client protocol version.
        if let Some((_, version)) = schedule.last() {
            if *version != client_protocol_version {
                return Err(ProtocolUpgradeVotingScheduleError::InvalidFinalUpgrade(
                    *version,
                    client_protocol_version,
                ));
            }
        }

        // The upgrades must be sorted by datetime.
        for i in 1..schedule.len() {
            let prev_time = schedule[i - 1].0;
            let this_time = schedule[i].0;
            if !(prev_time < this_time) {
                return Err(ProtocolUpgradeVotingScheduleError::InvalidDateTimeOrder);
            }
        }

        // The upgrades must be increasing by 1.
        for i in 1..schedule.len() {
            let prev_protocol_version = schedule[i - 1].1;
            let this_protocol_version = schedule[i].1;
            if prev_protocol_version + 1 != this_protocol_version {
                return Err(ProtocolUpgradeVotingScheduleError::InvalidProtocolVersionOrder);
            }
        }

        tracing::debug!(target: "protocol_upgrade", ?schedule, "created protocol upgrade schedule");

        Ok(Self { client_protocol_version, schedule })
    }

    /// This method returns the protocol version that the node should vote for.
    #[cfg(feature = "clock")]
    pub(crate) fn get_protocol_version(
        &self,
        now: DateTime<Utc>,
        // Protocol version that will be used in the next epoch.
        next_epoch_protocol_version: ProtocolVersion,
    ) -> ProtocolVersion {
        if next_epoch_protocol_version >= self.client_protocol_version {
            return self.client_protocol_version;
        }

        if self.schedule.is_empty() {
            return self.client_protocol_version;
        }

        // The datetime values in the schedule are sorted in ascending order.
        // Find the first datetime value that is less than the current time
        // and higher than next_epoch_protocol_version.
        // The schedule is sorted and the last value is the client_protocol_version
        // so we are guaranteed to find a correct protocol version.
        let mut result = next_epoch_protocol_version;
        for (time, version) in &self.schedule {
            if now < *time {
                break;
            }
            result = *version;
            if *version > next_epoch_protocol_version {
                break;
            }
        }

        result
    }

    /// Returns the schedule. Should only be used for exporting metrics.
    pub fn schedule(&self) -> &Vec<(chrono::DateTime<Utc>, ProtocolVersion)> {
        &self.schedule
    }

    /// A helper method to parse the datetime string.
    pub fn parse_datetime(s: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
        let datetime = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")?;
        let datetime = DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc);
        Ok(datetime)
    }

    // Parse the protocol version override from the environment.
    // The format is comma separate datetime:=version pairs.
    fn parse_override(
        override_str: &str,
    ) -> Result<ProtocolUpgradeVotingScheduleRaw, ProtocolUpgradeVotingScheduleError> {
        // The special value "now" means that the upgrade should happen immediately.
        if override_str.to_lowercase() == "now" {
            return Ok(vec![]);
        }

        let mut result = vec![];
        let datetime_and_version_vec = override_str.split(',').collect::<Vec<_>>();
        for datetime_and_version in datetime_and_version_vec {
            let datetime_and_version = datetime_and_version.split('=').collect::<Vec<_>>();
            let [datetime, version] = datetime_and_version[..] else {
                let input = format!("{:?}", datetime_and_version);
                let error = "The override must be in the format datetime=version!".to_string();
                return Err(ProtocolUpgradeVotingScheduleError::InvalidOverrideFormat(
                    input, error,
                ));
            };

            let datetime = Self::parse_datetime(datetime).map_err(|err| {
                ProtocolUpgradeVotingScheduleError::InvalidOverrideFormat(
                    datetime.to_string(),
                    err.to_string(),
                )
            })?;
            let version = version.parse::<u32>().map_err(|err| {
                ProtocolUpgradeVotingScheduleError::InvalidOverrideFormat(
                    version.to_string(),
                    err.to_string(),
                )
            })?;
            result.push((datetime, version));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    /// Make a simple schedule with a single protocol version upgrade.
    fn make_simple_voting_schedule(
        client_protocol_version: u32,
        datetime_str: &str,
    ) -> ProtocolUpgradeVotingSchedule {
        make_voting_schedule(client_protocol_version, vec![(datetime_str, client_protocol_version)])
    }

    fn make_voting_schedule(
        client_protocol_version: ProtocolVersion,
        schedule: Vec<(&str, u32)>,
    ) -> ProtocolUpgradeVotingSchedule {
        make_voting_schedule_impl(schedule, client_protocol_version).unwrap()
    }

    /// Make a schedule with a given list of protocol version upgrades.
    fn make_voting_schedule_impl(
        schedule: Vec<(&str, u32)>,
        client_protocol_version: ProtocolVersion,
    ) -> Result<ProtocolUpgradeVotingSchedule, ProtocolUpgradeVotingScheduleError> {
        let parse = |(datetime_str, version)| {
            let datetime = ProtocolUpgradeVotingSchedule::parse_datetime(datetime_str);
            let datetime = datetime.unwrap();
            (datetime, version)
        };
        let schedule = schedule.into_iter().map(parse).collect::<Vec<_>>();
        let schedule = ProtocolUpgradeVotingSchedule::new_from_env_or_schedule(
            client_protocol_version,
            schedule,
        );
        schedule
    }

    // The tests call `get_protocol_version()` with the following parameters:
    // No schedule:                  (X-2,X), (X,X), (X+2,X)
    // Before the scheduled upgrade: (X-2,X), (X,X), (X+2,X)
    // After the scheduled upgrade:  (X-2,X), (X,X), (X+2,X)

    #[test]
    fn test_default_upgrade_schedule() {
        // As no protocol upgrade voting schedule is set, always return the version supported by the client.
        let now = chrono::Utc::now();
        let client_protocol_version = 100;
        let schedule = ProtocolUpgradeVotingSchedule::new_immediate(client_protocol_version);

        assert_eq!(
            client_protocol_version,
            schedule.get_protocol_version(now, client_protocol_version - 2)
        );
        assert_eq!(
            client_protocol_version,
            schedule.get_protocol_version(now, client_protocol_version)
        );
        assert_eq!(
            client_protocol_version,
            schedule.get_protocol_version(now, client_protocol_version + 2)
        );
    }

    #[test]
    fn test_before_scheduled_time() {
        let now = chrono::Utc::now();

        let client_protocol_version = 100;
        let schedule = make_simple_voting_schedule(client_protocol_version, "3000-01-01 00:00:00");

        // The client supports a newer version than the version of the next epoch.
        // Upgrade voting will start in the far future, therefore don't announce the newest supported version.
        let next_epoch_protocol_version = client_protocol_version - 2;
        assert_eq!(
            next_epoch_protocol_version,
            schedule.get_protocol_version(now, next_epoch_protocol_version)
        );

        // An upgrade happened before the scheduled time.
        let next_epoch_protocol_version = client_protocol_version;
        assert_eq!(
            next_epoch_protocol_version,
            schedule.get_protocol_version(now, next_epoch_protocol_version)
        );

        // Several upgrades happened before the scheduled time. Announce only the currently supported protocol version.
        let next_epoch_protocol_version = client_protocol_version + 2;
        assert_eq!(
            client_protocol_version,
            schedule.get_protocol_version(now, next_epoch_protocol_version)
        );
    }

    #[test]
    fn test_after_scheduled_time() {
        let now = chrono::Utc::now();

        let client_protocol_version = 100;
        let schedule = make_simple_voting_schedule(client_protocol_version, "1900-01-01 00:00:00");

        // Regardless of the protocol version of the next epoch, return the version supported by the client.
        assert_eq!(
            client_protocol_version,
            schedule.get_protocol_version(now, client_protocol_version - 2)
        );
        assert_eq!(
            client_protocol_version,
            schedule.get_protocol_version(now, client_protocol_version)
        );
        assert_eq!(
            client_protocol_version,
            schedule.get_protocol_version(now, client_protocol_version + 2)
        );
    }

    #[test]
    fn test_double_upgrade_schedule() {
        let current_protocol_version = 100;
        let client_protocol_version = 102;
        let schedule = vec![
            ("2000-01-10 00:00:00", current_protocol_version + 1),
            ("2000-01-15 00:00:00", current_protocol_version + 2),
        ];

        let schedule = make_voting_schedule(client_protocol_version, schedule);

        // Test that the current version is returned before the first upgrade.
        let now = ProtocolUpgradeVotingSchedule::parse_datetime("2000-01-05 00:00:00").unwrap();
        assert_eq!(
            current_protocol_version,
            schedule.get_protocol_version(now, current_protocol_version)
        );

        // Test the first upgrade.
        let now = ProtocolUpgradeVotingSchedule::parse_datetime("2000-01-10 00:00:00").unwrap();
        assert_eq!(
            current_protocol_version + 1,
            schedule.get_protocol_version(now, current_protocol_version)
        );
        let now = ProtocolUpgradeVotingSchedule::parse_datetime("2000-01-12 00:00:00").unwrap();
        assert_eq!(
            current_protocol_version + 1,
            schedule.get_protocol_version(now, current_protocol_version)
        );

        // Test the final upgrade.
        let now = ProtocolUpgradeVotingSchedule::parse_datetime("2000-01-15 00:00:00").unwrap();
        assert_eq!(
            current_protocol_version + 2,
            schedule.get_protocol_version(now, current_protocol_version + 1)
        );
        let now = ProtocolUpgradeVotingSchedule::parse_datetime("2000-01-20 00:00:00").unwrap();
        assert_eq!(
            current_protocol_version + 2,
            schedule.get_protocol_version(now, current_protocol_version + 1)
        );
    }

    #[test]
    fn test_upgrades_are_voted_one_at_a_time() {
        let current_protocol_version = 100;
        let client_protocol_version = 103;
        let schedule = vec![
            ("2000-01-10 00:00:00", current_protocol_version + 1),
            ("2000-01-10 01:00:00", current_protocol_version + 2),
            ("2000-01-10 02:00:00", current_protocol_version + 3),
        ];

        let schedule = make_voting_schedule(client_protocol_version, schedule);

        // Test that the current version is returned before the first upgrade.
        let now = ProtocolUpgradeVotingSchedule::parse_datetime("2000-01-05 00:00:00").unwrap();
        assert_eq!(
            current_protocol_version,
            schedule.get_protocol_version(now, current_protocol_version)
        );

        // Upgrades are scheduled very close to each other, but they should be voted on at a time.
        // Test the first upgrade.
        let now = ProtocolUpgradeVotingSchedule::parse_datetime("2000-01-10 10:00:00").unwrap();
        assert_eq!(
            current_protocol_version + 1,
            schedule.get_protocol_version(now, current_protocol_version)
        );

        // Test the second upgrade.
        let now = ProtocolUpgradeVotingSchedule::parse_datetime("2000-01-10 10:00:00").unwrap();
        assert_eq!(
            current_protocol_version + 2,
            schedule.get_protocol_version(now, current_protocol_version + 1)
        );

        // Test the final upgrade.
        let now = ProtocolUpgradeVotingSchedule::parse_datetime("2000-01-10 10:00:00").unwrap();
        assert_eq!(
            current_protocol_version + 3,
            schedule.get_protocol_version(now, current_protocol_version + 2)
        );
    }

    #[test]
    fn test_errors() {
        let client_protocol_version = 110;

        // invalid last upgrade
        let schedule = vec![("2000-01-10 00:00:00", 108), ("2000-01-15 00:00:00", 109)];
        let schedule = make_voting_schedule_impl(schedule, client_protocol_version);
        assert!(schedule.is_err());

        // invalid protocol version order - decreasing versions
        let schedule = vec![("2000-01-10 00:00:00", 111), ("2000-01-15 00:00:00", 110)];
        let schedule = make_voting_schedule_impl(schedule, client_protocol_version);
        assert!(schedule.is_err());

        // invalid protocol version order - skip version
        let schedule = vec![("2000-01-10 00:00:00", 108), ("2000-01-15 00:00:00", 110)];
        let schedule = make_voting_schedule_impl(schedule, client_protocol_version);
        assert!(schedule.is_err());

        // invalid datetime order
        let schedule = vec![("2000-01-15 00:00:00", 109), ("2000-01-10 00:00:00", 110)];
        let schedule = make_voting_schedule_impl(schedule, client_protocol_version);
        assert!(schedule.is_err());
    }

    #[test]
    fn test_parse() {
        assert!(ProtocolUpgradeVotingSchedule::parse_datetime("2001-02-03 23:59:59").is_ok());
        assert!(ProtocolUpgradeVotingSchedule::parse_datetime("123").is_err());
    }

    #[test]
    fn test_parse_override() {
        // prepare some datetime strings

        let datetime_str_1 = "2001-01-01 23:59:59";
        let datetime_str_2 = "2001-01-02 23:59:59";
        let datetime_str_3 = "2001-01-03 23:59:59";

        let datetime_1 = ProtocolUpgradeVotingSchedule::parse_datetime(datetime_str_1).unwrap();
        let datetime_2 = ProtocolUpgradeVotingSchedule::parse_datetime(datetime_str_2).unwrap();
        let datetime_3 = ProtocolUpgradeVotingSchedule::parse_datetime(datetime_str_3).unwrap();

        let datetime_version_str_1 = format!("{}={}", datetime_str_1, 101);
        let datetime_version_str_2 = format!("{}={}", datetime_str_2, 102);
        let datetime_version_str_3 = format!("{}={}", datetime_str_3, 103);

        // test immediate upgrade

        let override_str = "now";
        let raw_schedule = ProtocolUpgradeVotingSchedule::parse_override(override_str).unwrap();
        assert_eq!(raw_schedule.len(), 0);

        // test single upgrade

        let override_str = datetime_version_str_1.clone();
        let raw_schedule = ProtocolUpgradeVotingSchedule::parse_override(&override_str).unwrap();
        assert_eq!(raw_schedule.len(), 1);

        assert_eq!(raw_schedule[0].0, datetime_1);
        assert_eq!(raw_schedule[0].1, 101);

        // test double upgrade

        let override_str =
            [datetime_version_str_1.clone(), datetime_version_str_2.clone()].join(",");
        let raw_schedule = ProtocolUpgradeVotingSchedule::parse_override(&override_str).unwrap();
        assert_eq!(raw_schedule.len(), 2);

        assert_eq!(raw_schedule[0].0, datetime_1);
        assert_eq!(raw_schedule[0].1, 101);

        assert_eq!(raw_schedule[1].0, datetime_2);
        assert_eq!(raw_schedule[1].1, 102);

        // test triple upgrade

        let override_str =
            [datetime_version_str_1, datetime_version_str_2, datetime_version_str_3].join(",");
        let raw_schedule = ProtocolUpgradeVotingSchedule::parse_override(&override_str).unwrap();
        assert_eq!(raw_schedule.len(), 3);

        assert_eq!(raw_schedule[0].0, datetime_1);
        assert_eq!(raw_schedule[0].1, 101);

        assert_eq!(raw_schedule[1].0, datetime_2);
        assert_eq!(raw_schedule[1].1, 102);

        assert_eq!(raw_schedule[2].0, datetime_3);
        assert_eq!(raw_schedule[2].1, 103);
    }

    #[test]
    fn test_env_override() {
        let client_protocol_version = 100;

        std::env::set_var(NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE, "now");
        let schedule = make_simple_voting_schedule(client_protocol_version, "2999-02-03 23:59:59");

        assert_eq!(schedule, ProtocolUpgradeVotingSchedule::new_immediate(client_protocol_version));

        let datetime_override = "2000-01-01 23:59:59";
        std::env::set_var(
            NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE,
            format!("{}={}", datetime_override, client_protocol_version),
        );
        let schedule = make_simple_voting_schedule(client_protocol_version, "2999-02-03 23:59:59");

        assert_eq!(
            schedule.schedule()[0].0,
            ProtocolUpgradeVotingSchedule::parse_datetime(datetime_override).unwrap()
        );
        assert_eq!(schedule.schedule()[0].1, client_protocol_version);
    }
}
