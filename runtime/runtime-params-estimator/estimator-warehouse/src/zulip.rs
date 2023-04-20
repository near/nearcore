use std::env;

use anyhow::Context;
use reqwest::blocking::Client;

use crate::check::{Notice, RelativeChange, Status, UncertainChange};

const ZULIP_SERVER: &str = "near.zulipchat.com";

pub(crate) struct ZulipEndpoint {
    client: Client,
    full_endpoint_url: String,
    user_list: Option<String>,
    stream: Option<String>,
}

pub(crate) struct ZulipReport {
    status: Status,
    before: String,
    after: String,
    changes: Vec<RelativeChange>,
    changes_uncertain: Vec<UncertainChange>,
}

impl ZulipEndpoint {
    pub(crate) fn to_user(user: u64) -> anyhow::Result<Self> {
        Ok(Self {
            client: Client::new(),
            full_endpoint_url: Self::form_url()?,
            stream: None,
            user_list: Some(format!("[{user}]")),
        })
    }
    pub(crate) fn to_stream(stream: String) -> anyhow::Result<Self> {
        Ok(Self {
            client: Client::new(),
            full_endpoint_url: Self::form_url()?,
            stream: Some(stream),
            user_list: None,
        })
    }
    pub(crate) fn post(&self, report: &ZulipReport) -> anyhow::Result<()> {
        self.send_raw_message(&report.to_string(), "Bot reports")
    }
    fn form_url() -> anyhow::Result<String> {
        let bot_email =
            env::var("ZULIP_BOT_EMAIL").context("ZULIP_BOT_EMAIL environment variable not set")?;
        let api_key = env::var("ZULIP_BOT_API_KEY")
            .context("ZULIP_BOT_API_KEY environment variable not set")?;
        Ok(format!("https://{bot_email}:{api_key}@{ZULIP_SERVER}/api/v1/messages"))
    }
    fn send_raw_message(&self, msg: &str, topic: &str) -> anyhow::Result<()> {
        let params = if let Some(user_list) = &self.user_list {
            vec![("type", "private"), ("to", user_list), ("content", msg)]
        } else {
            vec![
                ("type", "stream"),
                ("to", self.stream.as_deref().unwrap()),
                ("topic", topic),
                ("content", msg),
            ]
        };
        self.client.post(&self.full_endpoint_url).form(&params).send()?;
        Ok(())
    }
}

impl ZulipReport {
    pub(crate) fn new(before: String, after: String) -> Self {
        Self { status: Status::Ok, before, after, changes: vec![], changes_uncertain: vec![] }
    }
    pub(crate) fn add(&mut self, warning: Notice, status: Status) {
        self.status = std::cmp::max(self.status, status);
        match warning {
            Notice::RelativeChange(change) => self.changes.push(change),
            Notice::UncertainChange(change) => self.changes_uncertain.push(change),
        }
    }

    pub(crate) fn changes(&self) -> &[RelativeChange] {
        self.changes.as_ref()
    }
}

impl std::fmt::Display for ZulipReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "## Report ")?;
        writeln!(f, "*Status: {:?}*", self.status)?;
        writeln!(f, "*Current commit: {}*", self.after)?;
        writeln!(f, "*Compared to: {}*", self.before)?;
        writeln!(f, "### Relative gas estimation changes above threshold: {}", self.changes.len())?;
        if !self.changes.is_empty() {
            writeln!(f, "```")?;
            for change in &self.changes {
                let percent_change = 100.0 * (change.after - change.before) / change.before;
                writeln!(
                    f,
                    "{:<40} {:>16} ➜ {:>16} ({}{:.2}%)",
                    change.estimation,
                    format_gas(change.before),
                    format_gas(change.after),
                    if percent_change >= 0.0 { "+" } else { "" },
                    percent_change,
                )?;
            }
            writeln!(f, "```")?;
        }
        writeln!(f, "### Gas estimator uncertain estimations: {}", self.changes_uncertain.len())?;
        if !self.changes_uncertain.is_empty() {
            writeln!(f, "```")?;
            for change in &self.changes_uncertain {
                writeln!(
                    f,
                    "{:<40} {:>32} ➜ {:<32}",
                    change.estimation, change.before, change.after,
                )?;
            }
            writeln!(f, "```")?;
        }
        Ok(())
    }
}

fn format_gas(gas: f64) -> String {
    match gas {
        n if n > 1e12 => format!("{:.2} Tgas", n / 1e12),
        n if n > 1e9 => format!("{:.2} Ggas", n / 1e9),
        n if n > 1e6 => format!("{:.2} Mgas", n / 1e6),
        n => format!("{:.0} gas", n),
    }
}

#[test]
fn test_format_gas() {
    assert_eq!(format_gas(0.0).as_str(), "0 gas");
    assert_eq!(format_gas(12345.0).as_str(), "12345 gas");
    assert_eq!(format_gas(123e6).as_str(), "123.00 Mgas");
    assert_eq!(format_gas(123.456e9).as_str(), "123.46 Ggas");
    assert_eq!(format_gas(0.456e12).as_str(), "456.00 Ggas");
    assert_eq!(format_gas(123.456e12).as_str(), "123.46 Tgas");
    assert_eq!(format_gas(123.456e15).as_str(), "123456.00 Tgas");
}
