use crate::account::*;
use crate::asset::*;
use crate::rate::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Serialize, Deserialize)]
pub struct Agent {
    pub account: Account,
    pub is_alive: bool,
}

impl Agent {
    pub fn simulate(&mut self, rates: &HashMap<Exchange, Rate>, mission: &Account) {
        // Every tick agent should be able to purchase 1 MissionTime.
        // First it tries to purchase MissionTime with its Resource through Exchange::MissionTimeWithResource.
        // If this fails, it will try to purchase through Exchange::MissionTimeWithTrust.
        // If agent cannot purchase any more MissionTime it dies.
        let Quantity(lifetime_before) = self.account.quantity(&Asset::MissionTime);
        let exs = [Exchange::MissionTimeWithResource, Exchange::MissionTimeWithTrust];
        if let Some(Tranx::Approved(buyer, _)) = exs.iter().find_map(|ex| {
            match Account::exchange(rates.get(ex).unwrap(), Quantity(1), &self.account, mission) {
                Tranx::Denied(_) => None,
                tranx => Some(tranx),
            }
        }) {
            self.account = buyer;
        }
        let Quantity(lifetime_after) = self.account.quantity(&Asset::MissionTime);
        if lifetime_after <= lifetime_before {
            self.is_alive = false;
        }
    }
}
