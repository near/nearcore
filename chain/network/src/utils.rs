use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::net::IpAddr;

use crate::types::{BlockedPorts, PatternAddr};

pub fn blacklist_from_vec(blacklist: &Vec<String>) -> HashMap<IpAddr, BlockedPorts> {
    let mut blacklist_map = HashMap::new();
    for addr in blacklist.iter() {
        if let Ok(res) = addr.parse::<PatternAddr>() {
            match res {
                PatternAddr::Ip(addr) => {
                    blacklist_map
                        .entry(addr)
                        .and_modify(|blocked_ports| *blocked_ports = BlockedPorts::All)
                        .or_insert(BlockedPorts::All);
                }
                PatternAddr::IpPort(addr) => {
                    blacklist_map
                        .entry(addr.ip())
                        .and_modify(|blocked_ports| {
                            if let BlockedPorts::Some(ports) = blocked_ports {
                                ports.insert(addr.port());
                            }
                        })
                        .or_insert_with(|| {
                            BlockedPorts::Some(HashSet::from_iter(vec![addr.port()]))
                        });
                }
            }
        }
    }

    blacklist_map
}
