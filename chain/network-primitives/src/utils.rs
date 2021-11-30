use crate::types::{BlockedPorts, PatternAddr};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

/// converts list of addresses represented by strings to <IpAddr, BlockedPorts> HashMap
///
/// Arguments:
/// - `blacklist`- list of strings in following formats:
///    - "IP" - for example 127.0.0.1 - if only IP is provided we will block all ports
///    - "IP:PORT - for example 127.0.0.1:2134
pub fn blacklist_from_iter<T>(blacklist: T) -> HashMap<IpAddr, BlockedPorts>
where
    T: IntoIterator<Item = String>,
{
    let mut blacklist_map = HashMap::new();
    for addr in blacklist {
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
