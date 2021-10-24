use std::collections::{HashMap, HashSet};
use std::{hash::Hash, net::IpAddr};

use cached::SizedCache;

use crate::types::{BlockedPorts, PatternAddr};

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

pub fn cache_to_hashmap<K: Hash + Eq + Clone, V: Clone>(cache: &SizedCache<K, V>) -> HashMap<K, V> {
    let keys: Vec<_> = cache.key_order().cloned().collect();
    keys.into_iter().zip(cache.value_order().cloned()).collect()
}
