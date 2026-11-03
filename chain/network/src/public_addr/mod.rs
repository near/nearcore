//! Determines the public IP of this node from the IPs at which its peers see it.
//!
//! Any single peer may report a wrong IP, and honest peers on the same private network
//! report a private one. So the node takes a vote over the reports of all its peers,
//! in which peers from one network group count once.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[cfg(test)]
mod tests;

/// Whether `ip` is reachable from the public internet. Rejects the addresses which would make
/// a dialer connect into its own network: loopback, private, link-local (which includes the
/// 169.254.169.254 cloud metadata endpoint), shared (carrier-grade NAT) and unique local
/// addresses, as well as unspecified, multicast and broadcast addresses. IPv4-mapped IPv6
/// addresses are checked as IPv4.
pub(crate) fn is_public_ip(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(ip) => {
            let [a, b, _, _] = ip.octets();
            // 100.64.0.0/10, see RFC 6598. `Ipv4Addr::is_shared` is not stable yet.
            let is_shared = a == 100 && (b & 0b1100_0000) == 0b0100_0000;
            !(ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_private()
                || ip.is_link_local()
                || is_shared
                || ip.is_multicast()
                || ip.is_broadcast())
        }
        IpAddr::V6(ip) => {
            !(ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.is_multicast())
        }
    }
}

/// Network group of a peer at `ip`: the /16 prefix for IPv4 and the /32 prefix for IPv6.
/// Addresses within a group are typically controlled by one operator.
fn netgroup(ip: IpAddr) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V4(ip) => IpAddr::V4(Ipv4Addr::from(u32::from(ip) & 0xffff_0000)),
        IpAddr::V6(ip) => IpAddr::V6(Ipv6Addr::from(u128::from(ip) & !(u128::MAX >> 32))),
    }
}

/// Outcome of `vote`.
pub(crate) struct Vote {
    /// The winning IP, or `None` if no IP was reported.
    pub ip: Option<IpAddr>,
    /// Number of network groups which reported `ip`.
    pub groups: usize,
    /// Number of network groups which reported the runner-up IP, or 0 if there is none.
    pub runner_up_groups: usize,
}

/// Decides the public IP of this node. `reports` contains a `(peer_ip, reported_ip)` pair for
/// each peer which reported the IP at which it sees this node. Peers from one network group
/// count once per reported IP, so that a single operator can't outvote the other peers by
/// opening many connections. The IP reported from the most groups wins. Ties go to the greater
/// IP, so that the result is deterministic.
///
/// Reported IPs which are not public are ignored, unless `allow_private` is set, which is meant
/// for private test networks. So are reported IPs which the listener at `listen_ip` can't accept
/// connections on: those of the other address family, unless it listens on `[::]`, which
/// accepts IPv4 too by default.
pub(crate) fn vote(
    reports: impl IntoIterator<Item = (IpAddr, IpAddr)>,
    listen_ip: IpAddr,
    allow_private: bool,
) -> Vote {
    let listen_ip = listen_ip.to_canonical();
    let dual_stack = listen_ip == IpAddr::V6(Ipv6Addr::UNSPECIFIED);
    let mut netgroups = HashMap::<IpAddr, HashSet<IpAddr>>::new();
    for (peer_ip, reported_ip) in reports {
        let reported_ip = reported_ip.to_canonical();
        if reported_ip.is_ipv4() != listen_ip.is_ipv4() && !dual_stack {
            continue;
        }
        if !(allow_private || is_public_ip(reported_ip)) {
            continue;
        }
        netgroups.entry(reported_ip).or_default().insert(netgroup(peer_ip));
    }
    let mut tally: Vec<_> = netgroups.into_iter().map(|(ip, groups)| (groups.len(), ip)).collect();
    tally.sort_unstable();
    let winner = tally.pop();
    Vote {
        ip: winner.map(|(_, ip)| ip),
        groups: winner.map_or(0, |(groups, _)| groups),
        runner_up_groups: tally.last().map_or(0, |&(groups, _)| groups),
    }
}
