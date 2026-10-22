use crate::public_addr::{is_public_ip, vote};
use std::net::{IpAddr, Ipv4Addr};

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

/// IP of a peer in the network group 1.`netgroup`.0.0/16.
fn peer_ip(netgroup: u8, host: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(1, netgroup, 0, host))
}

/// The form in which peers listening on a dual-stack socket see an IPv4 address.
fn ipv4_mapped(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(ip) => IpAddr::V6(ip.to_ipv6_mapped()),
        ip => ip,
    }
}

const X: &str = "203.0.113.1";
const Y: &str = "198.51.100.1";

#[test]
fn is_public_ip_rejects_local_addresses() {
    for local in [
        "0.0.0.0",
        "127.0.0.1",
        "10.1.2.3",
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",
        "100.64.0.1",
        "100.127.255.255",
        "224.0.0.1",
        "255.255.255.255",
        "::",
        "::1",
        "fd00::1",
        "fe80::1",
        "ff02::1",
        "::ffff:127.0.0.1",
        "::ffff:10.0.0.1",
        "::ffff:169.254.169.254",
    ] {
        assert!(!is_public_ip(ip(local)), "{local} must not be public");
    }
    for public in ["1.1.1.1", "100.63.255.255", "100.128.0.1", "2606:4700::1111", "::ffff:1.1.1.1"]
    {
        assert!(is_public_ip(ip(public)), "{public} must be public");
    }
}

#[test]
fn vote_picks_ip_reported_from_most_netgroups() {
    assert_eq!(vote([(peer_ip(0, 1), ip(X))], false).ip, Some(ip(X)));
    let reports = [(peer_ip(0, 1), ip(X)), (peer_ip(1, 1), ip(Y)), (peer_ip(2, 1), ip(X))];
    assert_eq!(vote(reports, false).ip, Some(ip(X)));
}

#[test]
fn vote_counts_each_netgroup_once() {
    // Many peers from one network group are outvoted by peers from two groups.
    let mut reports: Vec<_> = (0..20).map(|host| (peer_ip(0, host), ip(Y))).collect();
    reports.extend((1..3).map(|g| (peer_ip(g, 1), ip(X))));
    let outcome = vote(reports, false);
    assert_eq!((outcome.ip, outcome.groups, outcome.runner_up_groups), (Some(ip(X)), 2, 1));

    // IPv6 peers are grouped by their /32 prefix.
    let reports = [
        (ip("2001:db8:1::1"), ip(X)),
        (ip("2001:db8:2::1"), ip(X)),
        (ip("2001:db9::1"), ip(Y)),
        (ip("2001:dba::1"), ip(Y)),
    ];
    assert_eq!(vote(reports, false).ip, Some(ip(Y)));
}

#[test]
fn vote_ignores_private_ips_unless_allowed() {
    let reports: Vec<_> = (0..5).map(|g| (peer_ip(g, 1), ip("10.0.0.1"))).collect();
    assert_eq!(vote(reports.clone(), false).ip, None);
    assert_eq!(vote(reports, true).ip, Some(ip("10.0.0.1")));
}

#[test]
fn vote_treats_ipv4_mapped_addresses_as_ipv4() {
    // A mapped report counts towards the IPv4 address, which is the one adopted.
    let reports =
        [(peer_ip(0, 1), ipv4_mapped(ip(X))), (peer_ip(1, 1), ip(X)), (peer_ip(2, 1), ip(Y))];
    assert_eq!(vote(reports, false).ip, Some(ip(X)));

    // A peer at a mapped address is in the network group of the IPv4 address.
    let reports = [
        (peer_ip(0, 1), ip(X)),
        (ipv4_mapped(peer_ip(0, 2)), ip(X)),
        (peer_ip(1, 1), ip(Y)),
        (peer_ip(2, 1), ip(Y)),
    ];
    assert_eq!(vote(reports, false).ip, Some(ip(Y)));
}
