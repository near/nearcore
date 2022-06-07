use std::collections::HashSet;
use std::net;

/// Only IPv6 addresses are stored.  IPv4 addresses are mapped to IPv6 before being added.
///
/// Without the mapping, we could blacklist an IPv4 and still interact with that address if
/// it is presented as IPv6.
/// TODO: alternatively we could use IpAddr::to_canonical(), but then the variants of
/// the Entry enum would have to be private.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum Entry {
    Ip(net::Ipv6Addr),
    IpPort(net::Ipv6Addr, u16),
}

impl Entry {
    pub fn from_ip(ip: net::IpAddr) -> Entry {
        Entry::Ip(match ip {
            net::IpAddr::V4(ip) => ip.to_ipv6_mapped(),
            net::IpAddr::V6(ip) => ip,
        })
    }

    pub fn from_addr(addr: net::SocketAddr) -> Entry {
        Entry::IpPort(
            match addr.ip() {
                net::IpAddr::V4(ip) => ip.to_ipv6_mapped(),
                net::IpAddr::V6(ip) => ip,
            },
            addr.port(),
        )
    }
}

impl std::str::FromStr for Entry {
    type Err = std::net::AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<std::net::IpAddr>() {
            Ok(ip) => Ok(Entry::from_ip(ip)),
            Err(_) => Ok(Entry::from_addr(s.parse::<net::SocketAddr>()?)),
        }
    }
}

/// A blacklist for socket addresses.  Supports adding individual IP:port tuples
/// to the blacklist or entire IPs.
#[derive(Debug, Default, Clone)]
pub struct Blacklist(HashSet<Entry>);

// TODO(CP-34): merge Blacklist with whitelist functionality and replace them with sth
// like AuthorizationConfig.
impl FromIterator<Entry> for Blacklist {
    fn from_iter<I: IntoIterator<Item = Entry>>(i: I) -> Self {
        Self(i.into_iter().collect())
    }
}

impl Blacklist {
    /// Returns whether given address is on the blacklist.
    pub fn contains(&self, addr: net::SocketAddr) -> bool {
        self.0.contains(&Entry::from_ip(addr.ip())) || self.0.contains(&Entry::from_addr(addr))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const LO4: net::IpAddr = net::IpAddr::V4(net::Ipv4Addr::LOCALHOST);
    const LO6: net::IpAddr = net::IpAddr::V6(net::Ipv6Addr::LOCALHOST);

    #[test]
    fn test_parse_entry() {
        fn parse(value: &str) -> Option<Entry> {
            value.parse().ok()
        }

        assert_eq!(None, parse("foo"));
        assert_eq!(None, parse("192.0.2.*"));
        assert_eq!(None, parse("192.0.2.0/24"));
        assert_eq!(None, parse("192.0.2.4.5"));
        assert_eq!(None, parse("192.0.2.4:424242"));

        assert_eq!(parse("::ffff:192.0.2.4").unwrap(), parse("192.0.2.4").unwrap());
        assert_eq!(parse("[::ffff:192.0.2.4]:0").unwrap(), parse("192.0.2.4:0").unwrap());
        assert_eq!(parse("[::ffff:192.0.2.4]:42").unwrap(), parse("192.0.2.4:42").unwrap());

        assert_eq!(Entry::from_ip(LO6), parse("::1").unwrap());
        assert_eq!(Entry::from_addr(net::SocketAddr::new(LO6, 42)), parse("[::1]:42").unwrap());

        assert_eq!(Entry::from_ip(LO4), parse("::ffff:127.0.0.1").unwrap());
        assert_eq!(
            Entry::from_addr(net::SocketAddr::new(LO4, 42)),
            parse("[::ffff:127.0.0.1]:42").unwrap()
        );
    }

    #[test]
    fn test_blacklist() {
        use std::net::*;

        let ip: net::IpAddr = net::Ipv4Addr::new(192, 0, 2, 4).into();

        let mapped_ip = IpAddr::V6("::ffff:192.0.2.4".parse().unwrap());
        let mapped_lo4 = IpAddr::V6("::ffff:127.0.0.1".parse().unwrap());

        let blacklist: Blacklist = [
            Entry::from_ip(LO4),
            Entry::from_addr(SocketAddr::new(ip, 42)),
            Entry::from_addr(SocketAddr::new(LO6, 42)),
        ]
        .into_iter()
        .collect();

        assert!(blacklist.contains(SocketAddr::new(LO4, 42)));
        assert!(blacklist.contains(SocketAddr::new(LO4, 8080)));
        assert!(blacklist.contains(SocketAddr::new(ip, 42)));
        assert!(!blacklist.contains(SocketAddr::new(ip, 8080)));
        assert!(blacklist.contains(SocketAddr::new(LO6, 42)));
        assert!(!blacklist.contains(SocketAddr::new(LO6, 8080)));
        assert!(blacklist.contains(SocketAddr::new(mapped_lo4, 42)));
        assert!(blacklist.contains(SocketAddr::new(mapped_lo4, 8080)));
        assert!(blacklist.contains(SocketAddr::new(mapped_ip, 42)));
        assert!(!blacklist.contains(SocketAddr::new(mapped_ip, 8080)));
    }
}
