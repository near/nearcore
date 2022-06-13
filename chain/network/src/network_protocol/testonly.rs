use super::*;

use crate::tests::data;
use near_network_primitives::time;
use rand::Rng;
use std::net;

pub fn make_ipv4(rng: &mut impl Rng) -> net::IpAddr {
    net::IpAddr::V4(net::Ipv4Addr::from(rng.gen::<[u8; 4]>()))
}

pub fn make_ipv6(rng: &mut impl Rng) -> net::IpAddr {
    net::IpAddr::V6(net::Ipv6Addr::from(rng.gen::<[u8; 16]>()))
}

pub fn make_peer_addr(rng: &mut impl Rng, ip: net::IpAddr) -> PeerAddr {
    PeerAddr { addr: net::SocketAddr::new(ip, rng.gen()), peer_id: Some(data::make_peer_id(rng)) }
}

pub fn make_validator(rng: &mut impl Rng, clock: &time::Clock, account_id: AccountId) -> Validator {
    Validator {
        peers: vec![
            // Can't inline make_ipv4/ipv6 calls, because 2-phase borrow
            // doesn't work.
            {
                let ip = make_ipv4(rng);
                make_peer_addr(rng, ip)
            },
            {
                let ip = make_ipv4(rng);
                make_peer_addr(rng, ip)
            },
            {
                let ip = make_ipv6(rng);
                make_peer_addr(rng, ip)
            },
        ],
        account_id,
        epoch_id: EpochId::default(),
        timestamp: clock.now_utc(),
    }
}

pub fn make_signed_validator(rng: &mut impl Rng, clock: &time::Clock) -> SignedValidator {
    let signer = data::make_signer(rng);
    make_validator(rng, clock, signer.account_id.clone()).sign(&signer)
}
