/// `PeerInfo` as `str`.
pub mod peer_info_to_str {
    use near_network::PeerInfo;
    use serde::{Deserialize, Deserializer, Serializer};
    use std::str::FromStr;

    pub fn serialize<S>(peer_info: &Option<PeerInfo>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match peer_info {
            Some(peer_info) => serializer.serialize_str(peer_info.to_string().as_str()),
            None => serializer.serialize_str(""),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<PeerInfo>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.is_empty() {
            Ok(None)
        } else {
            Ok(Some(PeerInfo::from_str(s.as_str()).expect("Error deserializing PeerInfo.")))
        }
    }
}

/// `Vec<PublicKey>` as str.
pub mod pks_as_str {
    use near_crypto::PublicKey;
    use serde::{Deserialize, Deserializer, Serializer};
    use std::convert::TryFrom;

    pub fn serialize<S>(peer_info: &Vec<PublicKey>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let res: Vec<_> = peer_info.iter().map(|pk| pk.to_string()).collect();
        serializer.serialize_str(&res.join(","))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<PublicKey>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.is_empty() {
            Ok(vec![])
        } else {
            Ok(s.split(",").map(|c| PublicKey::try_from(c).unwrap()).collect())
        }
    }
}
