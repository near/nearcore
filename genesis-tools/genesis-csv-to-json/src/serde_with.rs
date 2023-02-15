/// `PeerInfo` as `str`.
pub mod peer_info_to_str {
    use near_network::types::PeerInfo;
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

    pub fn serialize<S>(peer_info: &[PublicKey], serializer: S) -> Result<S::Ok, S::Error>
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
        let mut s = String::deserialize(deserializer)?;
        if s.is_empty() {
            Ok(vec![])
        } else {
            s.retain(|c| !c.is_whitespace());
            Ok(s.split(',').map(|c| c.parse().unwrap()).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use near_crypto::PublicKey;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Test {
        #[serde(with = "crate::serde_with::pks_as_str")]
        keys: Vec<PublicKey>,
    }

    #[test]
    fn test_deserialize_pubkeys_with_whitespace() {
        let test_str = r#"{"keys":  "ed25519:EsjyvmBb2ESGiyjPHMBUnTGCe1P6hPjmxxY2b2hrTBAv,  ed25519:2djz3u3CjV4dpDZryudwA4JNDcGnVwNtphjZQbUzrhLE, ed25519:2f9Zv5kuyuPM5DCyEP5pSqg58NQ8Ct9uSRerZXnCS9fK,ed25519:3xCFas58RKvD5UpF9GqvEb6q9rvgfbEJPhLf85zc4HpC,  ed25519:4588iQsoG9mWjDPLbipQvaGNqo9UCphGsgon8u2yXArE,ed25519:5Me9NjXh3br1Rp2zvqaTUo8qvXcDPZ3YxafewzUKW7zc,\ned25519:93A8upKEMoZG9bBFyXJjQhzcMJBvSHHtPjZP3173FARk"}"#;
        serde_json::from_str::<Test>(test_str).unwrap();
    }
}
