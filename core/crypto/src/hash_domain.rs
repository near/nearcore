/// Domain-separation tags for hashing in the protocol. The tag is prepended
/// to the input bytes before hashing, so a digest from one domain can never
/// collide with a digest from another. Each variant is one domain; uses that
/// deliberately share a digest share a variant.
#[derive(Debug, Clone, Copy)]
pub enum HashDomainTag {
    /// `MlDsa65PublicKey`-to-digest derivation. The digest serves as the
    /// on-trie access-key identifier (`MlDsa65PublicKeyHandle`) and is
    /// expected to also serve as the account-id payload for universal
    /// implicit accounts when that feature lands - the same digest in both
    /// roles, by design.
    MlDsa65PubkeyHash,
}

impl HashDomainTag {
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            HashDomainTag::MlDsa65PubkeyHash => b"near:ml-dsa-65-pubkey-hash:v1",
        }
    }
}
