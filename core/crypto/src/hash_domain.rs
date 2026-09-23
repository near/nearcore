/// Domain-separation tags for hashing in the protocol. The tag is prepended to
/// the input bytes before hashing, so a digest from one domain can never
/// collide with a digest from another. Each variant is one domain; uses that
/// are meant to produce the same digest must use the same variant.
#[derive(Debug, Clone, Copy)]
pub enum HashDomainTag {
    /// `MlDsa65PublicKey`-to-digest derivation. The digest serves as the
    /// on-trie access-key identifier (`MlDsa65PublicKeyHandle`).
    MlDsa65PubkeyV1,
}

impl HashDomainTag {
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            HashDomainTag::MlDsa65PubkeyV1 => b"near:ml-dsa-65-pubkey-hash:v1",
        }
    }
}
