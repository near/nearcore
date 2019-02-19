#[cfg(test)]
mod tests {
    use crate::nightshade;
    use primitives::aggregate_signature::BlsSecretKey;
    use crate::nightshade::BareState;
    use primitives::hash::hash_struct;

    fn bare_state() -> BareState {
        BareState::new(0, hash_struct(&0))
    }

    #[test]
    /// 124 ms
    fn bs_encode() {
        let bs = bare_state();
        for _ in 0..100000 {
            let _x = bs.bs_encode();
        }
    }

    #[test]
    /// 1s 793ms
    fn sign() {
        let sk = BlsSecretKey::generate();
        let bs = bare_state();
        for _ in 0..10 {
            let _sig = bs.sign(&sk);
        }
    }
}