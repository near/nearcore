use crate::curve::*;
use crate::hash::Hash512;
use c2_chacha::guts::ChaCha;
use curve25519_dalek::constants::{
    RISTRETTO_BASEPOINT_POINT as G, RISTRETTO_BASEPOINT_TABLE as GT,
};
use curve25519_dalek::traits::{Identity, VartimeMultiscalarMul};
use rand_core::{CryptoRng, RngCore};
use std::borrow::Borrow;
use std::convert::TryFrom;
use std::iter::once;
use std::ops::{Add, Deref, DerefMut, Sub};

pub use crate::vrf::{PublicKey, SecretKey};

#[derive(Clone)]
struct ChaChaScalars(ChaCha, Option<[u8; 32]>);

impl ChaChaScalars {
    fn from_hash(hash: [u8; 32]) -> Self {
        ChaChaScalars(ChaCha::new(&hash, &[0; 8]), None)
    }
}

impl Iterator for ChaChaScalars {
    type Item = Scalar;

    fn next(&mut self) -> Option<Scalar> {
        Some(Scalar::from_bytes_mod_order(match self.1 {
            Some(s) => {
                self.1 = None;
                s
            }
            None => {
                let mut block = [0; 64];
                self.0.refill(10, &mut block);
                let (b1, b2) = array_refs!(&block, 32, 32);
                self.1 = Some(*b2);
                *b1
            }
        }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (usize::max_value(), None)
    }
}

struct ExpandIter<T>(Box<[T]>);

fn expand<T: Copy, E: Iterator>(els: E) -> ExpandIter<T>
where
    E::Item: Borrow<T>,
    for<'a> &'a T: Sub<Output = T>,
{
    let mut res = Vec::with_capacity(els.size_hint().0);
    for vv in els {
        let mut v = *vv.borrow();
        for v2 in res.iter_mut() {
            let dif = &v - v2;
            *v2 = v;
            v = dif;
        }
        res.push(v);
    }
    ExpandIter(res.into_boxed_slice())
}

impl<T: Copy + Default + 'static> Iterator for ExpandIter<T>
where
    for<'a> &'a T: Add<Output = T>,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        Some(if self.0.is_empty() {
            T::default()
        } else {
            let mut v = self.0[self.0.len() - 1];
            let r = 0..self.0.len() - 1;
            for v2 in self.0[r].iter_mut().rev() {
                v = &*v2 + &v;
                *v2 = v;
            }
            v
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (usize::max_value(), None)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct Params {
    n: usize,
    k: usize,
}

impl Params {
    pub fn new(n: usize, k: usize) -> Self {
        if !Self::is_valid(n, k) {
            panic!("Invalid parameters");
        }
        Params { n, k }
    }

    pub const fn is_valid(n: usize, k: usize) -> bool {
        Self::is_valid_n(n) & (k <= n)
    }

    const fn is_valid_n(n: usize) -> bool {
        (n <= u32::max_value() as usize) & (n <= (usize::max_value() - 64) / 32)
    }

    pub const fn n(&self) -> usize {
        self.n
    }

    pub const fn k(&self) -> usize {
        self.k
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct PublicShare(pub Box<[u8]>);
#[derive(Clone, PartialEq, Eq)]
pub struct SecretShare(Box<[Scalar]>);
#[derive(Clone, PartialEq, Eq)]
pub struct ValidatedPublicShare(Box<[Point]>);
value_type!(pub, EncryptedShare, 32, "encrypted share");
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct DecryptedShare(Scalar);
value_type!(pub, DecryptionFailureProof, 96, "decryption failure proof");
#[derive(Clone, PartialEq, Eq)]
pub struct RandomEpoch(Box<[Point]>);
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct RandomEpochSecret(Scalar);
#[derive(Copy, Clone)]
pub struct RandomRound([u8; 32], Point);
value_type!(pub, RandomShare, 96, "random share");
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct ValidatedRandomShare(Point);
value_type!(pub, RandomValue, 32, "random value");

pub fn generate_share(
    Params { n, k }: Params,
    key: &PublicKey,
    rng: &mut (impl RngCore + CryptoRng),
) -> (PublicShare, SecretShare) {
    let mut public = Vec::with_capacity(k * 32 + 64);
    let mut secret = Vec::with_capacity(n);
    for _ in 0..k {
        let s = Scalar::random(rng);
        public.extend_from_slice(&(&s * &GT).pack());
        secret.push(s);
    }
    let mut r = Scalar::random(rng);
    public.extend_from_slice(&(&r * &GT).pack());
    secret.iter().zip(ChaChaScalars::from_hash(hash!(key, &public))).for_each(|(s, c)| r -= c * s);
    public.extend_from_slice(&r.pack());
    secret.extend(expand::<Scalar, _>(secret.iter()).take(n - k));
    debug_assert!(public.len() == PublicShare::length(Params { n, k }) && secret.len() == n);
    (PublicShare(public.into_boxed_slice()), SecretShare(secret.into_boxed_slice()))
}

impl PublicShare {
    pub const fn length(Params { k, .. }: Params) -> usize {
        k * 32 + 64
    }

    pub fn validate(&self, key: &PublicKey) -> Option<ValidatedPublicShare> {
        let k = (self.0.len() - 64) / 32;
        assert!(self.0.len() >= 64 && self.0.len() % 32 == 0 && Params::is_valid_n(k));
        let mut res = Vec::with_capacity(k);
        for i in 0..k {
            res.push(try_unpack!(array_ref!(self.0, 32 * i, 32)));
        }
        let comm = array_ref!(self.0, 32 * k, 32);
        let r = try_unpack!(array_ref!(self.0, 32 * k + 32, 32));
        if Point::vartime_multiscalar_mul(
            ChaChaScalars::from_hash(hash!(key, &self.0[..32 * k + 32])).take(k).chain(once(r)),
            res.iter().chain(once(&G)),
        )
        .pack()
            != *comm
        {
            return None;
        }
        Some(ValidatedPublicShare(res.into_boxed_slice()))
    }
}

fn xor32(a: [u8; 32], b: [u8; 32]) -> [u8; 32] {
    let mut res = [0; 32];
    for i in 0..32 {
        res[i] = a[i] ^ b[i];
    }
    res
}

impl SecretShare {
    pub fn encrypt(&self, index: usize, key: &PublicKey) -> EncryptedShare {
        let s = &self.0[index];
        EncryptedShare(xor32(hash!(s * &key.1), s.pack()))
    }
}

impl ValidatedPublicShare {
    fn get_element(&self, index: usize) -> Point {
        if index < self.0.len() {
            self.0[index as usize]
        } else {
            expand(self.0.iter()).nth(index - self.0.len()).unwrap()
        }
    }

    pub fn try_decrypt(
        &self,
        index: usize,
        share: &EncryptedShare,
        key: &SecretKey,
    ) -> Result<DecryptedShare, DecryptionFailureProof> {
        let p = self.get_element(index);
        let ss = (&key.0 * &p).pack();
        if let Some(s) = Scalar::unpack(&xor32(hash!(&ss), share.0)) {
            if &s * &GT == p {
                return Ok(DecryptedShare(s));
            }
        }
        let kk = prs!(key.0, p);
        let c = hash_s!(&(key.1).0, p, &ss, &kk * &GT, &kk * &p);
        let r = kk - c * key.0;
        Err(DecryptionFailureProof((ss, r, c).pack()))
    }

    pub fn is_valid(
        &self,
        index: usize,
        share: &EncryptedShare,
        key: &PublicKey,
        proof: &DecryptionFailureProof,
    ) -> bool {
        let p = self.get_element(index);
        if let Some(s) = Scalar::unpack(&xor32(hash!(&proof.0[..32]), share.0)) {
            if &s * &GT == p {
                return false;
            }
        }
        let (ss, r, c) = try_unpack!(&proof.0);
        if hash_s!(&key.0, p, ss, vmul2(r, &G, c, &key.1), vmul2(r, &p, c, &ss)) != c {
            return false;
        }
        true
    }
}

fn i2s(i: usize) -> Scalar {
    Scalar::from(i as u64)
}

impl RandomEpoch {
    pub fn from_shares(
        Params { n, k }: Params,
        mut shares: impl Iterator<Item = ValidatedPublicShare>,
    ) -> Self {
        let mut res = Vec::with_capacity(n);
        match shares.next() {
            None => {
                res.resize_with(n, Point::identity);
            }
            Some(s) => {
                assert!(s.0.len() == k);
                res.extend_from_slice(s.0.deref());
                for s in shares {
                    assert!(s.0.len() == k);
                    for i in 0..k {
                        res[i] += s.0[i];
                    }
                }
                res.extend(expand::<Point, _>(res.iter()).take(n - k));
            }
        }
        RandomEpoch(res.into_boxed_slice())
    }

    pub fn compute_share(
        &self,
        round: &RandomRound,
        index: usize,
        secret: &RandomEpochSecret,
    ) -> RandomShare {
        let ss = (&secret.0 * &round.1).pack();
        let k = prs!(secret.0, &round.0);
        let c = hash_s!(self.0[index], &ss, &k * &GT, &k * &round.1);
        RandomShare((ss, k - c * secret.0, c).pack())
    }

    pub fn validate_share(
        &self,
        round: &RandomRound,
        index: usize,
        share: &RandomShare,
    ) -> Option<ValidatedRandomShare> {
        let key = self.0[index];
        let (ss, r, c) = try_unpack!(&share.0);
        let uss = try_unpack!(&ss);
        if hash_s!(key, &ss, vmul2(r, &G, c, &key), vmul2(r, &round.1, c, &uss)) != c {
            return None;
        }
        Some(ValidatedRandomShare(uss))
    }

    pub fn finalize(shares: &[(usize, ValidatedRandomShare)]) -> RandomValue {
        let n = shares.len();
        debug_assert!(shares.windows(2).all(|w| w[0].0 < w[1].0));
        let mut coeff = Vec::with_capacity(n);
        for (i, (xi, _)) in shares.iter().enumerate() {
            let mut v = if i & 1 != 0 { -Scalar::one() } else { Scalar::one() };
            for (xj, _) in &shares[..i] {
                v *= i2s(xi - xj);
            }
            for (xj, _) in &shares[i + 1..] {
                v *= i2s(xj - xi);
            }
            coeff.push(v);
        }
        Scalar::batch_invert(coeff.deref_mut());
        for (i, v) in coeff.iter_mut().enumerate() {
            for (x, _) in shares[..i].iter().chain(&shares[i + 1..]) {
                *v *= i2s(x + 1);
            }
        }
        RandomValue(Point::vartime_multiscalar_mul(coeff, shares.iter().map(|p| (p.1).0)).pack())
    }
}

impl RandomEpochSecret {
    pub fn from_shares(mut shares: impl Iterator<Item = DecryptedShare>) -> Self {
        RandomEpochSecret(match shares.next() {
            None => Scalar::zero(),
            Some(DecryptedShare(mut s)) => {
                for DecryptedShare(s2) in shares {
                    s += s2;
                }
                s
            }
        })
    }
}

impl RandomRound {
    pub fn new(epoch_id: &[u8; 32], index: u32) -> Self {
        // We don't really need to compute Elligator twice, but curve25519-dalek doesn't provide a function which does it only once.
        let p = Point::from_hash(hasher!(Hash512, epoch_id, &index.to_le_bytes()));
        RandomRound(p.pack(), p)
    }
}

impl From<&[u8]> for PublicShare {
    fn from(value: &[u8]) -> Self {
        PublicShare(value.into())
    }
}

impl AsRef<[u8]> for PublicShare {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsMut<[u8]> for PublicShare {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

impl TryFrom<&str> for PublicShare {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, ()> {
        match bs58::decode(value).into_vec() {
            Ok(v) => Ok(PublicShare(v.into_boxed_slice())),
            Err(_) => Err(()),
        }
    }
}

common_conversions!(PublicShare, "public share");

eq!(RandomRound, |a, b| &a.0 == &b.0);

#[cfg(test)]
mod tests {
    use super::*;
    use rand::seq::index;
    use rand_core::OsRng;

    #[test]
    fn test_u32_max_value_fits_usize() {
        // This is used in Params::is_valid_n().
        assert_eq!(u32::max_value() as usize as u32, u32::max_value());
    }

    #[test]
    fn test_operation() {
        let params = Params::new(13, 8);
        let gens: usize = 10;
        let mut gen_keys = Vec::new();
        for _ in 0..gens {
            gen_keys.push(SecretKey::random(&mut OsRng));
        }
        let mut recv_keys = Vec::new();
        for _ in 0..params.n {
            recv_keys.push(SecretKey::random(&mut OsRng));
        }
        let mut public_shares = Vec::new();
        let mut decrypted_shares = Vec::new();
        for i in 0..gens {
            let (ps, ss) = generate_share(params, &gen_keys[i].public_key(), &mut OsRng);
            let vs = ps.validate(&gen_keys[i].public_key()).unwrap();
            for j in 0..params.n {
                let es = ss.encrypt(j, &recv_keys[j].public_key());
                let ds = vs.try_decrypt(j, &es, &recv_keys[j]).unwrap();
                decrypted_shares.push(ds);
            }
            public_shares.push(vs);
        }
        let epoch = RandomEpoch::from_shares(params, public_shares.into_iter());
        let mut epoch_secrets = Vec::new();
        for i in 0..params.n {
            let mut dss = Vec::new();
            for j in 0..gens {
                dss.push(decrypted_shares[j * params.n + i]);
            }
            epoch_secrets.push(RandomEpochSecret::from_shares(dss.into_iter()));
        }
        let mut epoch_id = [0; 32];
        OsRng.fill_bytes(&mut epoch_id);
        let random_round = RandomRound::new(&epoch_id, OsRng.next_u32());
        let mut random_shares = Vec::new();
        for i in 0..params.n {
            let rs = epoch.compute_share(&random_round, i, &epoch_secrets[i]);
            let vrs = epoch.validate_share(&random_round, i, &rs).unwrap();
            random_shares.push(vrs);
        }
        let produce_value = || {
            let mut selected_shares = Vec::new();
            for i in index::sample(&mut OsRng, params.n, params.k).iter() {
                selected_shares.push((i, random_shares[i]));
            }
            selected_shares.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            RandomEpoch::finalize(selected_shares.as_slice())
        };
        let v = produce_value();
        for _ in 0..10 {
            assert_eq!(v, produce_value());
        }
    }
}
