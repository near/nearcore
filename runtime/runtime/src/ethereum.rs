use std::mem;

use bigint::{H256, H64};
use std::fs::{create_dir_all, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const ETHASH_EPOCH_LENGTH: u64 = 30000;
const NODE_BYTES: usize = 64;

pub struct LightCache {
    epoch: u64,
    cache: Vec<u8>,
}

impl LightCache {
    fn storage_file(cache_dir: &PathBuf, epoch: u64) -> PathBuf {
        let mut path = cache_dir.clone();
        path.push(format!("{}", epoch));
        path
    }

    pub fn new(cache_dir: &PathBuf, epoch: u64) -> Self {
        match Self::from_file(cache_dir, epoch) {
            Ok(light_cache) => light_cache,
            Err(_) => {
                let cache_size = ethash::get_cache_size(epoch as usize);
                let mut cache = Vec::with_capacity(cache_size);
                cache.resize(cache_size, 0);
                let seed = ethash::get_seedhash(epoch as usize);
                ethash::make_cache(&mut cache, seed);
                let _ = Self::to_file(cache_dir, epoch, &cache);
                LightCache { epoch, cache }
            }
        }
    }

    fn to_file(cache_dir: &PathBuf, epoch: u64, cache: &[u8]) -> io::Result<usize> {
        create_dir_all(cache_dir)?;
        let mut file = File::create(Self::storage_file(cache_dir, epoch))?;
        file.write(cache)
    }

    pub fn from_file(cache_dir: &PathBuf, epoch: u64) -> io::Result<Self> {
        let mut file = File::open(Self::storage_file(cache_dir, epoch))?;
        let mut cache: Vec<u8> = Vec::with_capacity(
            file.metadata().map(|m| m.len() as _).unwrap_or(NODE_BYTES * 1_000_000),
        );
        file.read_to_end(&mut cache)?;
        cache.shrink_to_fit();

        if cache.len() % NODE_BYTES != 0 || cache.capacity() % NODE_BYTES != 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Node cache is not a multiple of node size",
            ));
        }
        Ok(LightCache { epoch, cache })
    }
}

pub struct EthashProvider {
    cache_dir: PathBuf,
    recent: Option<LightCache>,
    prev: Option<LightCache>,
}

impl EthashProvider {
    pub fn new(cache_dir: &Path) -> Self {
        EthashProvider { cache_dir: cache_dir.to_path_buf(), recent: None, prev: None }
    }

    pub fn check_ethash(
        &mut self,
        block_number: u64,
        header_hash: H256,
        nonce: H64,
        mix_hash: H256,
    ) -> bool {
        let epoch = block_number / ETHASH_EPOCH_LENGTH;
        let cache = {
            let cache = match &self.recent {
                Some(recent) if recent.epoch == epoch => self.recent.as_ref(),
                _ => match self.prev {
                    Some(ref prev) if prev.epoch == epoch => {
                        if self.recent.is_some() && self.recent.as_ref().unwrap().epoch > prev.epoch
                        {
                            None
                        } else {
                            mem::swap(&mut self.prev, &mut self.recent);
                            self.recent.as_ref()
                        }
                    }
                    _ => None,
                },
            };
            match cache {
                None => {
                    let cache = LightCache::new(&self.cache_dir, epoch);
                    self.prev = mem::replace(&mut self.recent, Some(cache));
                    self.recent.as_ref().unwrap()
                }
                Some(cache) => cache,
            }
        };
        let full_size = ethash::get_full_size(epoch as usize);
        let (result_mix_hash, _result) =
            ethash::hashimoto_light(header_hash, nonce, full_size, &cache.cache);
        result_mix_hash == mix_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use block::Header;
    use hexutil::*;
    use tempdir::TempDir;

    #[test]
    #[ignore]
    fn test_header1() {
        let cache_dir = TempDir::new("ethashtest").unwrap();
        let mut provider = EthashProvider::new(Path::new(cache_dir.path()));
        let hash = H256::from("2a8de2adf89af77358250bf908bf04ba94a6e8c3ba87775564a41d269a05e4ce");
        let nonce = H64::from("4242424242424242");
        let mix_hash =
            H256::from("58f759ede17a706c93f13030328bcea40c1d1341fb26f2facd21ceb0dae57017");
        assert!(provider.check_ethash(0, hash, nonce, mix_hash));
    }

    #[test]
    #[ignore]
    fn test_header2() {
        let header: Header = rlp::decode(&read_hex("f901f3a00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a09178d0f23c965d81f0834a4c72c6253ce6830f4022b1359aaebfc1ecba442d4ea056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008302000080830f4240808080a058f759ede17a706c93f13030328bcea40c1d1341fb26f2facd21ceb0dae57017884242424242424242").unwrap());
        let mut provider = EthashProvider::new(Path::new("ethashtest"));
        let block_number = header.number.as_usize() as u64;
        assert!(provider.check_ethash(
            block_number,
            header.partial_hash(),
            H64::from("4242424242424242"),
            header.mix_hash
        ));
    }
}
