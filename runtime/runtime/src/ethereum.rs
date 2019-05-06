#![allow(unused)]
use std::mem;

use bigint::{H256, H64, U256};
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
        difficulty: U256,
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
        let (result_mix_hash, result) =
            ethash::hashimoto_light(header_hash, nonce, full_size, &cache.cache);
        if result_mix_hash == mix_hash {
            let target = ethash::cross_boundary(difficulty);
            return U256::from(result) <= target;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use block::Header;
    use hexutil::*;
    use std::process::Command;
    use tempdir::TempDir;

    #[test]
    #[ignore]
    fn test_header() {
        let dir = TempDir::new("ethashtest_header").unwrap();
        let header: Header = rlp::decode(&read_hex("f901f9a0d405da4e66f1445d455195229624e133f5baafe72b5cf7b3c36c12c8146e98b7a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347948888f1f195afa192cfee860698584c030f4c9db1a05fb2b4bfdef7b314451cb138a534d225c922fc0e5fbe25e451142732c3e25c25a088d2ec6b9860aae1a2c3b299f72b6a5d70d7f7ba4722c78f2c49ba96273c2158a007c6fdfa8eea7e86b81f5b0fc0f78f90cc19f4aa60d323151e0cac660199e9a1b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008302008003832fefba82524d84568e932a80a0a0349d8c3df71f1a48a9df7d03fd5f14aeee7d91332c009ecaff0a71ead405bd88ab4e252a7e8c2a23").unwrap());

        let mut provider = EthashProvider::new(dir.path());
        let block_number = header.number.as_usize() as u64;
        assert!(provider.check_ethash(
            block_number,
            header.partial_hash(),
            H64::from("ab4e252a7e8c2a23"),
            header.mix_hash,
            header.difficulty,
        ));
    }

    #[test]
    #[ignore]
    fn test_invalid_header() {
        let dir = TempDir::new("ethashtest_invalid_header").unwrap();
        let header: Header = rlp::decode(&read_hex("f901f7a01bef91439a3e070a6586851c11e6fd79bbbea074b2b836727b8e75c7d4a6b698a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d4934794ea3cb5f94fa2ddd52ec6dd6eb75cf824f4058ca1a00c6e51346be0670ce63ac5f05324e27d20b180146269c5aab844d09a2b108c64a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008302004002832fefd880845511ed2a80a0e55d02c555a7969361cf74a9ec6211d8c14e4517930a00442f171bdb1698d17588307692cf71b12f6d").unwrap());

        let mut provider = EthashProvider::new(dir.path());
        let block_number = header.number.as_usize() as u64;
        assert!(!provider.check_ethash(
            block_number,
            header.partial_hash(),
            //H64::from("4242424242424242"),
            H64::from("307692cf71b12f6d"),
            header.mix_hash,
            header.difficulty,
        ));
    }
}
