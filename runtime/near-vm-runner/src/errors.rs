#[derive(Debug, PartialEq)]
pub enum ContractPrecompilatonResult {
    ContractCompiled,
    ContractAlreadyInCache,
    CacheNotAvailable,
}
