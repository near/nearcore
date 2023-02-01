# On-Disk Database Format

We store the database in RocksDB. This document is an attempt to give hints about how to navigate it.

## RocksDB

- The column families are defined in `DBCol`, defined in `core/store/src/columns.rs`
- The column families are seen on the rocksdb side as per the `col_name` function defined in `core/store/src/db/rocksdb.rs`

## The Trie (col5)

- The trie is stored in column family `State`, number 5
- In this family, each key is of the form `ShardUId | CryptoHash` where `ShardUId: u64` and `CryptoHash: [u8; 32]`

## All Historical State Changes (col35)

- The state changes are stored in column family `StateChanges`, number 35
- In this family, each key is of the form `BlockHash | Column | AdditionalInfo` where:
  + `BlockHash: [u8; 32]` is the block hash for this change
  + `Column: u8` is defined near the top of `core/primitives/src/trie_key.rs`
  + `AdditionalInfo` depends on `Column` and is can be found in the code for the `TrieKey` struct, same file as `Column`

### Contract Deployments

- Contract deployments happen with `Column = 0x01`
- `AdditionalInfo` is the account id for which the contract is being deployed
- The key value contains the contract code alongside other pieces of data. It is possible to extract the contract code by removing everything until the wasm magic number, 0061736D01000000
- As such, it is possible to dump all the contracts that were ever deployed on-chain using this command on an archival node:
  ```
  ldb --db=~/.near/data scan --column_family=col35 --hex | \
      grep -E '^0x.{64}01' | \
      sed 's/0061736D01000000/x/' | \
      sed 's/^.*x/0061736D01000000/' | \
      grep -v ' : '
  ```
  (Note that the last grep is required because not every such value appears to contain contract code)
  We should implement a feature to state-viewer that’d allow better visualization of this data, but in the meantime this seems to work.
