# Chain fetcher

This binary takes a hash of a block as an input and then fetches
from the network the following:
1. All block headers up to the newest header (or until block-limit is reached).
1. All blocks for the headers fetched.
1. All fragments of all chunks for all blocks fetched.
The binary doesn't interpret the data it received (except for checking what it
should fetch next), but rather discards it immediately. This way it is able to
benchmark the raw throughput of the network from the point of view of a single node.

Flags:
* chain-id - the name of the chain.
  The binary fetches the config file of the chain automatically.
  The binary doesn't use the genesis file at all (it has the genesis file hashes hardcoded instead)
  TODO: add a flag for genesis file hash.
* start-block-hash - the Base58 encoded block hash. The binary will fetch everything starting
  with this block up to the newest block (or until block-limit is reached).
* qps-limit - maximum number of requests per second that the binary is allowed to send.
  This is a global limit (NOT per connection). The requests are distributed uniformly across
  all the connections that the program establishes. Peer discovery works the same way as for neard.
* block-limit - number of blocks to fetch

## Example usage

1. Go to [https://explorer.testnet.near.org/blocks].
1. Select the block from which you would like to start fetching from.
1. Copy the hash of the block.
1. run
  
  cargo run -- --chain-id=testnet --qps-limit=200 --block-limit=2000 --start-block-hash=<block hash>

1. First you will see that the program is establishing connections.
1. Once there are enough connections it will start sending the requests.
1. Every few seconds you will see a log indicating the progress:
   * how many requests have been sent, how many responses received
   * how many headers/blocks/chunks are being fetched, how many have been successfully fetched.
1. Once everything is fetched, the program will print final stats, then "Fetch completed" and terminate.
