# mock-network
This crate hosts libraries to start a test env for a single node by replacing the network module with a mock network environment. 
The mock network environment simulates the interaction that the client will usually have with other nodes by 
responding to the client's network messages and broadcasting new blocks. The mock network reads a pre-generated chain 
history from storage.

The crate has two files and another binary sits in ../bin/start_mock_network

## mod.rs
Implements `ChainHistoryAccess` and `MockPeerManagerActor`, which is the main 
components of the mock network.
## setup.rs
Provides functions for setting up a mock network from configs and home dirs.
## start_mock_network.rs
A binary that starts a mock testing environment for ClientActor.
It simulates the entire network by substituting PeerManagerActor with a mock network,
responding to the client's network requests by reading from a pre-generated chain history
in storage.

### Usage
```bash
start_mock_network [FLAGS] [OPTIONS] <chain-history-home-dir> --mode <mode> [client-home-dir]
```
Run
```bash
start_mock_network --help
```
to see all flags and options the command supports. 

Note
- Must compile with --release and --features mock_network
- The new client dir must be mounted on a SSD disk for optimal rocksdb performance.
The default booting disk of GCP nodes are HDD, please mount a new SSD disk for testing.

Examples:

#### Replay localnet history
```bash
start_mock_network ~/.near/localnet/node0 --mode no_new_blocks
```
Here we take the home dir of an existing node in a localnet as chain history home dir, 
so the mock network will reproduce the entire history of the localnet from genesis.
Without specified client-home-dir, the binary will create a temporary directory as home directory of the new client.

#### Replay mainnet history from a certain height
```bash
start_mock_network ~/.near ~/mock_network_client_dir -s 60925880 -h 60925900 --mode "no_new_blocks"
```
To replay mainnet history, pass a home dir of a mainnet node as chain-history-home-dir to the commmand.
The command also supports specifying a start_height for the client. If `client-start-height` is specified,
the binary will set up the data dir before starting the client, by copying the state at the specified height
and other chain info necessary for processing the blocks afterwards (such as block headers and blocks).

Note that the start height must be the last block of an epoch.

The initial setup for client dir takes a long time, so we suggest specifying a client dir
so you can reuse it again without having to copy the state again. If you run the above command,
and then run 
```bash
start_mock_network ~/.near ~/mock_network_client_dir -h 60926000 --mode "no_new_blocks"
```
Without specifying a start height, the binary will not modify the client dir and 
the client will start from height 60925900 because that was the position of the chain head
in the client dir storage.

