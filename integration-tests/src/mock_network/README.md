# mock-network
This crate hosts libraries to start a test env for a single node by replacing the network module with a mock network environment. 
The mock network environment simulates the interaction that the client will usually have with other nodes by 
responding to the client's network messages and broadcasts new blocks. The mock network reads a pre-generated chain 
history from storage.

The crate has two files and another binary sits in ../bin/start_mock_network

## mod.rs
Implements `ChainHistoryAccess` and `MockPeerManagerActor`, which is the main 
components of the mock network.
## setup.rs
Provides functions for setting up a mock network from configs and home dirs.
## start_mock_network.rs
A binary that starts a mock network from the home dir of an existing node