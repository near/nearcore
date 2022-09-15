# mock-node
This crate hosts libraries to start a test env for a single node by replacing the network module with a mock network environment.
The mock network environment simulates the interaction that the client will usually have with other nodes by
responding to the client's network messages and broadcasting new blocks. The mock network reads a pre-generated chain
history from storage.

## Quick Start

```console
$ cargo run --release -p mock-node -F mock_node -- ~/.near/localnet/node0
```

where the `node0` directory contains some pre-generated chain history in storage.
You can find two examples in the ./benches directory.

If you are running a mock node for mainnet or testnet on a GCP node, you want to place the new client home
dir on a SSD disk for optimal rocksdb performance. Note that the
default booting disk of GCP notes are HDD, so you need to mount a new SSD disk on
your node and put the mock node's home dir there. See https://cloud.google.com/compute/docs/disks/add-persistent-disk
for how to attach and mount a new disk to an existing GCP node.

See `$ cargo run -p mock-node -F mock_node -- --help` for the list of available options and their documentation.

## Examples

#### Replay localnet history

```console
$ cargo r -r -p mock-node -F mock_node -- ~/.near/localnet/node0
```
Here we take the home dir of an existing node in a localnet as chain history home dir,
so the mock network will reproduce the client catching up with the entire history of the localnet from genesis.

#### Replay mainnet history from a certain height

To replay mainnet or testnet history, in most use cases, we want to start replaying from a certain height, instead
of from genesis block. The following comment replays mainnet history from block height 60925880 to block height 60925900.

```console
$ cargo r -r -p mock-node -F mock_node --  ~/.near ~/mock_node_home_dir --start_height 60925880 --target-height 60925900
```

By providing a starting height,
the binary will set up the data dir before starting the client, by copying the state at the specified height
and other chain info necessary for processing the blocks afterwards (such as block headers and blocks).
This initial setup may take a long time (The exact depends on your
source dir, my experiment takes about an hour from a non-archival source dir. Copying from archival node source
dir may take longer as rocksdb is slower). So we suggest specifying a client dir (the `~/mock_node_home_dir` argument)
so you can reuse it again without having to copy the state again.

Note that the start height must be the last block of an epoch.

Once you have the source dir already set up, you can run the command without `--start_height`,

```console
$  cargo r -r -p mock-node -F mock_node --  ~/.near ~/mock_node_home_dir --target-height 60926000
```
Without `--starting_height`, the binary will not modify the client home dir before starting the mock node. Therefore,
the mock node will start from the chain head stored in the client dir.

## Mock Network Configuration

Certain details around how the mock network behaves can be configured with the file `mock.json` in the chain history
home directory. Currently, the only supported configuration options tell how long to wait before replying to requests
(the same as the --network_delay flag), and how often to send unrequested blocks and chunk part requests. By default,
no such unrequested messages are sent, but the following config file will have the mock code produce unrequested
blocks every 100 milliseconds, and chunk part requests every 50 milliseconds.

```json
{
    "response_delay": {
        "secs": 0,
        "nanos": 100000000
    },
    "incoming_requests": {
        "block": {
            "interval": {
                "secs": 0,
                "nanos": 100000000
            }
        },
        "chunk_request": {
            "interval": {
                "secs": 0,
                "nanos": 50000000
            }
        }
    }
}
```
