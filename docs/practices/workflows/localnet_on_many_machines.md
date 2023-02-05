# Running near localnet on 2 machines

Quick instructions on how to run a localnet on 2 separate machines.

## Setup

* Machine1: "pc" - 192.168.0.1
* Machine2: "laptop" - 192.168.0.2


Run on both machines (make sure that they are using the same version of the code):
```
cargo build -p neard
```

Then on machine1 run the command below, which will generate the configurations:

```
./target/debug/neard --home ~/.near/localnet_multi localnet --shards 3 --v 2
```

This command has generated configuration for 3 shards and 2 validators (in directories ~/.near/localnet_multi/node0 and ~/.near/localnet_multi/node1).

Now - copy the contents of node1 directory to the machine2

```
rsync -r ~/.near/localnet_multi/node1 192.168.0.2:~/.near/localnet_multi/node1
```

Now open the config.json file on both machines (node0/config.json on machine1 and node1/config.json on machine2) and:
* for rpc->addr and network->addr:
  * Change the addres from 127.0.0.1 to 0.0.0.0 (this means that the port will be accessible from other computers)
  * Remember the port numbers (they are generated randomly). 
* Also write down the node0's node_key (it is probably: "ed25519:7PGseFbWxvYVgZ89K1uTJKYoKetWs7BJtbyXDzfbAcqX")

## Running

On machine1:
```
./target/debug/neard --home ~/.near/localnet_multi/node0 run
```

On machine2:
```
./target/debug/neard --home ~/.near/localnet_multi/node1 run --boot-nodes ed25519:7PGseFbWxvYVgZ89K1uTJKYoKetWs7BJtbyXDzfbAcqX@192.168.0.1:37665
```
The boot node address should be the IP of the machine1 + the network addr port **from the node0/config.json**


And if everything goes well, the nodes should communicate and start producing blocks.

## Troubleshooting

The debug mode is enabled by default, so you should be able to see what's going on by going to ``http://machine1:RPC_ADDR_PORT/debug`` 


### If node keeps saying "waiting for peers"
See if you can see the machine1's debug page from machine2. (if not - there might be a firewall blocking the connection).

Make sure that you set the right ports (it should use node0's NETWORK port) and that you set the ip add there to 0.0.0.0


### Resetting the state
Simply stop both nodes, and remove the ``data`` subdirectory (~/.near/localnet_multi/node0/data and ~/.near/localnet_multi/node1/data).

Then after restart, the nodes will start the blockchain from scratch.

