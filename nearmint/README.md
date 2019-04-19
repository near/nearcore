# NEARMint

NEAR application layer running with Tendermint consensus.

## Installation

0. Install general development tools.

Ubuntu:

    sudo apt-get install binutils-dev libcurl4-openssl-dev zlib1g-dev libdw-dev libiberty-dev cmake gcc build-essential libssl-dev pkg-config protobuf-compiler

1. Follow instruction on Tendermint installation: https://tendermint.com/docs/introduction/install.html

2. Install rust

```
curl https://sh.rustup.rs -sSf | sh
```

3. Download this repo

    git clone https://github.com/nearprotocol/nearcore

## Configure

### Tendermint

    tendermint init

You can modify configuration of the node in `~/.tendermint/config/config.toml`.

Specifically, change RPC server to 3030 port:
```$toml
[rpc]
laddr = "tcp://0.0.0.0:3030"
```

## Running

Start in one console tendermint (from any location if it was installed, or from directory you put binary into):

    tendermint node
    
Note, that if you want to reset state of tendermint that has already ran, use `tendermint unsafe_reset_all`.

In the second console:

    cargo run --package nearmint
    
Note, that if you want to reset state of nearmint that has already ran, use `rm -rf storage`.
    
## Running local cluster

Note, this is done way easier on Ubuntu and we will be working on simplifying it for Mac OS.

Link `tendermint` binary to target/release folder

    ln -s 

We use docker compose to spin up 4 node local cluster:

    cargo run --release --package nearmint
    cd ops/local
    docker-compose up


## Development

To run single validator mode (e.g. DevNet mode), you can set Tendermint validator key to `alice.near` key in `~/.tendermint/config/priv_validator_key.json`:

```json
{
  "address": "27B2B6C138DDF7B77E4318A22CAE1A38F55AA29A",
  "pub_key": {
    "type": "tendermint/PubKeyEd25519",
    "value": "D1al8CjfwInsfDnBGDsyG02Pibpb7J4XYoA8wkkfbvg="
  },
  "priv_key": {
    "type": "tendermint/PrivKeyEd25519",
    "value": "YWxpY2UubmVhciAgICAgICAgICAgICAgICAgICAgICAPVqXwKN/Aiex8OcEYOzIbTY+JulvsnhdigDzCSR9u+A=="
  }
}
```


And then when running `nearmint` use `--devnet` flag, e.g. `cargo run --package nearmint -- --devnet` or `target/release/nearmint --devnet`.
    
## Interacting

Use JSONRPC to send transaction:

    http post localhost:26657 method=broadcast_tx_commit jsonrpc=2.0 id=dontcare tx=0x<hex bytes of transaction>

See full list of JSONRPC

    http get localhost:26657
