# NEARMint

NEAR application layer running with Tendermint consensus.

## Running Locally

NEARMint can be run locally in several different ways:
* Running on a host machine as a single validator;
* Dockerized local cluster.

If you are not planning to do development, but only want to try running the local cluster then skip to [Dockerized Local Cluster](#dockerized-local-cluster).

## NEARMint with Tendermint core as a single validator

### Installation

This mode of running requires compilation of the Rust code and installed Tendermint.

**1. Dependencies**

Install protobufs:
```bash
# Mac OS:
brew install protobuf

# Ubuntu:
apt-get install protobuf-compiler
```

**2. Rust**

Currently, the only way to install NEARMint application layer is to compile it from the source.
For that we would need to install Rust.

```bash
curl https://sh.rustup.rs -sSf | sh
rustup default nightly
```


**3. Source code**

We would need to copy the entire repository:

```bash
git clone https://github.com/nearprotocol/nearcore
cd nearcore
```

**4. Tendermint**

Follow the official instructions to install Tendermint: https://tendermint.com/docs/introduction/install.html

### Configure and run

To configure Tendermint run:

```bash
tendermint init
```

Configure Tendermint to use `3030` for its RPC. Open `~/.tendermint/config/config.toml` and change:
```yaml
[rpc]
laddr = "tcp://0.0.0.0:3030"
```

For local development we also recommend setting:
```yaml
create_empty_blocks = false
```

First, we want to set `alice.near` as a single validator. Open `~/.tendermint/config/priv_validator_key.json` and replace with:

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

Navigate to the root of the repository, and run:
```bash
cargo run --package nearmint -- --devnet
```

This will start NEARMint in application mode, without consensus backing it up.

To start Tendermint run:
```bash
tendermint node
```

Now we can issue specific commands to the node:
```bash
curl http://localhost:3030/status
curl -s 'localhost:3030/abci_query?path="account/alice.near"'
curl -s 'localhost:3030/validators'
```

See full list of RPC endpoints here: https://tendermint.com/rpc/#endpoints

Unfortunately, transactions can be only submitted in base64 encoding, which is hard to do from the command line.

## Dockerized Local Cluster

### Installation

If you are not planning to build your own NEARMint image then Docker installation is enough, otherwise follow the
installation instructions from the previous section.

To install docker, follow [the official instructions](https://www.docker.com/get-started).

### Run

To run local cluster simply run:

```bash
./nearmint/ops/local_testnet
```

You can then issue RPC requests either on port `3030` or `3031`.

### Building image

To run local code as a dockerized cluster you would need to build an image:

```bash
./nearmint/ops/build mytag1
```
where `mytag1` is some unique name.

To start the local cluster using this image run:

```bash
./nearmint/ops/local_testnet mytag1
```

## Dockerized Local Cluster

TODO
