# Running NEAR

#### Table of contents

1. [Running Locally](#running-locally)
2. [Running Remotely](#running-remotely)
3. [Developing NEAR](#developing-near)
4. [Building and Pushing Docker Image](#building-and-pushing-docker-image)

## Running Locally

To run NEAR locally you would need docker, see [installation instructions](https://www.docker.com/get-started).
Then run the following:
```bash
./ops/deploy_local.sh
```

After it starts you can open studio at [http://localhost](http://localhost).

To tear it down run:
```bash
./ops/teardown_local.sh
```

Note, it is not advised to run more than two nodes locally, especially on low performance machines. While the network
itself is able to function and produce blocks, the development tools might currently timeout on certain commands,
because the nodes do not produce blocks fast enough.

## Running Remotely
Similarly you deploy the network to the GCloud:
```bash
./ops/deploy_remote.sh
```
When the network is deployed it will print the address of the studio.

## Developing NEAR
This section is for those who want to develop the NEAR Runtime. If you only want to develop dApps then the above sections suffice.
Developing NEAR Runtime locally requires more installation and configuration.

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
rustup component add clippy-preview
rustup default nightly
```


**3. Source code**

We would need to copy the entire repository:

```bash
git clone https://github.com/nearprotocol/nearcore
cd nearcore
```

**4. Running locally**


Navigate to the root of the repository, and run:
```bash
cargo run --package near -- init
cargo run --package near -- run
```

This will setup a local chain with `init` and will run the node.

You can now check the status of the node with `http` tool (`brew install http` or `apt install http` or configure CURL for POST requests) via RPC:
```bash
http get http://localhost:3030/status
http post http://localhost:3030/ method=query jsonrpc=2.0 id=1 params:='["account/test.near", ""]' 
```

See full list of RPC endpoints here: https://docs.nearprotocol.com/api-documentation/rpc

Unfortunately, transactions needs to be signed and encoded in base58, which is hard to do from the command line.
Use `near-shell` tool for that (`npm install -g near-shell`).

## Building and Pushing Docker Image

If you have modified the source code of NEAR Runtime and want to see how it performs in prod you would need to build
an docker image from it. Once the docker image is built you can run it locally or remotely.

### Building NEAR client
To build docker image run from the root:
```bash
make docker-nearcore
```

This will build an image with `nearcore` name.

### Publishing NEAR client

To publish docker image, use 

```bash
sudo docker tag nearcore <your username>/mynearcore:<version>
sudo docker push <your username>/mynearcore:<version>
```

Official image is published at `nearprotocol/nearcore`

### Running locally

To run this image locally, run:
```bash
./ops/deploy_local.sh nearcore
```

### Running remotely

If you have published image to `<your username>/mynearcore`, you can use:

```bash
./ops/deploy_remote.sh <your username>/mynearcore
```
