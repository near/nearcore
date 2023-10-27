# Cheat sheet/overview of testing utils

This page covers the different testing utils/libraries that we have for easier
unit testing in Rust.

## Basics

### CryptoHash

To create a new crypto hash:

```rust
"ADns6sqVyFLRZbSMCGdzUiUPaDDtjTmKCWzR8HxWsfDU".parse().unwrap();
```

### Account

Also, prefer doing parse + unwrap:

```rust
let alice: AccountId = "alice.near".parse().unwrap();
```

### Signatures

In memory signer (generates the key based on a seed). There is a slight preference
to use the seed that is matching the account name.

This will create a signer for account 'test' using 'test' as a seed.

```rust
let signer: InMemoryValidatorSigner = create_test_signer("test");
```

### Block

Use ``TestBlockBuilder`` to create the block that you need. This class allows you to set custom values for most of the fields.

```rust
let test_block = test_utils::TestBlockBuilder::new(prev, signer).height(33).build();
```

## Store

Use the in-memory test store in tests:

```rust
let store = create_test_store();
```

## EpochManager

See usages of MockEpochManager. Note that this is deprecated. Try to use
EpochManager itself wherever possible.

## Runtime

You can use the KeyValueRuntime (instead of the Nightshade one):

```rust
KeyValueRuntime::new(store, &epoch_manager);
```

## Chain

No fakes or mocks.

### Chain genesis

We have a test method:

```rust
ChainGenesis::test();
```

## Client

TestEnv - for testing multiple clients (without network):

```rust
TestEnvBuilder::new(genesis).client(vec!["aa"]).validators(..).epoch_managers(..).build();
```

## Network

### PeerManager

To create a PeerManager handler:

```rust
let pm = peer_manager::testonly::start(...).await;
```

To connect to others:

```rust
pm.connect_to(&pm2.peer_info).await;
```

### Events handling

To wait/handle a given event (as a lot of network code is running in an async fashion):

```rust
pm.events.recv_util(|event| match event {...}).await;
```

## End to End 

### chain, runtime, signer

In chain/chain/src/test_utils.rs:

```rust
// Creates 1-validator (test):  chain, KVRuntime and a signer
let (chain, runtime, signer) = setup();
```

### block, client actor, view client

In chain/client/src/test_utils.rs

```rust
let (block, client, view_client) = setup(MANY_FIELDS);
```

