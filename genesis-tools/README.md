# Genesis Tools

* `genesis-populate` -- tool for creating genesis state dump populated with large number of accounts;
* TODO `genesis-rebase`-- tool for rebasing the entire chain to a new genesis;
* TODO `genesis-mainnet` -- tool for creating the main genesis used at the mainnet launch;

## `genesis-populate`

Performance of our node varies drastically depending on the size of the trie it operates with.
As the baseline, we agreed to take the trie roughly equivalent to the trie of Ethereum (as of October 2019) in
its complexity. We achieve it by populating trie with 80M accounts and uploading a smart contract to each of them.
We also make sure the trie does not compress the subtrees due to similar account names. We then use this trie for
benchmarking, loadtesting, and estimating system parameters.

To start node with 20k accounts first create configs:
```bash
cargo run --package neard --bin neard -- init --test-seed=alice.near --account-id=test.near --fast
```

Then create state dump with how many accounts you want:
```bash
cargo run --package genesis-populate --bin genesis-populate -- --additional-accounts-num=20000
```

This will produce `state_dump` and `genesis_roots` files in home directory of the node, you can also
use `--home` on all commands here to specify an absolute path to a home directory to use.

Then start a single local node:
```bash
cargo run --package neard --bin neard -- run --boot-nodes=
```
