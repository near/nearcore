# Upgradability

This part of specification describes specifics of upgrading the protocol, and touches on few different parts of the system.

Three different levels of upgradability are:

1. Updating without any changes to underlying data structures or protocol;
2. Updating when underlying data structures changed (config, database or something else internal to the node and probably client specific);
3. Updating with protocol changes that all validating nodes must adjust to.

## Versioning

There are 2 different important versions:

- Version of binary defines it's internal data structures / database and configs. This version is client specific and doesn't need to be matching between nodes.
- Version of the protocol, defining the "language" nodes are speaking.

```rust
/// Latest version of protocol that this binary can work with.
type ProtocolVersion = u32;
```

## Client versioning

Clients should follow [semantic versioning](https://semver.org/).
Specifically:

- MAJOR version defines protocol releases.
- MINOR version defines changes that are client specific but require database migration, change of config or something similar. This includes client-specific features. Client should execute migrations on start, by detecting that information on disk is produced by previous version and auto-migrate it to new one.
- PATCH version defines when bug fixes, which should not require migrations or protocol changes.

Clients can define how current version of data is stored and migrations applied.
General recommendation is to store version in the database and on binary start, check version of database and perform required migrations.

## Protocol Upgrade

Generally, we handle data structure upgradability via enum wrapper around it. See `BlockHeader` structure for example.

### Versioned data structures

Given we expect many data structures to change or get updated as protocol evolves, a few changes are required to support that.

The major one is adding backward compatible `Versioned` data structures like this one:

```rust
enum VersionedBlockHeader {
    BlockHeaderV1(BlockHeaderV1),
    /// Current version, where `BlockHeader` is used internally for all operations.
    BlockHeaderV2(BlockHeader),
}
```

Where `VersionedBlockHeader` will be stored on disk and sent over the wire.
This allows to encode and decode old versions (up to 256 given https://borsh.io specification). If some data structures has more than 256 versions, old versions are probably can be retired and reused.

Internally current version is used. Previous versions either much interfaces / traits that are defined by different components or are up-casted into the next version (saving for hash validation).

### Consensus

| Name | Value |
| - | - |
| `PROTOCOL_UPGRADE_BLOCK_THRESHOLD` | `80%` |
| `PROTOCOL_UPGRADE_NUM_EPOCHS` | `2` |

The way the version will be indicated by validators, will be via

```rust
/// Add `version` into block header.
struct BlockHeaderInnerRest {
    ...
    /// Latest version that current producing node binary is running on.
    version: ProtocolVersion,
}
```

<!-- cspell:ignore defaultdict -->
The condition to switch to next protocol version is based on % of stake `PROTOCOL_UPGRADE_NUM_EPOCHS` epochs prior indicated about switching to the next version:

```python
def next_epoch_protocol_version(last_block):
    """Determines next epoch's protocol version given last block."""
    epoch_info = epoch_manager.get_epoch_info(last_block)
    # Find epoch that decides if version should change by walking back.
    for _ in PROTOCOL_UPGRADE_NUM_EPOCHS:
        epoch_info = epoch_manager.prev_epoch(epoch_info)
        # Stop if this is the first epoch.
        if epoch_info.prev_epoch_id == GENESIS_EPOCH_ID:
            break
    versions = collections.defaultdict(0)
    # Iterate over all blocks in previous epoch and collect latest version for each validator.
    authors = {}
    for block in epoch_info:
        author_id = epoch_manager.get_block_producer(block.header.height)
        if author_id not in authors:
            authors[author_id] = block.header.rest.version
    # Weight versions with stake of each validator.
    for author in authors:
        versions[authors[author] += epoch_manager.validators[author].stake
    (version, stake) = max(versions.items(), key=lambda x: x[1])
    if stake > PROTOCOL_UPGRADE_BLOCK_THRESHOLD * epoch_info.total_block_producer_stake:
        return version
    # Otherwise return version that was used in that deciding epoch.
    return epoch_info.version
```
