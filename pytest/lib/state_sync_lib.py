def approximate_epoch_height(block_height, epoch_length):
    if block_height == 0:
        return 0
    if block_height <= epoch_length:
        # According to the protocol specifications, there are two epochs with height 1.
        return "1*"
    return int((block_height - 1) / epoch_length)


def get_state_sync_configs_pair(tracked_shards_config='AllShards'):
    """Generate a pair of configs for decentralized (peer-to-peer) state sync.

    - config_dump: tracks all shards and takes state snapshots, so it can serve
      state parts to peers over the network.
    - config_sync: syncs state from peers. `SyncConfig::Peers` is the default,
      so no explicit `sync` config is needed.
    """
    config_dump = {
        "store.state_snapshot_config.state_snapshot_type": "Enabled",
        "tracked_shards_config": 'AllShards',
    }
    config_sync = {
        "consensus.state_sync_p2p_timeout": {
            "secs": 0,
            "nanos": 500000000
        },
    }
    if tracked_shards_config is not None:
        config_sync['tracked_shards_config'] = tracked_shards_config

    return (config_dump, config_sync)


def get_state_sync_config_combined():
    """Generate a single config for decentralized (peer-to-peer) state sync that:

    - Tracks all shards
    - Takes state snapshots so it can serve state parts to peers
    - Syncs state from peers (`SyncConfig::Peers`, the default)
    """
    config = {
        "consensus.state_sync_p2p_timeout": {
            "secs": 0,
            "nanos": 500000000
        },
        "store.state_snapshot_config.state_snapshot_type": "Enabled",
        "tracked_shards_config": 'AllShards'
    }

    return config
