import pathlib
import tempfile


def approximate_epoch_height(block_height, epoch_length):
    if block_height == 0:
        return 0
    if block_height <= epoch_length:
        # According to the protocol specifications, there are two epochs with height 1.
        return "1*"
    return int((block_height - 1) / epoch_length)


"""
Generates a config with p2p state sync configured. The node does state sync from peers.
The node also generates snapshots and serves headers and parts to peers as requested.
"""


def get_state_sync_config_p2p(tracked_shards_config):
    # Even though we are testing p2p sync, we intentionally configure the external storage
    # and set a low fallback threshold (1). The storage will be empty because no dumpers
    # are run in the p2p tests; all requests to it will fail. We want to see that the
    # state sync logic is fault-tolerant and will succeed via p2p in spite of this.
    state_parts_dir = str(pathlib.Path(tempfile.gettempdir()) / "state_parts")

    config = {
        "consensus.state_sync_external_timeout": {
            "secs": 0,
            "nanos": 500000000
        },
        "consensus.state_sync_p2p_timeout": {
            "secs": 0,
            "nanos": 500000000
        },
        "consensus.state_sync_external_backoff": {
            "secs": 0,
            "nanos": 500000000
        },
        "store.state_snapshot_config.state_snapshot_type": "Enabled",
        "state_sync": {
            "sync": {
                "ExternalStorage": {
                    "location": {
                        "Filesystem": {
                            "root_dir": state_parts_dir
                        }
                    },
                    "external_storage_fallback_threshold": 1,
                }
            }
        },
    }
    if tracked_shards_config is not None:
        config['tracked_shards_config'] = tracked_shards_config

    return config


"""
Generates a pair of configs with a local directory configured for dumping state sync data.
    - config_dump: a node which generates snapshots and dumps headers and parts to the local directory
    - config_sync: a node configured to use the local directory as a data source for state sync
"""


def get_state_sync_configs_pair(tracked_shards_config='AllShards'):
    state_parts_dir = str(pathlib.Path(tempfile.gettempdir()) / "state_parts")

    config_dump = {
        "state_sync": {
            "dump": {
                "location": {
                    "Filesystem": {
                        "root_dir": state_parts_dir
                    }
                },
                "iteration_delay": {
                    "secs": 0,
                    "nanos": 100000000
                },
            }
        },
        "store.state_snapshot_config.state_snapshot_type": "Enabled",
        "tracked_shards_config": 'AllShards'
    }
    config_sync = {
        "consensus.state_sync_external_timeout": {
            "secs": 0,
            "nanos": 500000000
        },
        "consensus.state_sync_p2p_timeout": {
            "secs": 0,
            "nanos": 500000000
        },
        "consensus.state_sync_external_backoff": {
            "secs": 0,
            "nanos": 500000000
        },
        "state_sync": {
            "sync": {
                "ExternalStorage": {
                    "location": {
                        "Filesystem": {
                            "root_dir": state_parts_dir
                        }
                    }
                }
            }
        },
    }
    if tracked_shards_config is not None:
        config_sync['tracked_shards_config'] = tracked_shards_config

    return (config_dump, config_sync)


"""
Generates a config which:
    - Tracks all shards
    - Dumps headers and parts to local storage
    - Has state sync enabled with local storage source configured
"""


def get_state_sync_config_combined():
    state_parts_dir = str(pathlib.Path(tempfile.gettempdir()) / "state_parts")
    config = {
        "consensus.state_sync_external_timeout": {
            "secs": 0,
            "nanos": 500000000
        },
        "consensus.state_sync_p2p_timeout": {
            "secs": 0,
            "nanos": 500000000
        },
        "consensus.state_sync_external_backoff": {
            "secs": 0,
            "nanos": 500000000
        },
        "state_sync": {
            "dump": {
                "location": {
                    "Filesystem": {
                        "root_dir": state_parts_dir
                    }
                },
                "iteration_delay": {
                    "secs": 0,
                    "nanos": 100000000
                },
            },
            "sync": {
                "ExternalStorage": {
                    "location": {
                        "Filesystem": {
                            "root_dir": state_parts_dir
                        }
                    }
                }
            }
        },
        "store.state_snapshot_config.state_snapshot_type": "Enabled",
        "tracked_shards_config": 'AllShards'
    }

    return config
