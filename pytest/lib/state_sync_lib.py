import pathlib
import tempfile


def get_state_sync_configs_pair():
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
        "store.state_snapshot_enabled": True,
        "tracked_shards": [0],  # Track all shards
    }
    config_sync = {
        "consensus.state_sync_timeout": {
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
        "state_sync_enabled": True,
        "tracked_shards": [0],  # Track all shards
    }

    return (config_dump, config_sync)


def get_state_sync_config_combined():
    state_parts_dir = str(pathlib.Path(tempfile.gettempdir()) / "state_parts")
    config = {
        "consensus.state_sync_timeout": {
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
        "state_sync_enabled": True,
        "store.state_snapshot_enabled": True,
        "tracked_shards": [0],  # Track all shards
    }

    return config
