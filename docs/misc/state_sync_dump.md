# Dump State to External Storage

## Purpose

A node can dump the state of every epoch to external storage. The following
kinds of external storage are supported:

* Local filesystem
* Google Cloud Storage
* Amazon S3

Note: consuming state parts from external storage (centralized state sync) has
been removed — nodes now always sync state from peers
([State Sync](../architecture/how/sync.md#step-2-state-sync-normal-node)). Only
the dump side documented here remains, for archival and state-distribution
tooling.

## How-to

To manage your own dumps of state, keep reading.

### Google Cloud Storage

To enable Google Cloud Storage as your external storage, add this to your
`config.json` file:

```json
"state_sync": {
  "dump": {
    "location": {
      "GCS": {
        "bucket": "my-gcs-bucket",
      }
    }
  }
}
```

And run your node with an environment variable `SERVICE_ACCOUNT` or
`GOOGLE_APPLICATION_CREDENTIALS` pointing to the credentials json file

```shell
SERVICE_ACCOUNT=/path/to/file ./neard run
```

### Amazon S3

To enable Amazon S3 as your external storage, add this to your `config.json`
file:

```json
"state_sync": {
  "dump": {
    "location": {
      "S3": {
        "bucket": "my-aws-bucket",
        "region": "my-aws-region"
      }
    }    
  }
}
```

And run your node with environment variables `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`:

```shell
AWS_ACCESS_KEY_ID="MY_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="MY_AWS_SECRET_ACCESS_KEY" ./neard run
```

## Dump to a local filesystem

Add this to your `config.json` file to dump state of every epoch to local
filesystem:

```json
"state_sync": {
  "dump": {
    "location": {
      "Filesystem": {
        "root_dir": "/tmp/state-dump"
      }
    }    
  }
}
```

In this case you don't need any extra environment variables. Simply run your
node:

```shell
./neard run
```
