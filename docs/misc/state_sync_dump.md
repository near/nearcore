# Experimental: Dump State to External Storage

## Purpose

[State Sync](../architecture/how/sync.md#step-2-state-sync-normal-node) is being
reworked.

A new version is available for experimental use. This version gets state parts
from external storage. The following kinds of external storage are supported:

* Local filesystem
* Google Cloud Storage
* Amazon S3

A new version of decentralized state sync is work in progress.

## How-to

neard release `1.36.0-rc.1` adds an experimental option to sync state from
external storage.

See [how-to](state_sync_from_external_storage.md) how to configure your node to
State Sync from External Storage.

In case you would like to manage your own dumps of State, keep reading.

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
