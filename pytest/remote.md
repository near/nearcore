# Running pytest remotely

## Prerequisites
1. Same as local pytest
2. gcloud cli in PATH

## Steps
1. Choose or upload a near binary here: https://console.cloud.google.com/storage/browser/nearprotocol_nearcore_release?project=near-core
2. fill the binary filename in remote.json. Modify zones as needed, they'll be used in round-robin manner. 
3. (in venv) `NEAR_PYTEST_CONFIG=remote.json python tests/...`
4. If tests fail early it might be not clean up gcloud resources, in that case, run `python tests/delete_remote_nodes.py` to clean up