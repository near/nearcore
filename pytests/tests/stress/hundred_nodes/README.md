# Steps to run hundred node tests

## Prerequisites

- Have a [gcloud cli installed](https://cloud.google.com/sdk/install), [being logged in](https://cloud.google.com/sdk/gcloud/reference/auth/login), default project set.
(If you can `gcloud compute instances list` and your account can create gcloud instances then it's good)
- python3, virtualenv, pip3
- Locally compile these packages (used for create config, keys and genesis)

```bash
cargo build -p neard --release
cargo build -p genesis-csv-to-json --release
cargo build -p keypair-generator --release
```

## Steps

1. Install python dependencies:

    ```bash
    sudo apt install python3-dev
    cd pytest
    virtualenv venv -p `which python3` # First time only
    . venv/bin/activate
    pip install -r requirements.txt
    ```

    Note: You need python3.6 or greater.

2. Create a gcloud (vm disk) image that has compiled near binary

    ```bash
    # This will build from current branch, image name as near-<branch>-YYYYMMDD-<username>, cargo build -p near --release
    python tests/stress/hundred_nodes/create_gcloud_image.py
    # If you want different branch, image name or additional flags passed to cargo
    python tests/stress/hundred_nodes/create_gcloud_image image_name branch 'additional flags'
    ```

3. Start hundred nodes

    ```bash
    # will use the near-<branch>-YYYYMMDD-<username> image, instance name will be pytest-node-<username>-0 to 99
    python tests/stress/hundred_nodes/start_100_nodes.py
    # If you have a different image name, or want different instance name
    ... start_100_nodes.py image_name instance_name_prefix
    ```

    Nodes are running after this step

4. Access every node

    ```bash
    gcloud compute ssh pytest-node-<i>
    tmux a
    ```

## Clean up

- Logs are stored in each instance in `/tmp/python-rc.log`. You can collect them all by `tests/stress/hundred_nodes/collect_logs.py`.
- Delete all instances quickly with `tests/delete_remote_nodes.py [prefix]`

## Some notes

If you have volatile or slow ssh access to gcloud instances, these scripts can fail at any step. I recommend create an instance on digitalocean, mosh to digitalocean instance (mosh is reliable), running all pytest script there (access gcloud from digital ocean is fast). In the reliable office network or mosh-digitalocean over an unreliable network, scripts never failed.
