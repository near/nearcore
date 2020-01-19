# Steps to run hundred node tests

## Prerequisites
- have a gcloud cli installed, logined, default project set.
(If you can `gcloud compute instances list` and your account can create gcloud instances then it's good)

- python3, virtualenv, pip3

## Steps
1. install python dependencies:
```
cd pytest
virtualenv venv -p `which python3` # First time only
. venv/bin/activate
pip install -r requirements.txt
```

2. Create a gcloud (vm disk) image that has compiled near binary
```
# This will build from current branch, image name as near-<branch>-YYYYMMDD-<username>, cargo build -p near --release
python tests/stress/hundred_nodes/create_gcloud_image.py
# If you want different branch, image name or additional flags passed to cargo
python tests/stress/hundred_nodes/create_gcloud_image image_name branch 'additional flags' 
```

3. Start hundred nodes
```
# will use the near-<branch>-YYYYMMDD-<username> image, instance name will be pytest-node-0 to pytest-node-99
python tests/stress/hundred_nodes/hundred_nodes/start_100_nodes.py
# If you have a different image name, or want different instance name
... start_100_nodes.py image_name instance_name_prefix
```
Nodes are running after this step

4. Access every node
```
gcloud compute ssh pytest-node-<i>
tmux a
```

## Clean up

- Logs are stored in each instance in `/tmp/python-rc.log`. You can collect them all by `tests/stress/hundred_nodes/collect_logs.py
- Delete all instances quickly with `tests/delete_remote_nodes.py`

## Some notes
If you have volatile or slow ssh access to gcloud instances, these scripts can fail at any step. I recommend create an instance on digitalocean, mosh to digitalocean instance (mosh is reliable), running all pytest script there (access gcloud from digital ocean is fast). In the reliable office network or mosh-digitalocean over an unreliable network, scripts never failed.