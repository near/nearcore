from rc import gcloud
import datetime
import json
import pathlib
import sys
import tempfile

machines = gcloud.list()
pytest_nodes = list(filter(lambda m: m.name.startswith('pytest-node'), machines))
test_id = datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d%H%M%S")
data = []

for node in pytest_nodes:
    data.append({'targets': [f'{node.ip}:3030'], 'labels': {'test_id': test_id, 'name': node.name}})

targets_file = pathlib.Path(tempfile.gettempdir()) / 'near' / 'targets.json'
targets_file.parent.mkdir(parents=True, exist_ok=True)
with open(targets_file, 'w') as f:
    json.dump(data, f)

prometheus = gcloud.get('prometheus-grafana')
prometheus.upload(str(targets_file), '/mnt/disks/sdb/prometheus')
prometheus.run('bash', input='''
cd /mnt/disks/sdb/prometheus
docker stop prometheus
./start.sh
''')
