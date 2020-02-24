from rc import gcloud
import sys
import json
import datetime

machines = gcloud.list()
pytest_nodes = list(filter(lambda m: m.name.startswith('pytest-node'), machines))
test_id = datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d%H%M%S")
data = []

for node in pytest_nodes:
    data.append({'targets': [f'{node.ip}:3030'], 'labels': {'test_id': test_id, 'name': node.name}})

with open('/tmp/near/targets.json', 'w') as f:
    json.dump(data, f)

prometheus = gcloud.get('prometheus-grafana')
prometheus.upload('/tmp/near/targets.json', '/mnt/disks/sdb/prometheus')
prometheus.run('bash', input='''
cd /mnt/disks/sdb/prometheus
docker stop prometheus
./start.sh
''')
