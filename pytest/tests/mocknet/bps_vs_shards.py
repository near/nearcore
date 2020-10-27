import datetime, json, os, shutil, statistics, sys, time
from rc import pmap
from collections import OrderedDict

from unthrottled_load_testing_helper import TEST_TIMEOUT

sys.path.append('lib')
import mocknet
import data

def measure_bps(nodes, num_shards, output_file):
    mocknet.setup_python_environments(nodes, 'tests/mocknet/transfer_only_load_testing_helper.py')
    pmap(mocknet.start_node, nodes)
    time.sleep(60)
    mocknet.start_load_test_helpers(nodes, 'transfer_only_load_testing_helper.py')
    time.sleep(TEST_TIMEOUT + 60)
    
    input_tx_events = mocknet.get_tx_events(nodes)
    n = int(0.05 * len(input_tx_events))
    input_tx_events = input_tx_events[n:-n]
    input_tps = data.compute_rate(input_tx_events)
    measurement = mocknet.chain_measure_bps_and_tps(archival_node=nodes[-1], start_time=input_tx_events[0], end_time=input_tx_events[-1])
    measurement['num_shards'] = num_shards
    measurement['input_tps'] = input_tps
    
    data.dict_to_csv([measurement], output_file, mode='a')
    pmap(mocknet.reset_data, nodes)
    print(measurement)


def update_genesis_timestamp(genesis_file):
    new_timestamp = datetime.datetime.utcnow().isoformat() + '000Z'
    lines = []
    with open(genesis_file) as input:
        for line in input.readlines():
            if 'genesis_time' in line:
                lines.append(f'  "genesis_time": "{new_timestamp}",\n')
            else:
                lines.append(line)
    with open(genesis_file, 'w') as f:
        for line in lines:
            f.write(line)

def update_tracked_shards(config_file, tracked_shards):
    with open(config_file) as f:
        config = json.load(f, object_pairs_hook=OrderedDict)
    config['tracked_shards'] = tracked_shards
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)

def set_node_tracked_shards(node, base_config_file, shards_mapping):
    validator = mocknet.get_validator_account(node)
    if validator.pk not in shards_mapping:
        return
    tracked_shards = shards_mapping[validator.pk]
    config_file = f'{node.machine.name}.config.json'
    shutil.copy(base_config_file, config_file)
    update_tracked_shards(config_file, tracked_shards)
    node.machine.upload(config_file, '/home/ubuntu/.near/config.json', switch_user='ubuntu')
    os.remove(config_file)

def set_all_nodes_tracked_shards(nodes, base_config_file, shards_mapping):
    pmap(lambda node: set_node_tracked_shards(node, base_config_file, shards_mapping), nodes)

def create_shards_mapping(archival_node):
    shards_mapping = {}
    validators = archival_node.get_validators()['result']
    for v in validators['current_validators']:
        shards_mapping[v['public_key']] = v['shards']
    return shards_mapping


def change_transfer_sleep_time(new_value):
    script_file = 'tests/mocknet/transfer_only_load_testing_helper.py'
    lines = []
    with open(script_file) as f:
        for line in f.readlines():
            if 'TRANSFER_SLEEP_TIME = ' in line:
                lines.append(f'TRANSFER_SLEEP_TIME = {new_value}\n')
            else:
                lines.append(line)
    with open(script_file, 'w') as f:
        for line in lines:
            f.write(line)


def setup_shard_tracking(nodes, num_shards):
    config_file = f'config_{num_shards}.json'
    genesis_file = f'genesis_{num_shards}.json'
    update_genesis_timestamp(genesis_file)
    pmap(lambda node: node.machine.upload(config_file, '/home/ubuntu/.near/config.json', switch_user='ubuntu'), nodes)
    pmap(lambda node: node.machine.upload(genesis_file, '/home/ubuntu/.near/genesis.json', switch_user='ubuntu'), nodes)
    pmap(mocknet.start_node, nodes)
    time.sleep(20)
    shards_mapping = create_shards_mapping(nodes[-1])
    pmap(mocknet.reset_data, nodes)
    set_all_nodes_tracked_shards(nodes, config_file, shards_mapping)
    target_tps = 800 * num_shards + 200
    sleep_time = 50.0 / target_tps
    change_transfer_sleep_time(sleep_time)



if __name__ == '__main__':
    nodes = mocknet.get_nodes(prefix='sharded-')
    output_file = 'shards_vs_tps.csv'
    num_shards_to_test = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] # , 11, 12, 13, 14, 15, 16, 20, 30 ,40 ,50, 60, 70, 80, 90, 100]
    for num_shards in num_shards_to_test:
        setup_shard_tracking(nodes, num_shards)
        measure_bps(nodes, num_shards, output_file)
