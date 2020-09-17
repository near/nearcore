import sys, time
from rc import pmap

sys.path.append('lib')
import mocknet
import data

def measure_bps(nodes, num_shards, output_file):
    pmap(mocknet.start_node, nodes)
    time.sleep(180)
    measurement = mocknet.chain_measure_bps_and_tps(archival_node=nodes[-1], start_time=None, end_time=None, duration=120)
    measurement['num_shards'] = num_shards
    data.dict_to_csv([measurement], output_file, mode='a')
    pmap(mocknet.reset_data, nodes)
    print(measurement)

if __name__ == '__main__':
    nodes = mocknet.get_nodes(prefix='sharded-')
    output_file = 'shards_vs_bps.csv'
    num_shards_to_test = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    for num_shards in num_shards_to_test:
        config_file = f'config_{num_shards}.json'
        genesis_file = f'genesis_{num_shards}.json'
        pmap(lambda node: node.machine.upload(config_file, '/home/ubuntu/.near/config.json', switch_user='ubuntu'), nodes)
        pmap(lambda node: node.machine.upload(genesis_file, '/home/ubuntu/.near/genesis.json', switch_user='ubuntu'), nodes)
        measure_bps(nodes, num_shards, output_file)
