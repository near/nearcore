import argparse
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from mocknet.nodes import get_gcloud_nodes, get_nodes_by_label, get_localnet_nodes, exclude_nodes, intersect_nodes
from mocknet.stats import NodeAccumulationStats, NodeMetrics
from mocknet.helper import stop_nodes, restart_nodes, set_state_parts_dump_mode_for_nodes, set_tracked_shard_for_node, \
    set_tracked_shard_for_nodes, set_state_parts_sync_mode_for_nodes, enable_state_sync
from mocknet.test_flow import loop_for, wait_for_metric, wait_n_epochs, wait_for_nodes_to_sync, nodes_are_in_sync, \
    init_nodes, wait_for_nodes_to_be_up
from configured_logger import logger


class TestStateSync:

    def __init__(self, args):
        self.s3_bucket = args.s3_bucket
        self.s3_region = args.s3_region
        self.parts_local_dir = args.parts_local_dir

        self.sync_duration = args.sync_time_limit

        self.localnet = args.localnet
        if self.localnet:
            self.nodes, self.traffic_nodes = get_localnet_nodes(
                'state_sync',
                num_validators=5,
                num_rpc=3,
                default_port=args.port)
        else:
            self.nodes, self.traffic_nodes = get_gcloud_nodes(
                args.project, args.chain_id)
            for node in self.nodes:
                node['address'] = node['address'] + ':' + str(args.port)

        self.stats_bounds = {
            'bps': {
                'min': args.min_bps,
            },
            'missed_chunks': {
                'max': args.max_missed_chunks
            },
            'epoch_height': {},
            'sync_status': {
                'max': 0
            },
        }

    # if late is True, returns late validators
    # if late is False, returns not late validators
    # if late is None, returns all validators
    def get_validators(self, late=None):
        # TODO(posvyatokum) this will depend on terraform script implementation
        labels = {'role': 'validator'}
        if late is not None:
            labels['late'] = 'yes' if late else 'no'
        return get_nodes_by_label(self.nodes, labels)

    def get_dumping_nodes(self):
        # TODO(posvyatokum) this will depend on terraform script implementation
        return get_nodes_by_label(self.nodes, {'state-parts-producer': 'yes'})

    def get_late_nodes(self):
        # TODO(posvyatokum) this will depend on terraform script implementation
        return get_nodes_by_label(self.nodes, {'late': 'yes'})

    def setup_dump_nodes(self):
        if self.localnet:
            set_state_parts_dump_mode_for_nodes(self.get_dumping_nodes(),
                                                root_dir=self.parts_local_dir)
        else:
            set_state_parts_dump_mode_for_nodes(self.get_dumping_nodes(),
                                                bucket=self.s3_bucket,
                                                region=self.s3_region)

        for i, node in enumerate(self.get_dumping_nodes()[:2]):
            set_tracked_shard_for_node(node, [i * 2, i * 2 + 1])

    def setup_sync_nodes(self, nodes):
        if self.localnet:
            set_state_parts_sync_mode_for_nodes(nodes,
                                                root_dir=self.parts_local_dir)
        else:
            set_state_parts_sync_mode_for_nodes(nodes,
                                                bucket=self.s3_bucket,
                                                region=self.s3_region)
        enable_state_sync(nodes)

    # Starts the traffic node in mirroring mode
    def start_traffic(self):
        # TODO(posvyatokum): implement
        pass

    def run(self):
        all_stats = []

        logger.info("Step 0: setup nodes")
        stop_nodes(self.nodes)
        init_nodes(self.nodes, self.traffic_nodes)
        self.setup_dump_nodes()

        logger.info("Step 1: run early nodes for 5 epochs")
        early_nodes = exclude_nodes(self.nodes, self.get_late_nodes())
        restart_nodes(early_nodes)
        wait_for_nodes_to_be_up(early_nodes)
        self.start_traffic()
        wait_n_epochs(5,
                      early_nodes,
                      all_stats=all_stats,
                      stats_bounds=self.stats_bounds)

        all_late_nodes = self.get_late_nodes()

        logger.info("Step 2: setup and run late validator for 2 epochs")
        self.setup_sync_nodes(all_late_nodes)

        late_validators = self.get_validators(late=True)
        set_tracked_shard_for_nodes(late_validators, [])
        restart_nodes(late_validators)
        wait_for_nodes_to_be_up(late_validators)
        wait_n_epochs(2,
                      early_nodes + late_validators,
                      all_stats=all_stats,
                      stats_bounds=self.stats_bounds)

        logger.info(
            "Step 3: setup and run late rpc; wait until late nodes sync")
        late_rpc = exclude_nodes(self.get_late_nodes(), late_validators)
        set_tracked_shard_for_nodes(late_validators, [1, 2, 3, 4])
        restart_nodes(late_rpc)
        wait_for_nodes_to_be_up(late_rpc)
        final_metrics = wait_for_nodes_to_sync(sync_nodes=all_late_nodes,
                                               all_nodes=self.nodes,
                                               duration=self.sync_duration,
                                               all_stats=all_stats,
                                               stats_bounds=self.stats_bounds)
        NodeAccumulationStats(all_stats).print_report(self.stats_bounds)
        if nodes_are_in_sync(final_metrics, all_late_nodes):
            logger.info('Nodes synced successfully')
        else:
            logger.error('Nodes failed to sync')
            sys.exit('FAIL')


def main():
    parser = argparse.ArgumentParser(description='Run S3 State Sync Test')
    parser.add_argument('--port',
                        type=int,
                        required=True,
                        help='helper scripts port')
    parser.add_argument('--sync-time-limit',
                        type=int,
                        required=True,
                        help='Time given to nodes to do a full sync in seconds')
    parser.add_argument('--s3-bucket', type=str, required=True)
    parser.add_argument('--s3-region', type=str, required=True)
    parser.add_argument('--parts-local-dir', type=str)
    parser.add_argument('--min-bps', type=float, default=1.0)
    parser.add_argument('--max-missed-chunks', type=float, default=0.1)
    parser.add_argument('--localnet', type=bool, default=False)
    args = parser.parse_args()

    test = TestStateSync(args)
    test.run()


if __name__ == '__main__':
    main()
