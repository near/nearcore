#!/usr/bin/env python3
"""
Runs a loadtest on mocknet.

The setup requires you to have a few nodes that will be validators and at least one node that will be an RPC node.
Specify the number of validator nodes using the `--num-nodes` flag.

Use https://github.com/near/near-ops/tree/master/provisioning/terraform/network/mocknet to bring up a set of VM
instances for the test. It also takes care of downloading necessary binaries and smart contracts.

Depending on the `--chain-id` flag, the nodes will be initialized with an empty state, or with the most recent state
dump of `mainnet`, `testnet` and `betanet`. Note that `mainnet` and `testnet` state dumps are huge. Starting a node with
`testnet` state dump takes about 1.5 hours.

You can run multiple tests in parallel, use `--pattern` to disambiguate.

Configure the desirable generated load with the `--max-tps` flag, or disable load altogether with `--skip-load`.

Example from the recent loadtest run:
1) terraform apply -var="chain_id=mainnet" -var="size=big" -var="override_chain_id=rc3-22" -var="neard_binary_url=https://s3.us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux/1.23.0/1eaa01d6abc76757b2ef50a1a127f98576b750c4/neard" -var="upgrade_neard_binary_url=https://near-protocol-public.s3.ca-central-1.amazonaws.com/mocknet/neard.rc3-22"
2) python3 tests/mocknet/load_test_spoon.py --chain-id=mainnet-spoon --pattern=rc3-22 --epoch-length=1000 --num-nodes=120 --max-tps=100 --script=add_and_delete --increasing-stakes=1 --progressive-upgrade --num-seats=100

Things to look out for when running the test:
1) Test init phase completes before any binary upgrades start.
2) At the beginning of each of the first epochs some nodes get upgraded.
3) If the protocol upgrades becomes effective at epoch T, check that binaries upgraded at epochs T-1, T-2 have started successfully.
4) BPS and TPS need to be at some sensible values.
5) CPU usage and RAM usage need to be reasonable as well.
6) Ideally we should check the percentage of generated transactions that succeed, but there is no easy way to do that. Maybe replay some blocks using `neard view_state apply_range --help`.

Other notes:
1) This grafana dashboard can help: https://grafana.near.org/d/jHbiNgSnz/mocknet
2) Logs are in /home/ubuntu/neard.log and /home/ubuntu/neard.upgrade.log

For the terraform command to work, please do the following:
1) Install terraform cli: https://learn.hashicorp.com/tutorials/terraform/install-cli
2) git clone https://github.com/near/near-ops
3) cd provisioning/terraform/network/mocknet
4) Run `terraform init`
5) Run `terraform apply` as specified above and check the output.
6) If you don't have permissions for the GCP project `near-mocknet`, please ask your friendly SREs to give you permissions.
7) Make the pair of neard binaries available for download. The pair of binaries is the baseline version, e.g. the current mainnet binary version, and your experimental binary. The simplest way to make the binaries available is to upload them to https://s3.console.aws.amazon.com/s3/buckets/near-protocol-public?region=ca-central-1&prefix=mocknet/
8) If you don't have access to S3 AWS, please ask your friendly SREs to give you access.
9) A reliable way to make an experimental binary compatible with mocknet instances, is to build it on machine "builder" in project "near-mocknet". You may need to "Resume" the instance in the GCP console: https://console.cloud.google.com/compute/instancesDetail/zones/us-central1-a/instances/builder?project=near-mocknet

For the `load_test_spoon.py` to work, please do the following:
1) git clone https://github.com/near/nearcore
2) cd pytest
3) Follow instructions in https://github.com/near/nearcore/blob/master/pytest/README.md to setup your Python environment
4) Run load_test_spoon.py as specified above.

"""
import argparse
import random
import sys
import time
from rc import pmap
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import mocknet
import data

from metrics import Metrics
from configured_logger import logger


def measure_tps_bps(nodes, tx_filename):
    input_tx_events = mocknet.get_tx_events(nodes, tx_filename)
    # drop first and last 5% of events to avoid edges of test
    n = int(0.05 * len(input_tx_events))
    input_tx_events = input_tx_events[n:-n]
    input_tps = data.compute_rate(input_tx_events)
    measurement = mocknet.chain_measure_bps_and_tps(
        nodes[-1],
        input_tx_events[0],
        input_tx_events[-1],
    )
    result = {
        'bps': measurement['bps'],
        'in_tps': input_tps,
        'out_tps': measurement['tps']
    }
    logger.info(f'{result}')
    return result


def check_tps(measurement, expected_in, expected_out=None, tolerance=0.05):
    if expected_out is None:
        expected_out = expected_in
    almost_equal = lambda x, y: (abs(x - y) / y) <= tolerance
    return (almost_equal(measurement['in_tps'], expected_in) and
            almost_equal(measurement['out_tps'], expected_out))


def check_memory_usage(node):
    metrics = mocknet.get_metrics(node)
    mem_usage = metrics.memory_usage / 1e6
    logger.info(f'Memory usage (MB) = {mem_usage}')
    return mem_usage < 4500


def check_slow_blocks(initial_metrics, final_metrics):
    delta = Metrics.diff(final_metrics, initial_metrics)
    slow_process_blocks = delta.block_processing_time[
        'le +Inf'] - delta.block_processing_time['le 1']
    logger.info(
        f'Number of blocks processing for more than 1s: {slow_process_blocks}')
    return slow_process_blocks == 0


class LoadTestSpoon:
    EPOCH_HEIGHT_CHECK_DELAY = 30

    def run(self):
        logger.info('Starting load test spoon.')

        self.__parse_args()

        self.__prepare_nodes()

        self.__prepare_upgrade_schedule()

        if not self.skip_setup:
            self.__setup_remote_python_environments()

        if not self.skip_restart:
            self.__upload_genesis_and_restart()

        mocknet.wait_all_nodes_up(self.all_nodes)

        initial_metrics = mocknet.get_metrics(self.archival_node)
        initial_validator_accounts = mocknet.list_validators(self.archival_node)

        if not self.skip_load:
            self.__load()

        time.sleep(5)

        final_metrics = mocknet.get_metrics(self.archival_node)
        final_validator_accounts = mocknet.list_validators(self.archival_node)

        self.__final_checks(
            initial_metrics,
            final_metrics,
            initial_validator_accounts,
            final_validator_accounts,
        )

        logger.info('Load test complete.')

    def __parse_args(self):
        parser = argparse.ArgumentParser(description='Run a load test')
        parser.add_argument('--chain-id', required=True)
        parser.add_argument('--pattern', required=False)
        parser.add_argument('--epoch-length', type=int, required=True)
        parser.add_argument('--num-nodes', type=int, required=True)
        parser.add_argument('--max-tps', type=float, required=True)
        parser.add_argument('--increasing-stakes', type=float, default=0.0)
        parser.add_argument('--progressive-upgrade', action='store_true')

        parser.add_argument('--skip-load', action='store_true')
        parser.add_argument('--skip-setup', action='store_true')
        parser.add_argument('--skip-restart', action='store_true')
        parser.add_argument('--no-sharding', action='store_true')
        parser.add_argument('--num-seats', type=int, required=True)

        parser.add_argument('--test-timeout', type=int, default=12 * 60 * 60)
        parser.add_argument(
            '--contract-deploy-time',
            type=int,
            default=10 * mocknet.NUM_ACCOUNTS,
            help=
            'We need to slowly deploy contracts, otherwise we stall out the nodes',
        )

        parser.add_argument('--log-level', default="INFO")

        # The flag is no longer needed but is kept for backwards-compatibility.
        parser.add_argument('--script', required=False)

        args = parser.parse_args()

        logger.setLevel(args.log_level)

        self.chain_id = args.chain_id
        self.pattern = args.pattern
        self.epoch_length = args.epoch_length
        self.num_nodes = args.num_nodes
        self.max_tps = args.max_tps

        self.increasing_stakes = args.increasing_stakes
        self.progressive_upgrade = args.progressive_upgrade
        self.num_seats = args.num_seats

        self.skip_setup = args.skip_setup
        self.skip_restart = args.skip_restart
        self.skip_load = args.skip_load
        self.no_sharding = args.no_sharding

        self.test_timeout = args.test_timeout
        self.contract_deploy_time = args.contract_deploy_time

        self.log_level = args.log_level

        assert self.epoch_length > 0
        assert self.num_nodes > 0
        assert self.max_tps > 0

    def __prepare_nodes(self):
        all_nodes = mocknet.get_nodes(pattern=self.pattern)
        random.shuffle(all_nodes)
        assert len(all_nodes) > self.num_nodes, \
            f'Need at least one RPC node. ' \
            f'Nodes available in mocknet: {len(all_nodes)} ' \
            f'Requested validator nodes num: {self.num_nodes}'

        self.all_nodes = all_nodes
        self.validator_nodes = all_nodes[:self.num_nodes]
        self.rpc_nodes = all_nodes[self.num_nodes:]
        self.archival_node = self.rpc_nodes[0]

        logger.info(
            f'validator_nodes: {[n.instance_name for n in self.validator_nodes]}'
        )
        logger.info(
            f'rpc_nodes      : {[n.instance_name for n in self.rpc_nodes]}')
        logger.info(f'archival node  : {self.archival_node.instance_name}')

    def __prepare_upgrade_schedule(self):
        logger.info(f'Preparing upgrade schedule')
        self.upgrade_schedule = mocknet.create_upgrade_schedule(
            self.rpc_nodes,
            self.validator_nodes,
            self.progressive_upgrade,
            self.increasing_stakes,
            self.num_seats,
        )
        logger.info(f'Preparing upgrade schedule -- done.')

    def __setup_remote_python_environments(self):
        logger.info('Setting remote python environments')
        mocknet.setup_python_environments(
            self.all_nodes,
            'add_and_delete_state.wasm',
        )
        logger.info('Setting remote python environments -- done')

    def __upload_genesis_and_restart(self):
        logger.info(f'Restarting')
        # Make sure nodes are running by restarting them.
        mocknet.stop_nodes(self.all_nodes)
        time.sleep(10)
        validator_node_pks = pmap(
            lambda node: mocknet.get_node_keys(node)[0],
            self.validator_nodes,
        )
        all_node_pks = pmap(
            lambda node: mocknet.get_node_keys(node)[0],
            self.all_nodes,
        )
        pmap(lambda node: mocknet.init_validator_key(node), self.all_nodes)
        node_ips = [node.machine.ip for node in self.all_nodes]
        mocknet.create_and_upload_genesis(
            self.validator_nodes,
            self.chain_id,
            rpc_nodes=self.rpc_nodes,
            epoch_length=self.epoch_length,
            node_pks=validator_node_pks,
            increasing_stakes=self.increasing_stakes,
            num_seats=self.num_seats,
            single_shard=self.no_sharding,
            all_node_pks=all_node_pks,
            node_ips=node_ips,
        )
        mocknet.start_nodes(self.all_nodes, self.upgrade_schedule)
        time.sleep(60)

        logger.info(f'Restarting -- done')

    def __start_load_test_helpers(self):
        logger.info('Starting transaction spamming scripts.')
        script = 'load_test_spoon_helper.py'
        mocknet.start_load_test_helpers(
            script,
            self.validator_nodes,
            self.rpc_nodes,
            self.max_tps,
            # Make the helper run a bit longer - there is significant delay
            # between the time when the first helper is started and when this
            # scipts begins the test.
            self.test_timeout + 4 * self.EPOCH_HEIGHT_CHECK_DELAY,
            self.contract_deploy_time,
        )
        logger.info('Starting transaction spamming scripts -- done.')

    def __check_neard_and_helper_are_running(self):
        neard_running = mocknet.is_binary_running_all_nodes(
            'neard',
            self.all_nodes,
        )
        helper_running = mocknet.is_binary_running_all_nodes(
            'load_test_spoon_helper.py',
            self.validator_nodes,
        )

        logger.debug(
            f'neard is running on {neard_running.count(True)}/{len(neard_running)} nodes'
        )
        logger.debug(
            f'helper is running on {helper_running.count(True)}/{len(helper_running)} validator nodes'
        )

        for node, is_running in zip(self.all_nodes, neard_running):
            if not is_running:
                raise Exception(
                    'The neard process is not running on some of the nodes! '
                    f'The neard is not running on {node.instance_name}')

        for node, is_running in zip(self.validator_nodes, helper_running):
            if not is_running:
                raise Exception(
                    'The helper process is not running on some of the nodes! '
                    f'The helper is not running on {node.instance_name}')

    def __wait_to_deploy_contracts(self):
        msg = 'Waiting for the helper to deploy contracts'
        logger.info(f'{msg}: {self.contract_deploy_time}s')

        start_time = time.monotonic()
        while True:
            self.__check_neard_and_helper_are_running()

            now = time.monotonic()
            remaining_time = self.contract_deploy_time + start_time - now
            logger.info(f'{msg}: {round(remaining_time)}s')
            if remaining_time < 0:
                break

            time.sleep(self.EPOCH_HEIGHT_CHECK_DELAY)

    def __wait_to_complete(self):
        msg = 'Waiting for the loadtest to complete'
        logger.info(f'{msg}: {self.test_timeout}s')

        initial_epoch_height = mocknet.get_epoch_height(self.rpc_nodes, -1)
        logger.info(f'initial_epoch_height: {initial_epoch_height}')
        assert initial_epoch_height >= 0

        prev_epoch_height = initial_epoch_height
        start_time = time.monotonic()
        while True:
            self.__check_neard_and_helper_are_running()

            epoch_height = mocknet.get_epoch_height(
                self.rpc_nodes,
                prev_epoch_height,
            )
            if epoch_height > prev_epoch_height:
                mocknet.upgrade_nodes(
                    epoch_height - initial_epoch_height,
                    self.upgrade_schedule,
                    self.all_nodes,
                )
                prev_epoch_height = epoch_height

            remaining_time = self.test_timeout + start_time - time.monotonic()
            logger.info(f'{msg}: {round(remaining_time)}s')
            if remaining_time < 0:
                break

            time.sleep(self.EPOCH_HEIGHT_CHECK_DELAY)

        logger.info(f'{msg} -- done')

    def __load(self):
        msg = 'Running load test'
        logger.info(f'{msg}')

        self.__start_load_test_helpers()

        self.__wait_to_deploy_contracts()

        self.__wait_to_complete()

        logger.info(f'{msg} -- done')

    def __final_checks(
        self,
        initial_metrics,
        final_metrics,
        initial_validator_accounts,
        final_validator_accounts,
    ):
        msg = 'Running final checks'
        logger.info(msg)

        test_passed = True
        if not check_memory_usage(self.validator_nodes[0]):
            test_passed = False
            logger.error('Memory usage too large')
        if not check_slow_blocks(initial_metrics, final_metrics):
            test_passed = False
            logger.error('Too many slow blocks')

        if set(initial_validator_accounts) != set(final_validator_accounts):
            logger.warning(
                f'Mismatching set of validators:\n'
                f'initial_validator_accounts: {initial_validator_accounts}\n'
                f'final_validator_accounts  : {final_validator_accounts}')

        assert test_passed

        logger.info(f'{msg} -- done.')


if __name__ == '__main__':
    load_test_spoon = LoadTestSpoon()
    load_test_spoon.run()
