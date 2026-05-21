"""
This script is used to run a forknet scenario.
"""

from argparse import ArgumentParser, ArgumentTypeError, FileType
import json
import math
import sys
import pathlib
import subprocess
from enum import Enum
import os
import re

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from configured_logger import logger

from forknet_scenarios import get_test_case, get_available_test_cases

CHAIN_ID = "mainnet"
MOCKNET_STORE_PATH = os.getenv("MOCKNET_STORE_PATH",
                               "gs://near-mocknet-artefact-store")


class Action(Enum):
    APPLY = "apply"
    DESTROY = "destroy"

    def __str__(self):
        return self.value


def call_gh_workflow(wf_params: dict):
    cmd = "gh workflow run mocknet_terraform.yml --repo Near-One/infra-ops "
    cmd += " ".join([
        f"-f {key}={value}" for key, value in wf_params.items() if value != None
    ])
    logger.info(f"Calling GH workflow with command: {cmd}")
    result = subprocess.run(cmd, shell=True)
    logger.info(
        f"GH workflow call completed with return code: {result.returncode}")
    return result.returncode


def handle_create(test_setup, dump_workflow_params=None):
    """
    Create the infrastructure for the test case.
    """
    if test_setup.start_height is None:
        raise ValueError("Start height is not set")

    workflow_params = {
        "action": Action.APPLY.value,
        "unique_id": test_setup.unique_id,
        "start_height": test_setup.start_height,
    }
    if test_setup.regions != None:
        workflow_params["location_set"] = test_setup.regions
    if test_setup.node_hardware_config != None:
        workflow_params[
            "chunk_producers"] = test_setup.node_hardware_config.chunk_producers_hosts(
            )
        workflow_params[
            "chunk_validators"] = test_setup.node_hardware_config.only_chunk_validators_hosts(
            )
    if test_setup.has_archival != None:
        workflow_params[
            "archival_nodes"] = "true" if test_setup.has_archival else "false"
    if test_setup.has_state_dumper != None:
        workflow_params[
            "state_dumper"] = "true" if test_setup.has_state_dumper else "false"
    if test_setup.tracing_server != None:
        workflow_params[
            "tracing_server"] = "true" if test_setup.tracing_server else "false"

    if dump_workflow_params:
        json.dump(workflow_params, dump_workflow_params)
        return

    call_gh_workflow(workflow_params)


def handle_destroy(test_setup, dump_workflow_params=None):
    unique_id = test_setup.unique_id
    start_height = test_setup.start_height
    if start_height is None:
        raise ValueError("Start height is not set")

    # Remove mocknet info bucket folder when destroying cluster
    mocknet_id = f"{CHAIN_ID}-{start_height}-{unique_id}"
    bucket_path = f"{MOCKNET_STORE_PATH}/{mocknet_id}"
    logger.info(f"Removing mocknet bucket folder: {bucket_path}")

    cmd = ['gcloud', 'storage', 'rm', '--recursive', bucket_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError as e:
        logger.warning(f"Failed to remove bucket directory {bucket_path}: {e}")
    else:
        if result.returncode != 0:
            logger.warning(
                f"Failed to remove bucket directory {bucket_path}: {result.stderr}"
            )
        else:
            logger.info(f"Successfully removed bucket directory {bucket_path}")

    workflow_params = {
        "action": Action.DESTROY.value,
        "unique_id": unique_id,
        "start_height": start_height,
    }

    if dump_workflow_params:
        json.dump(workflow_params, dump_workflow_params)
        return

    call_gh_workflow(workflow_params)


def handle_start_test(test_setup):
    logger.info("🚀 Starting test...")
    test_setup.fail_if_args_not_set()
    logger.info("🔄 Initializing environment...")
    test_setup.init_env()
    logger.info("🔄 Running before test setup...")
    test_setup.before_test_setup()
    logger.info("🔄 Running new test...")
    test_setup.new_test()
    logger.info("🔄 Waiting for network to be ready...")
    test_setup.wait_for_network_to_be_ready()
    logger.info("🔄 Amending epoch config...")
    test_setup.amend_epoch_config()
    logger.info("🔄 Amending configs before test start...")
    test_setup.amend_configs_before_test_start()
    logger.info("🔄 Starting network...")
    test_setup.start_network()
    logger.info("🔄 Running after test start...")
    test_setup.after_test_start()
    logger.info("🎉 Test setup completed!")


def validate_unique_id(value):
    pattern = r"^(?:[a-z](?:[-a-z0-9]{3,10}[a-z0-9])?)$"
    if not re.match(pattern, value):
        raise ArgumentTypeError(
            f"'{value}' is not a valid unique ID. Must match pattern: {pattern}"
        )
    return value


def positive_float(value):
    """argparse type for --tps. Matches LoadTestRunner's float --tps but
    requires a positive value: the forknet scenarios sweep block delays under
    a known throughput, so LoadTestRunner's `0 = no throttle` mode doesn't
    fit. Rejects nan/inf so downstream arithmetic (concurrency sizing, sleep
    intervals) doesn't blow up."""
    try:
        parsed = float(value)
    except ValueError:
        raise ArgumentTypeError(f"'{value}' is not a number")
    if not math.isfinite(parsed):
        raise ArgumentTypeError(f"--tps must be finite, got {parsed}")
    if parsed <= 0:
        raise ArgumentTypeError(f"--tps must be > 0, got {parsed}")
    return parsed


def main():
    parser = ArgumentParser(
        description='Forknet cluster parameters to launch a release test')
    parser.set_defaults(
        chain_id=CHAIN_ID,
        local_test=False,
        host_type="all",
        host_filter=None,
        select_partition=None,
    )

    parser.add_argument(
        '--dump-workflow-params',
        help=
        'Print infra-ops workflow JSON parameters for the selected command to the specified file (or stdout if not given) without dispatching it.',
        type=FileType('w'),
        nargs='?',
        const='-',
        default=None,
    )

    parser.add_argument(
        '--unique-id',
        help='Unique ID for the test case',
        type=validate_unique_id,
        required=True,
    )

    parser.add_argument(
        '--test-case',
        help=
        f'Name of the test case to run (available test cases: {", ".join(get_available_test_cases())})',
        required=True,
    )

    parser.add_argument(
        '--start-height',
        type=int,
        help=
        'Height of image used to start the network. Used as default if test case class does not set it.',
        required=False,
    )

    subparsers = parser.add_subparsers(
        dest='command',
        help='Available commands',
    )

    create_parser = subparsers.add_parser('create',
                                          help='Create the infrastructure')

    destroy_parser = subparsers.add_parser('destroy',
                                           help='Destroy the infrastructure')

    start_parser = subparsers.add_parser('start',
                                         help='Start the selected scenario')
    start_parser.add_argument(
        '--neard-binary-url',
        help=
        'URL of the neard binary to start with. Can be set in the test case class.',
    )

    start_parser.add_argument(
        '--neard-upgrade-binary-url',
        help=
        'URL of the neard binary to upgrade to. Can be set in the test case class.',
    )

    start_parser.add_argument(
        '--genesis-protocol-version',
        type=int,
        help=
        'Genesis protocol version to use. Used as default if test case class does not set it.',
        required=False,
    )

    start_parser.add_argument(
        '--contract',
        help=
        'WASM source spec forwarded to the stress-test script (alias / http(s) URL / local path). Overrides the test case default when set.',
        required=False,
    )

    start_parser.add_argument(
        '--tps',
        type=positive_float,
        default=1.0,
        help=
        'Per-signer deploys-per-second for the stress-test script (positive float).',
        required=False,
    )

    args = parser.parse_args()

    test_setup = get_test_case(args.test_case, args)
    # Route to appropriate handler based on command
    if args.command == 'create':
        handle_create(test_setup,
                      dump_workflow_params=args.dump_workflow_params)
    elif args.command == 'destroy':
        handle_destroy(test_setup,
                       dump_workflow_params=args.dump_workflow_params)
    elif args.command == 'start':
        handle_start_test(test_setup)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
