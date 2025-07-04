"""
This script is used to run a release tests on a forknet.
"""

from argparse import ArgumentParser
import sys
import pathlib
import subprocess

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from configured_logger import logger

from release_scenarios import get_test_case, get_available_test_cases

CHAIN_ID = "mainnet"


def call_gh_workflow(action,
                     unique_id,
                     start_height,
                     regions=None,
                     validators=None,
                     has_archival=None,
                     has_state_dumper=None,
                     tracing_server=None):
    cmd = "gh workflow run mocknet_terraform.yml --repo Near-One/infra-ops "
    cmd += f"-f action={action} "
    cmd += f"-f unique_id={unique_id} "
    cmd += f"-f start_height={start_height} "
    if regions:
        cmd += f"-f location_set={regions} "
    if validators != None:
        cmd += f"-f validators={validators} "
    if has_archival != None:
        cmd += f"-f archival_nodes={'true' if has_archival else 'false'} "
    if has_state_dumper != None:
        cmd += f"-f state_dumper={'true' if has_state_dumper else 'false'} "
    if tracing_server != None:
        cmd += f"-f tracing_server={'true' if tracing_server else 'false'} "
    logger.info(f"Calling GH workflow with command: {cmd}")
    result = subprocess.run(cmd, shell=True)
    logger.info(
        f"GH workflow call completed with return code: {result.returncode}")
    return result.returncode


def handle_create(args):
    """
    Create the infrastructure for the test case.
    """
    test_setup = get_test_case(args.test_case, args)
    unique_id = args.unique_id
    start_height = test_setup.start_height
    regions = test_setup.regions
    num_validators = test_setup.validators
    has_archival = test_setup.has_archival
    has_state_dumper = test_setup.has_state_dumper
    tracing_server = test_setup.tracing_server
    call_gh_workflow('apply', unique_id, start_height, regions, num_validators,
                     has_archival, has_state_dumper, tracing_server)


def handle_destroy(args):
    test_setup = get_test_case(args.test_case, args)
    unique_id = args.unique_id
    start_height = test_setup.start_height
    call_gh_workflow('destroy', unique_id, start_height)


def handle_start_test(args):
    test_setup = get_test_case(args.test_case, args)
    test_setup.init_env()
    test_setup.before_test_setup()
    test_setup.new_test()
    test_setup.wait_for_network_to_be_ready()
    test_setup.amend_epoch_config()
    test_setup.amend_configs_before_test_start()
    test_setup.start_network()
    test_setup.after_test_start()


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
        '--unique-id',
        help='Unique ID for the test case',
        required=True,
    )

    parser.add_argument(
        '--test-case',
        help=
        f'Name of the test case to run (available test cases: {", ".join(get_available_test_cases())})',
        required=True,
    )

    subparsers = parser.add_subparsers(
        dest='command',
        help='Available commands',
    )

    create_parser = subparsers.add_parser('create',
                                          help='Create the infrastructure')

    destroy_parser = subparsers.add_parser('destroy',
                                           help='Destroy the infrastructure')

    start_parser = subparsers.add_parser('start_test',
                                         help='Start the release test')
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

    args = parser.parse_args()

    # Route to appropriate handler based on command
    if args.command == 'create':
        handle_create(args)
    elif args.command == 'destroy':
        handle_destroy(args)
    elif args.command == 'start_test':
        handle_start_test(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
