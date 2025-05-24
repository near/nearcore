"""
This script is used to run a sharded benchmark on a forknet.
"""

from argparse import ArgumentParser
import os
import sys
import json
import copy
import pathlib
import subprocess

import time
from types import SimpleNamespace
from mirror import CommandContext, get_nodes_status, init_cmd, new_test_cmd, \
    reset_cmd, run_env_cmd, run_remote_cmd, run_remote_upload_file, \
    start_nodes_cmd, stop_nodes_cmd, update_binaries_cmd

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from configured_logger import logger

# cspell:words BENCHNET

# TODO: consider moving source directory to pytest.
SOURCE_BENCHNET_DIR = "../benchmarks/sharded-bm"

BENCHNET_DIR = "/home/ubuntu/bench"
NEAR_HOME = "/home/ubuntu/.near"
CONFIG_PATH = f"{NEAR_HOME}/config.json"


def fetch_forknet_details(forknet_name, bm_params):
    """Fetch the forknet details from GCP."""
    find_instances_cmd = [
        "gcloud", "compute", "instances", "list", "--project=nearone-mocknet",
        f"--filter=name~'-{forknet_name}-' AND -name~'traffic' AND -name~'tracing'",
        "--format=get(name,networkInterfaces[0].networkIP)"
    ]
    find_instances_cmd_result = subprocess.run(
        find_instances_cmd,
        capture_output=True,
        text=True,
        check=True,
    )
    output = find_instances_cmd_result.stdout.splitlines()

    num_cp_instances = bm_params['chunk_producers']
    if len(output) != num_cp_instances + 1:
        logger.error(
            f"Expected {num_cp_instances + 1} instances, got {len(output)}")
        sys.exit(1)

    rpc_instance = output[-1]
    rpc_instance_name, rpc_instance_ip = rpc_instance.split()
    cp_instances = list(map(lambda x: x.split(), output[:num_cp_instances]))
    cp_instance_names = [instance[0] for instance in cp_instances]

    find_tracing_server_cmd = [
        "gcloud", "compute", "instances", "list", "--project=nearone-mocknet",
        f"--filter=name~'-{forknet_name}-' AND name~'tracing'",
        "--format=get(networkInterfaces[0].networkIP,networkInterfaces[0].accessConfigs[0].natIP)"
    ]
    tracing_server_cmd_result = subprocess.run(
        find_tracing_server_cmd,
        capture_output=True,
        text=True,
        check=True,
    )
    output = tracing_server_cmd_result.stdout.strip()
    internal_ip, external_ip = output.split() if output else (None, None)
    return {
        "rpc_instance_name": rpc_instance_name,
        "rpc_instance_ip": rpc_instance_ip,
        "cp_instance_names": cp_instance_names,
        "tracing_server_internal_ip": internal_ip,
        "tracing_server_external_ip": external_ip
    }


def handle_init(args):
    """Handle the init command - initialize the benchmark before running it."""

    if args.neard_binary_url is not None:
        logger.info(f"Using neard binary URL from CLI: {args.neard_binary_url}")
    elif os.environ.get('NEARD_BINARY_URL') is not None:
        logger.info(
            f"Using neard binary URL from env: {os.environ['NEARD_BINARY_URL']}"
        )
        args.neard_binary_url = os.environ['NEARD_BINARY_URL']
    else:
        logger.info(
            f"Using neard binary URL from benchmark params: {args.bm_params['forknet']['binary_url']}"
        )
        args.neard_binary_url = args.bm_params['forknet']['binary_url']

    init_args = SimpleNamespace(
        neard_upgrade_binary_url="",
        **vars(args),
    )
    init_cmd(CommandContext(init_args))

    update_binaries_args = copy.deepcopy(args)
    update_binaries_cmd(CommandContext(update_binaries_args))

    run_cmd_args = copy.deepcopy(args)
    run_cmd_args.cmd = f"mkdir -p {BENCHNET_DIR}"
    run_remote_cmd(CommandContext(run_cmd_args))

    # TODO: check neard binary version

    upload_file_args = copy.deepcopy(args)
    upload_file_args.src = f"{SOURCE_BENCHNET_DIR}/cases"
    upload_file_args.dst = BENCHNET_DIR
    run_remote_upload_file(CommandContext(upload_file_args))

    upload_file_args = copy.deepcopy(args)
    upload_file_args.src = "tests/mocknet/helpers"
    upload_file_args.dst = BENCHNET_DIR
    run_remote_upload_file(CommandContext(upload_file_args))

    new_test_cmd_args = SimpleNamespace(
        state_source="empty",
        patches_path=f"{BENCHNET_DIR}/{args.case}",
        # Epoch length is required to be set but will get overwritten by the
        # genesis patch.
        epoch_length=1000,
        num_validators=args.bm_params['chunk_producers'],
        num_seats=None,
        new_chain_id=args.unique_id,
        genesis_protocol_version=None,
        gcs_state_sync=False,
        stateless_setup=True,
        yes=True,
        **vars(args),
    )
    new_test_cmd(CommandContext(new_test_cmd_args))

    status_cmd_args = copy.deepcopy(args)
    status_cmd_ctx = CommandContext(status_cmd_args)
    targeted_nodes = status_cmd_ctx.get_targeted()
    not_ready_nodes = get_nodes_status(targeted_nodes)
    while len(not_ready_nodes) > 0:
        logger.info(f"Waiting for {len(not_ready_nodes)} nodes to be ready...")
        logger.info(f"Example of not ready nodes: {not_ready_nodes[:5]}")
        time.sleep(10)
        not_ready_nodes = get_nodes_status(targeted_nodes)
    logger.info("All nodes are ready")

    tracing_server_ip = args.forknet_details['tracing_server_internal_ip']
    if tracing_server_ip is None:
        logger.info("No tracing server found, skipping tracing setup")
    else:
        env_cmd_args = SimpleNamespace(
            key_value=[
                f"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://{tracing_server_ip}:4317"
            ],
            clear_all=False,
            **vars(args),
        )
        run_env_cmd(CommandContext(env_cmd_args))

    handle_apply_json_patches(args)

    start_nodes(args)

    time.sleep(10)

    run_cmd_args = copy.deepcopy(args)
    run_cmd_args.host_filter = f"({'|'.join(args.forknet_details['cp_instance_names'])})"
    accounts_path = f"{BENCHNET_DIR}/user-data/shard.json"
    run_cmd_args.cmd = f"\
        shard=$(python3 {BENCHNET_DIR}/helpers/get_tracked_shard.py) && \
        echo \"Tracked shard: $shard\" && \
        rm -rf {BENCHNET_DIR}/user-data && \
        mkdir -p {BENCHNET_DIR}/user-data && \
        cp {NEAR_HOME}/user-data/shard_$shard.json {accounts_path} \
    "

    run_remote_cmd(CommandContext(run_cmd_args))

    stop_nodes(args)


def handle_apply_json_patches(args):
    """Handle the apply-json-patches command."""
    genesis = f"{NEAR_HOME}/genesis.json"
    base_genesis_patch = f"{BENCHNET_DIR}/{args.case}/{args.bm_params['base_genesis_patch']}"

    base_config_patch = f"{BENCHNET_DIR}/{args.case}/{args.bm_params['base_config_patch']}"
    config_patch = f"{BENCHNET_DIR}/{args.case}/config_patch.json"

    log_config = f"{NEAR_HOME}/log_config.json"
    log_config_patch = f"{BENCHNET_DIR}/cases/log_patch.json"

    run_cmd_args = copy.deepcopy(args)
    run_cmd_args.cmd = f"\
        python3 {BENCHNET_DIR}/helpers/json_updater.py {genesis} {base_genesis_patch} \
        && python3 {BENCHNET_DIR}/helpers/json_updater.py {CONFIG_PATH} {base_config_patch} {config_patch} \
        && touch {log_config} \
        && python3 {BENCHNET_DIR}/helpers/json_updater.py {log_config} {log_config_patch} \
    "

    if args.forknet_details['tracing_server_internal_ip'] is None:
        run_cmd_args.cmd += f"\
            && jq '.opentelemetry = null' {log_config} >tmp.$$.json && mv tmp.$$.json {log_config} || rm tmp.$$.json \
        "

    run_remote_cmd(CommandContext(run_cmd_args))


def stop_nodes(args, disable_tx_generator=False):
    """Stop the benchmark nodes."""
    logger.info("Stopping nodes")
    stop_nodes_cmd_args = copy.deepcopy(args)
    stop_nodes_cmd_args.host_filter = f"({'|'.join(args.forknet_details['cp_instance_names'])})"
    stop_nodes_cmd(CommandContext(stop_nodes_cmd_args))

    if disable_tx_generator:
        run_cmd_args = copy.deepcopy(args)
        run_cmd_args.cmd = f"\
            jq 'del(.tx_generator)' {CONFIG_PATH} > tmp.$$.json && mv tmp.$$.json {CONFIG_PATH} || rm tmp.$$.json \
        "

        run_remote_cmd(CommandContext(run_cmd_args))


def handle_stop(args):
    """Handle the stop command - stop the benchmark."""
    stop_nodes(args, args.disable_tx_generator)


def handle_reset(args):
    """Handle the reset command - reset the benchmark state."""
    logger.info("Resetting benchmark state")
    stop_nodes(args)

    run_cmd_args = copy.deepcopy(args)
    run_cmd_args.cmd = f"\
        find {NEAR_HOME}/data -mindepth 1 -delete && \
        rm -rf {BENCHNET_DIR} && \
        jq 'del(.tx_generator)' {CONFIG_PATH} > tmp.$$.json && mv tmp.$$.json {CONFIG_PATH} || rm tmp.$$.json \
    "

    run_remote_cmd(CommandContext(run_cmd_args))

    env_cmd_args = copy.deepcopy(args)
    env_cmd_args.clear_all = True
    run_env_cmd(CommandContext(env_cmd_args))

    reset_cmd_args = copy.deepcopy(args)
    reset_cmd_args.backup_id = "start"
    reset_cmd_args.yes = True
    reset_cmd(CommandContext(reset_cmd_args))


def start_nodes(args, enable_tx_generator=False):
    """Start the benchmark nodes with the given parameters."""
    if enable_tx_generator:
        logger.info("Setting tx generator parameters")

        tps = int(args.bm_params['tx_generator']['tps'])
        volume = int(args.bm_params['tx_generator']['volume'])
        accounts_path = f"{BENCHNET_DIR}/user-data/shard.json"

        run_cmd_args = copy.deepcopy(args)
        run_cmd_args.host_filter = f"({'|'.join(args.forknet_details['cp_instance_names'])})"
        run_cmd_args.cmd = f"\
            jq --arg accounts_path {accounts_path} \
            '.tx_generator = {{\"tps\": {tps}, \"volume\": {volume}, \
            \"accounts_path\": $accounts_path, \"thread_count\": 2}}' \
            {NEAR_HOME}/config.json > tmp.$$.json && \
            mv tmp.$$.json {NEAR_HOME}/config.json || rm tmp.$$.json \
        "

        run_remote_cmd(CommandContext(run_cmd_args))

    logger.info("Starting nodes")
    start_nodes_cmd_args = copy.deepcopy(args)
    start_nodes_cmd_args.host_filter = f"({'|'.join(args.forknet_details['cp_instance_names'])})"
    start_nodes_cmd(CommandContext(start_nodes_cmd_args))


def handle_start(args):
    """Handle the start command - start the benchmark."""
    start_nodes(args, args.enable_tx_generator)


def main():
    chain_id = "mainnet"
    try:
        start_height = int(os.environ['FORKNET_START_HEIGHT'])
        unique_id = os.environ['FORKNET_NAME']
        case = os.environ['CASE']
    except KeyError as e:
        logger.error(f"Error: Required environment variable {e} is not set")
        sys.exit(1)
    except ValueError:
        logger.error("Error: FORKNET_START_HEIGHT must be an integer")
        sys.exit(1)

    try:
        bm_params_path = f"{SOURCE_BENCHNET_DIR}/{case}/params.json"
        with open(bm_params_path) as f:
            bm_params = json.load(f)
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error reading binary_url from {bm_params_path}: {e}")
        sys.exit(1)

    forknet_details = fetch_forknet_details(unique_id, bm_params)
    logger.info(forknet_details)

    parser = ArgumentParser(
        description='Forknet cluster parameters to launch a sharded benchmark')
    parser.set_defaults(
        chain_id=chain_id,
        start_height=start_height,
        unique_id=unique_id,
        case=case,
        bm_params=bm_params,
        forknet_details=forknet_details,
        local_test=False,
        host_filter=None,
        host_type="nodes",
        select_partition=None,
    )

    subparsers = parser.add_subparsers(
        dest='command',
        help='Available commands',
    )

    init_parser = subparsers.add_parser('init', help='Initialize the benchmark')
    init_parser.add_argument(
        '--neard-binary-url',
        help='URL of the neard binary to use',
    )

    subparsers.add_parser(
        'apply-json-patches',
        help='Apply the patches to genesis, config and log_config',
    )

    start_parser = subparsers.add_parser('start', help='Start the benchmark')
    start_parser.add_argument(
        '--enable-tx-generator',
        action='store_true',
        help='Enable the tx generator',
    )

    stop_parser = subparsers.add_parser('stop', help='Stop the benchmark')
    stop_parser.add_argument(
        '--disable-tx-generator',
        action='store_true',
        help='Disable the tx generator',
    )

    subparsers.add_parser('reset', help='Reset the benchmark state')

    args = parser.parse_args()

    # Route to appropriate handler based on command
    if args.command == 'init':
        handle_init(args)
    elif args.command == 'apply-json-patches':
        handle_apply_json_patches(args)
    elif args.command == 'stop':
        handle_stop(args)
    elif args.command == 'start':
        handle_start(args)
    elif args.command == 'reset':
        handle_reset(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
