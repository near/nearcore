"""
This script is used to run a sharded benchmark on a forknet.
"""

from argparse import ArgumentParser
import os
import sys
import json
import copy
import pathlib
import requests
import subprocess
import time
import datetime
from tqdm import tqdm

from types import SimpleNamespace
from mirror import CommandContext, get_nodes_status, init_cmd, new_test_cmd, \
    reset_cmd, run_env_cmd, run_remote_cmd, run_remote_download_file, run_remote_upload_file, \
    start_nodes_cmd, stop_nodes_cmd, update_binaries_cmd

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from configured_logger import logger

# cspell:words BENCHNET
CHAIN_ID = "mainnet"

# This height should be used for forknet cluster creation as well.
# It corresponds to the existing setup with minimal disk usage.
START_HEIGHT = 138038232

# TODO: consider moving source directory to pytest.
SOURCE_BENCHNET_DIR = "../benchmarks/sharded-bm"

REMOTE_HOME = "/home/ubuntu"
BENCHNET_DIR = f"{REMOTE_HOME}/bench"
NEAR_HOME = f"{REMOTE_HOME}/.near"
CONFIG_PATH = f"{NEAR_HOME}/config.json"


def fetch_forknet_details(forknet_name, bm_params):
    """Fetch the forknet details from GCP."""
    find_instances_cmd = [
        "gcloud", "compute", "instances", "list", "--project=nearone-mocknet",
        f"--filter=name~'-{forknet_name}-' AND -name~'traffic' AND -name~'tracing'",
        "--format=table(name,networkInterfaces[0].networkIP,zone)"
    ]
    find_instances_cmd_result = subprocess.run(
        find_instances_cmd,
        capture_output=True,
        text=True,
        check=True,
    )

    # drop the table header line in the output
    nodes_data = find_instances_cmd_result.stdout.splitlines()[1:]

    num_cp_instances = bm_params['chunk_producers']
    if len(nodes_data) != num_cp_instances + 1:
        logger.error(
            f"Expected {num_cp_instances + 1} instances, got {len(nodes_data)}")
        sys.exit(1)

    # cratch to refresh the local keystore
    for node_data in nodes_data:
        columns = node_data.split()
        name, zone = columns[0], columns[2]
        login_cmd = [
            "gcloud", "compute", "ssh", "--zone", zone, f"ubuntu@{name}",
            "--project", "nearone-mocknet", "--command", "pwd"
        ]
        subprocess.run(login_cmd, text=True, check=True)

    rpc_instance = nodes_data[-1]
    rpc_instance_name, rpc_instance_ip, _ = rpc_instance.split()
    cp_instances = list(map(lambda x: x.split(), nodes_data[:num_cp_instances]))
    cp_instance_names = [instance[0] for instance in cp_instances]
    cp_instance_zones = [instance[2] for instance in cp_instances]

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


def upload_local_neard(args):
    """ 
    uploads the local `neard` binary to every node to the ${BENCHNET_DIR}. 
    @return the absolute path (local to the remote node) to the uploaded `neard`

    """
    logger.info("uploading the neard ")
    upload_file_args = copy.deepcopy(args)
    upload_file_args.src = args.neard_binary_url
    upload_file_args.dst = BENCHNET_DIR
    run_remote_upload_file(CommandContext(upload_file_args))
    return os.path.join(BENCHNET_DIR, "neard")


def upload_json_patches(args):
    """Upload the json patches to the benchmark directory."""
    upload_file_args = copy.deepcopy(args)
    upload_file_args.src = f"{SOURCE_BENCHNET_DIR}/cases"
    upload_file_args.dst = BENCHNET_DIR
    run_remote_upload_file(CommandContext(upload_file_args))

    upload_file_args = copy.deepcopy(args)
    upload_file_args.src = "tests/mocknet/helpers"
    upload_file_args.dst = BENCHNET_DIR
    run_remote_upload_file(CommandContext(upload_file_args))


def handle_init(args):
    """Handle the init command - initialize the benchmark before running it."""

    run_cmd_args = copy.deepcopy(args)
    run_cmd_args.cmd = f"mkdir -p {BENCHNET_DIR}"
    run_remote_cmd(CommandContext(run_cmd_args))

    if args.neard_binary_url is not None:
        logger.info(f"Using neard binary URL from CLI: {args.neard_binary_url}")
    elif os.environ.get('NEARD_BINARY_URL') is not None:
        logger.info(
            f"Using neard binary URL from env: {os.environ['NEARD_BINARY_URL']}"
        )
        args.neard_binary_url = os.environ['NEARD_BINARY_URL']
    else:
        logger.info(
            "Please provide neard binary URL via CLI or env var NEARD_BINARY_URL"
        )
        sys.exit(1)

    # if neard_binary_url is a local path - upload the file to each node
    if os.path.isfile(args.neard_binary_url):
        logger.info(f"handling local `neard` at {args.neard_binary_url}")
        local_path_on_remote = upload_local_neard(args)
        args.neard_binary_url = local_path_on_remote
        logger.info(f"`neard` local path on remote: {args.neard_binary_url}")
    else:
        logger.info("no local `neard` found, continue assuming the remote url")

    init_args = SimpleNamespace(
        neard_upgrade_binary_url="",
        **vars(args),
    )
    init_cmd(CommandContext(init_args))

    update_binaries_args = copy.deepcopy(args)
    update_binaries_cmd(CommandContext(update_binaries_args))

    # TODO: check neard binary version

    upload_json_patches(args)

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

    apply_json_patches(args)

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


def apply_json_patches(args):
    """Apply the json patches to the genesis, config and log_config."""
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


def handle_tweak_config(args):
    """
    Handle the tweak-config command.

    Used when you want to tweak non-critical parameters of the benchmark, such
    as block production time, load schedule, log levels.
    Note that for critical parameters like number of accounts per shard you 
    must reinitialize the benchmark!
    """
    upload_json_patches(args)
    apply_json_patches(args)


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

        accounts_path = f"{BENCHNET_DIR}/user-data/shard.json"
        schedule_file = f"{BENCHNET_DIR}/{args.case}/load-schedule.json"

        run_cmd_args = copy.deepcopy(args)
        run_cmd_args.host_filter = f"({'|'.join(args.forknet_details['cp_instance_names'])})"
        run_cmd_args.cmd = f"\
            jq --arg accounts_path {accounts_path} \
            '.tx_generator = {{ \"accounts_path\": $accounts_path }}' {CONFIG_PATH} > tmp.$$.json && \
            mv tmp.$$.json {CONFIG_PATH} || rm tmp.$$.json \
        "

        run_remote_cmd(CommandContext(run_cmd_args))

        run_cmd_args = copy.deepcopy(args)
        run_cmd_args.host_filter = f"({'|'.join(args.forknet_details['cp_instance_names'])})"
        run_cmd_args.cmd = f"\
            jq --slurpfile patch {schedule_file} \
            '. as $orig | $patch[0].schedule as $sched | .[\"tx_generator\"] += {{\"schedule\": $sched }}' \
            {CONFIG_PATH} > tmp.$$.json && mv tmp.$$.json {CONFIG_PATH} || rm tmp.$$.json \
        "

        run_remote_cmd(CommandContext(run_cmd_args))

    logger.info("Starting nodes")
    start_nodes_cmd_args = copy.deepcopy(args)
    start_nodes_cmd_args.host_filter = f"({'|'.join(args.forknet_details['cp_instance_names'])})"
    start_nodes_cmd(CommandContext(start_nodes_cmd_args))


def handle_get_traces(args):
    """Handle the get-traces command - fetch traces from the tracing server.
    """
    if args.forknet_details['tracing_server_external_ip'] is None:
        logger.error("Error: Tracing server external IP is not set")
        return

    try:
        if args.time is None:
            cur_time = datetime.datetime.now(datetime.timezone.utc)
        else:
            cur_time = datetime.datetime.strptime(args.time,
                                                  "%Y-%m-%d %H:%M:%S")
            cur_time = cur_time.replace(tzinfo=datetime.timezone.utc)

        cur_time_seconds = cur_time.timestamp()
    except ValueError as e:
        logger.error(
            f"Invalid datetime format. Expected 'YYYY-MM-DD HH:MM:SS'. Error: {e}"
        )
        return

    start_time = int((cur_time_seconds - args.lag - args.len) * 1000)
    end_time = int((cur_time_seconds - args.lag) * 1000)

    logger.info(
        f"Start time: {datetime.datetime.fromtimestamp(start_time/1000, tz=datetime.timezone.utc).isoformat()}"
    )
    logger.info(
        f"End time: {datetime.datetime.fromtimestamp(end_time/1000, tz=datetime.timezone.utc).isoformat()}"
    )

    os.makedirs(args.output_dir, exist_ok=True)

    trace_file = os.path.join(args.output_dir, f"trace_{start_time}.json")

    response = requests.post(
        f"http://{args.forknet_details['tracing_server_external_ip']}:8080/raw_trace",
        headers={
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Accept': '*/*'
        },
        json={
            "start_timestamp_unix_ms": start_time,
            "end_timestamp_unix_ms": end_time,
            "filter": {
                "nodes": [],
                "threads": []
            }
        },
        stream=True)

    if response.status_code == 200:
        content_encoding = response.headers.get('content-encoding', 'none')
        logger.info(f"Response encoding: {content_encoding}")

        block_size = 1024
        total_bytes = 0

        with open(trace_file, 'wb') as f, tqdm(
                desc="Downloading trace",
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
                bar_format='{desc}: {n_fmt} [{elapsed}, {rate_fmt}]') as bar:
            for data in response.iter_content(block_size):
                size = f.write(data)
                total_bytes += size
                bar.update(size)

        logger.info(f"Downloaded size: {total_bytes/1024/1024:.1f}MB")
        logger.info(f"=> Trace saved to {trace_file}")
    else:
        logger.error(
            f"Failed to fetch traces: {response.status_code} {response.text}")


def handle_get_profiles(args):
    args = copy.deepcopy(args)

    # If no host filter is provided, target the first alphabetical cp instance.
    if args.host_filter is None:
        machines = sorted(args.forknet_details['cp_instance_names'])
        machine = machines[0]
        logger.info(f"Targeting {machine}")
        args.host_filter = machine

    run_cmd_args = copy.deepcopy(args)
    run_cmd_args.cmd = f"bash {BENCHNET_DIR}/helpers/get-profile.sh {args.record_secs}"
    run_remote_cmd(CommandContext(run_cmd_args))

    os.makedirs(args.output_dir, exist_ok=True)
    download_args = copy.deepcopy(args)
    download_args.src = f"{REMOTE_HOME}/perf*.script.gz"
    download_args.dst = args.output_dir
    run_remote_download_file(CommandContext(download_args))


def handle_start(args):
    """Handle the start command - start the benchmark."""
    start_nodes(args, args.enable_tx_generator)


def main():
    try:
        unique_id = os.environ['FORKNET_NAME']
        case = os.environ['CASE']
    except KeyError as e:
        logger.error(f"Error: Required environment variable {e} is not set")
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
        chain_id=CHAIN_ID,
        start_height=START_HEIGHT,
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
        'tweak-config',
        help='Reupload and apply the patches to genesis, config and log_config',
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

    get_traces_parser = subparsers.add_parser(
        'get-traces', help='Fetch traces from the tracing server')
    get_traces_parser.add_argument(
        '--output-dir',
        default='.',
        help='Directory to save the trace files (default: current directory)')
    get_traces_parser.add_argument(
        '--time',
        help=
        'End time in UTC format (YYYY-MM-DD HH:MM:SS). Default: current time')
    get_traces_parser.add_argument(
        '--lag',
        type=int,
        default=10,
        help='Number of seconds to look back from the end time (default: 10)')
    get_traces_parser.add_argument(
        '--len',
        type=int,
        default=10,
        help='Length of the trace window in seconds (default: 10)')

    get_profiles_parser = subparsers.add_parser(
        'get-profiles', help='Fetch profiles from the benchmark nodes')
    get_profiles_parser.add_argument(
        '--output-dir',
        default='.',
        help='Directory to save the profile files (default: current directory)')
    get_profiles_parser.add_argument(
        '--host-filter',
        default=None,
        help=
        'Filter to select specific hosts (default: first alphabetical cp instance)'
    )
    get_profiles_parser.add_argument(
        '--record-secs',
        type=int,
        default=10,
        help='Number of seconds to record the profile (default: 10)')

    args = parser.parse_args()

    # Route to appropriate handler based on command
    if args.command == 'init':
        handle_init(args)
    elif args.command == 'tweak-config':
        handle_tweak_config(args)
    elif args.command == 'stop':
        handle_stop(args)
    elif args.command == 'start':
        handle_start(args)
    elif args.command == 'reset':
        handle_reset(args)
    elif args.command == 'get-traces':
        handle_get_traces(args)
    elif args.command == 'get-profiles':
        handle_get_profiles(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
