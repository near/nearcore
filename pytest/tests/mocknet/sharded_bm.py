"""
This script is used to run a sharded benchmark on a forknet.
"""

from argparse import ArgumentParser
from collections import defaultdict
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
from rc import pmap

from types import SimpleNamespace
from mirror import CommandContext, get_nodes_status, init_cmd, new_test_cmd, \
    reset_cmd, run_env_cmd, run_remote_cmd, run_remote_download_file, run_remote_upload_file, \
    start_nodes_cmd, stop_nodes_cmd, update_binaries_cmd

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from configured_logger import logger

# cspell:words BENCHNET setcap
CHAIN_ID = "mainnet"

# This height should be used for forknet cluster creation as well.
# It corresponds to the existing setup with minimal disk usage.
START_HEIGHT = 138038232

PROJECT = os.getenv('MOCKNET_PROJECT', 'nearone-mocknet')

# TODO: consider moving source directory to pytest.
SOURCE_BENCHNET_DIR = "../benchmarks/sharded-bm"

REMOTE_HOME = "/home/ubuntu"
BENCHNET_DIR = f"{REMOTE_HOME}/bench"
NEAR_HOME = f"{REMOTE_HOME}/.near"
CONFIG_PATH = f"{NEAR_HOME}/config.json"

FUNGLIBLE_TOKEN_WASM_LOCAL_PATH = f"{BENCHNET_DIR}/contracts/fungible_token.wasm"

def fetch_forknet_details(forknet_name, bm_params):
    """Fetch the forknet details from GCP."""
    find_instances_cmd = [
        "gcloud", "compute", "instances", "list", f"--project={PROJECT}",
        f"--filter=name~'-{forknet_name}-' AND -name~'traffic' AND -name~'tracing' AND -name~'prometheus'",
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
    if len(nodes_data) != num_cp_instances:
        logger.error(
            f"Expected {num_cp_instances} instances, got {len(nodes_data)}")
        sys.exit(1)

    cp_instances = list(map(lambda x: x.split(), nodes_data[:num_cp_instances]))
    cp_instance_names = [instance[0] for instance in cp_instances]

    find_tracing_server_cmd = [
        "gcloud", "compute", "instances", "list", f"--project={PROJECT}",
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
        "cp_instance_names": cp_instance_names,
        "tracing_server_internal_ip": internal_ip,
        "tracing_server_external_ip": external_ip
    }

def upload_ft_contract(args):
    """Upload the fungible_token contract from the google bucket
    https://storage.cloud.google.com/infraops-benchmark-data/contracts/fungible_token.wasm
    to the benchmark directory on each node
    @return the absolute path (local to the remote node) to the uploaded contract
    """
    FUNGIBLE_TOKEN_GS_PATH = "gs://infraops-benchmark-data/contracts/fungible_token.wasm"

    logger.info("uploading the contracts from {FUNGIBLE_TOKEN_GS_PATH} to {FUNGLIBLE_TOKEN_WASM_LOCAL_PATH}...")

    run_cmd_args = copy.deepcopy(args)
    run_cmd_args.cmd = f"gcloud storage cp {FUNGIBLE_TOKEN_GS_PATH} {FUNGLIBLE_TOKEN_WASM_LOCAL_PATH}"
    run_remote_cmd(CommandContext(run_cmd_args))


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
    is_local_neard = os.path.isfile(args.neard_binary_url)
    if is_local_neard:
        logger.info(f"handling local `neard` at {args.neard_binary_url}")
        local_path_on_remote = upload_local_neard(args)
        args.neard_binary_url = local_path_on_remote
        logger.info(f"`neard` local path on remote: {args.neard_binary_url}")
    else:
        logger.info("no local `neard` found, continue assuming the remote url")

    upload_ft_contract(args)

    init_args = SimpleNamespace(
        neard_upgrade_binary_url="",
        **vars(args),
    )
    init_cmd(CommandContext(init_args))

    update_binaries_args = copy.deepcopy(args)
    update_binaries_cmd(CommandContext(update_binaries_args))

    # TODO: check neard binary version

    # Grant CAP_SYS_NICE to neard binaries for realtime thread scheduling
    run_cmd_args = copy.deepcopy(args)
    if is_local_neard:
        run_cmd_args.cmd = f"sudo setcap cap_sys_nice+ep \"{args.neard_binary_url}\""
    else:
        run_cmd_args.cmd = "sudo setcap cap_sys_nice+ep ~/.near/neard-runner/binaries/neard*"
    run_remote_cmd(CommandContext(run_cmd_args))

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
        yes=True,
        **vars(args),
    )
    new_test_cmd(CommandContext(new_test_cmd_args))

    # Must be run after new_test to override some OS settings
    apply_network_config(args)

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

    # Force save_untracked_partial_chunks_parts=true for the initialization run.
    # This ensures that chunks produced during init are persisted to disk and
    # survive the node restart that happens when the benchmark actually starts.
    run_cmd_args = copy.deepcopy(args)
    run_cmd_args.host_filter = f"({'|'.join(args.forknet_details['cp_instance_names'])})"
    run_cmd_args.cmd = f"jq '.save_untracked_partial_chunks_parts = true' {CONFIG_PATH} > tmp.json && mv tmp.json {CONFIG_PATH}"
    run_remote_cmd(CommandContext(run_cmd_args))

    start_nodes(args)

    time.sleep(10)

    # Each CP gets its own account file with unique access keys
    cp_names = sorted(args.forknet_details['cp_instance_names'])

    cp_nodes = []
    for cp_name in cp_names:
        run_cmd_args = copy.deepcopy(args)
        run_cmd_args.host_filter = cp_name
        ctx = CommandContext(run_cmd_args)
        cp_nodes.append(ctx.get_targeted()[0])

    # Query all CPs in parallel to get their tracked shards
    query_cmd = f"python3 {BENCHNET_DIR}/helpers/get_tracked_shard.py"
    results = pmap(
        lambda node: (node, node.run_cmd(query_cmd, return_on_fail=True)),
        cp_nodes)

    # Build mapping: shard_id -> list of CP names tracking that shard
    shard_to_cps = defaultdict(list)
    for node, result in results:
        shard = result.stdout.strip()
        if not shard.isdigit():
            logger.error(
                f"Failed to get shard for {node.name()}: {result.stdout}")
            sys.exit(1)
        shard_to_cps[shard].append(node.name())

    # For each shard, copy account files to all CPs tracking that shard
    logger.info(
        f"Distributing account files across {len(shard_to_cps)} shards...")
    for shard, cps_in_shard in shard_to_cps.items():
        cp_names_csv = ','.join(sorted(cps_in_shard))

        logger.info(f"Shard {shard}: assigning {len(cps_in_shard)} CPs")

        run_cmd_args = copy.deepcopy(args)
        run_cmd_args.host_filter = f"({'|'.join(cps_in_shard)})"
        source_accounts_path = f"{BENCHNET_DIR}/user-data/accounts.json"
        source_accounts_dir = os.path.dirname(source_accounts_path)
        receiver_accounts_dir = f"{BENCHNET_DIR}/user-data/receiver-accounts/"
        run_cmd_args.cmd = f"""
            my_hostname=$(hostname)
            slot=$(python3 -c "print('{cp_names_csv}'.split(',').index('$my_hostname'))")
            rm -rf {BENCHNET_DIR}/user-data
            mkdir -p {source_accounts_dir}
            mkdir -p {receiver_accounts_dir}
            cp {NEAR_HOME}/user-data/shard_{shard}_cp_${{slot}}.json {source_accounts_path}
            cp {NEAR_HOME}/user-data/shard_*.json {receiver_accounts_dir}
            echo "CP $my_hostname assigned shard {shard}, slot $slot"
        """
        run_remote_cmd(CommandContext(run_cmd_args))

    stop_nodes(args)

    # Re-apply JSON patches to restore the original config
    # so the benchmark runs with the intended settings.
    apply_json_patches(args)


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


def enable_tx_generator(args, receivers_from_senders_ratio: float):
    logger.info("Setting tx generator parameters")

    # todo(slavas): these paths are implicitly assumed to correspond to similar from the handle_init() - terribly fragile.
    accounts_path = f"{BENCHNET_DIR}/user-data/accounts.json"
    receiver_accounts_dir = f"{BENCHNET_DIR}/user-data/receiver-accounts"
    tx_generator_settings = f"{BENCHNET_DIR}/{args.case}/tx-generator-settings.json"
    tx_generator_settings_tmp = f"{BENCHNET_DIR}/{args.case}/tx-generator-settings-tmp.json"

    run_cmd_args = copy.deepcopy(args)
    run_cmd_args.host_filter = f"({'|'.join(args.forknet_details['cp_instance_names'])})"
    run_cmd_args.cmd = f"""
            jq --arg accounts_path {accounts_path}                                                \
               --arg receiver_accounts_dir {receiver_accounts_dir}                                \
               --argjson same_shard_traffic {receivers_from_senders_ratio}                        \
               --arg ft_contract_path {FUNGLIBLE_TOKEN_WASM_LOCAL_PATH}                           \
               '.tx_generator.accounts_path = $accounts_path                                      \
                | .tx_generator.receiver_accounts_path = $receiver_accounts_dir                   \
                | .tx_generator.receivers_from_senders_ratio = $same_shard_traffic                \
                | if .tx_generator.transaction_type.FungibleToken? != null                        \
                  then .tx_generator.transaction_type.FungibleToken.wasm_path = $ft_contract_path \
                  else .                                                                          \
                  end'                                                                            \
               {tx_generator_settings} > {tx_generator_settings_tmp} &&                           
            jq -s '.[0] * .[1]' {CONFIG_PATH} {tx_generator_settings_tmp} > tmp.$$.json && mv tmp.$$.json {CONFIG_PATH}
        """

    run_remote_cmd(CommandContext(run_cmd_args))


def start_nodes(args):
    """Start the benchmark nodes with the given parameters."""
    logger.info("Starting nodes")
    start_nodes_cmd_args = copy.deepcopy(args)
    start_nodes_cmd_args.host_filter = f"({'|'.join(args.forknet_details['cp_instance_names'])})"
    start_nodes_cmd(CommandContext(start_nodes_cmd_args))


def apply_network_config(args):
    """Apply network configuration optimizations."""
    logger.info("Applying network configuration optimizations")

    run_cmd_args = copy.deepcopy(args)

    # This command does the following:
    # - Set overrides for some configs
    # - Update default interface to use larger initial congestion window
    run_cmd_args.cmd = "\
        sudo sysctl -w net.core.rmem_max=134217728 && \
        sudo sysctl -w net.core.wmem_max=134217728 && \
        sudo sysctl -w net.ipv4.tcp_rmem='10240 87380 134217728' && \
        sudo sysctl -w net.ipv4.tcp_wmem='4096 87380 134217728' && \
        sudo sysctl -w net.core.netdev_max_backlog=30000 && \
        sudo sysctl -w net.ipv4.tcp_no_metrics_save=1 && \
        gw=$(ip route show default | head -1 | awk '{{print $3}}') && \
        dev=$(ip route show default | head -1 | awk '{{print $5}}') && \
        sudo ip route flush table main exact 0.0.0.0/0 2>/dev/null || true && \
        sudo ip route add default via $gw dev $dev metric 100 initcwnd 255 initrwnd 255 \
    "

    run_remote_cmd(CommandContext(run_cmd_args))


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


def handle_get_profiles(args, extra):
    args = copy.deepcopy(args)

    # If no host filter is provided, target the first alphabetical cp instance.
    if args.host_filter is None:
        machines = sorted(args.forknet_details['cp_instance_names'])
        machine = machines[0]
        logger.info(f"Targeting {machine} for profile fetching")
        args.host_filter = machine

    extra_parameters = ' '.join(extra)
    args.cmd = f"bash {BENCHNET_DIR}/helpers/get-profile.sh {extra_parameters}"
    run_remote_cmd(CommandContext(args))

    os.makedirs(args.output_dir, exist_ok=True)
    args.src = f"{REMOTE_HOME}/perf*.gz"
    args.dst = args.output_dir
    run_remote_download_file(CommandContext(args))


def handle_get_logs(args):
    args = copy.deepcopy(args)

    # compress logs and prepare for download
    remote_log_dir = f'{REMOTE_HOME}/neard-logs'
    compressed_log_file = '/tmp/neard-logs.tar.gz'
    args.cmd = f'rm -f {compressed_log_file} && tar -czf {compressed_log_file} {remote_log_dir}'
    run_remote_cmd(CommandContext(args))

    os.makedirs(args.output_dir, exist_ok=True)
    args.src = compressed_log_file
    args.dst = args.output_dir
    run_remote_download_file(CommandContext(args), include_node_name=True)


def handle_start(args):
    """Handle the start command - start the benchmark."""
    if args.enable_tx_generator:
        enable_tx_generator(args, args.receivers_from_senders_ratio)
    start_nodes(args)


def main():
    parser = ArgumentParser(
        description='Forknet cluster parameters to launch a sharded benchmark')
    parser.add_argument(
        '--unique-id',
        help='Forknet unique ID (FORKNET_NAME)',
        default=os.environ.get('FORKNET_NAME'),
    )
    parser.add_argument(
        '--mocknet-id',
        help='Mocknet ID (MOCKNET_ID)',
        default=os.environ.get('MOCKNET_ID'),
    )
    parser.add_argument(
        '--case',
        help='Benchmark case name',
        default=os.environ.get('CASE'),
        required=os.environ.get('CASE') is None,
    )
    parser.add_argument("--start-height", default=START_HEIGHT)

    # Parse early to get unique_id, mocknet_id, and case
    if '--' in sys.argv:
        idx = sys.argv.index('--')
        my_args = sys.argv[1:idx]
        extra_args = sys.argv[idx + 1:]
    else:
        my_args = sys.argv[1:]
        extra_args = []
    early_args, _ = parser.parse_known_args(my_args)

    unique_id = early_args.unique_id
    mocknet_id = early_args.mocknet_id
    case = early_args.case

    if unique_id is None and mocknet_id is None:
        logger.error(
            f"Error: Either --unique-id or --mocknet-id must be provided")
        sys.exit(1)
    if case is None:
        logger.error(f"Error: --case must be provided")
        sys.exit(1)

    try:
        bm_params_path = f"{SOURCE_BENCHNET_DIR}/{case}/params.json"
        with open(bm_params_path) as f:
            bm_params = json.load(f)
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error reading binary_url from {bm_params_path}: {e}")
        sys.exit(1)

    forknet_details = fetch_forknet_details(unique_id or mocknet_id, bm_params)
    logger.info(forknet_details)

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
        mocknet_id=mocknet_id,
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
    start_parser.add_argument(
        '--receivers-from-senders-ratio',
        type=float,
        default=1.0,
        help=
        'Ratio of receiver accounts selected from the sender accounts (default: 1.0 (all receivers are selected from senders list))',
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

    get_logs_parser = subparsers.add_parser(
        'get-logs', help='Fetch logs from the benchmark nodes')
    get_logs_parser.add_argument(
        '--output-dir',
        default='.',
        help='Directory to save the log files (default: current directory)')
    get_logs_parser.add_argument('--host-filter',
                                 default=None,
                                 help='Filter to select specific hosts')

    args = parser.parse_args(my_args)

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
        handle_get_profiles(args, extra_args)
    elif args.command == 'get-logs':
        handle_get_logs(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
