#!/usr/bin/env python3
"""
Library for controlling mocknet instances.
"""
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict, Any
import sys
import pathlib
from argparse import Namespace

# Add parent directory to path to import from lib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from mirror import (
    CommandContext, NodeHandle, to_list, pmap,
    init_neard_runners, new_test_cmd, status_cmd,
    start_nodes_cmd, start_traffic_cmd, stop_nodes_cmd,
    stop_traffic_cmd, update_config_cmd, make_backup_cmd,
    reset_cmd, update_binaries_cmd, amend_binaries_cmd,
    run_remote_cmd, run_remote_upload_file, run_env_cmd
)

@dataclass
class NetworkParams:
    """Network parameters for mocknet control"""
    chain_id: Optional[str] = None
    start_height: Optional[int] = None
    unique_id: Optional[str] = None
    mocknet_id: Optional[str] = None
    local_test: bool = False

class MocknetControl:
    """Main class for controlling mocknet instances"""
    
    def __init__(self):
        self._ctx: Optional[CommandContext] = None
        self._network_params: Optional[NetworkParams] = None

    def set_network_params(self, params: NetworkParams) -> None:
        """Set the network parameters for the mocknet instance.
        
        Args:
            params: NetworkParams object containing the network configuration
        """
        self._network_params = params
        args_dict = {
            'chain_id': params.chain_id,
            'start_height': params.start_height,
            'unique_id': params.unique_id,
            'mocknet_id': params.mocknet_id,
            'local_test': params.local_test,
            'host_type': 'all',
            'host_filter': None,
            'select_partition': None
        }
        args = Namespace(**args_dict)
        self._ctx = CommandContext(args)

    def _ensure_ctx(self) -> None:
        """Ensure that network parameters are set before executing commands"""
        if self._ctx is None:
            raise RuntimeError("Network parameters not set. Call set_network_params() first.")

    def init_neard_runner(self, neard_binary_url: str, neard_upgrade_binary_url: Optional[str] = None) -> None:
        """Initialize neard runners on all nodes.
        
        Args:
            neard_binary_url: URL to the neard binary
            neard_upgrade_binary_url: Optional URL to the upgrade binary
        """
        self._ensure_ctx()
        args = self._ctx.args
        args.neard_binary_url = neard_binary_url
        args.neard_upgrade_binary_url = neard_upgrade_binary_url
        init_neard_runners(self._ctx, remove_home_dir=False)

    def new_test(self, 
                 state_source: str = 'dump',
                 patches_path: Optional[str] = None,
                 epoch_length: Optional[int] = None,
                 num_validators: Optional[int] = None,
                 num_seats: Optional[int] = None,
                 new_chain_id: Optional[str] = None,
                 genesis_protocol_version: Optional[int] = None,
                 stateless_setup: bool = False,
                 gcs_state_sync: bool = False) -> None:
        """Set up new test state.
        
        Args:
            state_source: Source of the state ('dump' or other)
            patches_path: Path to patches
            epoch_length: Length of epochs
            num_validators: Number of validators
            num_seats: Number of seats
            new_chain_id: New chain ID
            genesis_protocol_version: Genesis protocol version
            stateless_setup: Whether to use stateless setup
            gcs_state_sync: Whether to enable GCS state sync
        """
        self._ensure_ctx()
        args = self._ctx.args
        args.state_source = state_source
        args.patches_path = patches_path
        args.epoch_length = epoch_length
        args.num_validators = num_validators
        args.num_seats = num_seats
        args.new_chain_id = new_chain_id
        args.genesis_protocol_version = genesis_protocol_version
        args.stateless_setup = stateless_setup
        args.gcs_state_sync = gcs_state_sync
        args.yes = True
        new_test_cmd(self._ctx)

    def status(self) -> List[str]:
        """Get status of all nodes.
        
        Returns:
            List of names of nodes that are not ready
        """
        self._ensure_ctx()
        return self._ctx.get_nodes_status(self._ctx.get_targeted())

    def start_nodes(self) -> None:
        """Start all nodes"""
        self._ensure_ctx()
        start_nodes_cmd(self._ctx)

    def start_traffic(self, batch_interval_millis: Optional[int] = None) -> None:
        """Start traffic generation.
        
        Args:
            batch_interval_millis: Interval between batches in milliseconds
        """
        self._ensure_ctx()
        args = self._ctx.args
        args.batch_interval_millis = batch_interval_millis
        start_traffic_cmd(self._ctx)

    def stop_nodes(self) -> None:
        """Stop all nodes"""
        self._ensure_ctx()
        stop_nodes_cmd(self._ctx)

    def stop_traffic(self) -> None:
        """Stop traffic generation"""
        self._ensure_ctx()
        stop_traffic_cmd(self._ctx)

    def update_config(self, config_change: str) -> None:
        """Update node configuration.
        
        Args:
            config_change: Configuration change in the format 'key=value'
        """
        self._ensure_ctx()
        args = self._ctx.args
        args.set = config_change
        update_config_cmd(self._ctx)

    def make_backup(self, backup_id: str, description: Optional[str] = None) -> None:
        """Create a backup of the current state.
        
        Args:
            backup_id: ID for the backup
            description: Optional description of the backup
        """
        self._ensure_ctx()
        args = self._ctx.args
        args.backup_id = backup_id
        args.description = description
        args.yes = True
        make_backup_cmd(self._ctx)

    def reset(self, backup_id: Optional[str] = None) -> None:
        """Reset nodes to a previous backup.
        
        Args:
            backup_id: Optional backup ID to reset to
        """
        self._ensure_ctx()
        args = self._ctx.args
        args.backup_id = backup_id
        args.yes = True
        reset_cmd(self._ctx)

    def update_binaries(self) -> None:
        """Update neard binaries"""
        self._ensure_ctx()
        update_binaries_cmd(self._ctx)

    def amend_binaries(self, 
                      neard_binary_url: str,
                      epoch_height: Optional[int] = None,
                      binary_idx: Optional[int] = None) -> None:
        """Amend neard binaries.
        
        Args:
            neard_binary_url: URL to the neard binary
            epoch_height: Optional epoch height for the binary
            binary_idx: Optional binary index to replace
        """
        self._ensure_ctx()
        args = self._ctx.args
        args.neard_binary_url = neard_binary_url
        args.epoch_height = epoch_height
        args.binary_idx = binary_idx
        amend_binaries_cmd(self._ctx)

    def run_remote_cmd(self, cmd: str) -> None:
        """Run a command on all nodes.
        
        Args:
            cmd: Command to run
        """
        self._ensure_ctx()
        args = self._ctx.args
        args.cmd = cmd
        run_remote_cmd(self._ctx)

    def upload_file(self, src: str, dst: str) -> None:
        """Upload a file to all nodes.
        
        Args:
            src: Source file path
            dst: Destination file path
        """
        self._ensure_ctx()
        args = self._ctx.args
        args.src = src
        args.dst = dst
        run_remote_upload_file(self._ctx)

    def update_env(self, key_value: Optional[List[str]] = None, clear_all: bool = False) -> None:
        """Update environment variables on nodes.
        
        Args:
            key_value: List of key-value pairs to set
            clear_all: Whether to clear all environment variables
        """
        self._ensure_ctx()
        args = self._ctx.args
        args.key_value = key_value
        args.clear_all = clear_all
        run_env_cmd(self._ctx) 