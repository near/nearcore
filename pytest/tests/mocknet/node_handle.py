import pathlib
import requests
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger


class NodeHandle:

    def __init__(self, node, can_validate=True, want_state_dump=False):
        self.node = node
        self.can_validate = can_validate
        self.want_state_dump = want_state_dump
        self.want_neard_runner = True

    def name(self):
        return self.node.name()

    def ip_addr(self):
        return self.node.ip_addr()

    def neard_port(self):
        return self.node.neard_port()

    def stop_neard_runner(self):
        self.node.stop_neard_runner()

    def start_neard_runner(self):
        self.node.start_neard_runner()

    def upload_neard_runner(self):
        self.node.upload_neard_runner()
        self.node.update_python()

    def run_cmd(self, cmd, raise_on_fail=False, return_on_fail=False):
        return self.node.run_cmd(cmd, raise_on_fail, return_on_fail)

    def init_neard_runner(self, config, remove_home_dir=False):
        self.node.stop_neard_runner()
        self.node.init()
        self.node.mk_neard_runner_home(remove_home_dir)
        self.node.upload_neard_runner()
        # TODO: this config file should just be replaced by parameters to the new-test
        # rpc method. This was originally made a config file instead because the rpc port
        # was open to the internet, but now that we call it via ssh instead (which we should
        # have done from the beginning), it's not really necessary and just an arbitrary difference
        self.node.upload_neard_runner_config(config)
        self.node.init_python()
        self.node.start_neard_runner()

    # TODO: is the validators RPC the best way to do this? What are we trying to
    # test for exactly? The use of this is basically just cargo culted from a while ago,
    # but maybe we should consider something else
    def wait_node_up(self):
        while True:
            try:
                res = self.node.get_validators()
                if 'error' not in res:
                    assert 'result' in res
                    logger.info(f'Node {self.node.name()} is up')
                    return
            except (ConnectionRefusedError,
                    requests.exceptions.ConnectionError) as e:
                pass
            time.sleep(10)

    # Same as neard_runner_jsonrpc() without checking the error
    # This should maybe be the behavior everywhere, and callers
    # should handle errors themselves
    def neard_runner_jsonrpc_nocheck(self, method, params=[]):
        body = {
            'method': method,
            'params': params,
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        return self.node.neard_runner_post(body)

    def neard_runner_jsonrpc(self, method, params=[]):
        response = self.neard_runner_jsonrpc_nocheck(method, params)
        if 'error' in response:
            # TODO: errors should be handled better here in general but just exit for now
            sys.exit(
                f'bad response trying to send {method} JSON RPC to neard runner on {self.node.name()}:\n{response}'
            )
        return response['result']

    def neard_runner_start(self, batch_interval_millis=None):
        if batch_interval_millis is None:
            params = []
        else:
            params = {'batch_interval_millis': batch_interval_millis}
        return self.neard_runner_jsonrpc('start', params=params)

    def neard_runner_stop(self):
        return self.neard_runner_jsonrpc('stop')

    def neard_runner_new_test(self):
        params = self.node.new_test_params()
        return self.neard_runner_jsonrpc('new_test', params)

    def neard_runner_network_init(self,
                                  validators,
                                  boot_nodes,
                                  epoch_length,
                                  num_seats,
                                  protocol_version,
                                  genesis_time=None):
        params = {
            'validators': validators,
            'boot_nodes': boot_nodes,
            'epoch_length': epoch_length,
            'num_seats': num_seats,
            'protocol_version': protocol_version,
        }
        if genesis_time is not None:
            params['genesis_time'] = genesis_time
        return self.neard_runner_jsonrpc('network_init', params=params)

    def neard_runner_ready(self):
        return self.neard_runner_jsonrpc('ready')

    def neard_runner_version(self):
        return self.neard_runner_jsonrpc_nocheck('version')

    def neard_runner_make_backup(self, backup_id, description=None):
        return self.neard_runner_jsonrpc('make_backup',
                                         params={
                                             'backup_id': backup_id,
                                             'description': description
                                         })

    def neard_runner_ls_backups(self):
        return self.neard_runner_jsonrpc('ls_backups')

    def neard_runner_reset(self, backup_id=None):
        return self.neard_runner_jsonrpc('reset',
                                         params={'backup_id': backup_id})

    def neard_runner_update_binaries(self,
                                     neard_binary_url=None,
                                     epoch_height=None,
                                     binary_idx=None):
        return self.neard_runner_jsonrpc(
            'update_binaries',
            params={
                'neard_binary_url': neard_binary_url,
                'epoch_height': epoch_height,
                'binary_idx': binary_idx,
            })

    def neard_update_config(self, key_value):
        return self.neard_runner_jsonrpc(
            'update_config',
            params={
                "key_value": key_value,
            },
        )

    def neard_update_env(self, key_value):
        return self.neard_runner_jsonrpc(
            'add_env',
            params={
                "key_values": key_value,
            },
        )

    def neard_clear_env(self):
        return self.neard_runner_jsonrpc('clear_env')
