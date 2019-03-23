import argparse
import json
import sys

from near.pynear.key_store import InMemoryKeyStore, FileKeyStore
from near.pynear.lib import NearLib


class MultiCommandParser(object):
    def __init__(self):
        parser = argparse.ArgumentParser(
            usage="""pynear <command> [<args>]

Commands:
call_view_function        {}
deploy                    {}
send_money                {}
schedule_function_call    {}
view_account              {}
view_state                {}
stake                     {}
create_account            {}
swap_key                  {}
view_latest_beacon_block  {}
get_beacon_block_by_hash  {}
view_latest_shard_block   {}
get_beacon_block_by_hash  {}
            """.format(
                self.call_view_function.__doc__,
                self.deploy.__doc__,
                self.send_money.__doc__,
                self.schedule_function_call.__doc__,
                self.view_account.__doc__,
                self.view_state.__doc__,
                self.stake.__doc__,
                self.create_account.__doc__,
                self.swap_key.__doc__,
                self.view_latest_beacon_block.__doc__,
                self.get_beacon_block_by_hash.__doc__,
                self.view_latest_shard_block.__doc__,
                self.get_shard_block_by_hash.__doc__,
            )
        )
        parser.add_argument('command', help='Command to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print("Unrecognized command: {}\n".format(args.command))
            parser.print_help()
            exit(1)
        else:
            response = getattr(self, args.command)()
            print(json.dumps(response))

    @staticmethod
    def _get_command_parser(description):
        parser = argparse.ArgumentParser(
            description=description,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            '-u',
            '--server-url',
            type=str,
            default='http://127.0.0.1:3030/',
            help='url of RPC server',
        )
        parser.add_argument(
            '--debug',
            action="store_true",
            default=False,
            help='set to emit debug logs',
        )
        return parser

    @staticmethod
    def _add_transaction_args(parser):
        parser.add_argument(
            '-o',
            '--originator',
            type=str,
            default='alice.near',
            help='account_id of originator',
        )
        parser.add_argument(
            '-d',
            '--keystore-path',
            type=str,
            help='location of keystore for signing transactions',
        )
        parser.add_argument(
            '-k',
            '--public-key',
            type=str,
            help='public key for signing transactions',
        )

    @staticmethod
    def _get_command_args(parser):
        # now that we're inside a sub-command, ignore the first
        # two args, ie the command (pynear) and the sub-command (send_money)
        return parser.parse_args(sys.argv[2:])

    @staticmethod
    def _get_rpc_client(command_args):
        keystore_path = getattr(command_args, 'keystore_path', None)
        if keystore_path is not None:
            keystore = FileKeyStore(keystore_path)
        else:
            keystore = InMemoryKeyStore()
            keystore.create_key_pair(seed='alice.near')

        public_key = getattr(command_args, 'public_key', None)
        return NearLib(
            command_args.server_url,
            keystore,
            public_key,
            command_args.debug,
        )

    def send_money(self):
        """Send money from one account to another"""
        parser = self._get_command_parser(self.send_money.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument(
            '-r',
            '--receiver',
            type=str,
            default='bob.near',
            help='account_id of receiver',
        )
        parser.add_argument(
            '-a',
            '--amount',
            type=int,
            default=0,
            help='amount of money being sent',
        )
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.send_money(args.originator, args.receiver, args.amount)

    def deploy(self):
        """Deploy a smart contract"""
        parser = self._get_command_parser(self.deploy.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument('contract_name', type=str)
        parser.add_argument('wasm_file_location', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.deploy_contract(
            args.contract_name,
            args.wasm_file_location,
        )

    def create_account(self):
        """Create an account"""
        parser = self._get_command_parser(self.create_account.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument('account_id', type=str)
        parser.add_argument('amount', type=int)
        parser.add_argument('--account_public-key', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.create_account(
            args.originator,
            args.account_id,
            args.amount,
            args.account_public_key,
        )

    def swap_key(self):
        """Swap key for an account"""
        parser = self._get_command_parser(self.swap_key.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument('current_key', type=str)
        parser.add_argument('new_key', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.swap_key(
            args.originator,
            args.current_key,
            args.new_key,
        )

    def schedule_function_call(self):
        """Schedule a function call on a smart contract"""
        parser = self._get_command_parser(self.schedule_function_call.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument('contract_name', type=str)
        parser.add_argument('function_name', type=str)
        parser.add_argument('--args', type=str, default="{}")
        parser.add_argument(
            '-a',
            '--amount',
            type=int,
            default=0,
            help='amount of money being sent with the function call',
        )
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.schedule_function_call(
            args.originator,
            args.contract_name,
            args.function_name,
            args.amount,
            args.args,
        )

    def call_view_function(self):
        """Call a view function on a smart contract"""
        parser = self._get_command_parser(self.call_view_function.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument('contract_name', type=str)
        parser.add_argument('function_name', type=str)
        parser.add_argument('--args', type=str, default="{}")
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.call_view_function(
            args.contract_name,
            args.function_name,
            args.args,
        )

    def view_account(self):
        """View an account"""
        parser = self._get_command_parser(self.view_account.__doc__)
        parser.add_argument(
            '-a',
            '--account',
            type=str,
            default='alice.near',
            help='id of account to view',
        )
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.view_account(args.account)

    def stake(self):
        """Stake money for validation"""
        parser = self._get_command_parser(self.stake.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument(
            '-a',
            '--amount',
            type=int,
            default=0,
            help='amount of money to stake',
        )
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.stake(args.originator, args.amount)

    def view_state(self):
        """View state of the contract."""
        parser = self._get_command_parser(self.view_state.__doc__)
        parser.add_argument('contract_name', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.view_state(args.contract_name)

    def view_latest_beacon_block(self):
        """View latest beacon block."""
        parser = self._get_command_parser(self.view_state.__doc__)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.view_latest_beacon_block()

    def get_beacon_block_by_hash(self):
        """Get beacon block by hash."""
        parser = self._get_command_parser(self.view_state.__doc__)
        parser.add_argument('hash', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.get_beacon_block_by_hash(args.hash)

    def view_latest_shard_block(self):
        """View latest shard block."""
        parser = self._get_command_parser(self.view_state.__doc__)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.view_latest_shard_block()

    def get_shard_block_by_hash(self):
        """Get shard block by hash."""
        parser = self._get_command_parser(self.view_state.__doc__)
        parser.add_argument('hash', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.get_shard_block_by_hash(args.hash)

    def generate_key_pair(self):
        """Write key file for seed."""
        parser = self._get_command_parser(self.generate_key_pair.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument('seed', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.generate_key_pair(args.seed)


def run():
    MultiCommandParser()


if __name__ == "__main__":
    run()
