import json
import random
import sys
import pathlib
from locust import events

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

import cluster
import key
import transaction

from account import TGAS
from common.base import Account, CreateSubAccount, Deploy, NearUser, is_tag_active, send_transaction, Transaction


class FTContract:
    # NEAR balance given to contracts, doesn't have to be much since users are
    # going to pay for storage
    INIT_BALANCE = NearUser.INIT_BALANCE

    def __init__(self, account: Account, code):
        self.account = account
        self.registered_users = []
        self.code = code

    def install(self, node):
        """
        Deploy and initialize the contract on chain.
        The account should already exist at this point.
        """
        self.account.refresh_nonce(node)
        send_transaction(node, Deploy(self.account, self.code, "FT"))
        send_transaction(node, InitFT(self.account))

    def register_user(self, user: NearUser):
        user.send_tx(InitFTAccount(self.account, user.account),
                     locust_name="Init FT Account")
        user.send_tx(TransferFT(self.account,
                                self.account,
                                user.account_id,
                                how_much=10**8),
                     locust_name="FT Funding")
        self.registered_users.append(user.account_id)

    def random_receiver(self, sender: str) -> str:
        rng = random.Random()
        receiver = rng.choice(self.registered_users)
        # Sender must be != receiver but maybe there is no other registered user
        # yet, so we just send to the contract account which is registered
        # implicitly from the start
        if receiver == sender:
            receiver = self.account.key.account_id
        return receiver


class TransferFT(Transaction):

    def __init__(self,
                 ft: Account,
                 sender: Account,
                 recipient_id: str,
                 how_much=1):
        super().__init__()
        self.ft = ft
        self.sender = sender
        self.recipient_id = recipient_id
        self.how_much = how_much

    def sign_and_serialize(self, block_hash) -> bytes:
        (ft, sender, recipient_id) = self.ft, self.sender, self.recipient_id
        args = {
            "receiver_id": recipient_id,
            "amount": str(int(self.how_much)),
        }
        return transaction.sign_function_call_tx(
            sender.key,
            ft.key.account_id,
            "ft_transfer",
            json.dumps(args).encode('utf-8'),
            300 * TGAS,
            # Attach exactly 1 yoctoNEAR according to NEP-141 to avoid calls from restricted access keys
            1,
            sender.use_nonce(),
            block_hash)


class InitFT(Transaction):

    def __init__(self, contract: Account):
        super().__init__()
        self.contract = contract

    def sign_and_serialize(self, block_hash) -> bytes:
        contract = self.contract
        args = json.dumps({
            "owner_id": contract.key.account_id,
            "total_supply": str(10**33)
        })
        return transaction.sign_function_call_tx(contract.key,
                                                 contract.key.account_id,
                                                 "new_default_meta",
                                                 args.encode('utf-8'),
                                                 int(3E14), 0,
                                                 contract.use_nonce(),
                                                 block_hash)


class InitFTAccount(Transaction):

    def __init__(self, contract: Account, account: Account):
        super().__init__()
        self.contract = contract
        self.account = account

    def sign_and_serialize(self, block_hash) -> bytes:
        contract, account = self.contract, self.account
        args = json.dumps({"account_id": account.key.account_id})
        return transaction.sign_function_call_tx(account.key,
                                                 contract.key.account_id,
                                                 "storage_deposit",
                                                 args.encode('utf-8'),
                                                 int(3E14), int(1E23),
                                                 account.use_nonce(),
                                                 block_hash)


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    if not is_tag_active(environment, "ft"):
        return

    if environment.parsed_options.fungible_token_wasm is None:
        raise SystemExit(
            f"Running FT workload requires `--fungible_token_wasm $FT_CONTRACT`. "
            "Either provide the WASM (e.g. nearcore/runtime/near-test-contracts/res/fungible_token.wasm) "
            "or run with `--exclude-tag ft`")

    # Note: These setup requests are not tracked by locust because we use our own http session
    host, port = environment.host.split(":")
    node = cluster.RpcNode(host, port)

    ft_contract_code = environment.parsed_options.fungible_token_wasm
    num_ft_contracts = environment.parsed_options.num_ft_contracts
    funding_account = NearUser.funding_account
    parent_id = funding_account.key.account_id
    worker_id = getattr(environment.runner, "worker_id", "local_id")

    funding_account.refresh_nonce(node)

    environment.ft_contracts = []
    # TODO: Create accounts in parallel
    for i in range(num_ft_contracts):
        # Prefix that makes accounts unique across workers
        # Shuffling with a hash avoids locality in the state trie.
        # TODO: Also make sure these are spread evenly across shards
        prefix = str(hash(str(worker_id) + str(i)))[-6:]
        contract_key = key.Key.from_random(f"{prefix}_ft.{parent_id}")
        ft_account = Account(contract_key)
        send_transaction(
            node,
            CreateSubAccount(funding_account,
                             ft_account.key,
                             balance=FTContract.INIT_BALANCE))

        ft_contract = FTContract(ft_account, ft_contract_code)
        ft_contract.install(node)
        environment.ft_contracts.append(ft_contract)


# FT specific CLI args
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--fungible-token-wasm",
                        type=str,
                        required=False,
                        help="Path to the compiled Fungible Token contract")
    parser.add_argument(
        "--num-ft-contracts",
        type=int,
        required=False,
        default=4,
        help=
        "How many different FT contracts to spawn from this worker (FT contracts are never shared between workers)"
    )
