import typing
from common.ft import FTContract, InitFTAccount
from common.base import Account, Deploy, NearNodeProxy, NearUser, FunctionCall, MultiFunctionCall, INIT_DONE
import locust
import sys
import pathlib
from locust import events
from collections import namedtuple

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

import key

RecipientSteps = namedtuple('RecipientSteps', ['account', 'steps'])


class SweatContract(FTContract):

    def __init__(self, main_account: Account, claim_account: Account,
                 oracle_account: Account, code: str):
        super().__init__(main_account, oracle_account, code)
        self.claim = claim_account
        self.oracle = oracle_account

    def init_contract(self, node: NearNodeProxy):
        node.send_tx_retry(InitSweat(self.account), "init sweat")
        # Unlike FT initialization that starts with a total supply and assigns
        # it to the user, the sweat main account doesn't start with tokens. We
        # need to register an account and then mint the tokens, so that it can
        # distribute them later.
        node.send_tx_retry(InitFTAccount(self.account, self.ft_distributor),
                           locust_name="Init Sweat Account")
        self.register_oracle(node, self.oracle.key.account_id)
        self.top_up(node, self.ft_distributor.key.account_id)

    def top_up(self, node: NearNodeProxy, receiver_id: str):
        """
        Adds a large amount of tokens to an account.
        Note: This should only be called on the master runner, as it requires
        the private key of the sweat contract account. (Multiple runners using
        this leads to frequent nonce invalidations.)
        """
        node.send_tx_retry(
            SweatMint(self.account, receiver_id, 1_000_000_000_000),
            "top up sweat")

    def register_oracle(self, node: NearNodeProxy, oracle_id: str):
        """
        Register an account as oracle to give it the power to mint tokens for steps.
        """
        node.send_tx_retry(SweatAddOracle(self.account, oracle_id),
                           "add sweat oracle")


class InitSweat(FunctionCall):

    def __init__(self, sweat_account: Account):
        super().__init__(sweat_account, sweat_account.key.account_id, "new")

    def args(self) -> dict:
        # Technical details about Sweat contract initialization:
        #
        # A postfix is used by the smart contract to decide whether or not to
        # hash an account id. It's an optimization that makes storage keys for
        # implicit accounts shorter, while preserving short keys of named
        # accounts.
        #
        # More specifically, any account id that matches .*<postfix> will be
        # stored in the trie normally, like in a unmodified FT contract. Any
        # other account ids are hashed.
        #
        # As an example, the postfix can be `.u.sweat.testnet`.
        #
        # Source code for reference:
        # https://github.com/sweatco/near-sdk-rs/blob/af6ba3cb75e0bbfc26e346e61aa3a0d1d7f5ac7b/near-contract-standards/src/fungible_token/core_impl.rs#L249-L259
        #
        # Here we don't provide a postfix, so everything will be hashed. This is
        # fine for new contracts we create. And when we reuse a contract, we
        # won't need to initialise it at all.
        return {"postfix": None}


class InitClaim(FunctionCall):

    def __init__(self, claim_account: Account, token_account_id: str):
        super().__init__(claim_account, claim_account.key.account_id, "init")
        self.token_account_id = token_account_id

    def args(self) -> dict:
        return {"token_account_id": self.token_account_id}


class SweatAddOracle(FunctionCall):
    """
    Oracle accounts are allowed to mint new tokens and can only be added by the
    account id of the contract itself.
    """

    def __init__(self, sweat_account: Account, oracle_id: str):
        super().__init__(sweat_account, sweat_account.key.account_id,
                         "add_oracle")
        self.oracle_id = oracle_id

    def args(self) -> dict:
        return {"account_id": self.oracle_id}


class SweatMint(FunctionCall):
    """
    A call to `sweat::tge_mint`.
    Token Generation Event (TGE) was day 0 when SWEAT launched.
    This is the transaction to get initial balance into accounts.
    """

    def __init__(self, sweat: Account, user_id: str, amount: int):
        super().__init__(sweat, sweat.key.account_id, "tge_mint")
        self.user_id = user_id
        self.amount = amount

    def args(self) -> dict:
        return {
            "account_id": self.user_id,
            "amount": f"{self.amount}",
        }


class SweatMintBatch(MultiFunctionCall):
    """
    A call to `sweat::record_batch`.
    Mints new tokens for walked steps for a batch of users.
    Might get split into multiple function calls to avoid log output limits.
    """

    def __init__(self, sweat_id: str, oracle: Account,
                 recipient_step_pairs: typing.List[RecipientSteps]):
        super().__init__(oracle, sweat_id, "record_batch")
        self.recipient_step_pairs = recipient_step_pairs

    def args(self) -> typing.List[dict]:
        # above a threshold, we hit the log output limit of 16kB
        # this depends a bit on the exact account id names
        chunk_len = 150
        chunks = [
            self.recipient_step_pairs[s:s + chunk_len]
            for s in range(0, len(self.recipient_step_pairs), chunk_len)
        ]

        return [{"steps_batch": chunk} for chunk in chunks]


class SweatDeferBatch(FunctionCall):
    """
    A call to `sweat::defer_batch`.
    """

    def __init__(self, sweat_id: str, oracle: Account, holding_account_id: str,
                 steps_batch: typing.List[RecipientSteps]):
        super().__init__(oracle, sweat_id, "defer_batch")
        self.holding_account_id = holding_account_id
        self.steps_batch = steps_batch

    def args(self) -> dict:
        return {
            "holding_account_id": self.holding_account_id,
            "steps_batch": self.steps_batch,
        }


class SweatGetClaimableBalanceForAccount(FunctionCall):
    """
    A call to `sweat.claim::get_claimable_balance_for_account`.

    We use it instead of `sweat.claim::claim` as it does not require to wait for funds to become
    claimable and performs similar amount of computation.
    """

    def __init__(self, sweat_claim_id: str, user: Account, account_id: str):
        super().__init__(user, sweat_claim_id,
                         "get_claimable_balance_for_account")
        self.account_id = account_id

    def args(self) -> dict:
        return {
            "account_id": self.account_id,
        }


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    INIT_DONE.wait()
    node = NearNodeProxy(environment)
    worker_id = getattr(environment.runner, "worker_index", "_master")
    run_id = environment.parsed_options.run_id

    funding_account = NearUser.funding_account
    funding_account.refresh_nonce(node.node)
    sweat_account_id = f"sweat{run_id}.{environment.master_funding_account.key.account_id}"
    sweat_claim_account_id = f"sweat-claim{run_id}.{environment.master_funding_account.key.account_id}"
    oracle_account_id = worker_oracle_id(worker_id, run_id,
                                         environment.master_funding_account)

    sweat_account = Account(key.Key.from_seed_testonly(sweat_account_id))
    sweat_claim_account = Account(
        key.Key.from_seed_testonly(sweat_claim_account_id))
    oracle_account = Account(key.Key.from_seed_testonly(oracle_account_id))

    environment.sweat = SweatContract(sweat_account, sweat_claim_account,
                                      oracle_account,
                                      environment.parsed_options.sweat_wasm)

    # Create Sweat contract, unless we are a worker, in which case the master already did it
    if not isinstance(environment.runner, locust.runners.WorkerRunner):
        node.prepare_account(oracle_account, environment.master_funding_account,
                             FTContract.INIT_BALANCE, "create oracle account")
        environment.sweat.install(node, environment.master_funding_account)

        existed = node.prepare_account(sweat_claim_account, funding_account,
                                       100000, "create contract account")
        if not existed:
            node.send_tx_retry(
                Deploy(sweat_claim_account,
                       environment.parsed_options.sweat_claim_wasm,
                       "Sweat-Claim"), "deploy Sweat-Claim contract")
            node.send_tx_retry(InitClaim(sweat_claim_account, sweat_account_id),
                               "init Sweat-Claim contract")
            # `sweat` account also must be registered as oracle to call methods on `sweat.claim`.
            node.send_tx_retry(
                SweatAddOracle(sweat_claim_account, sweat_account_id),
                "add sweat.claim oracle")

    # on master, register oracles for workers
    if isinstance(environment.runner, locust.runners.MasterRunner):
        num_oracles = int(environment.parsed_options.max_workers)
        oracle_accounts = [
            Account(
                key.Key.from_seed_testonly(
                    worker_oracle_id(id, run_id,
                                     environment.master_funding_account)))
            for id in range(num_oracles)
        ]
        node.prepare_accounts(oracle_accounts,
                              environment.master_funding_account, 100000,
                              "create contract account")
        for oracle in oracle_accounts:
            id = oracle.key.account_id
            environment.sweat.top_up(node, id)
            environment.sweat.register_oracle(node, id)
            node.send_tx_retry(SweatAddOracle(sweat_claim_account, id),
                               "add sweat.claim oracle")


def worker_oracle_id(worker_id, run_id, funding_account):
    return f"sweat{run_id}_oracle{worker_id}.{funding_account.key.account_id}"


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--sweat-wasm",
                        default="res/sweat.wasm",
                        help="Path to the compiled Sweat contract")
    parser.add_argument("--sweat-claim-wasm",
                        default="res/sweat_claim.wasm",
                        help="Path to the compiled Sweat-Claim contract")
