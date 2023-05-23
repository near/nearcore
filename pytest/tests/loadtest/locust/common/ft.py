import json
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

import transaction

from account import TGAS
from common.base import Transaction


class TransferFT(Transaction):

    def __init__(self, ft, sender, recipient_id, how_much=1):
        super().__init__()
        self.ft = ft
        self.sender = sender
        self.recipient_id = recipient_id
        self.how_much = how_much

    def sign_and_serialize(self, block_hash):
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

    def __init__(self, contract):
        super().__init__()
        self.contract = contract

    def sign_and_serialize(self, block_hash):
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

    def __init__(self, contract, account):
        super().__init__()
        self.contract = contract
        self.account = account

    def sign_and_serialize(self, block_hash):
        contract, account = self.contract, self.account
        args = json.dumps({"account_id": account.key.account_id})
        return transaction.sign_function_call_tx(account.key,
                                                 contract.key.account_id,
                                                 "storage_deposit",
                                                 args.encode('utf-8'),
                                                 int(3E14), int(1E23),
                                                 account.use_nonce(),
                                                 block_hash)
