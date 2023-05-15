import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import transaction
import key

class Account:

    def __init__(self, key):
        self.key = key
        self.nonce = multiprocessing.Value(ctypes.c_ulong, 0)

    def refresh_nonce(self, node):
        with self.nonce.get_lock():
            self.nonce.value = mocknet_helpers.get_nonce_for_key(
                self.key,
                addr=node.rpc_addr()[0],
                port=node.rpc_addr()[1],
            )

    def use_nonce(self):
        with self.nonce.get_lock():
            new_nonce = self.nonce.value + 1
            self.nonce.value = new_nonce
            return new_nonce


class Transaction:
    """
    A transaction future.
    """

    ID = 0

    def __init__(self):
        self.id = Transaction.ID
        Transaction.ID += 1

        # Number of times we are going to check this transaction for completion before retrying
        # submission
        self.ttl = 0
        self.expiration = 0
        # The transaction id hash
        #
        # str if the transaction has been submitted and may eventually conclude.
        self.transaction_id = None
        # The transaction caller (used for checking the transaction status.
        #
        # `str` if the transaction has been submitted and may eventually conclude.
        self.caller = None
        # The outcome of a successful transaction execution
        self.outcome = None

    def finish(self, block_hash):
        return None

    def send(self, block_hash):
        return (self.transaction_id, self.caller)

    def is_complete(self):
        return self.outcome is not None

    def is_success(self, tx_result):
        success = 'error' not in tx_result
        if not success:
            logger.debug(
                f"transaction {self.transaction_id} for {self.caller} is not successful: {tx_result}"
            )
        # only set TTL if we managed to check for success or failure...
        self.ttl = self.expiration - time.time()
        return success


class Deploy(Transaction):

    def __init__(self, account, contract, name):
        super().__init__()
        self.account = account
        self.contract = contract
        self.name = name

    def finish(self, block_hash):
        account = self.account
        logger.info(f"deploying {self.name} to {account.key.account_id}")
        wasm_binary = utils.load_binary_file(self.contract)
        tx = transaction.sign_deploy_contract_tx(account.key, wasm_binary,
                                                 account.use_nonce(),
                                                 block_hash)
        return tx
        
    def send(self, node, block_hash):
        tx = self.finish(block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.account)

class TransferNear(Transaction):

    def __init__(self, sender, recipient_id, how_much=2.0):
        super().__init__()
        self.recipient_id = recipient_id
        self.sender = sender
        self.how_much = how_much

    def finish(self, block_hash):
        sender = self.sender
        logger.debug(
            f"sending {self.how_much} NEAR from {sender.key.account_id} to {self.recipient_id}"
        )
        tx = transaction.sign_payment_tx(sender.key, self.recipient_id,
                                         int(self.how_much * 1E24),
                                         sender.use_nonce(), block_hash)
        return tx

    def send(self, node, block_hash):
        tx = self.finish(block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.sender)


class CreateSubAccount(Transaction):

    def __init__(self, sender, sub_key, balance=50.0):
        super().__init__()
        self.sender = sender
        self.sub_key = sub_key
        self.balance = balance

    def finish(self, block_hash):
        sender = self.sender
        sub = self.sub_key
        logger.debug(f"creating {sub.account_id}")
        tx = transaction.sign_create_account_with_full_access_key_and_balance_tx(
            sender.key, sub.account_id, sub, int(self.balance * 1E24),
            sender.use_nonce(), block_hash)
        return tx
    
    def send(self, node, block_hash):
        tx = self.finish(block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.sender)

class NearUser(HttpUser):
    abstract = True
    id_counter = 0

    @classmethod
    def get_next_id(cls):
        cls.id_counter += 1
        return cls.id_counter

    def __init__(self, environment):
        super().__init__(environment)
        host, port = self.host.split(":")
        self.node = cluster.RpcNode(host, port)
        self.node.session = self.client
        self.id = NearUser.get_next_id()

    def on_start(self):
        while FT_ACCOUNT is None:
            logger.debug("init not ready, yet")
            time.sleep(1)
        self.funding_account = FUNDING_ACCOUNT
        self.contract_account = FT_ACCOUNT
        # TODO: Random prefix for better trie spreading
        self.account_id = f"user{self.id}.{self.funding_account.key.account_id}"
        self.account = Account(key.Key.from_random(self.account_id))
        self.send_tx_retry(CreateSubAccount(self.funding_account, self.account.key, balance=5000.0))
        self.account.refresh_nonce(self.node)

    def send_tx(self, tx: Transaction):
        block_hash = base58.b58decode(self.node.get_latest_block().hash)
        signed_tx = tx.finish(block_hash)
        tx_result = self.node.send_tx_and_wait(signed_tx, timeout=DEFAULT_TRANSACTION_TTL_SECONDS)
        success = "error" not in tx_result
        if success:
            logger.info(
                f"SUCCESS {tx.transaction_id} for {self.account_id} is successful: {tx_result}"
            )
            return True, tx_result
        elif "UNKNOWN_TRANSACTION" in tx_result:
            logger.debug(
                f"transaction {tx.transaction_id} for {self.account_id} timed out / failed to be accepted"
            )
        else:
            logger.warn(
                f"transaction {tx.transaction_id} for {self.account_id} is not successful: {tx_result}"
            )
        return False, tx_result

    def send_tx_retry(self, tx: Transaction):
        ok, tx_result = self.send_tx(tx)
        while not ok:
            logger.warn(f"transaction {tx.transaction_id} for {self.account_id} is not successful: {tx_result}")
            time.sleep(0.25)
            ok, tx_result = self.send_tx(tx)

    def send_tx_retry_old(self, tx: Transaction):
        while True:
            block_hash = base58.b58decode(self.node.get_latest_block().hash)
            (tx.transaction_id, _) = tx.send(self.node, block_hash)
            for _ in range(DEFAULT_TRANSACTION_TTL_SECONDS):
                time.sleep(0.25)
                tx_result = self.node.json_rpc("tx", [tx.transaction_id, self.account_id])
                success = "error" not in tx_result
                if success:
                    logger.info(
                        f"SUCCESS {tx.transaction_id} for {self.account_id} is successful: {tx_result}"
                    )
                    return
                elif "UNKNOWN_TRANSACTION" in tx_result:
                    logger.warn(
                        f"transaction {tx.transaction_id} for {self.account_id} is not successful: {tx_result}"
                    )
                else:
                    logger.debug(
                        f"transaction {tx.transaction_id} for {self.account_id} not done yet"
                    )
            logger.warning(f"transaction {tx.transaction_id} expired, submitting a new one!")


def send_transaction(node, tx):
    while True:
        block_hash = base58.b58decode(node.get_latest_block().hash)
        signed_tx = tx.finish(block_hash)
        tx_result = node.send_tx_and_wait(signed_tx, timeout=DEFAULT_TRANSACTION_TTL_SECONDS)
        success = "error" not in tx_result
        if success:
            logger.info(f"SUCCESS {tx.transaction_id} (for no account) is successful: {tx_result}")
            return True, tx_result
        elif "UNKNOWN_TRANSACTION" in tx_result:
            logger.debug(
                f"transaction {tx.transaction_id} (for no account) timed out / failed to be accepted"
            )
        else:
            logger.warn(
                f"transaction {tx.transaction_id} (for no account) is not successful: {tx_result}"
            )
        logger.warning(f"transaction {tx.transaction_id} expired, submitting a new one!")
