from transaction import sign_payment_tx
import random, base58

class TxContext:
    def __init__(self, act_to_val, nodes):
        self.next_nonce = 2
        self.num_nodes = len(nodes)
        self.nodes = nodes
        self.act_to_val = act_to_val
        self.expected_balances = self.get_balances()
        assert len(act_to_val) == self.num_nodes
        assert self.num_nodes >= 2

    def get_balance(self, whose):
        r = self.nodes[self.act_to_val[whose]].get_account("test%s" % whose)
        assert 'result' in r, r
        return int(r['result']['amount'])

    def get_balances(self):
        return [
            self.get_balance(i)
            for i in range(self.num_nodes)
        ]

    def send_moar_txs(self, last_block_hash, num, use_routing):
        last_balances = [x for x in self.expected_balances]
        for i in range(num):
            from_ = random.randint(0, self.num_nodes - 1)
            to = random.randint(0, self.num_nodes - 2)
            if to >= from_:
                to += 1
            amt = random.randint(0, 500)
            if self.expected_balances[from_] >= amt:
                print("Sending a tx from %s to %s for %s" % (from_, to, amt));
                tx = sign_payment_tx(self.nodes[from_].signer_key, 'test%s' % to, amt, self.next_nonce, base58.b58decode(last_block_hash.encode('utf8')))
                if use_routing:
                    self.nodes[0].send_tx(tx)
                else:
                    self.nodes[self.act_to_val[from_]].send_tx(tx)
                self.expected_balances[from_] -= amt
                self.expected_balances[to] += amt
                self.next_nonce += 1

