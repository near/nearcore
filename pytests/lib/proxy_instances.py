import logging, multiprocessing, random
from proxy import ProxyHandler, NodesProxy


class RejectListHandler(ProxyHandler):

    def __init__(self, reject_list, drop_probability, ordinal):
        super().__init__(ordinal)
        self.reject_list = reject_list
        self.drop_probability = drop_probability

    async def handle(self, msg, fr, to):
        msg_type = msg.enum if msg.enum != 'Routed' else msg.Routed.body.enum

        if (self.drop_probability > 0 and 'Handshake' not in msg_type and
                random.uniform(0, 1) < self.drop_probability):
            logging.info(
                f'NODE {self.ordinal} dropping message {msg_type} from {fr} to {to}'
            )
            return False

        if fr in self.reject_list or to in self.reject_list:
            logging.info(
                f'NODE {self.ordinal} blocking message {msg_type} from {fr} to {to}'
            )
            return False
        else:
            return True


class RejectListProxy(NodesProxy):

    def __init__(self, reject_list, drop_probability):
        self.reject_list = reject_list
        self.drop_probability = drop_probability
        handler = lambda ordinal: RejectListHandler(reject_list,
                                                    drop_probability, ordinal)
        super().__init__(handler)

    @staticmethod
    def create_reject_list(size):
        return multiprocessing.Array('i', [-1 for _ in range(size)])
