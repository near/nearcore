import logging, multiprocessing
from proxy import ProxyHandler, NodesProxy


class RejectListHandler(ProxyHandler):

    def __init__(self, reject_list, ordinal):
        super().__init__(ordinal)
        self.reject_list = reject_list

    async def handle(self, msg, fr, to):
        if fr in self.reject_list or to in self.reject_list:
            logging.info(
                f'NODE {self.ordinal} blocking message from {fr} to {to}')
            return False
        else:
            return True


class RejectListProxy(NodesProxy):

    def __init__(self, reject_list):
        self.reject_list = reject_list
        handler = lambda ordinal: RejectListHandler(reject_list, ordinal)
        super().__init__(handler)

    @staticmethod
    def create_reject_list(size):
        return multiprocessing.Array('i', [-1 for _ in range(size)])
