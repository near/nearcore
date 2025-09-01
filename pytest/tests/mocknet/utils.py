from pydantic import BaseModel
from retrying import retry
from typing import Literal
import requests


class ScheduleMode(BaseModel):
    mode: Literal['calendar', 'active']
    value: str

    def get_systemd_time_spec(self):
        if self.mode == "calendar":
            return f'--on-calendar="{self.value}"'
        elif self.mode == "active":
            return f'--on-active="{self.value}"'
        else:
            raise ValueError(f'Invalid schedule context provided: {self}')


class ScheduleContext(BaseModel):
    id: str
    schedule: ScheduleMode


class StakeDistribution:
    """
    A stake management class that tracks and assigns stakes to validator and producer nodes.
    The get_stake() method returns the appropriate stake amount for each node type.
    The class maintains indices to track the next validator (validator_idx) and producer 
    (producer_idx) to be assigned stakes. The producer_idx is capped at num_chunk_producers
    to limit the number of chunk producers. When implementing this class, use validator_idx
    and producer_idx to determine which node should receive the next stake assignment.
    """

    def __init__(self, num_chunk_producers):
        self.num_chunk_producers = num_chunk_producers
        self.producer_idx = 0
        self.validator_idx = self.num_chunk_producers

    def _get_next_validator_stake(self):
        pass

    def _get_next_producer_stake(self):
        pass

    def get_stake(self, node):
        """
        Get the next stake for the node.
        If the node is a validator, the stake is the next validator stake.
        If the node is a producer, the stake is the next producer stake.
        If the node can be a producer but we already have enough producers,
        the stake of a validator will be used instead.
        """
        if node.role(
        ) == 'validator' or self.producer_idx >= self.num_chunk_producers:
            amount = self._get_next_validator_stake()
            self.validator_idx += 1
        else:
            amount = self._get_next_producer_stake()
            self.producer_idx += 1
        return amount


class MainnetStakeDistribution(StakeDistribution):
    """
    Uses the mainnet validator stakes as the stake distribution.
    To avoid going below the seat price, the stake of the bottom `DOUBLE_THRESHOLD` validators is doubled.
    If more validators are requested than available, the stake of the last validator is used for the remaining validators.
    To avoid mainnet validators leaking to the mocknet, the stake is amplified by `MULTIPLIER`.
    """

    DOUBLE_THRESHOLD = 100
    MULTIPLIER = 100000

    @staticmethod
    @retry(wait_fixed=500, stop_max_attempt_number=6)
    def _get_mainnet_stakes():
        """
        Makes an RPC call to mainnet to get current validator stakes
        """
        url = "https://rpc.mainnet.near.org"
        payload = {
            "jsonrpc": "2.0",
            "id": "dontcare",
            "method": "validators",
            "params": [None]
        }
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            data = response.json()
            return [
                int(v['stake']) for v in data['result']['current_validators']
            ]
        else:
            raise Exception(
                f"Failed to get mainnet stakes: {response.status_code}")

    def _set_mainnet_stakes(self):
        self._mainnet_stakes = MainnetStakeDistribution._get_mainnet_stakes()
        # Double the bottom stakes to avoid going below the seat price.
        self._mainnet_stakes[-MainnetStakeDistribution.DOUBLE_THRESHOLD:] = [
            s * 2 for s in
            self._mainnet_stakes[-MainnetStakeDistribution.DOUBLE_THRESHOLD:]
        ]
        # Amplify the stakes to avoid mainnet validators leaking to the mocknet.
        self._mainnet_stakes = [
            s * MainnetStakeDistribution.MULTIPLIER
            for s in self._mainnet_stakes
        ]
        self._mainnet_stakes.sort(reverse=True)

    def __init__(self, num_chunk_producers):
        super().__init__(num_chunk_producers)
        self._set_mainnet_stakes()

    def _get_next_validator_stake(self):
        idx = min(self.validator_idx, len(self._mainnet_stakes) - 1)
        return self._mainnet_stakes[idx]

    def _get_next_producer_stake(self):
        idx = min(self.producer_idx, len(self._mainnet_stakes) - 1)
        return self._mainnet_stakes[idx]


class StaticStakeDistribution(StakeDistribution):

    def _get_next_validator_stake(self):
        return 10**32

    def _get_next_producer_stake(self):
        return 10**33


def build_stake_distribution(distribution_type: str | None,
                             num_chunk_producers: int) -> StakeDistribution:
    if distribution_type == 'mainnet':
        return MainnetStakeDistribution(num_chunk_producers)
    else:
        return StaticStakeDistribution(num_chunk_producers)
