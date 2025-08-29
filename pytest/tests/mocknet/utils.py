from pydantic import BaseModel
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


### Stake distribution ###
class StakeDistribution:

    def __init__(self, num_chunk_producers):
        self.num_chunk_producers = num_chunk_producers
        self.producer_idx = 0

    def get_next_validator_stake(self):
        pass

    def get_next_producer_stake(self):
        pass

    def get_stake(self, node):
        if node.role(
        ) == 'validator' or self.producer_idx >= self.num_chunk_producers:
            amount = self.get_next_validator_stake()
        else:
            amount = self.get_next_producer_stake()
        return amount


class MainnetStakeDistribution(StakeDistribution):
    """
    Uses the mainnet validator stakes as the stake distribution.
    To avoid going below the seat price, the stake of the bottom `double_after` validators is doubled.
    If more validators are requested than available, the stake of the last validator is used for the remaining validators.
    To avoid mainnet stake leaking to the mocknet, the stake is amplified by `multiplier`.
    """

    @staticmethod
    def get_mainnet_stakes():
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
        if response.status_code != 200:
            raise Exception(
                f"Failed to get mainnet stakes: {response.status_code}")

        data = response.json()
        stakes = [int(v['stake']) for v in data['result']['current_validators']]

        # Multiply bottom-100 stakes by 2
        if len(stakes) > 100:
            for i in range(len(stakes) - 100, len(stakes)):
                stakes[i] *= 2

        # Multiply all stakes by 100000
        stakes = [stake * 100000 for stake in stakes]

        # Sort stakes in reverse order (highest to lowest)
        stakes.sort(reverse=True)

        return stakes

    def __init__(self, num_chunk_producers):
        super().__init__(num_chunk_producers)
        self.mainnet_stakes = MainnetStakeDistribution.get_mainnet_stakes()
        self.num_validators = len(self.mainnet_stakes)
        self.validator_idx = self.num_chunk_producers
        # self.double_after = 100
        # self.multiplier = 100000

    def get_next_validator_stake(self):
        if self.validator_idx >= self.num_validators:
            stake = self.mainnet_stakes[-1]
        else:
            stake = self.mainnet_stakes[self.validator_idx]
            self.validator_idx += 1
        # if self.validator_idx >= self.num_validators - self.double_after:
        #     stake = stake * 2
        return stake

    def get_next_producer_stake(self):
        stake = self.mainnet_stakes[self.producer_idx]
        self.producer_idx += 1
        return stake


class StaticStakeDistribution(StakeDistribution):

    def get_next_validator_stake(self):
        return 10**32

    def get_next_producer_stake(self):
        self.producer_idx += 1
        return 10**33


def build_stake_distribution(distribution_type: str | None,
                             num_chunk_producers: int) -> StakeDistribution:
    if distribution_type == 'mainnet':
        return MainnetStakeDistribution(num_chunk_producers)
    else:
        return StaticStakeDistribution(num_chunk_producers)
