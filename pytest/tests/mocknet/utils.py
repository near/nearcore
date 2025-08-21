from datetime import datetime
from pydantic import BaseModel
from typing import Literal


class ScheduleMode(BaseModel):
    mode: Literal['calendar', 'active']
    value: str

    @classmethod
    def from_str(cls, arg: str) -> 'ScheduleMode':
        try:
            mode, value = arg.split(' ', 1)
            return cls(mode=mode, value=value)
        except ValueError:
            raise ValueError(
                f"Invalid format. Expected 'MODE VALUE', got '{arg}'")

    def get_systemd_time_spec(self):
        if self.mode == "calendar":
            return f'--on-calendar="{self.value}"'
        elif self.mode == "active":
            return f'--on-active="{self.value}"'
        else:
            raise ValueError(f'Invalid schedule context provided: {self}')


class ScheduleContext(BaseModel):
    id: str = str(int(datetime.utcnow().timestamp()))
    schedule: ScheduleMode
