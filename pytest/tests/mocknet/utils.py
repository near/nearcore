from pydantic import BaseModel
from typing import Literal


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