from typing import Optional
from datetime import datetime


class ScheduleContext:

    def __init__(self, id: Optional[str], timespec: str):
        self.id = id or str(int(datetime.utcnow().timestamp()))
        self.timespec = timespec
