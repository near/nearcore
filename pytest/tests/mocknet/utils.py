from typing import Optional
from datetime import datetime


class ScheduleContext:

    def __init__(self,
                 id: str | None,
                 relative_time_spec: str | None = None,
                 calendar_time_spec: str | None = None):
        self.id = id or str(int(datetime.utcnow().timestamp()))
        self.relative_time_spec = relative_time_spec
        self.calendar_time_spec = calendar_time_spec
