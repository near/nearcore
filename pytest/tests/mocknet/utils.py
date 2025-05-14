from typing import Optional

class ScheduleContext:
    def __init__(self, id: Optional[str], timespec: str):
        self.id = id or datetime.datetime.utcnow().isoformat()
        self.timespec = timespec
