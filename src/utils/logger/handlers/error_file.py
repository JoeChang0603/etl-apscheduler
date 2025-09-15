import os
from typing import Literal, List
from datetime import datetime, timezone
from pathlib import Path
from utils.logger.handlers.base import BaseLogHandler
from utils.logger.config import LogLevel, LogEvent

class ErrorFileHandler(BaseLogHandler):
    def __init__(
        self,
        base_dir: str,
        filename_prefix: str = "",
        create: bool = True,
        rotation: Literal["daily", "hourly", "per_minute", "per_second"] = "daily"
    ) -> None:
        super().__init__()
        self.base_dir = Path(base_dir)
        self.filename_prefix = filename_prefix
        if create:
            self.base_dir.mkdir(parents=True, exist_ok=True)
        self._pattern = {
            "daily": "%Y-%m-%d",
            "hourly": "%Y%m%d %H:00:00",
            "per_minute": "%Y%m%d %H:%M:00",
            "per_second": "%Y%m%d %H:%M:%S",
        }[rotation]

    def _get_current_filepath(self) -> str:
        pattern = datetime.now(timezone.utc).strftime(self._pattern)
        filename = f"{pattern}.error.log"
        if self.filename_prefix:
            return str(self.base_dir / self.filename_prefix / filename)
        return str(self.base_dir / filename)

    async def push(self, records: List[LogEvent]) -> None:
        errors = [ev.text for ev in records if ev.level.value >= LogLevel.ERROR.value]
        if not errors:
            return
        path = self._get_current_filepath()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(errors))
            f.write("\n")
            f.flush()