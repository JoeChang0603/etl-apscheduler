"""Handler that isolates error-level logs into dedicated files."""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Literal

from utils.logger.config import LogEvent, LogLevel
from utils.logger.handlers.base import BaseLogHandler


class ErrorFileHandler(BaseLogHandler):
    """Persist only error and higher severity messages to rotating files."""

    def __init__(
        self,
        base_dir: str,
        filename_prefix: str = "",
        create: bool = True,
        rotation: Literal["daily", "hourly", "per_minute", "per_second"] = "daily",
    ) -> None:
        """Configure the handler target directory and rotation schedule.

        :param base_dir: Base directory where error logs are written.
        :param filename_prefix: Optional prefix to group log files.
        :param create: Whether missing directories should be created.
        :param rotation: Frequency granularity for the error log filenames.
        """
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
        """Compute the destination file path for the current rotation window."""

        pattern = datetime.now(timezone.utc).strftime(self._pattern)
        filename = f"{pattern}.error.log"
        if self.filename_prefix:
            return str(self.base_dir / self.filename_prefix / filename)
        return str(self.base_dir / filename)

    async def push(self, records: List[LogEvent]) -> None:
        """Append only error-or-higher events to the error log file.

        :param records: Buffered log events awaiting persistence.
        """

        errors = [ev.text for ev in records if ev.level.value >= LogLevel.ERROR.value]
        if not errors:
            return
        path = self._get_current_filepath()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(errors))
            f.write("\n")
            f.flush()
