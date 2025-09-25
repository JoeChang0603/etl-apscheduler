"""Log handler that writes scheduler job output to rotating files."""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Literal

from utils.logger.config import LogEvent
from utils.logger.handlers.base import BaseLogHandler


class JobRotatingFileHandler(BaseLogHandler):
    """Write buffered log events to per-job rotating log files."""

    def __init__(
        self,
        base_dir: str,
        filename_prefix: str = "",
        create: bool = True,
        rotation: Literal["daily", "hourly", "per_minute", "per_second"] = "daily",
    ) -> None:
        """Initialise the handler with target directory and rotation scheme.

        :param base_dir: Base directory where log files are written.
        :param filename_prefix: Optional prefix (subdirectory) for log files.
        :param create: Whether to create the directory if missing.
        :param rotation: Frequency granularity for rotating filenames.
        """
        super().__init__()

        self.base_dir = Path(base_dir)
        self.filename_prefix = filename_prefix
        self.current_date = None
        
        if create:
            self.base_dir.mkdir(parents=True, exist_ok=True)
        
        if rotation == "daily":
            self._pattern = "%Y-%m-%d" 
        elif rotation == "hourly": 
            self._pattern = "%Y%m%d %H:00:00"
        elif rotation == "per_minute": 
            self._pattern = "%Y%m%d %H:%M:00"
        elif rotation == "per_second": 
            self._pattern = "%Y%m%d %H:%M:%S"

    def _get_current_filepath(self) -> str:
        """Generate a log file path for the current rotation window."""
        pattern = datetime.now(timezone.utc).strftime(self._pattern)
        filename = f"{pattern}.log"

        if self.filename_prefix:
            # Create subdirectory for each prefix (e.g., logs/dot_usdt_liao/2025-08-04.log)
            return str(self.base_dir / self.filename_prefix / filename)
        else:
            # Default to base directory if no prefix
            return str(self.base_dir / filename)

    # async def push(self, buffer) -> None:
    #     """Write log messages to the daily rotating file"""
    async def push(self, records: List[LogEvent]) -> None:
        """Append log records to the current rotation file.

        :param records: Buffered log events awaiting persistence.
        """
        if not records:
            return
        combined_logs = "\n".join([ev.text for ev in records]) + "\n"
        filepath = self._get_current_filepath()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, "a", encoding="utf-8") as file:
            file.write(combined_logs)
            file.flush()
