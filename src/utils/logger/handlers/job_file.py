import os
from typing import Literal, List
from datetime import datetime, timezone
from pathlib import Path
from utils.logger.handlers.base import BaseLogHandler
from utils.logger.config import LogEvent


class JobRotatingFileHandler(BaseLogHandler):
    """
    A log handler that creates a new log file for each job execution.
    The file is named with the UTC timestamp down to the second, in the format YYYYMMDD_HHMMSS.log.
    """

    def __init__(
            self, 
            base_dir: str, 
            filename_prefix: str = "", 
            create: bool = True,
            rotation: Literal["daily", "hourly", "per_minute", "per_second"] = "daily") -> None:
        """
        Initialize the JobRotatingFileHandler with a base directory and optional prefix.

        Args:
            base_dir (str): Base directory where log files will be stored
            filename_prefix (str): Optional prefix for log files (e.g., "trading_bot")
            create (bool): Whether to create the directory if it doesn't exist
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
            self._pattern = "%Y%m%d_%H"
        elif rotation == "per_minute": 
            self._pattern = "%Y%m%d_%H%M"
        elif rotation == "per_second": 
            self._pattern = "%Y%m%d_%H%M%S"

    def _get_current_filepath(self) -> str:
        """Generate a unique log file path for the current job execution based on the current UTC timestamp (to the second)."""
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
    async def push(self, records: List[LogEvent]):
        if not records:
            return
        combined_logs = "\n".join([ev.text for ev in records]) + "\n"
        filepath = self._get_current_filepath()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, "a", encoding="utf-8") as file:
            file.write(combined_logs)
            file.flush()