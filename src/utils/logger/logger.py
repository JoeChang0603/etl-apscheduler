import sys
import traceback
import asyncio
from typing import Optional
from colorama import Fore, Style, init as colorama_init

from utils.misc import time_iso8601, time_s
from utils.logger.config import LogLevel, LoggerConfig, LogEvent
from utils.logger.handlers.base import BaseLogHandler

colorama_init(autoreset=True)


LOG_COLORS = {
    LogLevel.TRACE: Fore.LIGHTBLACK_EX,
    LogLevel.DEBUG: Fore.LIGHTBLACK_EX,
    LogLevel.INFO: Fore.GREEN,
    LogLevel.WARNING: Fore.YELLOW,
    LogLevel.ERROR: Fore.RED + Style.BRIGHT,
    LogLevel.CRITICAL: Fore.RED + Style.BRIGHT,
}

ICONS = {
    LogLevel.TRACE:"🔍", LogLevel.DEBUG:"🐞", LogLevel.INFO:"ℹ️",
    LogLevel.WARNING:"⚠️", LogLevel.ERROR:"❌", LogLevel.CRITICAL:"🔥",
}


class Logger:
    """
    A simple asynchronous logger that buffers messages and pushes them to
    configured handlers at an appropriate time or based on severity.
    """

    def __init__(
        self,
        config: LoggerConfig = None,
        name: str = "",
        handlers: Optional[list[BaseLogHandler]] = None,
    ):
        """
        Initializes a Logger with specified configuration and handlers.

        Args:
            config (LoggerConfig): Configuration settings for the logger (base level, stdout, buffer size, etc.).
            name (str): Name of the logger. Defaults to an empty string.
            handlers (list[BaseLogHandler], optional): A list of handler objects that inherit from BaseLogHandler.
                Defaults to an empty list if not provided.

        Raises:
            TypeError: If one of the provided handlers does not inherit from LogHandler.
        """
        self._config = config
        if self._config is None:
            self._config = LoggerConfig()

        self._name = name

        self._handlers = handlers
        if self._handlers is None:
            self._handlers = []

        for handler in self._handlers:
            handler_base_class = handler.__class__.__base__
            if not handler_base_class == BaseLogHandler:
                raise TypeError(f"Invalid handler base class; expected BaseLogHandler but got {handler_base_class}")

            # Mainly for forwarding the str_format to the handler for formatting log messages
            # where the final point is not a code environment (eg Discord, Telegram, etc).
            handler.add_primary_config(config)

        self._buffer_size = 0
        #self._buffer: list[str] = [None] * self._config.buffer_capacity
        self._buffer: list[LogEvent] = []  
        self._buffer_start_time = time_s()

        self._msg_queue = asyncio.Queue()
        self._is_running = True

        # Start the log ingestor task.
        self._log_ingestor_task = None

        # As opposed to the AdvancedLogger where the system info is sent on
        # each batch, here we only debug log it at the start of the programme.
        # It is highly unlikely that someone using the basic logger requires
        # such information, so if you do, use the other logger!
        # self.trace(str(self._system_info))

    # async def _flush_buffer(self):
    #     """
    #     Flushes the log message buffer to all handlers.
    #     """
    #     batch = self._buffer[: self._buffer_size]
    #     for handler in self._handlers:
    #         await handler.push(batch)

    #     self._buffer_size = 0
    #     self._buffer_start_time = time_s()

    async def _flush_buffer(self):
        batch = list(self._buffer)                        # ← 直接整批 LogEvent
        for handler in self._handlers:
            await handler.push(batch)
        self._buffer.clear()                              # ← 清空
        self._buffer_start_time = time_s()

    async def _log_ingestor(self):
        try:
            while self._is_running or not self._msg_queue.empty():
                try:
                    event: LogEvent = await self._msg_queue.get()
                except asyncio.CancelledError:
                    break  # 安全退出 loop，不處理任何 task_done
                except Exception:
                    traceback.print_exc(file=sys.stderr)
                    break

                try:
                    # log_msg, level = item
                    # self._buffer[self._buffer_size] = log_msg

                    self._buffer.append(event) 

                    if self._config.do_stdout:
                        color = LOG_COLORS.get(event.level, "")
                        print(color + event.text + Style.RESET_ALL)

                    # Immediate flush for WARNING+ or important INFO messages
                    should_flush_immediately = (
                        event.level.value >= LogLevel.WARNING.value or
                        any(keyword in event.text for keyword in [
                            "Starting strategy", "Starting dashboard", "initialized", 
                            "shutdown", "Application", "Critical"
                        ])
                    )
                    
                    if should_flush_immediately:
                        await self._flush_buffer()
                    else:
                        is_buffer_full = self._buffer_size >= self._config.buffer_capacity
                        is_buffer_expired = (time_s() - self._buffer_start_time) >= self._config.buffer_timeout

                        if is_buffer_full or is_buffer_expired:
                            await self._flush_buffer()

                except Exception:
                    traceback.print_exc(file=sys.stderr)
                finally:
                    self._msg_queue.task_done()

        except asyncio.CancelledError:
            print("[Logger] Log ingestor cancelled")

    def _process_log(self, level: LogLevel, msg: str):
        """
        Submits a log message to the queue if it meets the minimum base level.

        Args:
            level (LogLevel): The severity level of the message.
            msg (str): The actual log message.
        """
        try:
            icon = ICONS.get(level, "•")
            log_msg = self._config.str_format % {
                "asctime": time_iso8601(),
                "icon": icon,
                "name": self._name,
                "levelname": level.name,
                "message": msg,
                
            }

            self._msg_queue.put_nowait(LogEvent(text=log_msg, level=level))
        except Exception:
            traceback.print_exc(file=sys.stderr)
    
    async def _drain(self, timeout: float | None = None) -> None:
        """
        等待 queue 清空（背景 ingestor 對每一筆都會呼叫 task_done()）
        """
        async def _join():
            await self._msg_queue.join()
        if timeout is None:
            await _join()
        else:
            await asyncio.wait_for(_join(), timeout=timeout)


    def set_format(self, format_string: str) -> None:
        """
        Modify the format string for log messages in runtime.

        Args:
            format_string (str): The new format string.
                Supports {timestamp}, {level}, and {message} placeholders.
        """
        self.debug(f"Changing format string from {self._config.str_format} to {format_string}")
        self._config.str_format = format_string
        for handlers in self._handlers:
            handlers.add_primary_config(self._config)

    def set_log_level(self, level: LogLevel) -> None:
        """
        Modify the logger's base log level at runtime.

        Args:
            level (LogLevel): The new base log level.
        """
        self.debug(f"Changing base log level from {self._config.base_level} to {level}")
        self._config.base_level = level
        for handlers in self._handlers:
            handlers.add_primary_config(self._config)

    def trace(self, msg: str) -> None:
        """
        Send a trace-level log message.

        Args:
            msg (str): The log message text.
        """
        valid_level = self._config.base_level == LogLevel.TRACE
        if self._is_running and valid_level:
            self._process_log(LogLevel.TRACE, msg)

    def debug(self, msg: str) -> None:
        """
        Send a debug-level log message.

        Args:
            msg (str): The log message text.
        """
        valid_level = self._config.base_level <= LogLevel.DEBUG
        if self._is_running and valid_level:
            self._process_log(LogLevel.DEBUG, msg)

    def info(self, msg: str) -> None:
        """
        Send an info-level log message.

        Args:
            msg (str): The log message text.
        """
        valid_level = self._config.base_level <= LogLevel.INFO
        if self._is_running and valid_level:
            self._process_log(LogLevel.INFO, msg)

    def warning(self, msg: str) -> None:
        """
        Send a warning-level log message.

        Args:
            msg (str): The log message text.
        """
        valid_level = self._config.base_level <= LogLevel.WARNING
        if self._is_running and valid_level:
            self._process_log(LogLevel.WARNING, msg)

    def error(self, msg: str) -> None:
        """
        Send an error-level log message.

        Args:
            msg (str): The log message text.
        """
        valid_level = self._config.base_level <= LogLevel.ERROR
        if self._is_running and valid_level:
            self._process_log(LogLevel.ERROR, msg)

    def critical(self, msg: str) -> None:
        """
        Send a critical-level log message.

        Args:
            msg (str): The log message text.
        """
        valid_level = self._config.base_level <= LogLevel.CRITICAL
        if self._is_running and valid_level:
            self._process_log(LogLevel.CRITICAL, msg)

    async def start(self):
        self._is_running = True
        self._msg_queue = asyncio.Queue()
        for h in self._handlers:
            if hasattr(h, "start"):
                await h.start()
        self._log_ingestor_task = asyncio.create_task(self._log_ingestor())

    async def shutdown(self):
        self._is_running = False

        await asyncio.sleep(0)

        try:
            await self._drain(timeout=2.0)    # 視量調整 timeout
        except asyncio.TimeoutError:
            print("[Logger] drain timeout; forcing shutdown", file=sys.stderr)

        # if self._buffer_size > 0:
        #     await self._flush_buffer()
        if self._buffer:                                  # ← 用 truthy 判斷
            await self._flush_buffer()

        if self._log_ingestor_task is not None:
            try:
                await asyncio.wait_for(self._log_ingestor_task, timeout=1.0)
            except asyncio.TimeoutError:
                self._log_ingestor_task.cancel()
                try:
                    await self._log_ingestor_task
                except asyncio.CancelledError:
                    pass
        
        for h in self._handlers:
            if hasattr(h, "shutdown"):
                try: await h.shutdown()
                except Exception: traceback.print_exc(file=sys.stderr)
        
        self._buffer.clear()

        # if self._log_ingestor_task is not None:
        #     self._log_ingestor_task.cancel()
        #     try:
        #         await self._log_ingestor_task
        #     except asyncio.CancelledError:
        #         print("[Logger] Log ingestor task cancelled.")

        # if self._buffer_size > 0:
        #     await self._flush_buffer()
        #     self._buffer.clear()

    def is_running(self) -> bool:
        """
        Check if the master logger is running.
        """
        return self._is_running

    def get_name(self) -> str:
        """
        Get the name of the master logger.
        """
        return self._name

    def get_config(self) -> LoggerConfig:
        """
        Get the configuration of the master logger.
        """
        return self._config

    # def get_system_info(self) -> dict:
    #     """
    #     Get the system information of the master logger.
    #     """
    #     return self._system_info