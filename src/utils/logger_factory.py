"""Factories for application loggers and helper utilities."""

import traceback
from contextlib import asynccontextmanager

from bot.discord import DiscordHandler
from configs.env_config import Env
from utils.logger.config import LogLevel, LoggerConfig
from utils.logger.handlers.error_file import ErrorFileHandler
from utils.logger.handlers.job_file import JobRotatingFileHandler
from utils.logger.logger import Logger


class EnhancedLoggerFactory:
    """Convenience constructors for configured application loggers."""

    @staticmethod
    def create_application_logger(name: str = "etl_bot", 
                                  enable_stdout: bool = False,
                                  log_level: LogLevel = LogLevel.INFO,
                                  config_prefix: str = None) -> Logger:
        """Create the main application logger with rotating file handlers.

        :param name: Logger name used in records and filenames.
        :param enable_stdout: Whether to emit log lines to stdout.
        :param log_level: Minimum log level captured by the logger.
        :param config_prefix: Optional prefix for log filenames.
        :return: Configured :class:`Logger` instance.
        """
        config = LoggerConfig(
            base_level=log_level,
            do_stdout=enable_stdout,
            str_format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        )
        
        # Use config_prefix if provided, otherwise use name, otherwise no prefix
        prefix = config_prefix or name if config_prefix != "" else ""
        
        handlers = [
            # Single daily log file with config prefix
            JobRotatingFileHandler(
                base_dir="logs",
                filename_prefix=prefix,
                rotation="daily"
                
            ),
            # Separate error log file
            ErrorFileHandler(
                base_dir="logs",
                filename_prefix=prefix,
                rotation="daily"
            ),
            DiscordHandler(webhook_url=Env.ETL_PROCESS_WEBHOOK)
        ]
        
        return Logger(config=config, name=name, handlers=handlers)

    @staticmethod
    def create_job_run_logger(job_id: str,
                              base_dir: str = "logs",
                              prefix: str | None = None,
                              level: LogLevel = LogLevel.INFO,
                              enable_stdout: bool = False) -> Logger:
        """Create a per-job logger that writes to rotating log files.

        :param job_id: Scheduler job identifier used for naming logs.
        :param base_dir: Base directory where logs are stored.
        :param prefix: Optional custom filename prefix.
        :param level: Minimum log level recorded by the logger.
        :param enable_stdout: Whether to mirror output to stdout.
        :return: :class:`Logger` configured for a single job execution.
        """
        use_prefix = (prefix if prefix is not None else job_id)
        config = LoggerConfig(
            base_level=level,
            do_stdout=enable_stdout,
            str_format="%(asctime)s %(icon)s [%(levelname)s] JOB_%(name)s - %(message)s",
        )
        handlers = [
            JobRotatingFileHandler(base_dir=base_dir, filename_prefix=use_prefix, rotation="hourly"),
            ErrorFileHandler(base_dir=base_dir, filename_prefix=use_prefix, rotation="daily"),
            DiscordHandler(webhook_url=Env.ETL_PROCESS_WEBHOOK)
        ]
        return Logger(config=config, name=job_id, handlers=handlers)

    @staticmethod
    @asynccontextmanager
    async def job_run_logger(job_id: str,
                             base_dir: str = "logs",
                             prefix: str | None = None,
                             level: LogLevel = LogLevel.INFO,
                             enable_stdout: bool = False):
        """Async context manager yielding a started job logger.

        :param job_id: Scheduler job identifier used for naming logs.
        :param base_dir: Base directory where logs are stored.
        :param prefix: Optional custom filename prefix.
        :param level: Minimum log level recorded by the logger.
        :param enable_stdout: Whether to mirror output to stdout.
        :yield: Started :class:`Logger` instance with automatic shutdown.
        """
        log = EnhancedLoggerFactory.create_job_run_logger(
            job_id, base_dir=base_dir, prefix=prefix, level=level, enable_stdout=enable_stdout
        )
        await log.start()
        try:
            yield log
        finally:
            await log.shutdown()


def log_exception(logger: Logger, exc: Exception, context: str = ""):
    """Log an exception with traceback using the provided logger.

    :param logger: Logger instance used for reporting the failure.
    :param exc: Exception that should be logged.
    :param context: Optional textual context describing the failure.
    """
    tb_str = traceback.format_exc()
    
    error_msg = f"EXCEPTION in {context}: {type(exc).__name__}: {str(exc)}\n{tb_str}"
    logger.error(error_msg)
