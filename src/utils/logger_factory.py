import traceback
from contextlib import asynccontextmanager
from configs.env_config import Env
from utils.logger.logger import Logger
from utils.logger.config import LoggerConfig, LogLevel
from utils.logger.handlers.job_file import JobRotatingFileHandler
from utils.logger.handlers.error_file import ErrorFileHandler
from bot.discord import DiscordHandler

class EnhancedLoggerFactory:

    @staticmethod
    def create_application_logger(name: str = "etl_bot", 
                                  enable_stdout: bool = False,
                                  log_level: LogLevel = LogLevel.INFO,
                                  config_prefix: str = None) -> Logger:
        """
        Create main application logger with daily rotating files
        
        Args:
            name: Logger name
            enable_stdout: Whether to also print to terminal
            log_level: Minimum log level
            config_prefix: Prefix for log filename (e.g., config name)
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
        """每次 job 執行各自一個檔的 logger（檔名含時間戳）。"""
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
        """供 with/async with 使用，內部自動 start/shutdown。"""
        log = EnhancedLoggerFactory.create_job_run_logger(
            job_id, base_dir=base_dir, prefix=prefix, level=level, enable_stdout=enable_stdout
        )
        await log.start()
        try:
            yield log
        finally:
            await log.shutdown()


def log_exception(logger: Logger, exc: Exception, context: str = ""):
    """
    Enhanced exception logging with full traceback
    
    Args:
        logger: Logger instance to use
        exc: Exception to log
        context: Additional context information
    """
    tb_str = traceback.format_exc()
    
    error_msg = f"EXCEPTION in {context}: {type(exc).__name__}: {str(exc)}\n{tb_str}"
    logger.error(error_msg)
