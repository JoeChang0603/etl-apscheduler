"""Graceful shutdown helpers for integrating APScheduler with OS signals."""

import asyncio
import signal

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from utils.logger.logger import Logger
from utils.logger_factory import log_exception


def install_signal_handlers(
    scheduler: AsyncIOScheduler,
    loop: asyncio.AbstractEventLoop,
    etl_logger: Logger,
    stop_event: asyncio.Event,
) -> None:
    """Register SIGINT/SIGTERM handlers that shut down the scheduler cleanly.

    :param scheduler: Running ``AsyncIOScheduler`` to shut down on signals.
    :param loop: Event loop used to schedule the async shutdown coroutine.
    :param etl_logger: Logger for lifecycle messages and error reporting.
    :param stop_event: Event signalled once teardown completes.
    """

    shutting_down = False

    async def _shutdown():
        """Stop the scheduler and logger once per signal.

        :raises Exception: Propagates logger shutdown errors after logging.
        """
        nonlocal shutting_down
        if shutting_down:
            return
        shutting_down = True
        try:
            etl_logger.info("Shutting down scheduler ...")
            scheduler.shutdown(wait=True)
            etl_logger.info("Scheduler stopped.")
        except Exception as e:
                log_exception(etl_logger, e, context="system")
        finally:
            try:
                await etl_logger.shutdown()
            except Exception as e:
                log_exception(etl_logger, e, context="system")
            stop_event.set()  

    def _handle(signum, frame=None):
        """Schedule the asynchronous shutdown when a signal is received.

        :param signum: Signal number intercepted from the OS.
        :param frame: Optional current stack frame (unused).
        """
        try:
            etl_logger.info(f"Received signal {signum}. Graceful shutdown started.")
        finally:
            loop.call_soon_threadsafe(loop.create_task, _shutdown())
            

    # 優先使用 asyncio 的 signal handler（UNIX 可用）；不支援時退回 signal.signal
    try:
        loop.add_signal_handler(signal.SIGINT, _handle, signal.SIGINT, None)
        loop.add_signal_handler(signal.SIGTERM, _handle, signal.SIGTERM, None)
    except (NotImplementedError, AttributeError):
        signal.signal(signal.SIGINT, _handle)
        signal.signal(signal.SIGTERM, _handle)
