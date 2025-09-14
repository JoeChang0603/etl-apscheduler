import asyncio
import signal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from utils.logger.logger import Logger
from utils.logger_factory import log_exception 

def install_signal_handlers(
    scheduler: AsyncIOScheduler,
    loop: asyncio.AbstractEventLoop,
    etl_logger: Logger,
    stop_event: asyncio.Event
):

    shutting_down = False

    async def _shutdown():
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