import asyncio
from pathlib import Path
from scheduler.signal_handlers import install_signal_handlers
from scheduler.scheduler import build_scheduler, load_jobs_from_yaml
from utils.logger_factory import EnhancedLoggerFactory, log_exception

async def main():
    etl_logger = EnhancedLoggerFactory.create_application_logger(
        name="scheduler", enable_stdout=True, config_prefix="system"
    )
    await etl_logger.start()
    stop_event = asyncio.Event() 

    try:
        etl_logger.info("ETL Scheduler Start!")
        sched = build_scheduler()
        loop = asyncio.get_running_loop()
        install_signal_handlers(sched, loop, etl_logger=etl_logger, stop_event=stop_event)
        load_jobs_from_yaml(sched, Path("jobs.yaml"), etl_logger=etl_logger)
    
        sched.start()
        await stop_event.wait()  
    except Exception as e:
        log_exception(etl_logger, e, context="bootstrap")
        raise
    finally:
        try:
            await etl_logger.shutdown()
        except Exception as e:
            log_exception(etl_logger, e, context="main_final_shutdown")

if __name__ == "__main__":
    asyncio.run(main())